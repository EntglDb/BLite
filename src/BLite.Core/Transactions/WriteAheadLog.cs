using System.Security.Cryptography;
using BLite.Core.Encryption;

namespace BLite.Core.Transactions;

/// <summary>
/// WAL record types
/// </summary>
public enum WalRecordType : byte
{
    Begin = 1,
    Write = 2,
    Commit = 3,
    Abort = 4,
    Checkpoint = 5
}

/// <summary>
/// Write-Ahead Log (WAL) for durability and recovery.
/// All changes are logged before being applied.
/// Implements <see cref="IWriteAheadLog"/> — the pluggable WAL abstraction.
/// <para>
/// When an <see cref="ICryptoProvider"/> is supplied (e.g. <see cref="AesGcmCryptoProvider"/>
/// with <c>fileRole: 3</c>), each WAL record is transparently encrypted before it is written
/// and decrypted during <see cref="ReadAll"/> (recovery). The WAL file begins with a 64-byte
/// crypto header (identical in layout to the main database file header) followed by
/// variable-length encrypted record envelopes. Unencrypted WAL files (no crypto provider)
/// use exactly the same on-disk format as before — byte-for-byte compatible.
/// </para>
/// <para>
/// <b>Encrypted record envelope on-disk layout:</b>
/// <code>
/// [plaintext_size : int32 LE (4 bytes)]
/// [nonce          : 12 bytes          ]
/// [ciphertext     : plaintext_size bytes]
/// [GCM auth tag   : 16 bytes          ]
/// </code>
/// </para>
/// </summary>
public sealed class WriteAheadLog : IWriteAheadLog
{
    private readonly string _walPath;
    private FileStream? _walStream;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private bool _disposed;
    private readonly int _writeTimeoutMs;

    // Optional encryption provider. Non-null only when real encryption is configured
    // (i.e. crypto.FileHeaderSize > 0). NullCryptoProvider is treated as no encryption.
    private readonly ICryptoProvider? _crypto;

    // True when the crypto file header has been written to (or read from) the current
    // WAL file. Reset to false by TruncateAsync so the header is re-written on the
    // next record write (after truncation the file is empty and needs a fresh header).
    private bool _cryptoInitialized;

    /// <summary>
    /// Opens or creates a WAL file at <paramref name="walPath"/>.
    /// </summary>
    /// <param name="walPath">Path to the WAL file.</param>
    /// <param name="crypto">
    /// Optional encryption provider. When non-null and <see cref="ICryptoProvider.FileHeaderSize"/>
    /// is greater than zero, each WAL record is AES-256-GCM encrypted before being written.
    /// Pass <c>null</c> or <see cref="NullCryptoProvider"/> to keep WAL records in plaintext.
    /// </param>
    /// <param name="writeTimeoutMs">Lock-acquisition timeout in milliseconds.</param>
    public WriteAheadLog(string walPath, ICryptoProvider? crypto, int writeTimeoutMs = 5_000)
    {
        _walPath = walPath ?? throw new ArgumentNullException(nameof(walPath));
        _writeTimeoutMs = writeTimeoutMs;

        // Treat providers that add no overhead (NullCryptoProvider) as no encryption so
        // the on-disk format stays byte-for-byte identical to the pre-encryption format.
        _crypto = (crypto != null && crypto.FileHeaderSize > 0) ? crypto : null;

        _walStream = new FileStream(
            _walPath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.None,
            bufferSize: 64 * 1024);
        // REMOVED FileOptions.WriteThrough for SQLite-style lazy checkpointing
        // Durability is ensured by explicit Flush() calls

        // If the WAL file already exists with content and encryption is enabled,
        // read the file header to derive the encryption key before any I/O.
        if (_crypto != null && _walStream.Length >= _crypto.FileHeaderSize)
        {
            var header = System.Buffers.ArrayPool<byte>.Shared.Rent(_crypto.FileHeaderSize);
            try
            {
                _walStream.Position = 0;
                var read = _walStream.Read(header, 0, _crypto.FileHeaderSize);
                if (read == _crypto.FileHeaderSize)
                {
                    _crypto.LoadFromFileHeader(header.AsSpan(0, _crypto.FileHeaderSize));
                    _cryptoInitialized = true;
                }
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(header);
            }
        }

        // Position the stream at the end so subsequent writes append correctly.
        if (_walStream.Length > 0)
            _walStream.Position = _walStream.Length;
    }

    /// <summary>
    /// Opens or creates a WAL file at <paramref name="walPath"/> without encryption.
    /// </summary>
    public WriteAheadLog(string walPath, int writeTimeoutMs = 5_000)
        : this(walPath, null, writeTimeoutMs)
    {
    }

    // ── Crypto helpers ───────────────────────────────────────────────────────

    /// <summary>
    /// Writes the 64-byte crypto file header to the WAL if it has not been written yet.
    /// Must be called while holding <see cref="_lock"/>.
    /// </summary>
    private void EnsureCryptoHeader()
    {
        if (_crypto == null || _cryptoInitialized) return;

        var header = System.Buffers.ArrayPool<byte>.Shared.Rent(_crypto.FileHeaderSize);
        try
        {
            _crypto.GetFileHeader(header.AsSpan(0, _crypto.FileHeaderSize));
            _walStream!.Write(header.AsSpan(0, _crypto.FileHeaderSize));
            _cryptoInitialized = true;
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(header);
        }
    }

    /// <summary>
    /// Async version of <see cref="EnsureCryptoHeader"/>.
    /// </summary>
    private async ValueTask EnsureCryptoHeaderAsync(CancellationToken ct)
    {
        if (_crypto == null || _cryptoInitialized) return;

        var header = System.Buffers.ArrayPool<byte>.Shared.Rent(_crypto.FileHeaderSize);
        try
        {
            _crypto.GetFileHeader(header.AsSpan(0, _crypto.FileHeaderSize));
            await _walStream!.WriteAsync(new ReadOnlyMemory<byte>(header, 0, _crypto.FileHeaderSize), ct);
            _cryptoInitialized = true;
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(header);
        }
    }

    /// <summary>
    /// Writes a serialised WAL record to the stream — encrypting it first if a crypto
    /// provider is configured, or writing it verbatim otherwise.
    /// Must be called while holding <see cref="_lock"/>.
    /// </summary>
    private async ValueTask WriteRawAsync(ReadOnlyMemory<byte> plaintext, CancellationToken ct)
    {
        if (_crypto == null)
        {
            await _walStream!.WriteAsync(plaintext, ct);
            return;
        }

        await EnsureCryptoHeaderAsync(ct);

        // Encrypted envelope: [plaintext_size(4)][nonce(12)][ciphertext(N)][tag(16)]
        // where N = plaintext.Length and PageOverhead = 28 (12 nonce + 16 tag).
        var ciphertextSize = plaintext.Length + _crypto.PageOverhead;
        var buf = System.Buffers.ArrayPool<byte>.Shared.Rent(4 + ciphertextSize);
        try
        {
            BitConverter.TryWriteBytes(buf.AsSpan(0, 4), plaintext.Length);
            _crypto.Encrypt(0, plaintext.Span, buf.AsSpan(4, ciphertextSize));
            await _walStream!.WriteAsync(new ReadOnlyMemory<byte>(buf, 0, 4 + ciphertextSize), ct);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buf);
        }
    }

    /// <summary>
    /// Synchronous version of <see cref="WriteRawAsync"/> used by the sync write path.
    /// Must be called while holding <see cref="_lock"/>.
    /// </summary>
    private void WriteRawSync(ReadOnlySpan<byte> plaintext)
    {
        if (_crypto == null)
        {
            _walStream!.Write(plaintext);
            return;
        }

        EnsureCryptoHeader();

        var ciphertextSize = plaintext.Length + _crypto.PageOverhead;
        var buf = System.Buffers.ArrayPool<byte>.Shared.Rent(4 + ciphertextSize);
        try
        {
            BitConverter.TryWriteBytes(buf.AsSpan(0, 4), plaintext.Length);
            _crypto.Encrypt(0, plaintext, buf.AsSpan(4, ciphertextSize));
            _walStream!.Write(buf.AsSpan(0, 4 + ciphertextSize));
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buf);
        }
    }

    // ── Record write methods ─────────────────────────────────────────────────

    public async ValueTask WriteBeginRecordAsync(ulong transactionId, CancellationToken ct = default)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring WAL write lock.");
        try
        {
            // Use ArrayPool for async I/O compatibility (cannot use stackalloc with async)
            var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(17);
            try
            {
                buffer[0] = (byte)WalRecordType.Begin;
                BitConverter.TryWriteBytes(buffer.AsSpan(1, 8), transactionId);
                BitConverter.TryWriteBytes(buffer.AsSpan(9, 8), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

                await WriteRawAsync(new ReadOnlyMemory<byte>(buffer, 0, 17), ct);
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        finally
        {
            _lock.Release();
        }
    }


    public async ValueTask WriteCommitRecordAsync(ulong transactionId, CancellationToken ct = default)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring WAL write lock.");
        try
        {
            var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(17);
            try
            {
                buffer[0] = (byte)WalRecordType.Commit;
                BitConverter.TryWriteBytes(buffer.AsSpan(1, 8), transactionId);
                BitConverter.TryWriteBytes(buffer.AsSpan(9, 8), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

                await WriteRawAsync(new ReadOnlyMemory<byte>(buffer, 0, 17), ct);
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        finally
        {
            _lock.Release();
        }
    }


    public async ValueTask WriteAbortRecordAsync(ulong transactionId, CancellationToken ct = default)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring WAL write lock.");
        try
        {
            var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(17);
            try
            {
                buffer[0] = (byte)WalRecordType.Abort;
                BitConverter.TryWriteBytes(buffer.AsSpan(1, 8), transactionId);
                BitConverter.TryWriteBytes(buffer.AsSpan(9, 8), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

                await WriteRawAsync(new ReadOnlyMemory<byte>(buffer, 0, 17), ct);
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        finally
        {
            _lock.Release();
        }
    }


    public async ValueTask WriteDataRecordAsync(ulong transactionId, uint pageId, ReadOnlyMemory<byte> afterImage, CancellationToken ct = default)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring WAL write lock.");
        try
        {
            var headerSize = 17;
            var totalSize = headerSize + afterImage.Length;

            var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(totalSize);
            try
            {
                buffer[0] = (byte)WalRecordType.Write;
                BitConverter.TryWriteBytes(buffer.AsSpan(1, 8), transactionId);
                BitConverter.TryWriteBytes(buffer.AsSpan(9, 4), pageId);
                BitConverter.TryWriteBytes(buffer.AsSpan(13, 4), afterImage.Length);

                afterImage.Span.CopyTo(buffer.AsSpan(headerSize));

                await WriteRawAsync(new ReadOnlyMemory<byte>(buffer, 0, totalSize), ct);
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    private void WriteDataRecordInternal(ulong transactionId, uint pageId, ReadOnlySpan<byte> afterImage)
    {
        // Header: type(1) + txnId(8) + pageId(4) + afterSize(4) = 17 bytes
        var headerSize = 17;
        var totalSize = headerSize + afterImage.Length;

        var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(totalSize);
        try
        {
            buffer[0] = (byte)WalRecordType.Write;
            BitConverter.TryWriteBytes(buffer.AsSpan(1, 8), transactionId);
            BitConverter.TryWriteBytes(buffer.AsSpan(9, 4), pageId);
            BitConverter.TryWriteBytes(buffer.AsSpan(13, 4), afterImage.Length);

            afterImage.CopyTo(buffer.AsSpan(headerSize));

            WriteRawSync(buffer.AsSpan(0, totalSize));
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
        }
    }


    public async Task FlushAsync(CancellationToken ct = default)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring WAL write lock.");
        try
        {
            if (_walStream != null)
            {
                await _walStream.FlushAsync(ct);
                // FlushAsync doesn't guarantee flushToDisk on all platforms/implementations in the same way as Flush(true)
                // but FileStream in .NET 6+ handles this reasonable well. 
                // For strict durability, we might still want to invoke a sync flush or check platform specifics,
                // but typically FlushAsync(ct) is sufficient for "Async" pattern.
                // However, FileStream.FlushAsync() acts like flushToDisk=false by default in older .NET?
                // Actually, FileStream.Flush() has flushToDisk arg, FlushAsync does not but implementation usually does buffer flush.
                // To be safe for WAL, we might care about fsync.
                // For now, just FlushAsync();
            }
        }
        finally
        {
            _lock.Release();
        }
    }


    /// <summary>
    /// Gets the current size of the WAL file in bytes
    /// </summary>
    public long GetCurrentSize()
    {
        if (!_lock.Wait(_writeTimeoutMs))
            throw new TimeoutException("Timed out acquiring WAL write lock.");
        try
        {
            return _walStream?.Length ?? 0;
        }
        finally
        {
            _lock.Release();
        }
    }

    internal async Task BackupAsync(string destinationPath, CancellationToken ct = default)
    {
#if NET8_0_OR_GREATER
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);
#else
        if (string.IsNullOrWhiteSpace(destinationPath))
            throw new ArgumentException("The value cannot be null, empty, or whitespace.", nameof(destinationPath));
#endif

        if (!await _lock.WaitAsync(_writeTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring WAL write lock.");
        try
        {
            if (_walStream == null)
                throw new InvalidOperationException("WAL is not open.");

            var destinationDirectory = Path.GetDirectoryName(destinationPath);
            if (!string.IsNullOrEmpty(destinationDirectory))
                Directory.CreateDirectory(destinationDirectory);

            _walStream.Flush(flushToDisk: true);

            var savedPosition = _walStream.Position;
            _walStream.Position = 0;

            await using var destination = new FileStream(
                destinationPath, FileMode.Create, FileAccess.Write, FileShare.None,
                bufferSize: 1024 * 1024, FileOptions.Asynchronous);

            await _walStream.CopyToAsync(destination, ct);
            await destination.FlushAsync(ct);

            _walStream.Position = savedPosition;
        }
        finally
        {
            _lock.Release();
        }
    }


    /// <summary>
    /// Truncates the WAL file (removes all content).
    /// Should only be called after successful checkpoint.
    /// </summary>
    public async Task TruncateAsync(CancellationToken ct = default)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring WAL write lock.");
        try
        {
            if (_walStream != null)
            {
                _walStream.SetLength(0);
                _walStream.Position = 0;
                await _walStream.FlushAsync(ct);
                // Reset so the crypto file header is re-written on the next record write.
                _cryptoInitialized = false;
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Reads all WAL records (for recovery).
    /// Decrypts records transparently when a crypto provider is active.
    /// </summary>
    public List<WalRecord> ReadAll()
    {
        if (!_lock.Wait(_writeTimeoutMs))
            throw new TimeoutException("Timed out acquiring WAL write lock.");
        try
        {
            var records = new List<WalRecord>();

            if (_walStream == null || _walStream.Length == 0)
                return records;

            if (_crypto != null && _cryptoInitialized)
            {
                // ── Encrypted path ────────────────────────────────────────────────────
                // Skip the 64-byte file header and read encrypted record envelopes.
                var headerSize = _crypto.FileHeaderSize;
                if (_walStream.Length <= headerSize)
                    return records;

                _walStream.Position = headerSize;

                var sizeBuf = new byte[4];

                while (_walStream.Position < _walStream.Length)
                {
                    // Each envelope: [plaintext_size(4)][ciphertext(plaintext_size + PageOverhead)]
                    var read = _walStream.Read(sizeBuf, 0, 4);
                    if (read < 4) break;

                    var plaintextSize = BitConverter.ToInt32(sizeBuf, 0);
                    // Sanity check: valid range for a WAL record payload.
                    if (plaintextSize < 1 || plaintextSize > 100 * 1024 * 1024) break;

                    var ciphertextSize = plaintextSize + _crypto.PageOverhead;
                    var cipherBuf = System.Buffers.ArrayPool<byte>.Shared.Rent(ciphertextSize);
                    var plainBuf  = System.Buffers.ArrayPool<byte>.Shared.Rent(plaintextSize);
                    try
                    {
                        if (_walStream.Read(cipherBuf, 0, ciphertextSize) < ciphertextSize)
                            break;

                        try
                        {
                            _crypto.Decrypt(0, cipherBuf.AsSpan(0, ciphertextSize), plainBuf.AsSpan(0, plaintextSize));
                        }
                        catch (CryptographicException)
                        {
                            // Authentication tag mismatch — corrupted or truncated record; stop.
                            break;
                        }

                        var record = ParsePlaintextRecord(plainBuf.AsSpan(0, plaintextSize));
                        if (record == null) break;
                        records.Add(record.Value);
                    }
                    finally
                    {
                        System.Buffers.ArrayPool<byte>.Shared.Return(cipherBuf);
                        System.Buffers.ArrayPool<byte>.Shared.Return(plainBuf);
                    }
                }
            }
            else
            {
                // ── Plaintext path (existing format, unchanged) ────────────────────────
                _walStream.Position = 0;

                // Allocate buffers outside loop to avoid CA2014 warning
                Span<byte> headerBuf = stackalloc byte[16];

                while (_walStream.Position < _walStream.Length)
                {
                    var typeByte = _walStream.ReadByte();
                    if (typeByte == -1) break;

                    var type = (WalRecordType)typeByte;

                    // Check for invalid record type (file padding or corruption)
                    if (typeByte == 0 || !Enum.IsDefined(typeof(WalRecordType), type))
                    {
                        // Reached end of valid records (file may have padding)
                        break;
                    }

                    WalRecord record;

                    switch (type)
                    {
                        case WalRecordType.Begin:
                        case WalRecordType.Commit:
                        case WalRecordType.Abort:
                            // Read common fields (txnId + timestamp = 16 bytes)
                            var bytesRead = _walStream.Read(headerBuf);
                            if (bytesRead < 16)
                            {
                                // Incomplete record, stop reading
                                return records;
                            }

                            var txnId = BitConverter.ToUInt64(headerBuf[0..8]);
                            var timestamp = BitConverter.ToInt64(headerBuf[8..16]);

                            record = new WalRecord
                            {
                                Type = type,
                                TransactionId = txnId,
                                Timestamp = timestamp
                            };
                            break;

                        case WalRecordType.Write:
                            // Write records have different format: txnId(8) + pageId(4) + afterSize(4)
                            // Read txnId + pageId + afterSize = 16 bytes
                            bytesRead = _walStream.Read(headerBuf);
                            if (bytesRead < 16)
                            {
                                // Incomplete write record header, stop reading
                                return records;
                            }

                            txnId = BitConverter.ToUInt64(headerBuf[0..8]);
                            var pageId = BitConverter.ToUInt32(headerBuf[8..12]);
                            var afterSize = BitConverter.ToInt32(headerBuf[12..16]);

                            // Validate afterSize to prevent overflow or corruption
                            if (afterSize < 0 || afterSize > 100 * 1024 * 1024) // Max 100MB per record
                            {
                                // Corrupted size, stop reading
                                return records;
                            }

                            var afterImage = new byte[afterSize];

                            // Read afterImage
                            if (_walStream.Read(afterImage) < afterSize)
                            {
                                // Incomplete after image, stop reading
                                return records;
                            }

                            record = new WalRecord
                            {
                                Type = type,
                                TransactionId = txnId,
                                Timestamp = 0, // Write records don't have timestamp
                                PageId = pageId,
                                AfterImage = afterImage
                            };
                            break;

                        default:
                            // Unknown record type, stop reading
                            return records;
                    }

                    records.Add(record);
                }
            }

            return records;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Parses a decrypted (or plaintext) record buffer into a <see cref="WalRecord"/>.
    /// Returns <c>null</c> if the buffer is invalid or the record type is unknown.
    /// </summary>
    private static WalRecord? ParsePlaintextRecord(ReadOnlySpan<byte> data)
    {
        if (data.Length < 1) return null;

        var typeByte = data[0];
        var type = (WalRecordType)typeByte;

        if (typeByte == 0 || !Enum.IsDefined(typeof(WalRecordType), type))
            return null;

        switch (type)
        {
            case WalRecordType.Begin:
            case WalRecordType.Commit:
            case WalRecordType.Abort:
                if (data.Length < 17) return null;
                return new WalRecord
                {
                    Type      = type,
                    TransactionId = BitConverter.ToUInt64(data.Slice(1, 8)),
                    Timestamp = BitConverter.ToInt64(data.Slice(9, 8))
                };

            case WalRecordType.Write:
                if (data.Length < 17) return null;
                var txnId     = BitConverter.ToUInt64(data.Slice(1, 8));
                var pageId    = BitConverter.ToUInt32(data.Slice(9, 4));
                var afterSize = BitConverter.ToInt32(data.Slice(13, 4));
                if (afterSize < 0 || afterSize > 100 * 1024 * 1024) return null;
                if (data.Length < 17 + afterSize) return null;

                var afterImage = new byte[afterSize];
                data.Slice(17, afterSize).CopyTo(afterImage);

                return new WalRecord
                {
                    Type          = type,
                    TransactionId = txnId,
                    Timestamp     = 0, // Write records don't carry a timestamp
                    PageId        = pageId,
                    AfterImage    = afterImage
                };

            default:
                return null;
        }
    }


    public void Dispose()
    {
        if (_disposed)
            return;

        if (_lock.Wait(5_000))
        {
            try
            {
                _walStream?.Dispose();
                // Best-effort: zero key material; never let a crypto disposal failure
                // prevent the stream from being closed (matches PageFile pattern).
                if (_crypto is IDisposable disposableCrypto)
                    try { disposableCrypto.Dispose(); } catch { /* best-effort */ }
                _disposed = true;
            }
            finally
            {
                _lock.Release();
                _lock.Dispose();
            }
        }
        else
        {
            // Best-effort: dispose stream even without lock to avoid resource leak
            _walStream?.Dispose();
            // Best-effort: zero key material even when the lock could not be acquired.
            if (_crypto is IDisposable disposableCrypto2)
                try { disposableCrypto2.Dispose(); } catch { /* best-effort */ }
            _disposed = true;
            _lock.Dispose();
        }

        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// Represents a WAL record.
/// Implemented as struct for memory efficiency.
/// </summary>
public struct WalRecord
{
    public WalRecordType Type { get; set; }
    public ulong TransactionId { get; set; }
    public long Timestamp { get; set; }
    public uint PageId { get; set; }
    public byte[]? AfterImage { get; set; }
}
