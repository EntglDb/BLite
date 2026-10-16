using System.Buffers.Binary;
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

    /// <summary>Path of the WAL file (diagnostics).</summary>
    internal string FilePath => _walPath;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private bool _disposed;
    private readonly int _writeTimeoutMs;

    // When true, the WAL file is opened with FileShare.ReadWrite (so other processes
    // can open the same WAL); when false, FileShare.None preserves the historical
    // single-process semantics. Cross-process writer serialisation, atomic transaction-id
    // allocation, the .wal-shm sidecar, and stale-PID recovery are owned by
    // StorageEngine + WalSharedMemory; see roadmap/v5/MULTI_PROCESS_WAL.md.
    private readonly bool _allowMultiProcessAccess;

    /// <summary>
    /// Whether this WAL was opened in multi-process mode
    /// (<see cref="FileShare.ReadWrite"/> instead of <see cref="FileShare.None"/>).
    /// Exposed to internals (notably <c>BLite.Tests</c>) so the
    /// <see cref="BLite.Core.Storage.PageFileConfig.AllowMultiProcessAccess"/> forwarding
    /// path can be asserted from integration tests without reflection.
    /// </summary>
    internal bool AllowMultiProcessAccess => _allowMultiProcessAccess;

    // Optional encryption provider. Non-null only when real encryption is configured
    // (i.e. crypto.FileHeaderSize > 0). NullCryptoProvider is treated as no encryption.
    private readonly ICryptoProvider? _crypto;

    // True when the crypto file header has been written to (or read from) the current
    // WAL file. Reset to false by TruncateAsync so the header is re-written on the
    // next record write (after truncation the file is empty and needs a fresh header),
    // and by ResetCryptoHeader when another process has truncated the shared WAL.
    private volatile bool _cryptoInitialized;

    // Serialises loading or creating the crypto header: the key changes with it.
    private readonly object _cryptoHeaderLock = new();

    // Incremented under _cryptoHeaderLock whenever the WAL is truncated or its key forgotten:
    // header bytes a reader read before that belong to an older WAL and must not be loaded.
    private long _cryptoHeaderEpoch;

    /// <summary>
    /// Opens or creates a WAL file at <paramref name="walPath"/>.
    /// </summary>
    /// <param name="walPath">Path to the WAL file.</param>
    /// <param name="crypto">
    /// Optional encryption provider. When non-null and <see cref="ICryptoProvider.FileHeaderSize"/>
    /// is greater than zero, each WAL record is AES-256-GCM encrypted before being written.
    /// Pass <c>null</c> or <see cref="NullCryptoProvider"/> to keep WAL records in plaintext.
    /// <para>
    /// <b>Ownership:</b> <see cref="WriteAheadLog"/> takes ownership of <paramref name="crypto"/>
    /// and will dispose it when this instance is disposed. Do not share the same
    /// <see cref="ICryptoProvider"/> instance with another component (e.g. <see cref="BLite.Core.Storage.PageFile"/>)
    /// to avoid double-dispose.
    /// </para>
    /// </param>
    /// <param name="writeTimeoutMs">Lock-acquisition timeout in milliseconds.</param>
    /// <param name="allowMultiProcessAccess">
    /// When <c>true</c>, opens the WAL file with <see cref="FileShare.ReadWrite"/> instead
    /// of <see cref="FileShare.None"/> so other processes can open the same WAL.
    /// Cross-process writer serialisation and the <c>.wal-shm</c> sidecar are owned by
    /// the surrounding <see cref="BLite.Core.Storage.StorageEngine"/>; this WAL only
    /// relaxes its own <see cref="FileShare"/> mode based on the flag. See
    /// <c>roadmap/v5/MULTI_PROCESS_WAL.md</c> for the full design.
    /// </param>
    public WriteAheadLog(string walPath, ICryptoProvider? crypto, int writeTimeoutMs = 5_000, bool allowMultiProcessAccess = false)
    {
        _walPath = walPath ?? throw new ArgumentNullException(nameof(walPath));
        _writeTimeoutMs = writeTimeoutMs;
        _allowMultiProcessAccess = allowMultiProcessAccess;

        // Treat providers that add no overhead (NullCryptoProvider) as no encryption so
        // the on-disk format stays byte-for-byte identical to the pre-encryption format.
        _crypto = (crypto != null && crypto.FileHeaderSize > 0) ? crypto : null;

        _walStream = new FileStream(
            _walPath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            _allowMultiProcessAccess ? FileShare.ReadWrite : FileShare.None,
            bufferSize: 64 * 1024);
        // REMOVED FileOptions.WriteThrough for SQLite-style lazy checkpointing
        // Durability is ensured by explicit Flush() calls

        // If the WAL file already exists with content and encryption is enabled,
        // read the file header to derive the encryption key before any I/O.
        if (_crypto != null)
            TryLoadCryptoHeader(holdsLock: true);

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
    /// Loads the crypto file header already at the start of the WAL, if there is one, and
    /// derives the key from it. Guards against legacy plaintext or corrupt WAL files by
    /// checking the BLCE magic before passing the header to the crypto provider (which would
    /// throw on a magic mismatch); without it, <see cref="_cryptoInitialized"/> stays false.
    /// <paramref name="holdsLock"/>: the caller holds <see cref="_lock"/> (or is the
    /// constructor), so the header is read through the stream. Otherwise it is read at its
    /// offset before taking <see cref="_cryptoHeaderLock"/>: on runtimes without
    /// <c>RandomAccess</c> that read takes <see cref="_lock"/>, which a writer holds while it
    /// takes <see cref="_cryptoHeaderLock"/>. Bytes read while this process was writing the
    /// header are never loaded: the writer sets <see cref="_cryptoInitialized"/> before it
    /// releases <see cref="_cryptoHeaderLock"/>. Neither are bytes read before a truncation or a
    /// <see cref="ResetCryptoHeader"/>: they may be the old WAL's header (<see cref="_cryptoHeaderEpoch"/>).
    /// </summary>
    private bool TryLoadCryptoHeader(bool holdsLock)
    {
        if (_cryptoInitialized) return true;

        var size = _crypto!.FileHeaderSize;
        var header = System.Buffers.ArrayPool<byte>.Shared.Rent(size);
        try
        {
            int read = 0;
            long epoch = Interlocked.Read(ref _cryptoHeaderEpoch);
            if (!holdsLock)
                read = ReadAtOffset(0, header.AsSpan(0, size));

            lock (_cryptoHeaderLock)
            {
                if (_cryptoInitialized) return true;
                if (_cryptoHeaderEpoch != epoch) return false;

                if (holdsLock)
                {
                    if (_walStream!.Length < size) return false;
                    var saved = _walStream.Position;
                    _walStream.Position = 0;
                    read = _walStream.Read(header, 0, size);
                    _walStream.Position = saved;
                }

                if (read < size) return false;
                var magic = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(0, 4));
                if (magic != 0x424C4345u) // "BLCE"
                    return false;

                _crypto.LoadFromFileHeader(header.AsSpan(0, size));
                _cryptoInitialized = true;
                return true;
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(header);
        }
    }

    /// <summary>
    /// <c>true</c> when the WAL starts with a complete crypto file header (BLCE magic).
    /// Must be called while holding <see cref="_lock"/>.
    /// </summary>
    private bool HasCryptoHeaderNoLock()
    {
        var size = _crypto!.FileHeaderSize;
        if (_walStream!.Length < size) return false;
        Span<byte> magic = stackalloc byte[4];
        var saved = _walStream.Position;
        _walStream.Position = 0;
        var read = _walStream.Read(magic);
        _walStream.Position = saved;
        return read == 4 && BinaryPrimitives.ReadUInt32LittleEndian(magic) == 0x424C4345u; // "BLCE"
    }

    /// <summary>
    /// For readers: <c>true</c> when the key of the current WAL is known, loading the header
    /// first if another process has written it since this one last looked (multi-process mode,
    /// #203). <c>false</c> when the WAL has no header yet: there is nothing to decrypt.
    /// </summary>
    private bool EnsureCryptoHeaderLoaded() => _cryptoInitialized || TryLoadCryptoHeader(holdsLock: false);

    /// <summary>
    /// Forgets the key of the shared WAL after another process truncated it: the next read or
    /// write loads the header the new WAL starts with, or writes one if it is still empty.
    /// The caller must make sure no record of this process is being written meanwhile
    /// (the storage engine calls it under its commit lock).
    /// </summary>
    internal void ResetCryptoHeader()
    {
        if (_crypto == null) return;
        lock (_cryptoHeaderLock)
        {
            _cryptoInitialized = false;
            Interlocked.Increment(ref _cryptoHeaderEpoch);
        }
    }

    /// <summary>
    /// Makes sure the WAL starts with a crypto file header before a record is written.
    /// A header already in the file is loaded, never replaced: in multi-process mode it may be
    /// another process's, and its records are encrypted with that key (#203). Otherwise a new
    /// header is written at offset 0, discarding stale content (leftover plaintext from before
    /// encryption was enabled, or a torn header) so it is never appended mid-file. The header
    /// is written synchronously under <see cref="_cryptoHeaderLock"/>, so a reader of this
    /// process never loads it half-written and changes the key under the writer.
    /// Must be called while holding <see cref="_lock"/>.
    /// </summary>
    private void EnsureCryptoHeader()
    {
        if (_crypto == null || _cryptoInitialized) return;
        if (TryLoadCryptoHeader(holdsLock: true)) return;

        lock (_cryptoHeaderLock)
        {
            if (_walStream!.Length != 0)
                _walStream.SetLength(0);
            _walStream.Position = 0;

            var header = new byte[_crypto.FileHeaderSize];
            _crypto.GetFileHeader(header);
            _walStream.Write(header, 0, header.Length);
            _cryptoInitialized = true;
        }
    }

    /// <summary>
    /// Async path of <see cref="EnsureCryptoHeader"/>: the 64-byte header is written
    /// synchronously, once per WAL generation.
    /// </summary>
    private ValueTask EnsureCryptoHeaderAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        EnsureCryptoHeader();
        return default;
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

        // Encrypted envelope: [plaintext_size(4 LE)][nonce(12)][ciphertext(N)][tag(16)]
        // where N = plaintext.Length and PageOverhead = 28 (12 nonce + 16 tag).
        var ciphertextSize = plaintext.Length + _crypto.PageOverhead;
        var buf = System.Buffers.ArrayPool<byte>.Shared.Rent(4 + ciphertextSize);
        try
        {
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(0, 4), plaintext.Length);
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
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(0, 4), plaintext.Length);
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

    /// <summary>
    /// Returns the current write position in the WAL stream without acquiring
    /// <c>_lock</c>. Safe to call only when the caller already holds an exclusive
    /// write-side guard (e.g. the StorageEngine <c>_commitLock</c> + SHM writer
    /// lock), which prevents any concurrent WAL write from updating the position.
    /// </summary>
    internal long GetCurrentPositionNoLock() => _walStream?.Position ?? 0;

    /// <summary>
    /// Multi-process: moves the write position to <paramref name="sharedEnd"/>, the end of the
    /// last batch any process has published (the SHM WAL end), and cuts off whatever lies beyond
    /// it. Another process may have appended since this instance last wrote, so the position left
    /// by its own writes is stale. Bytes past the shared end belong to a batch whose writer died
    /// before publishing it, so that batch was never acknowledged. Appending after them would put
    /// a torn record in front of this batch, where every reader's replay stops (#198).
    /// Call under the cross-process writer lock before appending.
    /// </summary>
    /// <remarks>
    /// Falls back to the end of the file if <paramref name="sharedEnd"/> lies beyond it, or, for an
    /// encrypted WAL, inside the crypto file header, which only a truncation resets.
    /// </remarks>
    internal void SeekToSharedEnd(long sharedEnd)
    {
        if (!_lock.Wait(_writeTimeoutMs))
            throw new TimeoutException("Timed out acquiring WAL write lock.");
        try
        {
            if (_walStream == null) return;

            long length = _walStream.Length;
            if (_crypto != null && sharedEnd < _crypto.FileHeaderSize)
            {
                // Nothing published in this generation yet. A header already in the file is
                // kept (a reader may have loaded its key), but whatever follows it was never
                // published and is cut off, as below. Without a header, EnsureCryptoHeader
                // writes one into an emptied file.
                if (HasCryptoHeaderNoLock())
                {
                    if (length > _crypto.FileHeaderSize)
                        _walStream.SetLength(_crypto.FileHeaderSize);
                    _walStream.Position = _crypto.FileHeaderSize;
                }
                else
                {
                    _walStream.Position = length;
                }
                return;
            }

            if (sharedEnd > length)
            {
                _walStream.Position = length;
                return;
            }

            if (length > sharedEnd)
                _walStream.SetLength(sharedEnd);
            _walStream.Position = sharedEnd;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Writes a WAL data record and returns the byte offset at which the record
    /// starts in the WAL file. The offset is captured atomically inside the WAL
    /// write lock — so no concurrent WAL writer can shift the stream position between
    /// the snapshot and the actual write.
    /// Used by the group-commit path (Phase 4) to build the SHM page index.
    /// </summary>
    internal async ValueTask<long> WriteDataRecordAndGetOffsetAsync(
        ulong transactionId, uint pageId, ReadOnlyMemory<byte> afterImage,
        CancellationToken ct = default)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring WAL write lock.");
        long startOffset;
        try
        {
            // Capture position atomically with the write — no other writer can run here.
            startOffset = _walStream?.Position ?? 0;

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
        return startOffset;
    }

    /// <summary>
    /// Reads the page data for a Write WAL record that starts at
    /// <paramref name="recordOffset"/> in the WAL file.  Used by the cross-process
    /// read path (Phase 4) so that a process that missed a page in its local
    /// <c>_walIndex</c> can fetch it directly from the WAL file.
    /// </summary>
    /// <param name="recordOffset">
    /// Byte offset of the Write record (type byte) in the WAL file, as stored in
    /// the SHM WAL index. For encrypted WALs this is the offset of the ciphertext
    /// envelope; for plaintext WALs it is the offset of the raw record bytes.
    /// </param>
    /// <param name="expectedPageId">
    /// The page ID the caller expects to find at this offset. If the stored page ID
    /// does not match, <c>null</c> is returned (stale SHM entry).
    /// </param>
    /// <returns>
    /// Decrypted page bytes, or <c>null</c> if the offset is out of range,
    /// the record type is not Write, or the page ID does not match.
    /// </returns>
    internal byte[]? ReadPageAt(long recordOffset, uint expectedPageId)
    {
        if (_walStream == null || recordOffset <= 0 || recordOffset >= _walStream.Length)
            return null;

        if (_crypto != null)
        {
            // No header yet (the WAL is empty): nothing to decrypt — return null so the
            // caller falls through to the page-file read. A header another process wrote
            // after this one opened the WAL is loaded first (#203).
            if (!EnsureCryptoHeaderLoaded()) return null;
            return ReadPageAtEncrypted(recordOffset, expectedPageId);
        }

        return ReadPageAtPlaintext(recordOffset, expectedPageId);
    }

    private byte[]? ReadPageAtPlaintext(long recordOffset, uint expectedPageId)
    {
        // Plaintext record layout at recordOffset:
        //   [type(1)][txnId(8)][pageId(4)][dataLen(4)][data(dataLen)]
        // Total header: 17 bytes
        var header = new byte[17];
        int bytesRead = ReadAtOffset(recordOffset, header);

        if (bytesRead < 17) return null;
        if (header[0] != (byte)WalRecordType.Write) return null;

        var pageId = BitConverter.ToUInt32(header, 9);
        if (pageId != expectedPageId) return null;

        var dataLen = BitConverter.ToInt32(header, 13);
        if (dataLen <= 0 || dataLen > 100 * 1024 * 1024) return null;

        var data = new byte[dataLen];
        bytesRead = ReadAtOffset(recordOffset + 17, data);

        return bytesRead < dataLen ? null : data;
    }

    private byte[]? ReadPageAtEncrypted(long envelopeOffset, uint expectedPageId)
    {
        // Encrypted envelope layout at envelopeOffset:
        //   [plaintext_size(4 LE)][ciphertext(plaintext_size + PageOverhead)]
        // After decryption the plaintext is a WAL record as in ReadPageAtPlaintext.
        var sizeBuf = new byte[4];
        int bytesRead = ReadAtOffset(envelopeOffset, sizeBuf);

        if (bytesRead < 4) return null;
        var plaintextSize = BitConverter.ToInt32(sizeBuf, 0);
        if (plaintextSize < 17 || plaintextSize > 100 * 1024 * 1024) return null;

        var ciphertextSize = plaintextSize + _crypto!.PageOverhead;
        var cipherBuf = System.Buffers.ArrayPool<byte>.Shared.Rent(ciphertextSize);
        var plainBuf  = System.Buffers.ArrayPool<byte>.Shared.Rent(plaintextSize);
        try
        {
            bytesRead = ReadAtOffset(envelopeOffset + 4, cipherBuf.AsSpan(0, ciphertextSize));

            if (bytesRead < ciphertextSize) return null;

            try
            {
                _crypto.Decrypt(0, cipherBuf.AsSpan(0, ciphertextSize), plainBuf.AsSpan(0, plaintextSize));
            }
            catch (Exception ex) when (ex is System.Security.Cryptography.CryptographicException or ObjectDisposedException)
            {
                // Stale or corrupted envelope, or the key was replaced meanwhile (a new WAL
                // generation, see ResetCryptoHeader).
                return null;
            }

            if (plainBuf[0] != (byte)WalRecordType.Write) return null;
            var pageId = BitConverter.ToUInt32(plainBuf, 9);
            if (pageId != expectedPageId) return null;

            var dataLen = BitConverter.ToInt32(plainBuf, 13);
            if (dataLen <= 0 || 17 + dataLen > plaintextSize) return null;

            var data = new byte[dataLen];
            plainBuf.AsSpan(17, dataLen).CopyTo(data);
            return data;
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(cipherBuf);
            System.Buffers.ArrayPool<byte>.Shared.Return(plainBuf, clearArray: true);
        }
    }

    // ── Cross-platform positioned read ──────────────────────────────────────
    //
    // RandomAccess.Read is a .NET 6+ API that allows offset-based reads without
    // affecting the stream position (making concurrent reads + writes safe).
    // On netstandard2.1 we fall back to a seek+read on the write stream, which
    // requires holding the WAL write lock (_lock) so that no concurrent WAL write
    // can change _walStream.Position between our Seek and Read calls.

    /// <summary>
    /// Reads bytes at a specific offset in the WAL file without requiring
    /// the WAL write lock to be pre-held. Thread-safe on .NET 6+ via
    /// <c>RandomAccess</c>; on .NET Standard 2.1 the WAL write lock is acquired
    /// briefly to serialise the seek+read with concurrent writers.
    /// </summary>
    private int ReadAtOffset(long offset, Span<byte> buffer)
    {
#if NET6_0_OR_GREATER
        if (_walStream?.SafeFileHandle is { } handle)
            return (int)System.IO.RandomAccess.Read(handle, buffer, offset);
#endif
        return ReadViaSeek(offset, buffer);
    }

    private int ReadAtOffset(long offset, byte[] buffer)
    {
#if NET6_0_OR_GREATER
        if (_walStream?.SafeFileHandle is { } handle)
            return (int)System.IO.RandomAccess.Read(handle, buffer, offset);
#endif
        return ReadViaSeek(offset, buffer);
    }

    // Seek-based fallback for platforms without RandomAccess.
    // Acquires _lock to prevent WAL writers from changing _walStream.Position
    // concurrently — the same lock they hold during WriteRawAsync / WriteRawSync.
    private int ReadViaSeek(long offset, Span<byte> buffer)
    {
        if (!_lock.Wait(_writeTimeoutMs))
            throw new TimeoutException("Timed out acquiring WAL read lock (seek fallback).");
        try
        {
            _walStream!.Position = offset;
            return _walStream.Read(buffer);
        }
        finally
        {
            _lock.Release();
        }
    }

    // Overload for byte[] (avoids Span allocation on older runtimes).
    private int ReadViaSeek(long offset, byte[] buffer)
    {
        if (!_lock.Wait(_writeTimeoutMs))
            throw new TimeoutException("Timed out acquiring WAL read lock (seek fallback).");
        try
        {
            _walStream!.Position = offset;
            return _walStream.Read(buffer, 0, buffer.Length);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Reads all committed pages from WAL records between
    /// <paramref name="fromOffset"/> (inclusive) and <paramref name="toOffset"/>
    /// (exclusive), replaying only fully committed transactions. Returns the
    /// deduplicated list of <c>(pageId, data, walOffset)</c> tuples — latest committed
    /// write per page wins — for population into the local <c>_walIndex</c> and
    /// <c>_walOffsets</c> (Phase 7 replay).
    /// </summary>
    /// <param name="complete">
    /// <c>true</c> if the records ended exactly at <paramref name="toOffset"/>. <c>false</c> if the
    /// scan stopped earlier at a record it could not read (torn, truncated, corrupt or
    /// undecryptable) or ran past <paramref name="toOffset"/>: the records after that point were not
    /// read, so the caller must not treat the range as replayed.
    /// </param>
    /// <param name="stoppedAt">Where the scan stopped: <paramref name="toOffset"/> if complete.</param>
    internal List<(uint pageId, byte[] data, long walOffset)> ReadCommittedPagesSince(long fromOffset, long toOffset, out bool complete, out long stoppedAt)
    {
        complete = true;
        stoppedAt = toOffset;
        if (_walStream == null || fromOffset >= toOffset) return new List<(uint, byte[], long)>();
        long scanned = fromOffset;

        // Track writes per transaction as (walOffset, data) so we can determine which
        // version of a page is latest (highest walOffset) across concurrent transactions.
        var txnWrites = new Dictionary<ulong, List<(long walOffset, uint pageId, byte[] data)>>();
        var committed  = new HashSet<ulong>();

        // An encrypted WAL without a header holds no record that can be read: the published
        // range cannot be replayed, and parsing it as plaintext would misread it.
        if (_crypto != null && !EnsureCryptoHeaderLoaded())
        {
            complete = false;
            stoppedAt = fromOffset;
            return new List<(uint, byte[], long)>();
        }

        if (_crypto != null)
        {
            // Encrypted WAL records start after the per-file crypto header (64 bytes).
            // When fromOffset falls inside the header region (e.g. first replay with
            // lastKnown=0), advance past it to avoid misinterpreting the BLCE magic as
            // a record envelope size.
            long pos = (_crypto.FileHeaderSize > 0 && fromOffset < _crypto.FileHeaderSize)
                ? _crypto.FileHeaderSize
                : fromOffset;
            var sizeBuf = new byte[4];
            while (pos < toOffset)
            {
                var rb = ReadAtOffset(pos, sizeBuf);
                if (rb < 4) break;

                var plaintextSize = BitConverter.ToInt32(sizeBuf, 0);
                if (plaintextSize < 1 || plaintextSize > 100 * 1024 * 1024) break;

                var ciphertextSize = plaintextSize + _crypto!.PageOverhead;
                var cipherBuf = System.Buffers.ArrayPool<byte>.Shared.Rent(ciphertextSize);
                var plainBuf  = System.Buffers.ArrayPool<byte>.Shared.Rent(plaintextSize);
                try
                {
                    rb = ReadAtOffset(pos + 4, cipherBuf.AsSpan(0, ciphertextSize));
                    if (rb < ciphertextSize) break;

                    try
                    {
                        _crypto.Decrypt(0, cipherBuf.AsSpan(0, ciphertextSize), plainBuf.AsSpan(0, plaintextSize));
                    }
                    catch (Exception ex) when (ex is System.Security.Cryptography.CryptographicException or ObjectDisposedException) { break; }

                    long recOffset = pos; // WAL offset of the envelope start
                    var rec = ParsePlaintextRecord(plainBuf.AsSpan(0, plaintextSize));
                    if (rec == null) break;
                    ProcessRecord(rec.Value, recOffset, txnWrites, committed);
                }
                finally
                {
                    System.Buffers.ArrayPool<byte>.Shared.Return(cipherBuf);
                    System.Buffers.ArrayPool<byte>.Shared.Return(plainBuf, clearArray: true);
                }
                pos += 4 + ciphertextSize;
            }
            scanned = pos;
        }
        else
        {
            // Plaintext path: scan records from fromOffset forward.
            long pos = fromOffset;
            var hdr = new byte[16];
            while (pos < toOffset)
            {
                var typeBuf = new byte[1];
                var tb = ReadAtOffset(pos, typeBuf);
                if (tb < 1) break;

                var type = (WalRecordType)typeBuf[0];
                if (typeBuf[0] == 0) break; // end of written area
                switch (type)
                {
                    case WalRecordType.Begin:
                    case WalRecordType.Commit:
                    case WalRecordType.Abort:
                    {
                        var rb = ReadAtOffset(pos + 1, hdr);
                        if (rb < 16) goto done;
                        var txnId = BitConverter.ToUInt64(hdr, 0);
                        if (type == WalRecordType.Commit) committed.Add(txnId);
                        pos += 1 + 16;
                        break;
                    }
                    case WalRecordType.Write:
                    {
                        var rb = ReadAtOffset(pos + 1, hdr);
                        if (rb < 16) goto done;
                        var txnId   = BitConverter.ToUInt64(hdr, 0);
                        var pageId  = BitConverter.ToUInt32(hdr, 8);
                        var dataLen = BitConverter.ToInt32(hdr, 12);
                        if (dataLen < 0 || dataLen > 100 * 1024 * 1024) goto done;
                        var data = new byte[dataLen];
                        rb = ReadAtOffset(pos + 1 + 16, data);
                        if (rb < dataLen) goto done;
                        long recOffset = pos; // offset of the Write type byte
                        if (!txnWrites.TryGetValue(txnId, out var list))
                            txnWrites[txnId] = list = new List<(long, uint, byte[])>();
                        list.Add((recOffset, pageId, data));
                        pos += 1 + 16 + dataLen;
                        break;
                    }
                    default: goto done;
                }
            }
            done:
            scanned = pos;
        }
        complete = scanned == toOffset;
        stoppedAt = scanned;

        // Emit one entry per page: the latest committed write (highest walOffset) wins.
        // Iterate all committed transactions' writes in WAL order so that later commits
        // overwrite earlier ones in the result dictionary (correct last-writer-wins semantics).
        var latestPerPage = new Dictionary<uint, (long walOffset, byte[] data)>();
        foreach (var txnId in committed)
        {
            if (!txnWrites.TryGetValue(txnId, out var writes)) continue;
            foreach (var (walOffset, pageId, data) in writes)
            {
                if (!latestPerPage.TryGetValue(pageId, out var existing) || walOffset > existing.walOffset)
                    latestPerPage[pageId] = (walOffset, data);
            }
        }

        var result = new List<(uint, byte[], long)>(latestPerPage.Count);
        foreach (var kvp in latestPerPage)
            result.Add((kvp.Key, kvp.Value.data, kvp.Value.walOffset));
        return result;
    }

    private static void ProcessRecord(
        WalRecord rec,
        long walOffset,
        Dictionary<ulong, List<(long walOffset, uint pageId, byte[] data)>> txnWrites,
        HashSet<ulong> committed)
    {
        if (rec.Type == WalRecordType.Commit)
        {
            committed.Add(rec.TransactionId);
        }
        else if (rec.Type == WalRecordType.Write && rec.AfterImage != null)
        {
            if (!txnWrites.TryGetValue(rec.TransactionId, out var list))
                txnWrites[rec.TransactionId] = list = new List<(long, uint, byte[])>();
            list.Add((walOffset, rec.PageId, rec.AfterImage));
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
                lock (_cryptoHeaderLock)
                {
                    _cryptoInitialized = false;
                    Interlocked.Increment(ref _cryptoHeaderEpoch);
                }
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
    public List<WalRecord> ReadAll() => ReadAll(out _);

    /// <summary>
    /// Reads all WAL records, as <see cref="ReadAll()"/>, and returns in
    /// <paramref name="readThrough"/> the offset just past the last record read: the end of the
    /// file when every record could be read, earlier when a torn, corrupt or undecryptable record
    /// ended the scan.
    /// </summary>
    internal List<WalRecord> ReadAll(out long readThrough)
    {
        readThrough = 0;
        if (!_lock.Wait(_writeTimeoutMs))
            throw new TimeoutException("Timed out acquiring WAL write lock.");
        try
        {
            var records = new List<WalRecord>();

            if (_walStream == null || _walStream.Length == 0)
                return records;

            if (_crypto != null && TryLoadCryptoHeader(holdsLock: true))
            {
                // ── Encrypted path ────────────────────────────────────────────────────
                // Skip the 64-byte file header and read encrypted record envelopes.
                var headerSize = _crypto.FileHeaderSize;
                if (_walStream.Length <= headerSize)
                {
                    readThrough = _walStream.Length;
                    return records;
                }

                _walStream.Position = headerSize;
                readThrough = headerSize;

                var sizeBuf = new byte[4];

                while (_walStream.Position < _walStream.Length)
                {
                    // Each envelope: [plaintext_size(4 LE)][ciphertext(plaintext_size + PageOverhead)]
                    var read = _walStream.Read(sizeBuf, 0, 4);
                    if (read < 4) break;

                    var plaintextSize = BinaryPrimitives.ReadInt32LittleEndian(sizeBuf);
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
                        catch (Exception ex) when (ex is CryptographicException or ObjectDisposedException)
                        {
                            // Authentication tag mismatch — corrupted or truncated record; stop.
                            break;
                        }

                        var record = ParsePlaintextRecord(plainBuf.AsSpan(0, plaintextSize));
                        if (record == null) break;
                        records.Add(record.Value);
                        readThrough = _walStream.Position;
                    }
                    finally
                    {
                        System.Buffers.ArrayPool<byte>.Shared.Return(cipherBuf);
                        // clearArray: true zeros decrypted page bytes so they are not
                        // observable by subsequent renters of this pool slot.
                        System.Buffers.ArrayPool<byte>.Shared.Return(plainBuf, clearArray: true);
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
                    readThrough = _walStream.Position;
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
