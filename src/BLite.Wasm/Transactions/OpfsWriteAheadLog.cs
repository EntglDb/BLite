using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BLite.Core.Transactions;
using BLite.Wasm.Interop;

namespace BLite.Wasm.Transactions;

/// <summary>
/// OPFS-backed Write-Ahead Log for browser WASM.
/// <para>
/// WAL records are appended to a dedicated OPFS file (<c>{dbName}.wal</c>) using
/// the <c>FileSystemSyncAccessHandle</c> API. This provides crash-recovery
/// capability — if the browser tab or Worker restarts, committed records can be
/// replayed from the file.
/// </para>
/// <para>
/// The binary format matches <see cref="WriteAheadLog"/> (file-based):
/// <list type="bullet">
///   <item>BEGIN/COMMIT/ABORT: type(1) + txnId(8) + timestamp(8) = 17 bytes</item>
///   <item>WRITE: type(1) + txnId(8) + pageId(4) + afterSize(4) + afterImage(N)</item>
/// </list>
/// </para>
/// </summary>
public sealed class OpfsWriteAheadLog : IWriteAheadLog
{
    private readonly string _dbName;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly int _writeTimeoutMs;
    private bool _opened;
    private bool _disposed;

    /// <summary>
    /// Initialises a new OPFS WAL for the given database name.
    /// Call <see cref="OpenAsync"/> before use.
    /// </summary>
    /// <param name="dbName">Logical database name. The WAL file will be <c>{dbName}.wal</c>.</param>
    /// <param name="writeTimeoutMs">Timeout in milliseconds for acquiring the internal lock.</param>
    public OpfsWriteAheadLog(string dbName, int writeTimeoutMs = 5_000)
    {
        if (string.IsNullOrWhiteSpace(dbName))
            throw new ArgumentException("Database name must not be null or empty.", nameof(dbName));

        _dbName = dbName + ".wal";
        _writeTimeoutMs = writeTimeoutMs;
    }

    /// <summary>
    /// Opens the OPFS WAL file. Must be called once before any I/O.
    /// </summary>
    public async Task OpenAsync()
    {
        if (_opened) return;
        await OpfsInterop.EnsureLoadedAsync();
        // Open with pageSize=1 — we manage our own offsets.
        await OpfsInterop.OpenAsync(_dbName, 1);
        _opened = true;
    }

    /// <inheritdoc/>
    public async ValueTask WriteBeginRecordAsync(ulong transactionId, CancellationToken ct = default)
    {
        await WriteControlRecordAsync(WalRecordType.Begin, transactionId, ct);
    }

    /// <inheritdoc/>
    public async ValueTask WriteCommitRecordAsync(ulong transactionId, CancellationToken ct = default)
    {
        await WriteControlRecordAsync(WalRecordType.Commit, transactionId, ct);
    }

    /// <inheritdoc/>
    public async ValueTask WriteAbortRecordAsync(ulong transactionId, CancellationToken ct = default)
    {
        await WriteControlRecordAsync(WalRecordType.Abort, transactionId, ct);
    }

    /// <inheritdoc/>
    public async ValueTask WriteDataRecordAsync(ulong transactionId, uint pageId, ReadOnlyMemory<byte> afterImage, CancellationToken ct = default)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct).ConfigureAwait(false))
            throw new TimeoutException("Timed out acquiring OpfsWriteAheadLog lock.");
        try
        {
            // Header: type(1) + txnId(8) + pageId(4) + afterSize(4) = 17
            var totalSize = 17 + afterImage.Length;
            var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(totalSize);
            try
            {
                buffer[0] = (byte)WalRecordType.Write;
                BitConverter.TryWriteBytes(buffer.AsSpan(1, 8), transactionId);
                BitConverter.TryWriteBytes(buffer.AsSpan(9, 4), pageId);
                BitConverter.TryWriteBytes(buffer.AsSpan(13, 4), afterImage.Length);
                afterImage.Span.CopyTo(buffer.AsSpan(17));

                // Append at current end of file.
                var currentSize = (int)OpfsInterop.GetSize(_dbName);
                // Write the buffer as a "page" at the byte offset.
                // Since we opened with pageSize=1, pageId = byte offset.
                // But that's not how the OPFS interop works — it multiplies pageId * pageSize.
                // We need to write at a raw byte offset. Use the raw write.
                WriteRawBytes(buffer.AsSpan(0, totalSize), currentSize);
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

    /// <inheritdoc/>
    public Task FlushAsync(CancellationToken ct = default)
    {
        if (_opened)
            OpfsInterop.Flush(_dbName);
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public long GetCurrentSize()
    {
        // SemaphoreSlim.Wait(int) is not supported in browser WASM;
        // use async version with cooperative scheduling.
        if (!_lock.WaitAsync(_writeTimeoutMs).GetAwaiter().GetResult())
            throw new TimeoutException("Timed out acquiring OpfsWriteAheadLog lock.");
        try
        {
            return _opened ? (long)OpfsInterop.GetSize(_dbName) : 0;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc/>
    public async Task TruncateAsync(CancellationToken ct = default)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct).ConfigureAwait(false))
            throw new TimeoutException("Timed out acquiring OpfsWriteAheadLog lock.");
        try
        {
            if (_opened)
                OpfsInterop.Truncate(_dbName);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc/>
    public List<WalRecord> ReadAll()
    {
        // SemaphoreSlim.Wait(int) is not supported in browser WASM.
        if (!_lock.WaitAsync(_writeTimeoutMs).GetAwaiter().GetResult())
            throw new TimeoutException("Timed out acquiring OpfsWriteAheadLog lock.");
        try
        {
            return ReadAllInternal();
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        if (_opened)
            OpfsInterop.Close(_dbName);

        _lock.Dispose();
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private async ValueTask WriteControlRecordAsync(WalRecordType type, ulong transactionId, CancellationToken ct)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct).ConfigureAwait(false))
            throw new TimeoutException("Timed out acquiring OpfsWriteAheadLog lock.");
        try
        {
            var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(17);
            try
            {
                buffer[0] = (byte)type;
                BitConverter.TryWriteBytes(buffer.AsSpan(1, 8), transactionId);
                BitConverter.TryWriteBytes(buffer.AsSpan(9, 8), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

                var currentSize = (int)OpfsInterop.GetSize(_dbName);
                WriteRawBytes(buffer.AsSpan(0, 17), currentSize);
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

    /// <summary>
    /// Writes raw bytes to the OPFS file at the given byte offset.
    /// Since we opened with pageSize=1, pageId maps to byte offset.
    /// </summary>
    private void WriteRawBytes(Span<byte> data, int offset)
    {
        OpfsInterop.WritePage(_dbName, offset, data);
    }

    private List<WalRecord> ReadAllInternal()
    {
        var records = new List<WalRecord>();
        if (!_opened) return records;

        var fileSize = (int)OpfsInterop.GetSize(_dbName);
        if (fileSize == 0) return records;

        // Read the entire WAL file into memory for parsing.
        var buffer = new byte[fileSize];
        OpfsInterop.ReadPage(_dbName, 0, buffer.AsSpan());

        var position = 0;
        while (position < fileSize)
        {
            if (position + 1 > fileSize) break;
            var typeByte = buffer[position];
            if (typeByte == 0 || !Enum.IsDefined(typeof(WalRecordType), (WalRecordType)typeByte))
                break;

            var type = (WalRecordType)typeByte;

            switch (type)
            {
                case WalRecordType.Begin:
                case WalRecordType.Commit:
                case WalRecordType.Abort:
                    if (position + 17 > fileSize) return records;
                    var txnId = BitConverter.ToUInt64(buffer, position + 1);
                    var timestamp = BitConverter.ToInt64(buffer, position + 9);
                    records.Add(new WalRecord { Type = type, TransactionId = txnId, Timestamp = timestamp });
                    position += 17;
                    break;

                case WalRecordType.Write:
                    if (position + 17 > fileSize) return records;
                    txnId = BitConverter.ToUInt64(buffer, position + 1);
                    var pageId = BitConverter.ToUInt32(buffer, position + 9);
                    var afterSize = BitConverter.ToInt32(buffer, position + 13);
                    if (afterSize < 0 || afterSize > 100 * 1024 * 1024 || position + 17 + afterSize > fileSize)
                        return records;
                    var afterImage = new byte[afterSize];
                    Buffer.BlockCopy(buffer, position + 17, afterImage, 0, afterSize);
                    records.Add(new WalRecord
                    {
                        Type = type,
                        TransactionId = txnId,
                        PageId = pageId,
                        AfterImage = afterImage
                    });
                    position += 17 + afterSize;
                    break;

                default:
                    return records;
            }
        }

        return records;
    }
}
