using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using BLite.Core.Transactions;
using BLite.Wasm.Interop;

namespace BLite.Wasm.Transactions;

/// <summary>
/// IndexedDB-backed Write-Ahead Log for browser WASM.
/// <para>
/// WAL records are kept in an in-memory list so that <see cref="ReadAll"/> and
/// <see cref="GetCurrentSize"/> never block on async JavaScript interop (which
/// would deadlock the single-threaded WASM runtime). Writes are persisted to
/// IndexedDB asynchronously so data survives page reloads.
/// </para>
/// </summary>
public sealed class IndexedDbWriteAheadLog : IWriteAheadLog
{
    private readonly string _dbName;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly int _writeTimeoutMs;
    private readonly List<WalRecord> _records = new();
    private long _totalSize;
    private bool _opened;
    private bool _disposed;

    /// <summary>
    /// Initialises a new IndexedDB WAL for the given database name.
    /// Call <see cref="OpenAsync"/> before use.
    /// </summary>
    /// <param name="dbName">Logical database name.</param>
    /// <param name="writeTimeoutMs">Timeout in milliseconds for acquiring the internal lock.</param>
    public IndexedDbWriteAheadLog(string dbName, int writeTimeoutMs = 5_000)
    {
        if (string.IsNullOrWhiteSpace(dbName))
            throw new ArgumentException("Database name must not be null or empty.", nameof(dbName));

        _dbName = dbName;
        _writeTimeoutMs = writeTimeoutMs;
    }

    /// <summary>
    /// Opens the IndexedDB WAL store and loads existing records into memory.
    /// Must be called once before any I/O.
    /// </summary>
    public async Task OpenAsync()
    {
        if (_opened) return;
        await IndexedDbInterop.EnsureLoadedAsync();
        await IndexedDbInterop.WalOpenAsync(_dbName);

        // Pre-load existing records so ReadAll()/GetCurrentSize() are synchronous.
        var json = await IndexedDbInterop.WalReadAllAsync(_dbName);
        if (!string.IsNullOrEmpty(json))
        {
            var base64Records = JsonSerializer.Deserialize<string[]>(json);
            if (base64Records != null)
            {
                foreach (var base64 in base64Records)
                    ParseAndAddRecord(base64);
            }
        }

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
            throw new TimeoutException("Timed out acquiring IndexedDbWriteAheadLog lock.");
        try
        {
            // Header: type(1) + txnId(8) + pageId(4) + afterSize(4) = 17
            var totalSize = 17 + afterImage.Length;
            var buffer = new byte[totalSize];
            buffer[0] = (byte)WalRecordType.Write;
            BitConverter.TryWriteBytes(buffer.AsSpan(1, 8), transactionId);
            BitConverter.TryWriteBytes(buffer.AsSpan(9, 4), pageId);
            BitConverter.TryWriteBytes(buffer.AsSpan(13, 4), afterImage.Length);
            afterImage.Span.CopyTo(buffer.AsSpan(17));

            await IndexedDbInterop.WalAppendAsync(_dbName, Convert.ToBase64String(buffer));

            // Keep in-memory copy.
            var afterCopy = new byte[afterImage.Length];
            afterImage.Span.CopyTo(afterCopy);
            _records.Add(new WalRecord
            {
                Type = WalRecordType.Write,
                TransactionId = transactionId,
                PageId = pageId,
                AfterImage = afterCopy
            });
            _totalSize += totalSize;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>No-op: IndexedDB writes are durable once the IDB transaction commits.</summary>
    public Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;

    /// <inheritdoc/>
    public long GetCurrentSize() => _totalSize;

    /// <inheritdoc/>
    public async Task TruncateAsync(CancellationToken ct = default)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct).ConfigureAwait(false))
            throw new TimeoutException("Timed out acquiring IndexedDbWriteAheadLog lock.");
        try
        {
            if (_opened)
                await IndexedDbInterop.WalClearAsync(_dbName);
            _records.Clear();
            _totalSize = 0;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc/>
    public List<WalRecord> ReadAll() => new(_records);

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _lock.Dispose();
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private async ValueTask WriteControlRecordAsync(WalRecordType type, ulong transactionId, CancellationToken ct)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct).ConfigureAwait(false))
            throw new TimeoutException("Timed out acquiring IndexedDbWriteAheadLog lock.");
        try
        {
            var buffer = new byte[17];
            buffer[0] = (byte)type;
            BitConverter.TryWriteBytes(buffer.AsSpan(1, 8), transactionId);
            BitConverter.TryWriteBytes(buffer.AsSpan(9, 8), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

            await IndexedDbInterop.WalAppendAsync(_dbName, Convert.ToBase64String(buffer));

            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _records.Add(new WalRecord { Type = type, TransactionId = transactionId, Timestamp = timestamp });
            _totalSize += 17;
        }
        finally
        {
            _lock.Release();
        }
    }

    private void ParseAndAddRecord(string? base64)
    {
        if (string.IsNullOrEmpty(base64)) return;

        var buffer = Convert.FromBase64String(base64);
        if (buffer.Length < 1) return;

        var typeByte = buffer[0];
        if (typeByte == 0 || !Enum.IsDefined(typeof(WalRecordType), (WalRecordType)typeByte))
            return;

        var type = (WalRecordType)typeByte;

        switch (type)
        {
            case WalRecordType.Begin:
            case WalRecordType.Commit:
            case WalRecordType.Abort:
                if (buffer.Length < 17) return;
                var txnId = BitConverter.ToUInt64(buffer, 1);
                var timestamp = BitConverter.ToInt64(buffer, 9);
                _records.Add(new WalRecord { Type = type, TransactionId = txnId, Timestamp = timestamp });
                _totalSize += buffer.Length;
                break;

            case WalRecordType.Write:
                if (buffer.Length < 17) return;
                txnId = BitConverter.ToUInt64(buffer, 1);
                var pageId = BitConverter.ToUInt32(buffer, 9);
                var afterSize = BitConverter.ToInt32(buffer, 13);
                if (afterSize < 0 || afterSize > 100 * 1024 * 1024 || buffer.Length < 17 + afterSize)
                    return;
                var afterImage = new byte[afterSize];
                Buffer.BlockCopy(buffer, 17, afterImage, 0, afterSize);
                _records.Add(new WalRecord
                {
                    Type = type,
                    TransactionId = txnId,
                    PageId = pageId,
                    AfterImage = afterImage
                });
                _totalSize += buffer.Length;
                break;
        }
    }
}
