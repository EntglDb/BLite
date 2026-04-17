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
/// WAL records are stored as serialised byte arrays (base64-encoded for JS interop)
/// in an IndexedDB object store with auto-increment keys. This enables crash recovery
/// in browser contexts — if the tab reloads, committed records can be replayed from
/// IndexedDB.
/// </para>
/// <para>
/// <c>TruncateAsync</c> clears all WAL entries in a single IndexedDB transaction.
/// </para>
/// </summary>
public sealed class IndexedDbWriteAheadLog : IWriteAheadLog
{
    private readonly string _dbName;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly int _writeTimeoutMs;
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
    /// Opens the IndexedDB WAL store. Must be called once before any I/O.
    /// </summary>
    public async Task OpenAsync()
    {
        if (_opened) return;
        await IndexedDbInterop.EnsureLoadedAsync();
        await IndexedDbInterop.WalOpenAsync(_dbName);
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
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>No-op: IndexedDB writes are durable once the IDB transaction commits.</summary>
    public Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;

    /// <inheritdoc/>
    public long GetCurrentSize()
    {
        // SemaphoreSlim.Wait(int) is not supported in browser WASM;
        // use async version with cooperative scheduling.
        if (!_lock.WaitAsync(_writeTimeoutMs).GetAwaiter().GetResult())
            throw new TimeoutException("Timed out acquiring IndexedDbWriteAheadLog lock.");
        try
        {
            return _opened
                ? (long)IndexedDbInterop.WalGetSizeAsync(_dbName).GetAwaiter().GetResult()
                : 0;
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
            throw new TimeoutException("Timed out acquiring IndexedDbWriteAheadLog lock.");
        try
        {
            if (_opened)
                await IndexedDbInterop.WalClearAsync(_dbName);
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
            throw new TimeoutException("Timed out acquiring IndexedDbWriteAheadLog lock.");
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
        }
        finally
        {
            _lock.Release();
        }
    }

    private List<WalRecord> ReadAllInternal()
    {
        var records = new List<WalRecord>();
        if (!_opened) return records;

        // The JS side returns a JSON array of base64 strings: ["AQAA...","AgAA..."]
        var json = IndexedDbInterop.WalReadAllAsync(_dbName).GetAwaiter().GetResult();
        if (string.IsNullOrEmpty(json)) return records;

        var base64Records = JsonSerializer.Deserialize<string[]>(json);
        if (base64Records == null) return records;

        foreach (var base64 in base64Records)
        {
            if (string.IsNullOrEmpty(base64)) continue;

            var buffer = Convert.FromBase64String(base64);
            if (buffer.Length < 1) continue;

            var typeByte = buffer[0];
            if (typeByte == 0 || !Enum.IsDefined(typeof(WalRecordType), (WalRecordType)typeByte))
                continue;

            var type = (WalRecordType)typeByte;

            switch (type)
            {
                case WalRecordType.Begin:
                case WalRecordType.Commit:
                case WalRecordType.Abort:
                    if (buffer.Length < 17) continue;
                    var txnId = BitConverter.ToUInt64(buffer, 1);
                    var timestamp = BitConverter.ToInt64(buffer, 9);
                    records.Add(new WalRecord { Type = type, TransactionId = txnId, Timestamp = timestamp });
                    break;

                case WalRecordType.Write:
                    if (buffer.Length < 17) continue;
                    txnId = BitConverter.ToUInt64(buffer, 1);
                    var pageId = BitConverter.ToUInt32(buffer, 9);
                    var afterSize = BitConverter.ToInt32(buffer, 13);
                    if (afterSize < 0 || afterSize > 100 * 1024 * 1024 || buffer.Length < 17 + afterSize)
                        continue;
                    var afterImage = new byte[afterSize];
                    Buffer.BlockCopy(buffer, 17, afterImage, 0, afterSize);
                    records.Add(new WalRecord
                    {
                        Type = type,
                        TransactionId = txnId,
                        PageId = pageId,
                        AfterImage = afterImage
                    });
                    break;
            }
        }

        return records;
    }
}
