using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core.Transactions;

/// <summary>
/// In-memory Write-Ahead Log implementation. Records are stored in a <see cref="List{T}"/>
/// in process memory with no file-system dependencies, making it suitable for:
/// <list type="bullet">
///   <item>Ephemeral (non-persistent) embedded databases</item>
///   <item>Unit and integration tests that must not touch the file system</item>
///   <item>Browser-hosted .NET WASM applications (as a foundation before a full IndexedDB/OPFS backend)</item>
/// </list>
/// <para>
/// Durability guarantees are relaxed: records survive the current process lifetime only.
/// If the process exits, any uncommitted or un-checkpointed data is lost.
/// </para>
/// </summary>
public sealed class MemoryWriteAheadLog : IWriteAheadLog
{
    private readonly List<WalRecord> _records = new();
    private long _sizeBytes;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly int _writeTimeoutMs;
    private bool _disposed;

    /// <summary>
    /// Initialises a new in-memory WAL.
    /// </summary>
    /// <param name="writeTimeoutMs">Timeout in milliseconds for acquiring the internal lock.</param>
    public MemoryWriteAheadLog(int writeTimeoutMs = 5_000)
    {
        _writeTimeoutMs = writeTimeoutMs;
    }

    /// <inheritdoc/>
    public async ValueTask WriteBeginRecordAsync(ulong transactionId, CancellationToken ct = default)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct).ConfigureAwait(false))
            throw new TimeoutException("Timed out acquiring MemoryWriteAheadLog lock.");
        try
        {
            _records.Add(new WalRecord { Type = WalRecordType.Begin, TransactionId = transactionId, Timestamp = CurrentTimestampMs() });
            _sizeBytes += 17; // same fixed size as the file-based implementation
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask WriteCommitRecordAsync(ulong transactionId, CancellationToken ct = default)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct).ConfigureAwait(false))
            throw new TimeoutException("Timed out acquiring MemoryWriteAheadLog lock.");
        try
        {
            _records.Add(new WalRecord { Type = WalRecordType.Commit, TransactionId = transactionId, Timestamp = CurrentTimestampMs() });
            _sizeBytes += 17;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask WriteAbortRecordAsync(ulong transactionId, CancellationToken ct = default)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct).ConfigureAwait(false))
            throw new TimeoutException("Timed out acquiring MemoryWriteAheadLog lock.");
        try
        {
            _records.Add(new WalRecord { Type = WalRecordType.Abort, TransactionId = transactionId, Timestamp = CurrentTimestampMs() });
            _sizeBytes += 17;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask WriteDataRecordAsync(ulong transactionId, uint pageId, ReadOnlyMemory<byte> afterImage, CancellationToken ct = default)
    {
        if (!await _lock.WaitAsync(_writeTimeoutMs, ct).ConfigureAwait(false))
            throw new TimeoutException("Timed out acquiring MemoryWriteAheadLog lock.");
        try
        {
            _records.Add(new WalRecord
            {
                Type = WalRecordType.Write,
                TransactionId = transactionId,
                PageId = pageId,
                AfterImage = afterImage.ToArray()
            });
            _sizeBytes += 17 + afterImage.Length;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>No-op: in-memory WAL has no durable destination to flush to.</summary>
    public Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;

    /// <inheritdoc/>
    public long GetCurrentSize()
    {
        if (!_lock.Wait(_writeTimeoutMs))
            throw new TimeoutException("Timed out acquiring MemoryWriteAheadLog lock.");
        try
        {
            return _sizeBytes;
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
            throw new TimeoutException("Timed out acquiring MemoryWriteAheadLog lock.");
        try
        {
            _records.Clear();
            _sizeBytes = 0;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc/>
    public List<WalRecord> ReadAll()
    {
        if (!_lock.Wait(_writeTimeoutMs))
            throw new TimeoutException("Timed out acquiring MemoryWriteAheadLog lock.");
        try
        {
            return new List<WalRecord>(_records);
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
        if (_lock.Wait(_writeTimeoutMs))
        {
            try
            {
                _records.Clear();
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
            _records.Clear();
            _disposed = true;
            _lock.Dispose();
        }
        GC.SuppressFinalize(this);
    }

    private static long CurrentTimestampMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}
