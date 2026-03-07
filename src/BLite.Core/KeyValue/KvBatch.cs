using BLite.Core.Storage;

namespace BLite.Core.KeyValue;

/// <summary>
/// Fluent builder for batching multiple KV operations.
/// All queued operations execute atomically under a single write-lock acquisition
/// when <see cref="Execute"/> is called, and the batch is cleared automatically.
/// </summary>
public sealed class KvBatch
{
    private readonly List<KvBatchEntry> _ops = new();
    private readonly StorageEngine _storage;
    private readonly BLiteKvOptions _options;

    internal KvBatch(StorageEngine storage, BLiteKvOptions options)
    {
        _storage = storage;
        _options = options;
    }

    /// <summary>
    /// Queues a Set operation.
    /// </summary>
    /// <param name="key">UTF-8 key (≤ 255 bytes).</param>
    /// <param name="value">Raw payload. Must not be modified after calling this method.</param>
    /// <param name="ttl">
    /// Optional TTL. Falls back to <see cref="BLiteKvOptions.DefaultTtl"/> when <c>null</c>.
    /// <c>null</c> / <see cref="TimeSpan.Zero"/> means no expiry.
    /// </param>
    public KvBatch Set(string key, byte[] value, TimeSpan? ttl = null)
    {
        if (value is null) throw new ArgumentNullException(nameof(value));
        long expiryTicks = ComputeExpiry(ttl ?? _options.DefaultTtl);
        _ops.Add(new KvBatchEntry(key, value, expiryTicks));
        return this;
    }

    /// <summary>Queues a Delete operation.</summary>
    public KvBatch Delete(string key)
    {
        _ops.Add(new KvBatchEntry(key, null, 0));
        return this;
    }

    /// <summary>Number of pending operations.</summary>
    public int Count => _ops.Count;

    /// <summary>Clears all pending operations without executing them.</summary>
    public void Clear() => _ops.Clear();

    /// <summary>
    /// Executes all pending operations atomically and clears the batch.
    /// Returns the number of operations that had a visible effect
    /// (e.g. <c>Delete</c> on a missing key counts as 0).
    /// </summary>
    public int Execute()
    {
        if (_ops.Count == 0) return 0;
        var result = _storage.KvExecuteBatch(_ops);
        _ops.Clear();
        return result;
    }

    private static long ComputeExpiry(TimeSpan? ttl) =>
        ttl is { } t && t > TimeSpan.Zero ? DateTime.UtcNow.Add(t).Ticks : 0;
}
