using BLite.Core.Storage;

namespace BLite.Core.KeyValue;

/// <summary>
/// Default implementation of <see cref="IBLiteKvStore"/> backed by <see cref="StorageEngine"/>.
/// </summary>
internal sealed class BLiteKvStore : IBLiteKvStore
{
    private readonly StorageEngine _storage;
    private readonly BLiteKvOptions _options;

    internal BLiteKvStore(StorageEngine storage, BLiteKvOptions? options = null)
    {
        _storage = storage;
        _options = options ?? BLiteKvOptions.Default;
        if (_options.PurgeExpiredOnOpen)
            _storage.KvPurgeExpired();
    }

    /// <inheritdoc/>
    public byte[]? Get(string key) => _storage.KvGet(key);

    /// <summary>
    /// Retrieves both the value and the raw expiry timestamp (UTC ticks) for
    /// <paramref name="key"/>. Returns <c>(null, 0)</c> if the key is absent or has
    /// expired. An <c>ExpiryTicks</c> value of <c>0</c> means the entry has no expiry.
    /// </summary>
    /// <remarks>
    /// This method is <c>internal</c> and intended for engine-level operations such as
    /// cross-layout migration, where preserving the original expiry is necessary.
    /// </remarks>
    internal (byte[]? Value, long ExpiryTicks) GetWithExpiry(string key)
        => _storage.KvGetWithExpiry(key);

    /// <inheritdoc/>
    public void Set(string key, ReadOnlySpan<byte> value, TimeSpan? ttl = null)
    {
        var effectiveTtl = ttl ?? _options.DefaultTtl;
        long expiryTicks = effectiveTtl is { } t && t > TimeSpan.Zero
            ? DateTime.UtcNow.Add(t).Ticks
            : 0;
        _storage.KvSet(key, value, expiryTicks);
    }

    /// <inheritdoc/>
    public bool Delete(string key) => _storage.KvDelete(key);

    /// <inheritdoc/>
    public bool Exists(string key) => _storage.KvExists(key);

    /// <inheritdoc/>
    public bool Refresh(string key, TimeSpan ttl)
    {
        long newExpiry = DateTime.UtcNow.Add(ttl).Ticks;
        return _storage.KvRefresh(key, newExpiry);
    }

    /// <inheritdoc/>
    public IEnumerable<string> ScanKeys(string prefix = "") =>
        _storage.KvScanKeys(prefix);

    /// <inheritdoc/>
    public int PurgeExpired() => _storage.KvPurgeExpired();

    /// <inheritdoc/>
    public KvBatch Batch() => new(_storage, _options);
}
