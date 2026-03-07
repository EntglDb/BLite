namespace BLite.Core.KeyValue;

/// <summary>
/// A persistent, page-backed Key-Value store embedded in the same BLite database file.
/// Keys are UTF-8 strings (≤ 255 bytes). Values are arbitrary byte payloads.
/// Supports optional per-entry TTL with lazy expiry (expired entries are removed on first
/// access and cleaned up in bulk via <see cref="PurgeExpired"/>).
/// </summary>
public interface IBLiteKvStore
{
    /// <returns>The value bytes, or <c>null</c> if the key is absent or has expired.</returns>
    byte[]? Get(string key);

    /// <summary>
    /// Sets the value for <paramref name="key"/>. Overwrites any existing entry.
    /// </summary>
    /// <param name="key">UTF-8 key (≤ 255 bytes).</param>
    /// <param name="value">Raw payload.</param>
    /// <param name="ttl">Optional time-to-live. <c>null</c> or <see cref="TimeSpan.Zero"/> means no expiry.</param>
    void Set(string key, ReadOnlySpan<byte> value, TimeSpan? ttl = null);

    /// <summary>Removes a key. Returns <c>true</c> if the key existed.</summary>
    bool Delete(string key);

    /// <returns><c>true</c> if the key exists and has not expired.</returns>
    bool Exists(string key);

    /// <summary>
    /// Extends the TTL of an existing key without changing its value.
    /// Returns <c>false</c> if the key does not exist or has already expired.
    /// </summary>
    bool Refresh(string key, TimeSpan ttl);

    /// <summary>
    /// Enumerates keys that start with <paramref name="prefix"/> (ordinal, case-sensitive).
    /// Pass an empty string to enumerate all keys.
    /// </summary>
    IEnumerable<string> ScanKeys(string prefix = "");

    /// <summary>
    /// Removes all soft-deleted and expired entries from disk pages and rebuilds the in-memory index.
    /// Returns the number of entries that were purged.
    /// Call this periodically to reclaim disk space.
    /// </summary>
    int PurgeExpired();

    /// <summary>Returns a new batch builder for this store.</summary>
    KvBatch Batch();
}
