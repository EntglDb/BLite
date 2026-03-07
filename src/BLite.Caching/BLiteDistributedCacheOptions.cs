using BLite.Core.KeyValue;

namespace BLite.Caching;

/// <summary>
/// Options for the BLite-backed distributed cache registration.
/// </summary>
public sealed class BLiteDistributedCacheOptions
{
    /// <summary>Path to the BLite database file used as the cache store. Default: <c>blite-cache.db</c>.</summary>
    public string DatabasePath { get; set; } = "blite-cache.db";

    /// <summary>Key-Value store options (default TTL, purge-on-open behaviour).</summary>
    public BLiteKvOptions? KvOptions { get; set; }
}
