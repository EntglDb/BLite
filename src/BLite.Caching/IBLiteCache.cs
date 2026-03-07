using Microsoft.Extensions.Caching.Distributed;

namespace BLite.Caching;

/// <summary>
/// Extends <see cref="IDistributedCache"/> with typed get/set and cache-aside helpers.
/// </summary>
public interface IBLiteCache : IDistributedCache
{
    /// <summary>Deserializes a JSON-encoded value, or returns <c>null</c> if absent / expired.</summary>
    T? Get<T>(string key) where T : class;

    /// <summary>Serializes <paramref name="value"/> as JSON and stores it.</summary>
    void Set<T>(string key, T value, DistributedCacheEntryOptions? options = null) where T : class;

    /// <inheritdoc cref="Get{T}(string)"/>
    Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default) where T : class;

    /// <inheritdoc cref="Set{T}(string, T, DistributedCacheEntryOptions)"/>
    Task SetAsync<T>(string key, T value, DistributedCacheEntryOptions? options = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Returns the cached value if present; otherwise invokes <paramref name="factory"/>,
    /// stores the result, and returns it.
    /// Uses a per-key semaphore to prevent thundering-herd scenarios.
    /// </summary>
    T GetOrSet<T>(string key, Func<T> factory, DistributedCacheEntryOptions? options = null) where T : class;

    /// <inheritdoc cref="GetOrSet{T}(string, Func{T}, DistributedCacheEntryOptions)"/>
    Task<T> GetOrSetAsync<T>(string key, Func<Task<T>> factory, DistributedCacheEntryOptions? options = null, CancellationToken cancellationToken = default) where T : class;
}
