using BLite.Core;
using BLite.Core.KeyValue;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace BLite.Caching;

/// <summary>
/// Extension methods for registering BLite as an <see cref="IDistributedCache"/> provider.
/// </summary>
public static class BLiteCachingExtensions
{
    /// <summary>
    /// Registers BLite as the <see cref="IDistributedCache"/> and <see cref="IBLiteCache"/> provider.
    /// A dedicated <see cref="BLiteEngine"/> manages the cache database and is disposed with the DI container.
    /// </summary>
    public static IServiceCollection AddBLiteDistributedCache(
        this IServiceCollection services,
        Action<BLiteDistributedCacheOptions> configure)
    {
        services.Configure(configure);

        // BLiteEngine is IDisposable; the DI container disposes it on app shutdown.
        services.AddSingleton(sp =>
        {
            var opts = sp.GetRequiredService<IOptions<BLiteDistributedCacheOptions>>().Value;
            return new BLiteEngine(opts.DatabasePath, opts.KvOptions ?? BLiteKvOptions.Default);
        });

        services.AddSingleton<IBLiteKvStore>(sp =>
            sp.GetRequiredService<BLiteEngine>().KvStore);

        services.AddSingleton<IBLiteCache, BLiteDistributedCache>();
        services.AddSingleton<IDistributedCache>(sp =>
            sp.GetRequiredService<IBLiteCache>());

        return services;
    }

    /// <summary>
    /// Registers BLite as the <see cref="IDistributedCache"/> provider using the specified database path.
    /// </summary>
    public static IServiceCollection AddBLiteDistributedCache(
        this IServiceCollection services,
        string databasePath,
        BLiteKvOptions? kvOptions = null)
        => services.AddBLiteDistributedCache(opts =>
        {
            opts.DatabasePath  = databasePath;
            opts.KvOptions     = kvOptions;
        });
}
