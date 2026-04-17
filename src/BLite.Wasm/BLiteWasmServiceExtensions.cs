using System;
using BLite.Core;
using BLite.Core.KeyValue;
using Microsoft.Extensions.DependencyInjection;

namespace BLite.Wasm;

/// <summary>
/// Extension methods for registering BLite WASM services with the Blazor DI container.
/// </summary>
public static class BLiteWasmServiceExtensions
{
    /// <summary>
    /// Registers a <see cref="BLiteEngine"/> backed by browser storage (OPFS, IndexedDB, or in-memory)
    /// as a singleton service in the Blazor WASM dependency injection container.
    /// <para>
    /// The engine is initialised asynchronously on first resolution. By default, the factory
    /// auto-selects the best available backend (OPFS → IndexedDB → in-memory).
    /// </para>
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="dbName">Logical database name.</param>
    /// <param name="backend">Storage backend to use. Defaults to <see cref="WasmStorageBackend.Auto"/>.</param>
    /// <param name="pageSize">Page size in bytes. Defaults to 16 KB.</param>
    /// <param name="kvOptions">Optional Key-Value store configuration.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddBLiteWasm(
        this IServiceCollection services,
        string dbName,
        WasmStorageBackend backend = WasmStorageBackend.Auto,
        int pageSize = 16384,
        BLiteKvOptions? kvOptions = null)
    {
        if (string.IsNullOrWhiteSpace(dbName))
            throw new ArgumentException("Database name must not be null or empty.", nameof(dbName));

        // Register a factory that lazily creates the engine on first use.
        services.AddSingleton(sp =>
        {
            // In Blazor WASM, singleton factory delegates are called on the main thread.
            // BLiteWasm.CreateAsync().GetAwaiter().GetResult() works because the WASM
            // runtime uses cooperative (not pre-emptive) scheduling.
            return BLiteWasm.CreateAsync(dbName, backend, pageSize, kvOptions)
                .GetAwaiter().GetResult();
        });

        return services;
    }
}
