using System;
using System.Threading.Tasks;
using BLite.Core;
using BLite.Core.KeyValue;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Wasm.Storage;
using BLite.Wasm.Transactions;

namespace BLite.Wasm;

/// <summary>
/// Supported browser storage backends.
/// </summary>
public enum WasmStorageBackend
{
    /// <summary>
    /// Automatically select the best available backend:
    /// OPFS if available (Worker context), otherwise IndexedDB.
    /// </summary>
    Auto,

    /// <summary>
    /// Origin Private File System — highest throughput, requires a Worker thread context.
    /// Supported in Chrome 102+, Firefox 111+, Safari 15.2+.
    /// </summary>
    Opfs,

    /// <summary>
    /// IndexedDB — universally supported, works on the main thread.
    /// Lower throughput than OPFS but maximum compatibility.
    /// </summary>
    IndexedDb,

    /// <summary>
    /// In-memory only — no persistence. Data is lost when the tab closes.
    /// </summary>
    InMemory
}

/// <summary>
/// Factory for creating a <see cref="BLiteEngine"/> in a browser WASM environment.
/// <para>
/// Auto-selects the best available storage backend (OPFS → IndexedDB → in-memory)
/// or allows explicit backend selection.
/// </para>
/// </summary>
public static class BLiteWasm
{
    /// <summary>
    /// Creates a <see cref="BLiteEngine"/> backed by persistent browser storage.
    /// <para>
    /// By default (<paramref name="backend"/> = <see cref="WasmStorageBackend.Auto"/>),
    /// the factory probes for OPFS availability and falls back to IndexedDB.
    /// </para>
    /// </summary>
    /// <param name="dbName">Logical database name (used as OPFS file name or IndexedDB database name).</param>
    /// <param name="backend">Storage backend to use. Defaults to <see cref="WasmStorageBackend.Auto"/>.</param>
    /// <param name="pageSize">Page size in bytes. Defaults to 16 KB.</param>
    /// <param name="kvOptions">Optional Key-Value store configuration.</param>
    /// <returns>A fully initialised <see cref="BLiteEngine"/>.</returns>
    public static async Task<BLiteEngine> CreateAsync(
        string dbName,
        WasmStorageBackend backend = WasmStorageBackend.Auto,
        int pageSize = 16384,
        BLiteKvOptions? kvOptions = null)
    {
        if (string.IsNullOrWhiteSpace(dbName))
            throw new ArgumentException("Database name must not be null or empty.", nameof(dbName));

        var resolvedBackend = backend == WasmStorageBackend.Auto
            ? DetectBestBackend()
            : backend;

        IPageStorage pageStorage;
        IWriteAheadLog wal;

        switch (resolvedBackend)
        {
            case WasmStorageBackend.Opfs:
                var opfsStorage = new OpfsPageStorage(dbName, pageSize);
                await opfsStorage.OpenAsync();
                var opfsWal = new OpfsWriteAheadLog(dbName);
                await opfsWal.OpenAsync();
                pageStorage = opfsStorage;
                wal = opfsWal;
                break;

            case WasmStorageBackend.IndexedDb:
                var idbStorage = new IndexedDbPageStorage(dbName, pageSize);
                await idbStorage.OpenAsync();
                var idbWal = new IndexedDbWriteAheadLog(dbName);
                await idbWal.OpenAsync();
                pageStorage = idbStorage;
                wal = idbWal;
                break;

            case WasmStorageBackend.InMemory:
                var memStorage = new MemoryPageStorage(pageSize);
                memStorage.Open();
                pageStorage = memStorage;
                wal = new MemoryWriteAheadLog();
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(backend), backend, "Unknown storage backend.");
        }

        var storageEngine = new StorageEngine(pageStorage, wal);
        return BLiteEngine.CreateFromStorage(storageEngine, kvOptions);
    }

    /// <summary>
    /// Detects the best available storage backend in the current browser context.
    /// </summary>
    public static WasmStorageBackend DetectBestBackend()
    {
        try
        {
            if (OpfsPageStorage.IsAvailable())
                return WasmStorageBackend.Opfs;
        }
        catch
        {
            // OPFS detection failed — fall through.
        }

        try
        {
            if (IndexedDbPageStorage.IsAvailable())
                return WasmStorageBackend.IndexedDb;
        }
        catch
        {
            // IndexedDB detection failed — fall through.
        }

        return WasmStorageBackend.InMemory;
    }
}
