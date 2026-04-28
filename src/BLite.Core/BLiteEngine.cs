using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.KeyValue;
using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace BLite.Core;

/// <summary>
/// Schema-less database engine for dynamic/server mode.
/// Sits alongside <see cref="DocumentDbContext"/> as an equally valid alternative:
/// <list type="bullet">
///   <item><see cref="DocumentDbContext"/> — embedded path: compile-time types, Source Generators, LINQ, IDocumentMapper&lt;TId, T&gt;</item>
///   <item><see cref="BLiteEngine"/> — server/dynamic path: schema-less, <see cref="BsonDocument"/>, <see cref="BsonId"/>, no generics</item>
/// </list>
/// Both share the same kernel: <see cref="StorageEngine"/>, BTreeIndex, C-BSON, WAL.
/// </summary>
public sealed class BLiteEngine : IDisposable, ITransactionHolder
{
    private readonly StorageEngine _storage;
    private readonly string? _databasePath;
    private readonly ConcurrentDictionary<string, DynamicCollection> _collections = new(StringComparer.OrdinalIgnoreCase);
    private readonly FreeSpaceIndexProvider _freeSpaceIndexes;
    private readonly BLiteKvStore _kvStore;
    private bool _disposed;

    public event Action<BackupStartedEvent>? BackupStarted;
    public event Action<BackupCompletedEvent>? BackupCompleted;

    /// <summary>
    /// Exposes the underlying storage engine to session instances created by this engine.
    /// </summary>
    internal StorageEngine Storage => _storage;

    #region Constructors

    /// <summary>
    /// Creates a new BLiteEngine opening or creating a database at the given path.
    /// Auto-detects the page size for existing files; uses <see cref="PageFileConfig.Default"/> for new databases.
    /// </summary>
    /// <param name="databasePath">Path to the database file</param>
    public BLiteEngine(string databasePath)
        : this(databasePath, PageFileConfig.DetectFromFile(databasePath) ?? PageFileConfig.Default, null)
    {
    }

    /// <summary>
    /// Creates a new BLiteEngine with custom page configuration.
    /// </summary>
    public BLiteEngine(string databasePath, PageFileConfig config)
        : this(databasePath, config, null)
    {
    }

    /// <summary>
    /// Creates a new BLiteEngine with custom Key-Value store options.
    /// </summary>
    public BLiteEngine(string databasePath, BLiteKvOptions kvOptions)
        : this(databasePath, PageFileConfig.Default, kvOptions)
    {
    }

    /// <summary>
    /// Creates a new BLiteEngine with custom page and Key-Value store configuration.
    /// </summary>
    public BLiteEngine(string databasePath, PageFileConfig config, BLiteKvOptions? kvOptions)
    {
        if (string.IsNullOrWhiteSpace(databasePath))
            throw new ArgumentNullException(nameof(databasePath));

        _databasePath = databasePath;
        _storage = new StorageEngine(databasePath, config);
        _freeSpaceIndexes = new FreeSpaceIndexProvider(_storage);
        _kvStore = new BLiteKvStore(_storage, kvOptions);
    }

    /// <summary>
    /// Internal constructor used by <see cref="CreateInMemory"/> and other factory methods
    /// that supply a pre-built <see cref="StorageEngine"/>.
    /// </summary>
    internal BLiteEngine(StorageEngine storage, BLiteKvOptions? kvOptions = null)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _databasePath = null;
        _freeSpaceIndexes = new FreeSpaceIndexProvider(_storage);
        _kvStore = new BLiteKvStore(_storage, kvOptions);
    }

    /// <summary>
    /// Creates a fully in-memory <see cref="BLiteEngine"/> with no file-system dependencies.
    /// All data is stored in process memory and is lost when the engine is disposed or the
    /// process exits.
    /// <para>
    /// This mode is ideal for:
    /// <list type="bullet">
    ///   <item>Unit and integration tests that should not touch the file system.</item>
    ///   <item>Ephemeral caches or temporary working sets.</item>
    ///   <item>Browser-hosted .NET WASM applications, as a foundation before a full
    ///         IndexedDB/OPFS backend is available.</item>
    /// </list>
    /// </para>
    /// </summary>
    /// <param name="pageSize">
    /// Page size in bytes. Defaults to <see cref="PageFileConfig.Default"/> (16 KB).
    /// Use <see cref="PageFileConfig.Small"/> (8 KB) for workloads with many small documents.
    /// </param>
    /// <param name="kvOptions">Optional Key-Value store configuration.</param>
    public static BLiteEngine CreateInMemory(int pageSize = 16384, BLiteKvOptions? kvOptions = null)
    {
        var pageStorage = new MemoryPageStorage(pageSize);
        pageStorage.Open();
        var wal = new MemoryWriteAheadLog();
        var storageEngine = new StorageEngine(pageStorage, wal);
        return new BLiteEngine(storageEngine, kvOptions);
    }

    /// <summary>
    /// Creates a <see cref="BLiteEngine"/> from a pre-built <see cref="StorageEngine"/>.
    /// <para>
    /// This factory is intended for advanced scenarios (such as WASM browser backends)
    /// where the storage engine is assembled externally from custom
    /// <see cref="IPageStorage"/> and <see cref="IWriteAheadLog"/> implementations.
    /// </para>
    /// <para>
    /// The caller is responsible for opening the page storage and WAL before
    /// constructing the <see cref="StorageEngine"/>.
    /// </para>
    /// </summary>
    /// <param name="storage">A fully initialised <see cref="StorageEngine"/>.</param>
    /// <param name="kvOptions">Optional Key-Value store configuration.</param>
    /// <returns>A new <see cref="BLiteEngine"/> backed by the supplied storage engine.</returns>
    public static BLiteEngine CreateFromStorage(StorageEngine storage, BLiteKvOptions? kvOptions = null)
    {
        return new BLiteEngine(storage, kvOptions);
    }

    #endregion

    #region Session Management

    /// <summary>
    /// Opens a new <see cref="BLiteSession"/> against this engine.
    /// <para>
    /// Each session carries its own isolated transaction context, so multiple sessions can
    /// execute concurrent transactions against the same database — the typical pattern for
    /// server-mode usage where one <see cref="BLiteEngine"/> is shared across many
    /// client connections.
    /// </para>
    /// <para>
    /// The caller is responsible for disposing the session when the client disconnects.
    /// Disposing a session rolls back any uncommitted transaction automatically.
    /// </para>
    /// </summary>
    /// <returns>A new <see cref="BLiteSession"/> backed by this engine's storage.</returns>
    public BLiteSession OpenSession()
    {
        ThrowIfDisposed();
        return new BLiteSession(_storage, _freeSpaceIndexes);
    }

    #endregion

    #region Key-Value Store

    /// <summary>
    /// Provides access to the embedded Key-Value store that shares the same database file.
    /// </summary>
    public IBLiteKvStore KvStore
    {
        get { ThrowIfDisposed(); return _kvStore; }
    }

    #endregion

    #region Collection Management

    /// <summary>
    /// Gets or creates a dynamic collection by name.
    /// </summary>
    /// <param name="name">The collection name</param>
    /// <param name="idType">The ID type to use for new collections (default: ObjectId)</param>
    /// <returns>A <see cref="DynamicCollection"/> instance</returns>
    public DynamicCollection GetOrCreateCollection(string name, BsonIdType idType = BsonIdType.ObjectId)
    {
        ThrowIfDisposed();
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentNullException(nameof(name));

        return _collections.GetOrAdd(name, n => new DynamicCollection(_storage, this, n, idType, _freeSpaceIndexes.GetIndex()));
    }

    /// <summary>
    /// Gets a dynamic collection by name, returning null if it doesn't exist.
    /// </summary>
    public DynamicCollection? GetCollection(string name)
    {
        ThrowIfDisposed();
        return _collections.TryGetValue(name, out var collection) ? collection : null;
    }

    /// <summary>
    /// Drops a collection and removes it from the engine.
    /// Note: this removes the in-memory reference. Physical page cleanup is deferred.
    /// </summary>
    public bool DropCollection(string name)
    {
        ThrowIfDisposed();
        if (_collections.TryRemove(name, out var collection))
        {
            collection.Dispose();
            _storage.DeleteCollectionMetadata(name);
            return true;
        }
        return false;
    }

    /// <summary>
    /// Lists all collection names, including those persisted on disk but not yet
    /// loaded in memory. Discovers storage-level collections via the page catalog,
    /// then returns the union of in-memory and on-disk names.
    /// </summary>
    public IReadOnlyList<string> ListCollections()
    {
        ThrowIfDisposed();

        // Warm up: open any collection that is in the storage catalog but not yet
        // registered in the in-memory dictionary.  DynamicCollection reads its
        // own metadata from storage, so the idType argument is irrelevant for
        // existing collections (only matters when creating brand-new ones).
        foreach (var meta in _storage.GetAllCollectionsMetadata())
            _collections.GetOrAdd(meta.Name, n => new DynamicCollection(_storage, this, n, BsonIdType.ObjectId, _freeSpaceIndexes.GetIndex()));

        return _collections.Keys.ToList();
    }

    /// <summary>
    /// Returns storage metadata for the named collection, including index definitions
    /// and TimeSeries settings. Returns <c>null</c> if the collection does not exist.
    /// </summary>
    public CollectionMetadata? GetCollectionMetadata(string name)
    {
        ThrowIfDisposed();
        return _storage.GetCollectionMetadata(name);
    }

    /// <summary>
    /// Returns storage metadata for all collections in the database.
    /// </summary>
    public IReadOnlyList<CollectionMetadata> GetAllCollectionsMetadata()
    {
        ThrowIfDisposed();
        return _storage.GetAllCollectionsMetadata();
    }

    #endregion

    #region Transactions

    /// <summary>
    /// Creates a new caller-owned transaction.
    /// The caller must pass this transaction to every collection method and
    /// call <see cref="ITransaction.CommitAsync"/> to commit.
    /// </summary>
    public ITransaction BeginTransaction()
    {
        ThrowIfDisposed();
        return _storage.BeginTransaction();
    }

    /// <summary>
    /// Creates a new caller-owned transaction asynchronously.
    /// </summary>
    public ValueTask<ITransaction> BeginTransactionAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ct.ThrowIfCancellationRequested();
        return new ValueTask<ITransaction>(_storage.BeginTransaction(IsolationLevel.ReadCommitted));
    }

    /// <summary>
    /// No-op when called without a transaction.
    /// Exists for API compatibility — auto-commit write operations commit immediately.
    /// </summary>
    public ValueTask CommitAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();
        foreach (var col in _collections.Values)
            col.PersistIndexMetadata();
        return default;
    }

    /// <summary>
    /// No-op. Exists for API compatibility.
    /// Use <see cref="ITransaction.RollbackAsync"/> on a caller-owned transaction instead.
    /// </summary>
    public void Rollback()
    {
        ThrowIfDisposed();
    }

    /// <summary>
    /// Forces an immediate checkpoint: merges all committed WAL records into the main data file.
    /// Call this before disposing the engine when you need to guarantee that the page file is
    /// fully up-to-date on disk (e.g., before renaming or copying the database files).
    /// </summary>
    public async Task CheckpointAsync()
    {
        ThrowIfDisposed();
        await _storage.CheckpointAsync();
    }

    #endregion

    #region ITransactionHolder

    /// <summary>
    /// Creates an ephemeral read transaction for internal use by collection read paths.
    /// Write paths create their own auto-commit transactions when no caller-owned tx is provided.
    /// </summary>
    ValueTask<ITransaction> ITransactionHolder.GetCurrentTransactionOrStartAsync()
    {
        return new ValueTask<ITransaction>(_storage.BeginTransaction(IsolationLevel.ReadCommitted));
    }

    #endregion

    #region Backup

    /// <summary>
    /// Creates a consistent, hot backup of this database to <paramref name="destinationDbPath"/>.
    /// The engine does not need to be stopped or paused; concurrent reads and writes are safe.
    /// <para>
    /// The method checkpoints the WAL first, then copies the main database file plus any
    /// companion WAL, collection, and index files that belong to the same database.
    /// A <c>backup.manifest.json</c> file is written alongside the backup with per-file SHA-256 hashes.
    /// The destination database is a standalone, fully consistent backup that can be opened
    /// with a fresh <see cref="BLiteEngine"/> instance.
    /// </para>
    /// </summary>
    /// <param name="destinationDbPath">Full path to the target .db file.</param>
    public async Task BackupAsync(string destinationDbPath, CancellationToken ct = default)
    {
        await BackupAsync(new BackupOptions { DestinationPath = destinationDbPath }, ct);
    }

    /// <summary>
    /// Creates a consistent, hot backup of this database using the supplied <paramref name="options"/>.
    /// </summary>
    public async Task<BackupResult> BackupAsync(BackupOptions options, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        if (options == null)
            throw new ArgumentNullException(nameof(options));

        var destinationDbPath = ResolveBackupDestinationPath(options);
        var startedAt = DateTimeOffset.UtcNow;
        InvokeHandlers(BackupStarted, new BackupStartedEvent(destinationDbPath, options, startedAt));

        var sw = Stopwatch.StartNew();
        var stats = await _storage.BackupDetailedAsync(destinationDbPath, options.IncludeIndexes, ct);
        sw.Stop();

        var result = new BackupResult(destinationDbPath, stats.ManifestPath, sw.Elapsed, stats.FileCount, stats.TotalBytes);
        InvokeHandlers(BackupCompleted, new BackupCompletedEvent(result, options, DateTimeOffset.UtcNow));
        return result;
    }

    #endregion

    #region Convenience CRUD (collection + auto-commit)

    /// <summary>
    /// Creates a BsonDocument using the engine's key dictionary.
    /// Field names are automatically registered in the C-BSON key map.
    /// </summary>
    /// <param name="fieldNames">All field names that will be used in the document</param>
    /// <param name="buildAction">Builder action to populate the document</param>
    /// <returns>A new BsonDocument ready for insertion</returns>
    public BsonDocument CreateDocument(string[] fieldNames, Action<BsonDocumentBuilder> buildAction)
    {
        ThrowIfDisposed();
        _storage.RegisterKeys(fieldNames);
        return BsonDocument.Create(_storage.GetFrozenKeyMap(), _storage.GetKeyReverseMap(), buildAction);
    }

    /// <summary>
    /// Registers all supplied field names in the global C-BSON key dictionary,
    /// assigning a persistent <see cref="ushort"/> ID to each new name.
    /// Already-known names are left unchanged. Thread-safe.
    /// </summary>
    public void RegisterKeys(IEnumerable<string> keys)
    {
        ThrowIfDisposed();
        _storage.RegisterKeys(keys);
    }

    /// <summary>
    /// Returns the global field-name → ushort ID forward map.
    /// This is the source-of-truth for C-BSON serialization.
    /// </summary>
    public IReadOnlyDictionary<string, ushort> GetKeyMap()
    {
        ThrowIfDisposed();
        return _storage.GetKeyMap();
    }

    /// <summary>
    /// Returns the global ushort ID → field-name reverse map.
    /// Used for deserialization (reading C-BSON bytes back into <see cref="BsonDocument"/>).
    /// </summary>
    public IReadOnlyDictionary<ushort, string> GetKeyReverseMap()
    {
        ThrowIfDisposed();
        return _storage.GetKeyReverseMap();
    }

    /// <summary>
    /// Imports key→ID entries from <paramref name="sourceReverseMap"/> into this engine's
    /// C-BSON key dictionary, preserving the original <see cref="ushort"/> IDs.
    /// Entries already present (by name or ID) are silently skipped.
    /// <para>
    /// This method is <c>internal</c> and intended for cross-layout migration
    /// (<see cref="BLiteMigration"/>) to ensure raw BSON bytes from the source engine
    /// are decodable by the target without re-serialisation.
    /// </para>
    /// </summary>
    internal void ImportDictionary(IReadOnlyDictionary<ushort, string> sourceReverseMap)
    {
        ThrowIfDisposed();
        _storage.ImportDictionary(sourceReverseMap);
    }

    private string ResolveBackupDestinationPath(BackupOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.DestinationPath))
            return options.DestinationPath!;

        if (string.IsNullOrWhiteSpace(options.DestinationPathPattern))
            throw new ArgumentException("BackupOptions must specify DestinationPath or DestinationPathPattern.", nameof(options));

        if (string.IsNullOrWhiteSpace(_databasePath))
            throw new NotSupportedException("DestinationPathPattern requires a file-based BLiteEngine.");

        return options.DestinationPathPattern!
            .Replace("{databaseName}", Path.GetFileNameWithoutExtension(_databasePath), StringComparison.Ordinal)
            .Replace("{databasePath}", _databasePath, StringComparison.Ordinal)
            .Replace("{timestampUtc}", DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss"), StringComparison.Ordinal);
    }

    private static void InvokeHandlers<T>(Action<T>? handlers, T args)
    {
        if (handlers == null)
            return;

        foreach (Action<T> handler in handlers.GetInvocationList())
        {
            try { handler(args); }
            catch { }
        }
    }

    /// <summary>Async exact-match lookup in the named collection.</summary>
    public ValueTask<BsonDocument?> FindByIdAsync(string collectionName, BsonId id, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        return collection.FindByIdAsync(id, ct);
    }

    /// <summary>Async full-collection scan in the named collection.</summary>
    public IAsyncEnumerable<BsonDocument> FindAllAsync(string collectionName, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        return collection.FindAllAsync(ct);
    }

    /// <summary>
    /// Inserts a document and auto-commits asynchronously.
    /// </summary>
    public async ValueTask<BsonId> InsertAsync(string collectionName, BsonDocument document, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        return await collection.InsertAsync(document, null, ct);
    }

    /// <summary>Updates a document and auto-commits asynchronously.</summary>
    public async ValueTask<bool> UpdateAsync(string collectionName, BsonId id, BsonDocument document, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        return await collection.UpdateAsync(id, document, null, ct);
    }

    /// <summary>Deletes a document and auto-commits asynchronously.</summary>
    public async ValueTask<bool> DeleteAsync(string collectionName, BsonId id, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        return await collection.DeleteAsync(id, null, ct);
    }

    /// <summary>
    /// Inserts multiple documents asynchronously and auto-commits.
    /// Returns the list of generated/existing BsonIds in insertion order.
    /// </summary>
    public async Task<List<BsonId>> InsertBulkAsync(string collectionName, IEnumerable<BsonDocument> documents, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        return await collection.InsertBulkAsync(documents, null, ct);
    }

    /// <summary>Asynchronously yields documents in the named collection matching the specified predicate.</summary>
    public IAsyncEnumerable<BsonDocument> FindAsync(string collectionName, Func<BsonDocument, bool> predicate, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        return collection.FindAsync(predicate, ct);
    }

    /// <summary>
    /// Updates multiple documents asynchronously and auto-commits.
    /// Returns the number of documents successfully updated.
    /// </summary>
    public async Task<int> UpdateBulkAsync(string collectionName, IEnumerable<(BsonId Id, BsonDocument Document)> updates, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        return await collection.UpdateBulkAsync(updates, null, ct);
    }

    /// <summary>
    /// Deletes multiple documents asynchronously and auto-commits.
    /// Returns the number of documents successfully deleted.
    /// </summary>
    public async Task<int> DeleteBulkAsync(string collectionName, IEnumerable<BsonId> ids, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        return await collection.DeleteBulkAsync(ids, null, ct);
    }

    #endregion

    #region VectorSource Configuration

    /// <summary>
    /// Gets the VectorSource configuration for the named collection, or null if not configured.
    /// </summary>
    public VectorSourceConfig? GetVectorSource(string collectionName)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        return collection.GetVectorSource();
    }

    /// <summary>Returns typed descriptors for all secondary indexes of the named collection.</summary>
    public IReadOnlyList<DynamicIndexDescriptor> GetIndexDescriptors(string collectionName)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        return collection.GetIndexDescriptors();
    }

    /// <summary>
    /// Sets or updates the VectorSource configuration for the named collection.
    /// Pass null to clear the configuration.
    /// </summary>
    public void SetVectorSource(string collectionName, VectorSourceConfig? config)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        collection.SetVectorSource(config);
    }

    #endregion

    #region Schema

    /// <summary>Returns all persisted schema versions for the named collection. Latest version is last.</summary>
    public IReadOnlyList<BsonSchema> GetSchemas(string collectionName)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        return collection.GetSchemas();
    }

    /// <summary>Appends a new schema version to the named collection.</summary>
    public void SetSchema(string collectionName, BsonSchema schema)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        collection.SetSchema(schema);
    }

    #endregion

    #region TimeSeries Configuration

    /// <summary>
    /// Configures the named collection as a TimeSeries with a retention policy.
    /// This operation is irreversible once documents have been written.
    /// </summary>
    public void SetTimeSeries(string collectionName, string ttlFieldName, TimeSpan retentionPolicy)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        collection.SetTimeSeries(ttlFieldName, retentionPolicy);
    }

    /// <summary>
    /// Returns the TimeSeries configuration for the named collection.
    /// </summary>
    public (bool IsTimeSeries, long RetentionPolicyMs, string? TtlFieldName) GetTimeSeriesConfig(string collectionName)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        if (!collection.IsTimeSeries) return (false, 0, null);
        var (retentionMs, ttlField) = collection.GetTimeSeriesConfig();
        return (true, retentionMs, ttlField);
    }

    #endregion

    #region CDC

    /// <summary>
    /// Subscribes to post-commit CDC events for <paramref name="collectionName"/>.
    /// The <paramref name="writer"/> receives one entry per document written after each successful commit.
    /// Only <see cref="OperationType.Insert"/> and <see cref="OperationType.Update"/> events are forwarded.
    /// Returns an <see cref="IDisposable"/> — dispose it to unsubscribe.
    /// </summary>
    /// <remarks>
    /// <c>capturePayload</c> is intentionally <c>false</c>: callers receive the ID only.
    /// The document is fetched from storage at processing time to avoid stale in-memory payloads.
    /// </remarks>
    public IDisposable SubscribeToChanges(
        string collectionName,
        System.Threading.Channels.ChannelWriter<(OperationType Op, BsonId Id)> writer)
    {
        ThrowIfDisposed();
        var dispatcher = _storage.EnsureCdc();

        var adapterChannel = System.Threading.Channels.Channel.CreateUnbounded<CDC.InternalChangeEvent>();
        var subscription = dispatcher.Subscribe(collectionName, capturePayload: false, adapterChannel.Writer);

        // Adapter pump: translate InternalChangeEvent → (OperationType, BsonId)
        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var e in adapterChannel.Reader.ReadAllAsync())
                {
                    if (e.Type is OperationType.Insert or OperationType.Update)
                    {
                        var id = BsonId.FromBytes(e.IdBytes.Span, e.IdType);
                        writer.TryWrite((e.Type, id));
                    }
                }
            }
            catch (System.OperationCanceledException) { }
        });

        return subscription;
    }

    #endregion

    #region Metrics

    /// <summary>
    /// Enables the metrics subsystem. After this call, the engine starts collecting
    /// performance counters that can be read with <see cref="GetMetrics"/> or streamed
    /// with <see cref="WatchMetrics"/>.
    /// <para>
    /// Calling this method more than once is safe — subsequent calls are no-ops.
    /// </para>
    /// </summary>
    /// <param name="options">
    /// Optional configuration. Pass <c>null</c> to use <see cref="MetricsOptions.Default"/>.
    /// </param>
    public void EnableMetrics(Metrics.MetricsOptions? options = null)
    {
        ThrowIfDisposed();
        _storage.EnsureMetrics();
    }

    /// <summary>
    /// Returns an immutable point-in-time snapshot of the accumulated performance counters.
    /// Returns <c>null</c> if <see cref="EnableMetrics"/> has not been called.
    /// </summary>
    public Metrics.MetricsSnapshot? GetMetrics()
    {
        ThrowIfDisposed();
        return _storage.MetricsDispatcher?.GetSnapshot();
    }

    /// <summary>
    /// Returns an <see cref="IObservable{T}"/> that pushes a <see cref="Metrics.MetricsSnapshot"/>
    /// at the requested <paramref name="interval"/>.
    /// <para>
    /// <see cref="EnableMetrics"/> must be called first. If the metrics subsystem has not been
    /// enabled, this method enables it automatically.
    /// </para>
    /// </summary>
    /// <param name="interval">
    /// Sampling interval. Defaults to 1 second when <c>null</c>.
    /// </param>
    public IObservable<Metrics.MetricsSnapshot> WatchMetrics(TimeSpan? interval = null)
    {
        ThrowIfDisposed();
        var dispatcher = _storage.EnsureMetrics();
        return new Metrics.BLiteMetricsObservable(dispatcher, interval ?? TimeSpan.FromSeconds(1));
    }

    #endregion

    #region Disposal

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(BLiteEngine));
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        foreach (var collection in _collections.Values)
            collection.Dispose();
        _collections.Clear();
        _storage.Dispose();

        GC.SuppressFinalize(this);
    }

    #endregion
}
