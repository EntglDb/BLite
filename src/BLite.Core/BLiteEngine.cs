using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BLite.Bson;
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
    private readonly ConcurrentDictionary<string, DynamicCollection> _collections = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _transactionLock = new(1, 1);
    private bool _disposed;

    /// <summary>
    /// Gets or sets the current transaction. Returns null if no active transaction exists.
    /// </summary>
    public ITransaction? CurrentTransaction
    {
        get
        {
            ThrowIfDisposed();
            return field != null && field.State == TransactionState.Active ? field : null;
        }
        private set;
    }

    #region Constructors

    /// <summary>
    /// Creates a new BLiteEngine opening or creating a database at the given path.
    /// </summary>
    /// <param name="databasePath">Path to the database file</param>
    public BLiteEngine(string databasePath)
        : this(databasePath, PageFileConfig.Default)
    {
    }

    /// <summary>
    /// Creates a new BLiteEngine with custom page configuration.
    /// </summary>
    /// <param name="databasePath">Path to the database file</param>
    /// <param name="config">Page file configuration</param>
    public BLiteEngine(string databasePath, PageFileConfig config)
    {
        if (string.IsNullOrWhiteSpace(databasePath))
            throw new ArgumentNullException(nameof(databasePath));

        _storage = new StorageEngine(databasePath, config);
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

        return _collections.GetOrAdd(name, n => new DynamicCollection(_storage, this, n, idType));
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
            _collections.GetOrAdd(meta.Name, n => new DynamicCollection(_storage, this, n));

        return _collections.Keys.ToList();
    }

    #endregion

    #region Transactions

    /// <summary>
    /// Begins a new transaction or returns the current one.
    /// </summary>
    public ITransaction BeginTransaction()
    {
        ThrowIfDisposed();
        _transactionLock.Wait();
        try
        {
            if (CurrentTransaction != null)
                return CurrentTransaction;
            CurrentTransaction = _storage.BeginTransaction();
            return CurrentTransaction;
        }
        finally
        {
            _transactionLock.Release();
        }
    }

    /// <summary>
    /// Begins a new transaction asynchronously or returns the current one.
    /// </summary>
    public async Task<ITransaction> BeginTransactionAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();
        bool lockAcquired = false;
        try
        {
            await _transactionLock.WaitAsync(ct);
            lockAcquired = true;

            if (CurrentTransaction != null)
                return CurrentTransaction;
            CurrentTransaction = await _storage.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
            return CurrentTransaction;
        }
        finally
        {
            if (lockAcquired)
                _transactionLock.Release();
        }
    }

    /// <summary>
    /// Commits the current transaction, making all changes permanent.
    /// </summary>
    public void Commit()
    {
        ThrowIfDisposed();
        // Flush current root-page IDs into collection metadata before committing,
        // so vector/spatial indexes survive a reopen.
        foreach (var col in _collections.Values)
            col.PersistIndexMetadata();
        if (CurrentTransaction != null)
        {
            try
            {
                CurrentTransaction.Commit();
            }
            finally
            {
                CurrentTransaction = null;
            }
        }
    }

    /// <summary>
    /// Commits the current transaction asynchronously.
    /// </summary>
    public async Task CommitAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();
        foreach (var col in _collections.Values)
            col.PersistIndexMetadata();
        if (CurrentTransaction != null)
        {
            try
            {
                await CurrentTransaction.CommitAsync(ct);
            }
            finally
            {
                CurrentTransaction = null;
            }
        }
    }

    /// <summary>
    /// Rolls back the current transaction, discarding all changes.
    /// </summary>
    public void Rollback()
    {
        ThrowIfDisposed();
        if (CurrentTransaction != null)
        {
            try
            {
                CurrentTransaction.Rollback();
            }
            finally
            {
                CurrentTransaction = null;
            }
        }
    }

    #endregion

    #region ITransactionHolder

    ITransaction ITransactionHolder.GetCurrentTransactionOrStart() => BeginTransaction();

    Task<ITransaction> ITransactionHolder.GetCurrentTransactionOrStartAsync() => BeginTransactionAsync();

    #endregion

    #region Backup

    /// <summary>
    /// Creates a consistent, hot backup of this database to <paramref name="destinationDbPath"/>.
    /// The engine does not need to be stopped or paused; concurrent reads and writes are safe.
    /// <para>
    /// The method checkpoints the WAL into the PageFile first, then copies the resulting file.
    /// The destination file is a standalone, fully consistent database that can be opened
    /// with a fresh <see cref="BLiteEngine"/> instance.
    /// </para>
    /// </summary>
    /// <param name="destinationDbPath">Full path to the target .db file.</param>
    public async Task BackupAsync(string destinationDbPath, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationDbPath);
        await _storage.BackupAsync(destinationDbPath, ct).ConfigureAwait(false);
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
        return BsonDocument.Create(_storage.GetKeyMap(), _storage.GetKeyReverseMap(), buildAction);
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
    /// Inserts a document into the named collection and commits immediately.
    /// </summary>
    public BsonId Insert(string collectionName, BsonDocument document)
    {
        var collection = GetOrCreateCollection(collectionName);
        var id = collection.Insert(document);
        Commit();
        return id;
    }

    /// <summary>
    /// Finds a document by ID in the named collection.
    /// </summary>
    public BsonDocument? FindById(string collectionName, BsonId id)
    {
        var collection = GetOrCreateCollection(collectionName);
        return collection.FindById(id);
    }

    /// <summary>
    /// Returns all documents in the named collection.
    /// </summary>
    public IEnumerable<BsonDocument> FindAll(string collectionName)
    {
        var collection = GetOrCreateCollection(collectionName);
        return collection.FindAll();
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
    /// Inserts a document and commits asynchronously.
    /// The BTree write stays in-memory (WAL cache); only the WAL flush is async.
    /// </summary>
    public async ValueTask<BsonId> InsertAsync(string collectionName, BsonDocument document, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        var id = collection.Insert(document);
        await CommitAsync(ct).ConfigureAwait(false);
        return id;
    }

    /// <summary>Updates a document and commits asynchronously.</summary>
    public async ValueTask<bool> UpdateAsync(string collectionName, BsonId id, BsonDocument document, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        var result = collection.Update(id, document);
        await CommitAsync(ct).ConfigureAwait(false);
        return result;
    }

    /// <summary>Deletes a document and commits asynchronously.</summary>
    public async ValueTask<bool> DeleteAsync(string collectionName, BsonId id, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        var result = collection.Delete(id);
        await CommitAsync(ct).ConfigureAwait(false);
        return result;
    }

    /// <summary>
    /// Updates a document by ID in the named collection and commits immediately.
    /// </summary>
    public bool Update(string collectionName, BsonId id, BsonDocument document)
    {
        var collection = GetOrCreateCollection(collectionName);
        var result = collection.Update(id, document);
        Commit();
        return result;
    }

    /// <summary>
    /// Deletes a document by ID from the named collection and commits immediately.
    /// </summary>
    public bool Delete(string collectionName, BsonId id)
    {
        var collection = GetOrCreateCollection(collectionName);
        var result = collection.Delete(id);
        Commit();
        return result;
    }

    /// <summary>
    /// Inserts multiple documents and commits immediately.
    /// Returns the list of generated/existing BsonIds in insertion order.
    /// </summary>
    public List<BsonId> InsertBulk(string collectionName, IEnumerable<BsonDocument> documents)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        var ids = collection.InsertBulk(documents);
        Commit();
        return ids;
    }

    /// <summary>
    /// Inserts multiple documents asynchronously and commits.
    /// Returns the list of generated/existing BsonIds in insertion order.
    /// </summary>
    public async Task<List<BsonId>> InsertBulkAsync(string collectionName, IEnumerable<BsonDocument> documents, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        var ids = await collection.InsertBulkAsync(documents, ct).ConfigureAwait(false);
        await CommitAsync(ct).ConfigureAwait(false);
        return ids;
    }

    /// <summary>Returns documents in the named collection matching the specified predicate.</summary>
    public IEnumerable<BsonDocument> Find(string collectionName, Func<BsonDocument, bool> predicate)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        return collection.Find(predicate);
    }

    /// <summary>Asynchronously yields documents in the named collection matching the specified predicate.</summary>
    public IAsyncEnumerable<BsonDocument> FindAsync(string collectionName, Func<BsonDocument, bool> predicate, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        return collection.FindAsync(predicate, ct);
    }

    /// <summary>
    /// Updates multiple documents and commits immediately.
    /// Returns the number of documents successfully updated.
    /// </summary>
    public int UpdateBulk(string collectionName, IEnumerable<(BsonId Id, BsonDocument Document)> updates)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        var count = collection.UpdateBulk(updates);
        Commit();
        return count;
    }

    /// <summary>
    /// Updates multiple documents asynchronously and commits.
    /// Returns the number of documents successfully updated.
    /// </summary>
    public async Task<int> UpdateBulkAsync(string collectionName, IEnumerable<(BsonId Id, BsonDocument Document)> updates, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        var count = await collection.UpdateBulkAsync(updates, ct).ConfigureAwait(false);
        await CommitAsync(ct).ConfigureAwait(false);
        return count;
    }

    /// <summary>
    /// Deletes multiple documents and commits immediately.
    /// Returns the number of documents successfully deleted.
    /// </summary>
    public int DeleteBulk(string collectionName, IEnumerable<BsonId> ids)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        var count = collection.DeleteBulk(ids);
        Commit();
        return count;
    }

    /// <summary>
    /// Deletes multiple documents asynchronously and commits.
    /// Returns the number of documents successfully deleted.
    /// </summary>
    public async Task<int> DeleteBulkAsync(string collectionName, IEnumerable<BsonId> ids, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var collection = GetOrCreateCollection(collectionName);
        var count = await collection.DeleteBulkAsync(ids, ct).ConfigureAwait(false);
        await CommitAsync(ct).ConfigureAwait(false);
        return count;
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

        _transactionLock.Dispose();
        GC.SuppressFinalize(this);
    }

    #endregion
}
