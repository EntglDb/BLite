using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BLite.Bson;
using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace BLite.Core;

/// <summary>
/// Represents an isolated client session backed by a shared <see cref="BLiteEngine"/>.
/// <para>
/// In server mode, the BLite.Server layer creates one <see cref="BLiteEngine"/> per database
/// and one <see cref="BLiteSession"/> per connected client via
/// <see cref="BLiteEngine.OpenSession"/>. Each session carries its own transaction context so
/// multiple clients can run independent, concurrent transactions against the same database.
/// </para>
/// <para>
/// In single-process embedded mode the <see cref="BLiteEngine"/> itself serves as both engine
/// and implicit session; <see cref="BLiteSession"/> is only required when the kernel is shared
/// across several independent callers.
/// </para>
/// </summary>
public sealed class BLiteSession : ITransactionHolder, IDisposable
{
    private readonly StorageEngine _storage;
    private readonly ConcurrentDictionary<string, Lazy<DynamicCollection>> _collections =
        new(StringComparer.OrdinalIgnoreCase);
    private ITransaction? _currentTransaction;
    private bool _disposed;

    internal BLiteSession(StorageEngine storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Current transaction
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Gets the current transaction for this session, or <c>null</c> when no transaction is
    /// active.
    /// </summary>
    public ITransaction? CurrentTransaction
    {
        get
        {
            ThrowIfDisposed();
            return _currentTransaction != null && _currentTransaction.State == TransactionState.Active
                ? _currentTransaction : null;
        }
        private set => _currentTransaction = value;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Transaction management
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Begins a new transaction for this session, or returns the already-active one.
    /// </summary>
    public ITransaction BeginTransaction()
    {
        ThrowIfDisposed();
        if (CurrentTransaction != null)
            return CurrentTransaction;
        CurrentTransaction = _storage.BeginTransaction();
        return CurrentTransaction!;
    }

    /// <summary>
    /// Begins a new transaction asynchronously for this session, or returns the already-active one.
    /// </summary>
    public async Task<ITransaction> BeginTransactionAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();
        if (CurrentTransaction != null)
            return CurrentTransaction;
        CurrentTransaction = _storage.BeginTransaction(IsolationLevel.ReadCommitted);
        return CurrentTransaction!;
    }

    /// <summary>
    /// Asynchronously commits the active transaction.
    /// </summary>
    public async Task CommitAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();
        foreach (var lazy in _collections.Values)
            lazy.Value.PersistIndexMetadata();
        if (CurrentTransaction != null)
        {
            try { await CurrentTransaction.CommitAsync(ct); }
            finally { CurrentTransaction = null; }
        }
    }

    /// <summary>
    /// Rolls back the active transaction, discarding all uncommitted changes.
    /// </summary>
    public void Rollback()
    {
        ThrowIfDisposed();
        if (CurrentTransaction != null)
        {
            try { CurrentTransaction.RollbackAsync(); }
            finally { CurrentTransaction = null; }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ITransactionHolder
    // ─────────────────────────────────────────────────────────────────────────

    ValueTask<ITransaction> ITransactionHolder.GetCurrentTransactionOrStartAsync()
    {
        var current = CurrentTransaction;
        if (current != null)
            return new ValueTask<ITransaction>(current);

        // Slow path: no active transaction yet — start one synchronously (no real I/O).
        CurrentTransaction = _storage.BeginTransaction(IsolationLevel.ReadCommitted);
        return new ValueTask<ITransaction>(CurrentTransaction!);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Collection management
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Gets or creates a dynamic collection by name. The returned instance is scoped to this
    /// session — its operations use this session's transaction context.
    /// </summary>
    public DynamicCollection GetOrCreateCollection(string name, BsonIdType idType = BsonIdType.ObjectId)
    {
        ThrowIfDisposed();
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentNullException(nameof(name));

        return _collections.GetOrAdd(name,
            n => new Lazy<DynamicCollection>(
                () => new DynamicCollection(_storage, this, n, idType),
                System.Threading.LazyThreadSafetyMode.ExecutionAndPublication)).Value;
    }

    /// <summary>
    /// Gets a dynamic collection by name if it was previously opened in this session,
    /// returning <c>null</c> otherwise.
    /// </summary>
    public DynamicCollection? GetCollection(string name)
    {
        ThrowIfDisposed();
        return _collections.TryGetValue(name, out var lazy) ? lazy.Value : null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Convenience CRUD — collection + auto-commit
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>Inserts a document and commits immediately.</summary>

    /// <summary>Inserts a document and commits asynchronously.</summary>
    public async ValueTask<BsonId> InsertAsync(
        string collectionName, BsonDocument document, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var col = GetOrCreateCollection(collectionName);
        var id = await col.InsertAsync(document, CurrentTransaction, ct);
        if (CurrentTransaction == null) await CommitAsync(ct);
        return id;
    }

    /// <summary>Inserts multiple documents and commits asynchronously.</summary>
    public async Task<List<BsonId>> InsertBulkAsync(
        string collectionName, IEnumerable<BsonDocument> documents, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var col = GetOrCreateCollection(collectionName);
        var ids = await col.InsertBulkAsync(documents, CurrentTransaction, ct);
        if (CurrentTransaction == null) await CommitAsync(ct);
        return ids;
    }

    /// <summary>Finds a document by ID asynchronously.</summary>
    public ValueTask<BsonDocument?> FindByIdAsync(
        string collectionName, BsonId id, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        return GetOrCreateCollection(collectionName).FindByIdAsync(id, ct);
    }

    /// <summary>Asynchronously streams all documents in the named collection.</summary>
    public IAsyncEnumerable<BsonDocument> FindAllAsync(
        string collectionName, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        return GetOrCreateCollection(collectionName).FindAllAsync(ct);
    }

    /// <summary>Asynchronously yields documents matching the predicate.</summary>
    public IAsyncEnumerable<BsonDocument> FindAsync(
        string collectionName, Func<BsonDocument, bool> predicate, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        return GetOrCreateCollection(collectionName).FindAsync(predicate, ct);
    }

    /// <summary>Updates a document and commits asynchronously.</summary>
    public async ValueTask<bool> UpdateAsync(
        string collectionName, BsonId id, BsonDocument document, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var col = GetOrCreateCollection(collectionName);
        var result = await col.UpdateAsync(id, document, CurrentTransaction, ct);
        if (CurrentTransaction == null) await CommitAsync(ct);
        return result;
    }

    /// <summary>Updates multiple documents and commits asynchronously.</summary>
    public async Task<int> UpdateBulkAsync(
        string collectionName,
        IEnumerable<(BsonId Id, BsonDocument Document)> updates,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var col = GetOrCreateCollection(collectionName);
        var count = await col.UpdateBulkAsync(updates, CurrentTransaction, ct);
        if (CurrentTransaction == null) await CommitAsync(ct);
        return count;
    }

    /// <summary>Deletes a document and commits asynchronously.</summary>
    public async ValueTask<bool> DeleteAsync(
        string collectionName, BsonId id, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var col = GetOrCreateCollection(collectionName);
        var result = await col.DeleteAsync(id, CurrentTransaction, ct);
        if (CurrentTransaction == null) await CommitAsync(ct);
        return result;
    }

    /// <summary>Deletes multiple documents and commits asynchronously.</summary>
    public async Task<int> DeleteBulkAsync(
        string collectionName, IEnumerable<BsonId> ids, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var col = GetOrCreateCollection(collectionName);
        var count = await col.DeleteBulkAsync(ids, CurrentTransaction, ct);
        if (CurrentTransaction == null) await CommitAsync(ct);
        return count;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Disposal
    // ─────────────────────────────────────────────────────────────────────────

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(BLiteSession));
    }

    /// <summary>
    /// Disposes this session. Any uncommitted transaction is rolled back automatically.
    /// The underlying <see cref="BLiteEngine"/> (and its storage) is <b>not</b> affected.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // Roll back any uncommitted transaction so the engine's WAL stays clean.
        if (_currentTransaction?.State == TransactionState.Active)
        {
            try { _currentTransaction.RollbackAsync(); }
            catch { /* best-effort */ }
        }
        _currentTransaction = null;

        foreach (var lazy in _collections.Values)
        {
            try { lazy.Value.Dispose(); }
            catch { /* best-effort */ }
        }
        _collections.Clear();

        GC.SuppressFinalize(this);
    }
}
