using BLite.Bson;
using BLite.Core.CDC;
using BLite.Core.Indexing;
using BLite.Core.Query;
using BLite.Core.Transactions;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core.Collections;

/// <summary>
/// A sentinel collection that throws <see cref="InvalidOperationException"/> on every method call.
/// Installed by <see cref="DocumentDbContext"/> after <c>DropCollectionAsync&lt;T&gt;</c> completes,
/// so that strongly-typed properties on the context give a clear error rather than a
/// <see cref="NullReferenceException"/>.
/// </summary>
internal sealed class DroppedCollectionProxy<TId, T> : IDocumentCollection<TId, T>
    where T : class
{
    private readonly string _collectionName;

    public DroppedCollectionProxy(string collectionName)
    {
        _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
    }

    private InvalidOperationException Dropped([CallerMemberName] string? member = null)
        => new($"Collection '{_collectionName}' has been dropped.");

    // ── Metadata ──────────────────────────────────────────────────────────────

    public SchemaVersion? CurrentSchemaVersion => throw Dropped();

    // ── Insert ────────────────────────────────────────────────────────────────

    public ValueTask<TId> InsertAsync(T entity, CancellationToken ct = default) => throw Dropped();
    public ValueTask<TId> InsertAsync(T entity, ITransaction? transaction, CancellationToken ct = default) => throw Dropped();
    public ValueTask<List<TId>> InsertBulkAsync(IEnumerable<T> entities, CancellationToken ct = default) => throw Dropped();
    public ValueTask<List<TId>> InsertBulkAsync(IEnumerable<T> entities, ITransaction? transaction, CancellationToken ct = default) => throw Dropped();

    // ── Read ──────────────────────────────────────────────────────────────────

    public ValueTask<T?> FindByIdAsync(TId id, CancellationToken ct = default) => throw Dropped();
    public ValueTask<T?> FindByIdAsync(TId id, ITransaction? transaction, CancellationToken ct = default) => throw Dropped();

    public IAsyncEnumerable<T> FindAllAsync(CancellationToken ct = default) => throw Dropped();
    public IAsyncEnumerable<T> FindAllAsync(ITransaction? transaction, CancellationToken ct = default) => throw Dropped();

    [RequiresDynamicCode("LINQ-style find operations use Expression.Compile() and index optimization which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ-style find operations use reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    public IAsyncEnumerable<T> FindAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default) => throw Dropped();

    [RequiresDynamicCode("LINQ-style find operations use Expression.Compile() and index optimization which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ-style find operations use reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    public IAsyncEnumerable<T> FindAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction, CancellationToken ct = default) => throw Dropped();

    [RequiresDynamicCode("LINQ-style find operations use Expression.Compile() and index optimization which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ-style find operations use reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    public Task<T?> FindOneAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default) => throw Dropped();

    [RequiresDynamicCode("LINQ-style find operations use Expression.Compile() and index optimization which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ-style find operations use reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    public Task<T?> FindOneAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction, CancellationToken ct = default) => throw Dropped();

    [RequiresDynamicCode("LINQ queries over BLite collections use Expression.Compile() and MakeGenericMethod which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ queries over BLite collections use reflection to resolve methods at runtime. Ensure all entity types and their members are preserved.")]
    public IBLiteQueryable<T> AsQueryable() => throw Dropped();

    // ── Update ────────────────────────────────────────────────────────────────

    public ValueTask<bool> UpdateAsync(T entity, CancellationToken ct = default) => throw Dropped();
    public ValueTask<bool> UpdateAsync(T entity, ITransaction? transaction, CancellationToken ct = default) => throw Dropped();
    public ValueTask<int> UpdateBulkAsync(IEnumerable<T> entities, CancellationToken ct = default) => throw Dropped();
    public ValueTask<int> UpdateBulkAsync(IEnumerable<T> entities, ITransaction? transaction, CancellationToken ct = default) => throw Dropped();

    // ── Delete ────────────────────────────────────────────────────────────────

    public ValueTask<bool> DeleteAsync(TId id, CancellationToken ct = default) => throw Dropped();
    public ValueTask<bool> DeleteAsync(TId id, ITransaction? transaction, CancellationToken ct = default) => throw Dropped();
    public ValueTask<int> DeleteBulkAsync(IEnumerable<TId> ids, CancellationToken ct = default) => throw Dropped();
    public ValueTask<int> DeleteBulkAsync(IEnumerable<TId> ids, ITransaction? transaction, CancellationToken ct = default) => throw Dropped();

    // ── Index management ──────────────────────────────────────────────────────

    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    public Task<ICollectionIndex<TId, T>> CreateIndexAsync<TKey>(Expression<Func<T, TKey>> keySelector, string? name = null, bool unique = false, CancellationToken ct = default) => throw Dropped();

    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    public Task<ICollectionIndex<TId, T>> CreateVectorIndexAsync<TKey>(Expression<Func<T, TKey>> keySelector, int dimensions, VectorMetric metric = VectorMetric.Cosine, string? name = null, CancellationToken ct = default) => throw Dropped();

    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    public Task<ICollectionIndex<TId, T>> EnsureIndexAsync<TKey>(Expression<Func<T, TKey>> keySelector, string? name = null, bool unique = false, CancellationToken ct = default) => throw Dropped();

    public Task<bool> DropIndexAsync(string name, CancellationToken ct = default) => throw Dropped();
    public IEnumerable<CollectionIndexInfo> GetIndexes() => throw Dropped();
    public Task<ICollectionIndex<TId, T>?> GetIndexAsync(string name) => throw Dropped();

    public IAsyncEnumerable<T> QueryIndexAsync(string indexName, object? minKey, object? maxKey, bool ascending = true, int skip = 0, int take = int.MaxValue, CancellationToken ct = default) => throw Dropped();
    public IAsyncEnumerable<T> QueryIndexAsync(string indexName, object? minKey, object? maxKey, bool ascending, int skip, int take, ITransaction? transaction, CancellationToken ct = default) => throw Dropped();

    // ── Raw scan ──────────────────────────────────────────────────────────────

    public IAsyncEnumerable<T> ScanAsync(BsonReaderPredicate predicate, CancellationToken ct = default) => throw Dropped();
    public IAsyncEnumerable<T> ScanAsync(BsonReaderPredicate predicate, ITransaction? transaction, CancellationToken ct = default) => throw Dropped();
    public IAsyncEnumerable<T> ScanAsync(IndexQueryPlan plan, CancellationToken ct = default) => throw Dropped();
    public IAsyncEnumerable<TResult> ScanAsync<TResult>(BsonReaderProjector<TResult> projector, CancellationToken ct = default) => throw Dropped();
    public IAsyncEnumerable<T> ParallelScanAsync(BsonReaderPredicate predicate, int degreeOfParallelism = -1, CancellationToken ct = default) => throw Dropped();

    // ── Truncate ──────────────────────────────────────────────────────────────

    public Task<int> TruncateAsync(CancellationToken ct = default) => throw Dropped();
    public Task<int> TruncateAsync(ITransaction? transaction, CancellationToken ct = default) => throw Dropped();

    // ── TimeSeries ────────────────────────────────────────────────────────────

    public Task ForcePruneAsync() => throw Dropped();

    // ── Vacuum / Secure Erase ─────────────────────────────────────────────────

    public Task VacuumAsync(VacuumOptions? options = null, CancellationToken ct = default) => throw Dropped();

    // ── Change Data Capture ───────────────────────────────────────────────────

    public IObservable<ChangeStreamEvent<TId, T>> Watch(bool capturePayload = false) => throw Dropped();
}
