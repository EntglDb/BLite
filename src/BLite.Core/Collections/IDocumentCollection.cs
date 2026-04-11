using BLite.Bson;
using BLite.Core.CDC;
using BLite.Core.Indexing;
using BLite.Core.Query;
using BLite.Core.Transactions;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core.Collections;

/// <summary>
/// Abstraction over a typed document collection.
/// Implemented by <see cref="DocumentCollection{TId,T}"/> (local embedded engine)
/// and <c>RemoteDocumentCollection&lt;TId,T&gt;</c> (BLite.Client remote transport).
///
/// Infrastructure methods (<c>CreateIndexAsync</c>, <c>Scan</c>, <c>ForcePruneAsync</c>, etc.)
/// are present on the interface to preserve a consistent compile-time surface; remote
/// implementations that do not support a given operation throw
/// <see cref="NotSupportedException"/> at runtime.
/// </summary>
public interface IDocumentCollection<TId, T> where T : class
{
    // ── Metadata ──────────────────────────────────────────────────────────────

    SchemaVersion? CurrentSchemaVersion { get; }

    // ── Insert ────────────────────────────────────────────────────────────────

    ValueTask<TId> InsertAsync(T entity, CancellationToken ct = default);
    ValueTask<TId> InsertAsync(T entity, ITransaction? transaction, CancellationToken ct = default);

    ValueTask<List<TId>> InsertBulkAsync(IEnumerable<T> entities, CancellationToken ct = default);
    ValueTask<List<TId>> InsertBulkAsync(IEnumerable<T> entities, ITransaction? transaction, CancellationToken ct = default);

    // ── Read ──────────────────────────────────────────────────────────────────

    ValueTask<T?> FindByIdAsync(TId id, CancellationToken ct = default);
    ValueTask<T?> FindByIdAsync(TId id, ITransaction? transaction, CancellationToken ct = default);

    IAsyncEnumerable<T> FindAllAsync(CancellationToken ct = default);
    IAsyncEnumerable<T> FindAllAsync(ITransaction? transaction, CancellationToken ct = default);

    [RequiresDynamicCode("LINQ-style find operations use Expression.Compile() and index optimization which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ-style find operations use reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    IAsyncEnumerable<T> FindAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default);

    [RequiresDynamicCode("LINQ-style find operations use Expression.Compile() and index optimization which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ-style find operations use reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    IAsyncEnumerable<T> FindAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction, CancellationToken ct = default);

    /// <summary>
    /// Returns the first document matching <paramref name="predicate"/>, or <c>null</c> if none.
    /// Stops reading from the storage engine as soon as one document is found.
    /// </summary>
    [RequiresDynamicCode("LINQ-style find operations use Expression.Compile() and index optimization which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ-style find operations use reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    Task<T?> FindOneAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default);

    /// <inheritdoc cref="FindOneAsync(Expression{Func{T, bool}}, CancellationToken)"/>
    [RequiresDynamicCode("LINQ-style find operations use Expression.Compile() and index optimization which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ-style find operations use reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    Task<T?> FindOneAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction, CancellationToken ct = default);

    [RequiresDynamicCode("LINQ queries over BLite collections use Expression.Compile() and MakeGenericMethod which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ queries over BLite collections use reflection to resolve methods at runtime. Ensure all entity types and their members are preserved.")]
    IBLiteQueryable<T> AsQueryable();

    // ── Update ────────────────────────────────────────────────────────────────

    ValueTask<bool> UpdateAsync(T entity, CancellationToken ct = default);
    ValueTask<bool> UpdateAsync(T entity, ITransaction? transaction, CancellationToken ct = default);

    ValueTask<int> UpdateBulkAsync(IEnumerable<T> entities, CancellationToken ct = default);
    ValueTask<int> UpdateBulkAsync(IEnumerable<T> entities, ITransaction? transaction, CancellationToken ct = default);

    // ── Delete ────────────────────────────────────────────────────────────────

    ValueTask<bool> DeleteAsync(TId id, CancellationToken ct = default);
    ValueTask<bool> DeleteAsync(TId id, ITransaction? transaction, CancellationToken ct = default);

    ValueTask<int> DeleteBulkAsync(IEnumerable<TId> ids, CancellationToken ct = default);
    ValueTask<int> DeleteBulkAsync(IEnumerable<TId> ids, ITransaction? transaction, CancellationToken ct = default);

    // ── Index management ──────────────────────────────────────────────────────
    // Local engine: fully supported.
    // Remote (BLite.Client): CreateIndexAsync/Drop/List supported; Scan/ForcePruneAsync throw NotSupportedException.

    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    Task<ICollectionIndex<TId, T>> CreateIndexAsync<TKey>(
        Expression<Func<T, TKey>> keySelector,
        string? name = null,
        bool unique = false,
        CancellationToken ct = default);

    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    Task<ICollectionIndex<TId, T>> CreateVectorIndexAsync<TKey>(
        Expression<Func<T, TKey>> keySelector,
        int dimensions,
        VectorMetric metric = VectorMetric.Cosine,
        string? name = null,
        CancellationToken ct = default);

    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    Task<ICollectionIndex<TId, T>> EnsureIndexAsync<TKey>(
        Expression<Func<T, TKey>> keySelector,
        string? name = null,
        bool unique = false,
        CancellationToken ct = default);

    Task<bool> DropIndexAsync(string name, CancellationToken ct = default);

    IEnumerable<CollectionIndexInfo> GetIndexes();

    Task<ICollectionIndex<TId, T>?> GetIndexAsync(string name);

    IAsyncEnumerable<T> QueryIndexAsync(
        string indexName,
        object? minKey,
        object? maxKey,
        bool ascending = true,
        int skip = 0,
        int take = int.MaxValue,
        CancellationToken ct = default);

    IAsyncEnumerable<T> QueryIndexAsync(
        string indexName,
        object? minKey,
        object? maxKey,
        bool ascending,
        int skip,
        int take,
        ITransaction? transaction,
        CancellationToken ct = default);

    // ── Raw scan (local engine only; remote throws NotSupportedException) ──────

    IAsyncEnumerable<T> ScanAsync(BsonReaderPredicate predicate, CancellationToken ct = default);
    IAsyncEnumerable<T> ScanAsync(BsonReaderPredicate predicate, ITransaction? transaction, CancellationToken ct = default);

    /// <summary>
    /// Executes a query described by an <see cref="IndexQueryPlan"/>.
    /// If the plan targets an index, the collection's B-Tree is scanned between the plan's
    /// min and max keys. If the plan is a full-scan fallback (index absent or plan was built
    /// with <see cref="IndexQueryPlan.Scan"/>), every document is evaluated against the
    /// plan's <c>ScanPredicate</c>.
    ///
    /// An optional <see cref="IndexQueryPlan.ResiduePredicate"/> (attached via
    /// <see cref="IndexQueryPlan.And"/>) is applied as a BSON-level post-filter on every
    /// document returned by the index or scan before yielding it to the caller.
    /// </summary>
    IAsyncEnumerable<T> ScanAsync(IndexQueryPlan plan, CancellationToken ct = default);

    IAsyncEnumerable<TResult> ScanAsync<TResult>(
        BsonReaderProjector<TResult> projector,
        CancellationToken ct = default);

    IAsyncEnumerable<T> ParallelScanAsync(
        BsonReaderPredicate predicate,
        int degreeOfParallelism = -1,
        CancellationToken ct = default);

    // ── TimeSeries (local engine only; remote throws NotSupportedException) ───

    Task ForcePruneAsync();

    // ── Change Data Capture (local engine only; remote throws NotSupportedException) ───

    IObservable<ChangeStreamEvent<TId, T>> Watch(bool capturePayload = false);
}

/// <summary>
/// Non-generic convenience alias for collections keyed by <see cref="ObjectId"/>.
/// </summary>
public interface IDocumentCollection<T> : IDocumentCollection<ObjectId, T> where T : class { }
