using BLite.Bson;
using BLite.Core.CDC;
using BLite.Core.Indexing;
using BLite.Core.Query;
using System;
using System.Collections.Generic;
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

    Task<TId> InsertAsync(T entity, CancellationToken ct = default);

    Task<List<TId>> InsertBulkAsync(IEnumerable<T> entities, CancellationToken ct = default);

    // ── Read ──────────────────────────────────────────────────────────────────

    ValueTask<T?> FindByIdAsync(TId id, CancellationToken ct = default);

    IAsyncEnumerable<T> FindAllAsync(CancellationToken ct = default);

    IAsyncEnumerable<T> FindAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default);

    /// <summary>
    /// Returns the first document matching <paramref name="predicate"/>, or <c>null</c> if none.
    /// Stops reading from the storage engine as soon as one document is found.
    /// </summary>
    Task<T?> FindOneAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default);

    IBLiteQueryable<T> AsQueryable();

    // ── UpdateAsync ────────────────────────────────────────────────────────────────

    Task<bool> UpdateAsync(T entity, CancellationToken ct = default);

    Task<int> UpdateBulkAsync(IEnumerable<T> entities, CancellationToken ct = default);

    // ── Delete ────────────────────────────────────────────────────────────────

    Task<bool> DeleteAsync(TId id, CancellationToken ct = default);

    Task<int> DeleteBulkAsync(IEnumerable<TId> ids, CancellationToken ct = default);

    // ── Index management ──────────────────────────────────────────────────────
    // Local engine: fully supported.
    // Remote (BLite.Client): CreateIndexAsync/Drop/List supported; Scan/ForcePruneAsync throw NotSupportedException.

    Task<ICollectionIndex<TId, T>> CreateIndexAsync<TKey>(
        Expression<Func<T, TKey>> keySelector,
        string? name = null,
        bool unique = false,
        CancellationToken ct = default);

    Task<ICollectionIndex<TId, T>> CreateVectorIndexAsync<TKey>(
        Expression<Func<T, TKey>> keySelector,
        int dimensions,
        VectorMetric metric = VectorMetric.Cosine,
        string? name = null,
        CancellationToken ct = default);

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
        CancellationToken ct = default);

    // ── Raw scan (local engine only; remote throws NotSupportedException) ──────

    IAsyncEnumerable<T> ScanAsync(BsonReaderPredicate predicate, CancellationToken ct = default);

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
