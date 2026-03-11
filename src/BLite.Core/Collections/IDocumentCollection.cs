using BLite.Bson;
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
/// Infrastructure methods (<c>CreateIndex</c>, <c>Scan</c>, <c>ForcePrune</c>, etc.)
/// are present on the interface to preserve a consistent compile-time surface; remote
/// implementations that do not support a given operation throw
/// <see cref="NotSupportedException"/> at runtime.
/// </summary>
public interface IDocumentCollection<TId, T> where T : class
{
    // ── Metadata ──────────────────────────────────────────────────────────────

    SchemaVersion? CurrentSchemaVersion { get; }

    // ── Insert ────────────────────────────────────────────────────────────────

    TId Insert(T entity);

    Task<TId> InsertAsync(T entity, CancellationToken ct = default);

    List<TId> InsertBulk(IEnumerable<T> entities);

    Task<List<TId>> InsertBulkAsync(IEnumerable<T> entities, CancellationToken ct = default);

    // ── Read ──────────────────────────────────────────────────────────────────

    T? FindById(TId id);

    ValueTask<T?> FindByIdAsync(TId id, CancellationToken ct = default);

    IAsyncEnumerable<T> FindAllAsync(CancellationToken ct = default);

    IAsyncEnumerable<T> FindAsync(Func<T, bool> predicate, CancellationToken ct = default);

    IBLiteQueryable<T> AsQueryable();

    // ── Update ────────────────────────────────────────────────────────────────

    bool Update(T entity);

    Task<bool> UpdateAsync(T entity, CancellationToken ct = default);

    int UpdateBulk(IEnumerable<T> entities);

    Task<int> UpdateBulkAsync(IEnumerable<T> entities, CancellationToken ct = default);

    // ── Delete ────────────────────────────────────────────────────────────────

    bool Delete(TId id);

    Task<bool> DeleteAsync(TId id, CancellationToken ct = default);

    int DeleteBulk(IEnumerable<TId> ids);

    Task<int> DeleteBulkAsync(IEnumerable<TId> ids, CancellationToken ct = default);

    // ── Index management (local engine only; remote throws NotSupportedException) ──

    CollectionSecondaryIndex<TId, T> CreateIndex<TKey>(
        Expression<Func<T, TKey>> keySelector,
        string? name = null,
        bool unique = false);

    Task<CollectionSecondaryIndex<TId, T>> CreateIndexAsync<TKey>(
        Expression<Func<T, TKey>> keySelector,
        string? name = null,
        bool unique = false,
        CancellationToken ct = default);

    CollectionSecondaryIndex<TId, T> CreateVectorIndex<TKey>(
        Expression<Func<T, TKey>> keySelector,
        int dimensions,
        VectorMetric metric = VectorMetric.Cosine,
        string? name = null);

    Task<CollectionSecondaryIndex<TId, T>> CreateVectorIndexAsync<TKey>(
        Expression<Func<T, TKey>> keySelector,
        int dimensions,
        VectorMetric metric = VectorMetric.Cosine,
        string? name = null,
        CancellationToken ct = default);

    CollectionSecondaryIndex<TId, T> EnsureIndex<TKey>(
        Expression<Func<T, TKey>> keySelector,
        string? name = null,
        bool unique = false);

    Task<CollectionSecondaryIndex<TId, T>> EnsureIndexAsync<TKey>(
        Expression<Func<T, TKey>> keySelector,
        string? name = null,
        bool unique = false,
        CancellationToken ct = default);

    bool DropIndex(string name);

    Task<bool> DropIndexAsync(string name, CancellationToken ct = default);

    IEnumerable<CollectionIndexInfo> GetIndexes();

    CollectionSecondaryIndex<TId, T>? GetIndex(string name);

    IEnumerable<T> QueryIndex(string indexName, object? minKey, object? maxKey, bool ascending = true);

    IAsyncEnumerable<T> QueryIndexAsync(
        string indexName,
        object? minKey,
        object? maxKey,
        bool ascending = true,
        CancellationToken ct = default);

    // ── Raw scan (local engine only; remote throws NotSupportedException) ──────

    IEnumerable<T> Scan(BsonReaderPredicate predicate);

    IAsyncEnumerable<T> ScanAsync(BsonReaderPredicate predicate, CancellationToken ct = default);

    IEnumerable<TResult> Scan<TResult>(BsonReaderProjector<TResult> projector);

    IAsyncEnumerable<TResult> ScanAsync<TResult>(
        BsonReaderProjector<TResult> projector,
        CancellationToken ct = default);

    IEnumerable<T> ParallelScan(BsonReaderPredicate predicate, int degreeOfParallelism = -1);

    IAsyncEnumerable<T> ParallelScanAsync(
        BsonReaderPredicate predicate,
        int degreeOfParallelism = -1,
        CancellationToken ct = default);

    // ── TimeSeries (local engine only; remote throws NotSupportedException) ───

    void ForcePrune();
}

/// <summary>
/// Non-generic convenience alias for collections keyed by <see cref="ObjectId"/>.
/// </summary>
public interface IDocumentCollection<T> : IDocumentCollection<ObjectId, T> where T : class { }
