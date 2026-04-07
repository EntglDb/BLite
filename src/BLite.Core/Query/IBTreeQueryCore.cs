using BLite.Bson;
using BLite.Core.Indexing;

namespace BLite.Core.Query;

/// <summary>
/// Internal scan capability interface surfaced by <see cref="BTreeQueryProvider{TId,T}"/>
/// so that <see cref="BTreeQueryable{T}"/> can call AOT-safe collection methods without
/// knowing the <c>TId</c> type parameter.
/// </summary>
internal interface IBTreeQueryCore<T>
{
    IAsyncEnumerable<T> ScanAsync(BsonReaderPredicate predicate, CancellationToken ct = default);
    IAsyncEnumerable<T> ScanAsync(IndexQueryPlan plan, CancellationToken ct = default);
    IEnumerable<CollectionIndexInfo> GetIndexes();
    Task<int> CountAsync(CancellationToken ct = default);
    ValueTask<TResult> MinBoundaryAsync<TResult>(IndexMinMax plan, CancellationToken ct = default);
    ValueTask<TResult> MaxBoundaryAsync<TResult>(IndexMinMax plan, CancellationToken ct = default);
}
