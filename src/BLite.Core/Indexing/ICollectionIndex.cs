using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core.Indexing;

/// <summary>
/// Abstraction over a typed secondary index.
/// Implemented by <see cref="CollectionSecondaryIndex{TId,T}"/> (local embedded engine)
/// and <c>RemoteCollectionIndex&lt;TId,T&gt;</c> (BLite.Client remote transport).
///
/// <para>
/// Range queries and vector search are exposed directly on the handle so that
/// callers can hold a single reference after <c>CreateIndexAsync</c> /
/// <c>EnsureIndex</c> and query through it without going back to the collection.
/// </para>
/// </summary>
public interface ICollectionIndex<TId, T> where T : class
{
    // ── Metadata ──────────────────────────────────────────────────────────────

    string Name { get; }

    /// <summary>Dot-notation property paths that form the index key.</summary>
    string[] PropertyPaths { get; }

    IndexType Type { get; }

    bool IsUnique { get; }

    /// <summary>Number of dimensions (vector index only).</summary>
    int Dimensions { get; }

    /// <summary>Distance metric (vector index only).</summary>
    VectorMetric Metric { get; }

    // ── BTree range query ─────────────────────────────────────────────────────

    /// <inheritdoc cref="Query"/>
    IAsyncEnumerable<T> QueryAsync(
        object? minKey = null,
        object? maxKey = null,
        bool ascending = true,
        CancellationToken ct = default);

    // ── Vector search ─────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the <paramref name="k"/> nearest neighbours to <paramref name="query"/>.
    /// Valid only when <see cref="Type"/> is <see cref="IndexType.Vector"/>.
    /// </summary>
    IEnumerable<VectorSearchResult> VectorSearch(float[] query, int k, int efSearch = 100);

    /// <inheritdoc cref="VectorSearch"/>
    IAsyncEnumerable<T> VectorSearchAsync(
        float[] query, int k, int efSearch = 100, CancellationToken ct = default);
}
