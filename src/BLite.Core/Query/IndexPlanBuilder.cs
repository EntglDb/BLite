using BLite.Core.Indexing;
using System.Collections.Generic;
using System.Linq;

namespace BLite.Core.Query;

/// <summary>
/// Fluent builder returned by the private <c>Resolve</c> helper inside every
/// generated <c>{Entity}Filter</c> class. Converts operator semantics into an
/// <see cref="IndexQueryPlan"/> that targets a specific B-Tree index by name.
///
/// All range bounds are stored as raw <see cref="object?"/> values and passed
/// directly to <c>CollectionSecondaryIndex.Range</c>, which handles key encoding.
/// </summary>
public sealed class IndexPlanBuilder
{
    private readonly string _indexName;

    /// <param name="indexName">Name of the B-Tree index as returned by <c>CollectionIndexInfo.Name</c>.</param>
    public IndexPlanBuilder(string indexName)
        => _indexName = indexName;

    /// <summary>
    /// Returns an <see cref="IndexQueryPlan"/> that performs an exact point-lookup on <paramref name="key"/>.
    /// Equivalent to <c>Range(key, key)</c>.
    /// </summary>
    public IndexQueryPlan Exact(IndexKey key)
        => IndexQueryPlan.ForRange(_indexName, (object?)key, (object?)key);

    /// <summary>
    /// Returns an <see cref="IndexQueryPlan"/> for an inclusive range scan
    /// [<paramref name="min"/>, <paramref name="max"/>].
    ///
    /// To model a strict inequality, callers should add a BSON post-filter
    /// via <see cref="IndexQueryPlan.And"/> after constructing the plan:
    /// <code>
    /// builder.Range(IndexKey.Create(v), IndexKey.MaxKey)
    ///        .And(BsonPredicateBuilder.Gt("age", v))
    /// </code>
    /// </summary>
    public IndexQueryPlan Range(IndexKey min, IndexKey max)
        => IndexQueryPlan.ForRange(_indexName, (object?)min, (object?)max);

    /// <summary>
    /// Returns an <see cref="IndexQueryPlan"/> for a multi-point lookup.
    /// Each key is used as an individual exact probe against the index.
    /// </summary>
    public IndexQueryPlan In(IEnumerable<IndexKey> keys)
        => IndexQueryPlan.ForIn(_indexName, keys.ToList());

    /// <summary>Returns an <see cref="IndexMinMax"/> that reads the first (minimum-valued) B-Tree key.</summary>
    public IndexMinMax First() => IndexMinMax.ForIndex(_indexName, isMin: true);

    /// <summary>Returns an <see cref="IndexMinMax"/> that reads the last (maximum-valued) B-Tree key.</summary>
    public IndexMinMax Last() => IndexMinMax.ForIndex(_indexName, isMin: false);
}
