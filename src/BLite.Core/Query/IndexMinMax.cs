namespace BLite.Core.Query;

/// <summary>
/// A plan for an O(log n) minimum or maximum lookup against a B-Tree index,
/// or a BSON-level aggregate fallback when no index is available.
/// </summary>
public sealed class IndexMinMax
{
    internal enum PlanKind { IndexBoundary, BsonAggregate }

    internal PlanKind Kind { get; private init; }
    internal string? IndexName { get; private init; }
    internal bool IsMin { get; private init; }
    internal BsonAggregator? Fallback { get; private init; }

    /// <summary>Creates a plan that reads the first (minimum) or last (maximum) B-Tree key.</summary>
    internal static IndexMinMax ForIndex(string indexName, bool isMin)
        => new() { Kind = PlanKind.IndexBoundary, IndexName = indexName, IsMin = isMin };

    /// <summary>Creates a fallback plan that scans all BSON documents accumulating a min or max.</summary>
    public static IndexMinMax Scan(BsonAggregator fallback)
        => new() { Kind = PlanKind.BsonAggregate, Fallback = fallback };
}
