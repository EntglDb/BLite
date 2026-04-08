namespace BLite.Core.Query;

/// <summary>
/// Factory for BSON-level aggregation descriptors.
/// Each method returns a descriptor understood by the collection's aggregation engine.
/// No <c>Expression.Compile()</c> — all values are read directly from raw BSON bytes.
/// </summary>
public sealed class BsonAggregator
{
    internal enum AggregatorKind { Min, Max, Sum, Average }

    internal string FieldName { get; private init; } = "";
    internal AggregatorKind Kind { get; private init; }

    /// <summary>Returns a descriptor that accumulates the minimum value of <paramref name="field"/>.</summary>
    public static BsonAggregator Min(string field)
        => new() { FieldName = field, Kind = AggregatorKind.Min };

    /// <summary>Returns a descriptor that accumulates the maximum value of <paramref name="field"/>.</summary>
    public static BsonAggregator Max(string field)
        => new() { FieldName = field, Kind = AggregatorKind.Max };

    /// <summary>Returns a descriptor that accumulates the sum of <paramref name="field"/>.</summary>
    public static BsonAggregator Sum(string field)
        => new() { FieldName = field, Kind = AggregatorKind.Sum };

    /// <summary>Returns a descriptor that accumulates the average of <paramref name="field"/>.</summary>
    public static BsonAggregator Average(string field)
        => new() { FieldName = field, Kind = AggregatorKind.Average };
}
