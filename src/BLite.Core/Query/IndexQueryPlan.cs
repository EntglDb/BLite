using BLite.Bson;
using BLite.Core.Indexing;
using System.Collections.Generic;

namespace BLite.Core.Query;

/// <summary>
/// A query execution plan produced by a generated <c>{Entity}Filter</c> method.
///
/// A plan is either:
/// <list type="bullet">
///   <item><description>
///     An <em>index seek</em> – the collection's B-Tree index is scanned between
///     <see cref="MinKey"/> and <see cref="MaxKey"/> (both inclusive at the index level).
///     Use <see cref="IndexName"/> to identify which index to use.
///   </description></item>
///   <item><description>
///     A <em>full scan</em> – every document is passed to <see cref="ScanPredicate"/>
///     before deserialisation. Used as an automatic fallback when the expected index
///     is absent (e.g. not yet created, or dropped at runtime).
///   </description></item>
/// </list>
///
/// Call <see cref="And(BsonReaderPredicate)"/> to attach a post-filter residue that is
/// applied to documents returned by the index scan. This models the pattern:
/// <code>
/// ScanAsync(UserFilter.AgeGt(18, indexes).And(UserFilter.NameContains("Al")))
/// //   → B-Tree range seek on Age (index)
/// //   → BsonReaderPredicate post-filter for Name (applied to index results only)
/// </code>
/// </summary>
public sealed class IndexQueryPlan
{
    internal enum PlanKind { IndexRange, IndexIn, Scan }

    internal PlanKind Kind { get; private init; }

    // ── Index-range fields ─────────────────────────────────────────────────────

    /// <summary>Name of the B-Tree index to seek, or <c>null</c> for a full scan.</summary>
    public string? IndexName { get; private init; }

    /// <summary>Inclusive lower bound for the index range (passed directly to <c>CollectionSecondaryIndex.Range</c>).</summary>
    public object? MinKey { get; private init; }

    /// <summary>Inclusive upper bound for the index range.</summary>
    public object? MaxKey { get; private init; }

    // ── Index-in fields ────────────────────────────────────────────────────────

    /// <summary>Set of exact keys for a multi-point index lookup (used when <see cref="Kind"/> is <c>IndexIn</c>).</summary>
    internal IReadOnlyList<IndexKey>? InKeys { get; private init; }

    // ── Scan fallback ──────────────────────────────────────────────────────────

    /// <summary>
    /// BSON-level predicate used when no index is available, or as a post-filter on
    /// index results when <see cref="ResiduePredicate"/> is set.
    /// </summary>
    public BsonReaderPredicate? ScanPredicate { get; private init; }

    // ── Post-filter residue ────────────────────────────────────────────────────

    /// <summary>
    /// Optional post-filter applied to every document returned by the index scan.
    /// Accumulated via successive <see cref="And(BsonReaderPredicate)"/> calls.
    /// <c>null</c> means no residue (all index results are accepted).
    /// </summary>
    public BsonReaderPredicate? ResiduePredicate { get; private init; }

    /// <summary><c>true</c> when this plan uses an index (B-Tree range or multi-point lookup).</summary>
    public bool IsIndexScan => Kind == PlanKind.IndexRange || Kind == PlanKind.IndexIn;

    // ── Factories ──────────────────────────────────────────────────────────────

    /// <summary>Creates a full-scan plan that evaluates <paramref name="predicate"/> per document.</summary>
    public static IndexQueryPlan Scan(BsonReaderPredicate predicate)
        => new() { Kind = PlanKind.Scan, ScanPredicate = predicate };

    // ── Composition ────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns a new plan that is identical to this one but with <paramref name="residue"/>
    /// added as a BSON-level post-filter applied to every document the index (or scan) returns.
    ///
    /// Multiple <see cref="And"/> calls accumulate: the resulting residue is the conjunction
    /// of all supplied predicates.
    ///
    /// For full-scan plans (<see cref="IsIndexScan"/> is <c>false</c>), the residue is folded
    /// directly into the scan predicate rather than stored separately, so the collection performs
    /// only a single predicate evaluation pass per document.
    /// </summary>
    public IndexQueryPlan And(BsonReaderPredicate residue)
    {
        // For full-scan plans fold the residue into ScanPredicate to avoid a second
        // serialise-round-trip per document. The residue mechanism is only needed for
        // index-backed plans where the index returns candidates that still need an extra
        // BSON-level post-filter (e.g. strict inequalities at the boundary).
        if (Kind == PlanKind.Scan)
        {
            var combined = ScanPredicate == null
                ? residue
                : BsonPredicateBuilder.And(ScanPredicate, residue);

            return new IndexQueryPlan
            {
                Kind = PlanKind.Scan,
                ScanPredicate = combined,
                ResiduePredicate = null,
            };
        }

        var residueCombined = ResiduePredicate == null
            ? residue
            : BsonPredicateBuilder.And(ResiduePredicate, residue);

        return new IndexQueryPlan
        {
            Kind = Kind,
            IndexName = IndexName,
            MinKey = MinKey,
            MaxKey = MaxKey,
            InKeys = InKeys,
            ScanPredicate = ScanPredicate,
            ResiduePredicate = residueCombined,
        };
    }

    // ── Internal constructors used by IndexPlanBuilder ─────────────────────────

    internal static IndexQueryPlan ForRange(string indexName, object? min, object? max)
        => new() { Kind = PlanKind.IndexRange, IndexName = indexName, MinKey = min, MaxKey = max };

    internal static IndexQueryPlan ForIn(string indexName, IReadOnlyList<IndexKey> keys)
        => new() { Kind = PlanKind.IndexIn, IndexName = indexName, InKeys = keys };
}
