// BLite.Core — B-tree index scan candidate for BLQL query optimization
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

namespace BLite.Core.Query.Blql;

/// <summary>
/// Describes a B-tree index range scan that can serve a <see cref="BlqlFilter"/> condition.
/// Produced by <see cref="BlqlFilter.TryGetIndexCandidate"/>.
/// The scan covers a conservative (inclusive) range; callers must apply the original filter
/// as a residual predicate for correctness — this handles strict bounds (Gt/Lt) and compound
/// conditions not fully captured by a single range.
/// </summary>
internal readonly struct IndexScanCandidate
{
    /// <summary>Field path targeted by the candidate (lower-invariant).</summary>
    public readonly string Field;

    /// <summary>Lower bound to pass to <c>QueryIndexAsync</c>. Null = open (scan from MinKey).</summary>
    public readonly object? Min;

    /// <summary>Upper bound to pass to <c>QueryIndexAsync</c>. Null = open (scan to MaxKey).</summary>
    public readonly object? Max;

    public IndexScanCandidate(string field, object? min, object? max)
    {
        Field = field;
        Min = min;
        Max = max;
    }
}
