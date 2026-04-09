namespace BLite.Core.Metrics;

/// <summary>
/// A lightweight, immutable struct that records a single database operation event.
/// Published to <see cref="MetricsDispatcher"/> via a non-blocking <c>TryWrite</c> call
/// so the hot path is never stalled by metric collection.
/// </summary>
public readonly struct MetricEvent
{
    /// <summary>Timestamp of the event in <see cref="System.Diagnostics.Stopwatch"/> ticks.</summary>
    public long Timestamp { get; init; }

    /// <summary>The type of operation that generated this event.</summary>
    public MetricEventType Type { get; init; }

    /// <summary>
    /// Duration of the operation in microseconds, or 0 when not measured
    /// (e.g. <see cref="MetricEventType.TransactionBegin"/>).
    /// </summary>
    public long ElapsedMicros { get; init; }

    /// <summary>
    /// Collection name, populated only for collection-level events
    /// (<see cref="MetricEventType.CollectionInsert"/> etc.). Null otherwise.
    /// </summary>
    public string? CollectionName { get; init; }

    /// <summary>
    /// Number of items in a batch operation.
    /// Used by <see cref="MetricEventType.GroupCommitBatch"/> to carry the batch size.
    /// 0 for single-item operations.
    /// </summary>
    public int BatchSize { get; init; }

    /// <summary>Whether the operation completed successfully.</summary>
    public bool Success { get; init; }
}
