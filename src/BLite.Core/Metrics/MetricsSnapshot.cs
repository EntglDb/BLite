namespace BLite.Core.Metrics;

/// <summary>
/// An immutable point-in-time snapshot of the database performance counters.
/// Obtained via <c>BLiteEngine.GetMetrics()</c> (pull) or
/// <c>BLiteEngine.WatchMetrics()</c> (push, periodic).
/// </summary>
public sealed class MetricsSnapshot
{
    // ── Transaction counters ────────────────────────────────────────────────

    /// <summary>Total number of transactions started since metrics were enabled.</summary>
    public long TransactionBeginsTotal { get; init; }

    /// <summary>Total number of successful transaction commits.</summary>
    public long TransactionCommitsTotal { get; init; }

    /// <summary>Total number of transaction rollbacks.</summary>
    public long TransactionRollbacksTotal { get; init; }

    /// <summary>Average commit latency in microseconds over all recorded commits. 0 if no commits yet.</summary>
    public double AvgCommitLatencyUs { get; init; }

    // ── Group commit ────────────────────────────────────────────────────────

    /// <summary>Total number of group-commit batches flushed to WAL.</summary>
    public long GroupCommitBatchesTotal { get; init; }

    /// <summary>Average number of transactions per group-commit batch. 0 if no batches yet.</summary>
    public double GroupCommitAvgBatchSize { get; init; }

    // ── Checkpoint ──────────────────────────────────────────────────────────

    /// <summary>Total number of checkpoints performed.</summary>
    public long CheckpointsTotal { get; init; }

    /// <summary>Average checkpoint duration in microseconds. 0 if no checkpoints yet.</summary>
    public double AvgCheckpointLatencyUs { get; init; }

    // ── Collection operations ───────────────────────────────────────────────

    /// <summary>Total document inserts across all collections.</summary>
    public long InsertsTotal { get; init; }

    /// <summary>Total document updates across all collections.</summary>
    public long UpdatesTotal { get; init; }

    /// <summary>Total document deletes across all collections.</summary>
    public long DeletesTotal { get; init; }

    /// <summary>Total find/read operations across all collections.</summary>
    public long FindsTotal { get; init; }

    /// <summary>Total query/scan operations across all collections (FindAll, Scan, QueryIndex, Find, FindOne, Count).</summary>
    public long QueriesTotal { get; init; }

    /// <summary>Average insert latency in microseconds across all collections. 0 if none.</summary>
    public double AvgInsertLatencyUs { get; init; }

    /// <summary>Average update latency in microseconds across all collections. 0 if none.</summary>
    public double AvgUpdateLatencyUs { get; init; }

    /// <summary>Average delete latency in microseconds across all collections. 0 if none.</summary>
    public double AvgDeleteLatencyUs { get; init; }

    /// <summary>Average query latency in microseconds across all collections. 0 if none.</summary>
    public double AvgQueryLatencyUs { get; init; }

    // ── Per-collection statistics ───────────────────────────────────────────

    /// <summary>
    /// Per-collection operation counters.
    /// Key: collection name. Value: <see cref="CollectionMetricsSnapshot"/>.
    /// </summary>
    public IReadOnlyDictionary<string, CollectionMetricsSnapshot> Collections { get; init; }
        = new Dictionary<string, CollectionMetricsSnapshot>();

    // ── Security metrics ────────────────────────────────────────────────────

    /// <summary>
    /// Total audit events emitted since metrics were enabled, grouped by event type.
    /// Key: event type name (e.g. <c>"vacuum"</c>, <c>"backup.completed"</c>,
    /// <c>"security.failed_query"</c>).
    /// </summary>
    public IReadOnlyDictionary<string, long> AuditEventsTotal { get; init; }
        = new Dictionary<string, long>();

    /// <summary>Total queries rejected by BLQL hardening since metrics were enabled.</summary>
    public long SecurityFailedQueriesTotal { get; init; }

    /// <summary>UTC timestamp of the last successful VACUUM completion. <c>null</c> if no vacuum has run.</summary>
    public DateTimeOffset? VacuumLastRunAt { get; init; }

    /// <summary>Bytes compacted in the most recent VACUUM operation. 0 if no vacuum has run.</summary>
    public long VacuumBytesFreed { get; init; }

    /// <summary>UTC timestamp of the last successful backup completion. <c>null</c> if no backup has run.</summary>
    public DateTimeOffset? BackupLastSuccessAt { get; init; }

    /// <summary>Duration of the most recent successful backup in milliseconds. 0 if no backup has run.</summary>
    public long BackupLastDurationMs { get; init; }

    /// <summary>UTC timestamp at which this snapshot was captured.</summary>
    public DateTimeOffset SnapshotTimestamp { get; init; }
}

/// <summary>Aggregated operation counters for a single collection.</summary>
public sealed class CollectionMetricsSnapshot
{
    /// <summary>Collection name.</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>Total inserts into this collection.</summary>
    public long InsertCount { get; init; }

    /// <summary>Total updates in this collection.</summary>
    public long UpdateCount { get; init; }

    /// <summary>Total deletes in this collection.</summary>
    public long DeleteCount { get; init; }

    /// <summary>Total find operations in this collection.</summary>
    public long FindCount { get; init; }

    /// <summary>Total query/scan operations in this collection.</summary>
    public long QueryCount { get; init; }

    /// <summary>Average insert latency in microseconds. 0 if none.</summary>
    public double AvgInsertLatencyUs { get; init; }

    /// <summary>Average update latency in microseconds. 0 if none.</summary>
    public double AvgUpdateLatencyUs { get; init; }

    /// <summary>Average delete latency in microseconds. 0 if none.</summary>
    public double AvgDeleteLatencyUs { get; init; }
}
