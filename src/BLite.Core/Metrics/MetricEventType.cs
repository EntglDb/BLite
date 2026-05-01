namespace BLite.Core.Metrics;

/// <summary>
/// Identifies the kind of database operation recorded in a <see cref="MetricEvent"/>.
/// </summary>
public enum MetricEventType : byte
{
    TransactionBegin    = 0,
    TransactionCommit   = 1,
    TransactionRollback = 2,
    Checkpoint          = 3,
    GroupCommitBatch    = 4,
    CollectionInsert    = 5,
    CollectionUpdate    = 6,
    CollectionDelete    = 7,
    CollectionFind      = 8,

    /// <summary>
    /// A query/scan operation that materialised results (FindAllAsync, ScanAsync,
    /// QueryIndexAsync, FindAsync, FindOneAsync, CountAsync).
    /// Distinct from <see cref="CollectionFind"/> which covers only <c>FindByIdAsync</c>.
    /// </summary>
    CollectionQuery     = 9,

    // ── Security / audit event types ───────────────────────────────────────

    /// <summary>
    /// A generic audit event. The <see cref="MetricEvent.Tag"/> field carries the
    /// audit event type name (e.g. a custom label supplied by an external audit sink).
    /// </summary>
    AuditEvent          = 10,

    /// <summary>
    /// A query that was rejected by BLQL hardening (e.g. unknown operator or
    /// malformed filter JSON). Increments <c>SecurityFailedQueriesTotal</c> in
    /// the metrics snapshot.
    /// </summary>
    SecurityFailedQuery = 11,

    /// <summary>
    /// Emitted when a VACUUM pass completes successfully on a collection.
    /// <see cref="MetricEvent.BytesFreed"/> carries the bytes compacted during the pass.
    /// </summary>
    Vacuum              = 12,

    /// <summary>
    /// Emitted when a hot backup completes successfully.
    /// <see cref="MetricEvent.ElapsedMicros"/> carries the backup duration.
    /// </summary>
    BackupCompleted     = 13,
}
