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
}
