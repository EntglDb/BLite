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
}
