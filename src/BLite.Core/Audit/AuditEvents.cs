namespace BLite.Core.Audit;

/// <summary>Emitted upon completion of every CommitTransaction.</summary>
public readonly record struct CommitAuditEvent(
    ulong TransactionId,
    /// <summary>Empty string when the commit spans multiple collections or is cross-collection.</summary>
    string CollectionName,
    int PagesWritten,
    long WalSizeBytes,
    TimeSpan Elapsed,
    /// <summary>The caller identity provided by <see cref="IAuditContextProvider"/>. <see langword="null"/> when not set.</summary>
    string? UserId);

/// <summary>Emitted upon completion of every InsertDataCore.</summary>
public readonly record struct InsertAuditEvent(
    ulong TransactionId,
    string CollectionName,
    int DocumentSizeBytes,
    TimeSpan Elapsed,
    /// <summary>The caller identity provided by <see cref="IAuditContextProvider"/>. <see langword="null"/> when not set.</summary>
    string? UserId);

/// <summary>Emitted upon completion of every Execute&lt;TResult&gt; in BTreeQueryProvider.</summary>
public readonly record struct QueryAuditEvent(
    string CollectionName,
    QueryStrategy Strategy,
    /// <summary><see langword="null"/> when <see cref="Strategy"/> is not <see cref="QueryStrategy.IndexScan"/>.</summary>
    string? IndexName,
    int ResultCount,
    TimeSpan Elapsed,
    /// <summary>The caller identity provided by <see cref="IAuditContextProvider"/>. <see langword="null"/> when not set.</summary>
    string? UserId);

/// <summary>Emitted when an operation exceeds the configured <see cref="BLiteAuditOptions.SlowQueryThreshold"/>.</summary>
public readonly record struct SlowOperationEvent(
    SlowOperationType OperationType,
    string CollectionName,
    TimeSpan Elapsed,
    /// <summary>Human-readable detail, e.g. LINQ expression or index name.</summary>
    string? Detail);

/// <summary>Describes how a query was executed.</summary>
public enum QueryStrategy : byte
{
    Unknown   = 0,
    IndexScan = 1,
    BsonScan  = 2,
    FullScan  = 3
}

/// <summary>Classifies the slow-operation type in a <see cref="SlowOperationEvent"/>.</summary>
public enum SlowOperationType : byte
{
    Insert = 1,
    Query  = 2,
    Commit = 3
}
