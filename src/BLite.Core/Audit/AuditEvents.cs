using System;

namespace BLite.Core.Audit;

public readonly record struct CommitAuditEvent(
    ulong TransactionId,
    string CollectionName,
    int PagesWritten,
    long WalSizeBytes,
    TimeSpan Elapsed);

public readonly record struct InsertAuditEvent(
    ulong TransactionId,
    string CollectionName,
    int DocumentSizeBytes,
    TimeSpan Elapsed);

public readonly record struct QueryAuditEvent(
    string CollectionName,
    QueryStrategy Strategy,
    string? IndexName,
    int ResultCount,
    TimeSpan Elapsed);

public readonly record struct SlowOperationEvent(
    SlowOperationType OperationType,
    string CollectionName,
    TimeSpan Elapsed,
    string? Detail);

public enum QueryStrategy : byte
{
    Unknown = 0,
    IndexScan = 1,
    BsonScan = 2,
    FullScan = 3,
}

public enum SlowOperationType : byte
{
    Insert = 0,
    Query = 1,
    Commit = 2,
}
