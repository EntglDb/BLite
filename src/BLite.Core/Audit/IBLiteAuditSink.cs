namespace BLite.Core.Audit;

/// <summary>
/// User-implementable interface for receiving BLite audit events.
/// All methods are called synchronously on the hot-path thread.
/// Slow implementations (e.g. writing to disk) must queue work internally.
/// </summary>
/// <remarks>
/// All methods have default no-op implementations so that implementations can
/// selectively override only the events they care about.
/// </remarks>
public interface IBLiteAuditSink
{
    /// <summary>Called after every document insert completes.</summary>
    void OnInsert(InsertAuditEvent e) { }

    /// <summary>Called after every LINQ / BLQL query completes.</summary>
    void OnQuery(QueryAuditEvent e) { }

    /// <summary>Called after every transaction commit completes.</summary>
    void OnCommit(CommitAuditEvent e) { }

    /// <summary>
    /// Called when an operation exceeds <see cref="BLiteAuditOptions.SlowOperationThreshold"/>.
    /// Requires <see cref="BLiteAuditOptions.SlowOperationThreshold"/> to be set.
    /// </summary>
    void OnSlowOperation(SlowOperationEvent e) { }
}
