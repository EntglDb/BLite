namespace BLite.Core.Audit;

/// <summary>
/// Receives audit events from a BLite engine. Callbacks are invoked
/// synchronously on the thread performing the operation, so implementations
/// that perform I/O or other slow work must enqueue internally and return
/// quickly. Default methods are no-ops so consumers only override what they
/// care about.
/// </summary>
public interface IBLiteAuditSink
{
    void OnInsert(in InsertAuditEvent evt) { }
    void OnQuery(in QueryAuditEvent evt) { }
    void OnCommit(in CommitAuditEvent evt) { }
    void OnSlowOperation(in SlowOperationEvent evt) { }
}
