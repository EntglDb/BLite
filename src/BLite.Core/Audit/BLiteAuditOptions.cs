namespace BLite.Core.Audit;

/// <summary>
/// Configuration for the BLite audit trail subsystem.
/// Pass an instance to <c>BLiteEngine.ConfigureAudit()</c> or
/// <c>DocumentDbContext.ConfigureAudit()</c> after construction.
/// </summary>
public sealed class BLiteAuditOptions
{
    /// <summary>
    /// Custom audit sink. When <see langword="null"/> no <see cref="IBLiteAuditSink"/> callbacks are invoked.
    /// </summary>
    public IBLiteAuditSink? Sink { get; init; }

    /// <summary>
    /// When <see langword="true"/>, populates the <see cref="BLiteMetrics"/> instance accessible via
    /// <c>BLiteEngine.AuditMetrics</c> / <c>DocumentDbContext.AuditMetrics</c>.
    /// </summary>
    public bool EnableMetrics { get; init; } = false;

    /// <summary>
    /// Provider that supplies the current caller identity.
    /// Defaults to <see cref="AmbientAuditContext"/> when <see langword="null"/>.
    /// </summary>
    public IAuditContextProvider? ContextProvider { get; init; }

    // ── Phase 2 ─────────────────────────────────────────────────────────────

    /// <summary>
    /// When set, a <see cref="SlowOperationEvent"/> is emitted on the <see cref="Sink"/>
    /// for any operation (insert, query, or commit) that exceeds this duration.
    /// <see langword="null"/> disables slow-operation detection.
    /// </summary>
    public TimeSpan? SlowOperationThreshold { get; init; }

    /// <summary>
    /// When <see langword="true"/>, emits <see cref="System.Diagnostics.Activity"/> spans via
    /// <see cref="BLiteDiagnostics.ActivitySource"/>. Compatible with OpenTelemetry and
    /// Application Insights.
    /// <para>
    /// No overhead is incurred unless an <see cref="System.Diagnostics.ActivityListener"/> is
    /// registered (the <c>ActivitySource.HasListeners()</c> check costs ~5 ns).
    /// </para>
    /// </summary>
    public bool EnableDiagnosticSource { get; init; } = false;
}
