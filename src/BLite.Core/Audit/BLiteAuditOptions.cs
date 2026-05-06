using System;

namespace BLite.Core.Audit;

/// <summary>
/// Configuration entry point for BLite audit and performance monitoring.
/// Passed to <c>BLiteEngine</c> / <c>DocumentDbContext</c> to enable any
/// combination of sink callbacks, in-memory metrics, slow-operation
/// detection, and <c>System.Diagnostics.Activity</c> emission.
/// </summary>
public sealed class BLiteAuditOptions
{
    public IBLiteAuditSink? Sink { get; set; }

    public bool EnableMetrics { get; set; }

    public TimeSpan? SlowQueryThreshold { get; set; }

    public bool EnableDiagnosticSource { get; set; }
}
