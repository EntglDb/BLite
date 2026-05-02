namespace BLite.Core.Metrics;

/// <summary>
/// Configuration options for the BLite metrics subsystem.
/// Passed to <c>BLiteEngine.EnableMetrics()</c>.
/// </summary>
public sealed class MetricsOptions
{
    /// <summary>
    /// How often <see cref="BLiteMetricsObservable"/> emits a snapshot to subscribers.
    /// Defaults to 1 second.
    /// </summary>
    public TimeSpan SnapshotInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// When <see langword="true"/>, registers a <c>Meter("BLite.Core")</c> with the security
    /// counters as OpenTelemetry-compatible instruments (<c>Counter&lt;long&gt;</c>).
    /// <para>
    /// Only effective on .NET 6 and later targets; the option is silently ignored on
    /// .NET Standard 2.1 where <c>System.Diagnostics.Metrics</c> is not available.
    /// </para>
    /// Defaults to <see langword="false"/>.
    /// </summary>
    public bool EnableDiagnosticSource { get; init; } = false;

    /// <summary>
    /// Default options: 1-second snapshot interval, diagnostic source disabled.
    /// </summary>
    public static MetricsOptions Default { get; } = new();
}
