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
    /// Default options: 1-second snapshot interval.
    /// </summary>
    public static MetricsOptions Default { get; } = new();
}
