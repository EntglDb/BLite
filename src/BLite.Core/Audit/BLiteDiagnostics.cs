using System.Diagnostics;

namespace BLite.Core.Audit;

/// <summary>
/// Static diagnostics sources for OpenTelemetry / Application Insights integration.
/// Activities are only created when at least one <see cref="ActivityListener"/> is registered
/// (the <c>HasListeners()</c> check costs ~5 ns — effectively zero overhead otherwise).
/// </summary>
/// <remarks>
/// Register a listener with:
/// <code>
/// ActivitySource.AddActivityListener(new ActivityListener { ... });
/// </code>
/// Or via OpenTelemetry SDK:
/// <code>
/// services.AddOpenTelemetry()
///     .WithTracing(b => b.AddSource(BLiteDiagnostics.ActivitySourceName));
/// </code>
/// </remarks>
public static class BLiteDiagnostics
{
    /// <summary>The name registered with <see cref="ActivitySource"/>.</summary>
    public const string ActivitySourceName = "BLite";

    /// <summary>
    /// The version reported by the <see cref="ActivitySource"/>.
    /// Derived from the executing assembly's version to avoid maintenance drift.
    /// </summary>
    public static readonly string ActivitySourceVersion =
        typeof(BLiteDiagnostics).Assembly.GetName().Version?.ToString(3) ?? "0.0.0";

    /// <summary>Activity operation name for transaction commit spans.</summary>
    public const string CommitActivityName    = "blite.commit";

    /// <summary>Activity operation name for document insert spans.</summary>
    public const string InsertActivityName    = "blite.insert";

    /// <summary>Activity operation name for query spans.</summary>
    public const string QueryActivityName     = "blite.query";

#if NET5_0_OR_GREATER
    /// <summary>
    /// <see cref="System.Diagnostics.ActivitySource"/> for BLite operations.
    /// Add a listener to receive OpenTelemetry-compatible spans.
    /// Only available on .NET 5.0 and later targets.
    /// </summary>
    public static readonly ActivitySource ActivitySource =
        new(ActivitySourceName, ActivitySourceVersion);
#endif
}
