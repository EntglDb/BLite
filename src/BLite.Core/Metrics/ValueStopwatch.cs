using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace BLite.Core.Metrics;

/// <summary>
/// A zero-allocation alternative to <see cref="Stopwatch"/> for use on async hot paths.
///
/// Being a <c>readonly struct</c> containing only a single <c>long</c> field, it is
/// stored inline in async state machines — unlike holding a class reference to
/// <see cref="MetricsDispatcher"/>, which would add a managed reference that the GC
/// must trace for every live async operation.
///
/// Usage pattern:
/// <code>
///   // Before the first await — only the struct (a single long) is captured:
///   var sw = _metrics != null ? ValueStopwatch.StartNew() : default;
///   bool success = false;
///
///   try { /* ... awaitable work ... */ success = true; }
///   finally
///   {
///       // The finally is synchronous — no await, so 'm' is never captured:
///       if (sw.IsActive)
///           _metrics?.Publish(new MetricEvent
///           {
///               Timestamp     = sw.StartTimestamp,
///               ElapsedMicros = sw.GetElapsedMicros(),
///               Success       = success,
///           });
///   }
/// </code>
/// </summary>
internal readonly struct ValueStopwatch
{
    // Precomputed to avoid a floating-point division on every GetElapsedMicros() call
    // (Stopwatch.Frequency is constant per process, so this is safe as a static field).
    private static readonly double TicksToMicros = 1_000_000.0 / Stopwatch.Frequency;

    private readonly long _startTimestamp;

    /// <summary>
    /// Returns <c>true</c> when timing is active, i.e. the struct was obtained via
    /// <see cref="StartNew"/> rather than from <c>default</c>.
    /// </summary>
    public bool IsActive => _startTimestamp != 0;

    /// <summary>
    /// The raw <see cref="Stopwatch.GetTimestamp"/> value captured at start,
    /// suitable for <see cref="MetricEvent.Timestamp"/>.  Returns 0 when inactive.
    /// </summary>
    public long StartTimestamp => _startTimestamp;

    private ValueStopwatch(long ts) => _startTimestamp = ts;

    /// <summary>Starts a new timer (active state).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueStopwatch StartNew() =>
        new ValueStopwatch(Stopwatch.GetTimestamp());

    /// <summary>
    /// Returns microseconds elapsed since <see cref="StartNew"/>, or 0 when inactive.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long GetElapsedMicros()
    {
        if (!IsActive) return 0L;
        return (long)((Stopwatch.GetTimestamp() - _startTimestamp) * TicksToMicros);
    }

    /// <summary>
    /// Returns the elapsed time since <see cref="StartNew"/>, or <see cref="TimeSpan.Zero"/> when inactive.
    /// Uses microsecond precision (1 µs = 10 TimeSpan ticks).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public TimeSpan GetElapsed() => new TimeSpan(GetElapsedMicros() * 10L);
}
