using System;

namespace BLite.Core.Retention;

/// <summary>
/// Specifies when the retention policy is evaluated and enforced.
/// Multiple triggers can be combined using bitwise OR.
/// </summary>
[Flags]
public enum RetentionTrigger
{
    /// <summary>No trigger is configured.</summary>
    None = 0,

    /// <summary>
    /// Retention is checked on every document insertion.
    /// The check is lightweight — only a counter or timestamp comparison is performed first.
    /// Full scan is deferred until the threshold is actually exceeded.
    /// </summary>
    OnInsert = 1,

    /// <summary>
    /// Retention is enforced on a configurable background timer interval.
    /// The timer does not run concurrently with itself — if a previous run is still in progress
    /// it is skipped for the current interval.
    /// </summary>
    Scheduled = 2,
}
