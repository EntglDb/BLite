using System;

namespace BLite.Core.Retention;

/// <summary>
/// Describes the retention rules for a collection.
/// Multiple constraints can be active simultaneously; any constraint that is
/// exceeded will trigger a deletion pass when the configured
/// <see cref="Triggers"/> fire.
/// </summary>
public sealed class RetentionPolicy
{
    /// <summary>
    /// Maximum age of documents in milliseconds.
    /// Documents whose timestamp field is older than <c>now - MaxAgeMs</c> are deleted.
    /// 0 means no age-based limit.
    /// Requires <see cref="TimestampField"/> to be set.
    /// Documents missing the timestamp field are exempt from this rule.
    /// </summary>
    public long MaxAgeMs { get; set; }

    /// <summary>
    /// Maximum number of documents in the collection.
    /// When the count exceeds this value the oldest documents are deleted until the
    /// count is within bounds.
    /// 0 means no document-count limit.
    /// </summary>
    public long MaxDocumentCount { get; set; }

    /// <summary>
    /// Maximum total size of collection data pages in bytes.
    /// When the data footprint exceeds this value the oldest documents are deleted.
    /// 0 means no size limit.
    /// </summary>
    public long MaxSizeBytes { get; set; }

    /// <summary>
    /// Interval in milliseconds between scheduled retention runs.
    /// Only used when <see cref="RetentionTrigger.Scheduled"/> is included in
    /// <see cref="Triggers"/>.
    /// 0 or negative means use the default interval (5 minutes).
    /// </summary>
    public long ScheduledIntervalMs { get; set; }

    /// <summary>
    /// Name of the BSON field that holds a timestamp value used to determine document age.
    /// Required for <see cref="MaxAgeMs"/>; also used by <see cref="MaxDocumentCount"/>
    /// and <see cref="MaxSizeBytes"/> when ordering documents for deletion.
    /// When absent those constraints fall back to primary-key (insertion) order.
    /// <para>
    /// <b>Note:</b> documents whose timestamp field is absent are exempt from
    /// age-based deletion (<see cref="MaxAgeMs"/>).  Large documents stored across
    /// overflow pages are also exempt from age-based deletion but are still counted
    /// for <see cref="MaxDocumentCount"/> and <see cref="MaxSizeBytes"/> purposes.
    /// </para>
    /// </summary>
    public string? TimestampField { get; set; }

    /// <summary>
    /// Specifies when retention is evaluated.
    /// Defaults to <see cref="RetentionTrigger.OnInsert"/>.
    /// </summary>
    public RetentionTrigger Triggers { get; set; } = RetentionTrigger.OnInsert;
}
