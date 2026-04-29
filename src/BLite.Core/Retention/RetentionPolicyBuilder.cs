using System;

namespace BLite.Core.Retention;

/// <summary>
/// Fluent builder for constructing a <see cref="RetentionPolicy"/> on a
/// <see cref="BLite.Core.DynamicCollection"/>.
/// </summary>
public sealed class RetentionPolicyBuilder
{
    private readonly RetentionPolicy _policy = new();

    /// <summary>
    /// Sets the maximum document age.
    /// Documents older than <paramref name="age"/> (measured against the configured
    /// timestamp field) are deleted.
    /// </summary>
    public RetentionPolicyBuilder MaxAge(TimeSpan age)
    {
        _policy.MaxAgeMs = (long)age.TotalMilliseconds;
        return this;
    }

    /// <summary>
    /// Sets the maximum number of documents the collection may hold.
    /// Oldest documents are deleted when the threshold is exceeded.
    /// </summary>
    public RetentionPolicyBuilder MaxDocumentCount(long count)
    {
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
        _policy.MaxDocumentCount = count;
        return this;
    }

    /// <summary>
    /// Sets the maximum total size (in bytes) for the collection's data pages.
    /// Oldest documents are deleted when the threshold is exceeded.
    /// </summary>
    public RetentionPolicyBuilder MaxSizeBytes(long bytes)
    {
        if (bytes < 0) throw new ArgumentOutOfRangeException(nameof(bytes));
        _policy.MaxSizeBytes = bytes;
        return this;
    }

    /// <summary>
    /// Specifies the BSON field that contains the document timestamp used to determine age.
    /// The field value may be a <c>DateTime</c> or <c>Int64</c> (UTC ticks).
    /// </summary>
    public RetentionPolicyBuilder OnField(string fieldName)
    {
        if (string.IsNullOrWhiteSpace(fieldName))
            throw new ArgumentNullException(nameof(fieldName));
        _policy.TimestampField = fieldName.ToLowerInvariant();
        return this;
    }

    /// <summary>
    /// Sets the interval between scheduled retention runs.
    /// Only relevant when <see cref="RetentionTrigger.Scheduled"/> is included.
    /// </summary>
    public RetentionPolicyBuilder ScheduleInterval(TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(interval));
        _policy.ScheduledIntervalMs = (long)interval.TotalMilliseconds;
        return this;
    }

    /// <summary>
    /// Specifies when the policy is evaluated.
    /// Defaults to <see cref="RetentionTrigger.OnInsert"/> when not called.
    /// </summary>
    public RetentionPolicyBuilder TriggerOn(RetentionTrigger triggers)
    {
        _policy.Triggers = triggers;
        return this;
    }

    internal RetentionPolicy Build() => _policy;
}
