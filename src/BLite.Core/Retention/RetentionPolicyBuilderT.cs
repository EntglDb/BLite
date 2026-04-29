using System;
using System.Linq.Expressions;
using BLite.Core.Indexing;

namespace BLite.Core.Retention;

/// <summary>
/// Fluent builder for constructing a <see cref="RetentionPolicy"/> on a strongly-typed
/// <see cref="BLite.Core.Metadata.EntityTypeBuilder{T}"/>.
/// </summary>
public sealed class RetentionPolicyBuilder<T> where T : class
{
    private readonly RetentionPolicy _policy = new();

    /// <summary>
    /// Sets the maximum document age.
    /// Documents older than <paramref name="age"/> (measured against the configured
    /// timestamp field) are deleted.
    /// </summary>
    public RetentionPolicyBuilder<T> MaxAge(TimeSpan age)
    {
        _policy.MaxAgeMs = (long)age.TotalMilliseconds;
        return this;
    }

    /// <summary>
    /// Sets the maximum number of documents the collection may hold.
    /// Oldest documents are deleted when the threshold is exceeded.
    /// </summary>
    public RetentionPolicyBuilder<T> MaxDocumentCount(long count)
    {
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
        _policy.MaxDocumentCount = count;
        return this;
    }

    /// <summary>
    /// Sets the maximum total size (in bytes) for the collection's data pages.
    /// Oldest documents are deleted when the threshold is exceeded.
    /// </summary>
    public RetentionPolicyBuilder<T> MaxSizeBytes(long bytes)
    {
        if (bytes < 0) throw new ArgumentOutOfRangeException(nameof(bytes));
        _policy.MaxSizeBytes = bytes;
        return this;
    }

    /// <summary>
    /// Specifies the entity property that contains the document timestamp used to determine age.
    /// The property must be of type <see cref="DateTime"/> or <see cref="DateTimeOffset"/>.
    /// </summary>
    public RetentionPolicyBuilder<T> OnField<TProperty>(Expression<Func<T, TProperty>> selector)
    {
        var fieldName = ExpressionAnalyzer.ExtractPropertyPaths(selector).FirstOrDefault();
        if (fieldName == null) throw new ArgumentException("Could not extract field name from expression.", nameof(selector));
        _policy.TimestampField = fieldName.ToLowerInvariant();
        return this;
    }

    /// <summary>
    /// Sets the interval between scheduled retention runs.
    /// Only relevant when <see cref="RetentionTrigger.Scheduled"/> is included.
    /// </summary>
    public RetentionPolicyBuilder<T> ScheduleInterval(TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(interval));
        _policy.ScheduledIntervalMs = (long)interval.TotalMilliseconds;
        return this;
    }

    /// <summary>
    /// Specifies when the policy is evaluated.
    /// Defaults to <see cref="RetentionTrigger.OnInsert"/> when not called.
    /// </summary>
    public RetentionPolicyBuilder<T> TriggerOn(RetentionTrigger triggers)
    {
        _policy.Triggers = triggers;
        return this;
    }

    internal RetentionPolicy Build() => _policy;
}
