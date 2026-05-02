using BLite.Core.Retention;

namespace BLite.Core.GDPR;

/// <summary>
/// Read-only projection of the existing <see cref="BLite.Core.Retention.RetentionPolicy"/>
/// for use in <see cref="CollectionInfo"/> inspection reports.
/// </summary>
/// <param name="TimestampField">
/// Name of the BSON field used as the document timestamp, or <see langword="null"/> if not set.
/// </param>
/// <param name="MaxAge">
/// Maximum document age derived from <see cref="RetentionPolicy.MaxAgeMs"/>,
/// or <see langword="null"/> when no age limit is configured.
/// </param>
/// <param name="MaxDocumentCount">
/// Maximum number of documents, or <see langword="null"/> when unlimited.
/// </param>
/// <param name="MaxSizeBytes">
/// Maximum total size of collection data in bytes, or <see langword="null"/> when unlimited.
/// </param>
/// <param name="Triggers">
/// Human-readable representation of the <see cref="RetentionPolicy.Triggers"/> flags
/// (e.g. <c>"OnInsert, Scheduled"</c>).
/// </param>
public sealed record RetentionPolicyInfo(
    string? TimestampField,
    TimeSpan? MaxAge,
    long? MaxDocumentCount,
    long? MaxSizeBytes,
    string Triggers)
{
    /// <summary>
    /// Projects a <see cref="RetentionPolicy"/> into a <see cref="RetentionPolicyInfo"/>.
    /// Returns <see langword="null"/> when <paramref name="policy"/> is <see langword="null"/>.
    /// </summary>
    internal static RetentionPolicyInfo? From(RetentionPolicy? policy)
    {
        if (policy is null) return null;

        return new RetentionPolicyInfo(
            TimestampField: policy.TimestampField,
            MaxAge: policy.MaxAgeMs > 0 ? TimeSpan.FromMilliseconds(policy.MaxAgeMs) : null,
            MaxDocumentCount: policy.MaxDocumentCount > 0 ? policy.MaxDocumentCount : null,
            MaxSizeBytes: policy.MaxSizeBytes > 0 ? policy.MaxSizeBytes : null,
            Triggers: policy.Triggers.ToString());
    }
}
