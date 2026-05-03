using System;
using System.Collections.Generic;
using BLite.Bson;

namespace BLite.Core.CDC;

/// <summary>
/// Controls the behaviour of a <see cref="ChangeStreamObservable{TId,T}"/> or
/// <see cref="DynamicChangeStreamObservable"/> returned by <c>Watch()</c>.
/// </summary>
/// <remarks>
/// All properties are init-only so instances are immutable after construction.
/// Create with an object initializer:
/// <code>
/// var opts = new WatchOptions { CapturePayload = true, RevealPersonalData = false };
/// </code>
/// </remarks>
public sealed class WatchOptions
{
    /// <summary>
    /// When <see langword="true"/>, each event includes the full BSON payload of the
    /// changed document (after applying any masking rules).
    /// When <see langword="false"/> (default), only metadata is included and all other
    /// masking properties are irrelevant.
    /// </summary>
    public bool CapturePayload { get; init; } = false;

    /// <summary>
    /// When <see langword="true"/>, personal-data fields (annotated with
    /// <c>[PersonalData]</c> or registered via <c>HasPersonalData</c>) are delivered in
    /// clear. When <see langword="false"/> (default, GDPR-safe), they are replaced by
    /// <see cref="PersonalDataMaskValue"/>.
    /// Ignored when <see cref="CapturePayload"/> is <see langword="false"/>.
    /// </summary>
    public bool RevealPersonalData { get; init; } = false;

    /// <summary>
    /// The value substituted for personal-data fields when
    /// <see cref="RevealPersonalData"/> is <see langword="false"/>.
    /// Defaults to <c>"***"</c>. Set to <see cref="BsonValue.Null"/> to remove the key
    /// from the cloned payload entirely instead of replacing it.
    /// Ignored when <see cref="CapturePayload"/> is <see langword="false"/>.
    /// </summary>
    public BsonValue PersonalDataMaskValue { get; init; } = BsonValue.FromString("***");

    /// <summary>
    /// Field names to remove from the delivered payload.
    /// Applied after personal-data masking (rule 3 of the masking pipeline).
    /// Ignored when <see cref="IncludeOnlyFields"/> is non-null (allowlist wins).
    /// Ignored when <see cref="CapturePayload"/> is <see langword="false"/>.
    /// </summary>
    public IReadOnlyList<string> ExcludeFields { get; init; } = Array.Empty<string>();

    /// <summary>
    /// When non-null, the delivered payload contains <em>only</em> these fields in clear.
    /// This is an allowlist: both <see cref="ExcludeFields"/> and personal-data masking
    /// are skipped. Listing a personal-data field here is the consumer's explicit opt-in
    /// for that field.
    /// Ignored when <see cref="CapturePayload"/> is <see langword="false"/>.
    /// </summary>
    public IReadOnlyList<string>? IncludeOnlyFields { get; init; }
}
