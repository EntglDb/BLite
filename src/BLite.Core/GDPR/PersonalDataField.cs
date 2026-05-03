namespace BLite.Core.GDPR;

/// <summary>
/// Describes a single property annotated as personal data on an entity model.
/// Produced by <see cref="PersonalDataMetadataCache"/> (reflection path) and by
/// the BLite source generator (compile-time path) for models routed through a
/// <c>DocumentDbContext</c>.
/// </summary>
/// <param name="PropertyName">The CLR property name (not the BSON field name).</param>
/// <param name="Sensitivity">Sensitivity tier of this field.</param>
/// <param name="IsTimestamp">
/// <see langword="true"/> when this field holds the timestamp used by retention/MaxAge policies.
/// </param>
/// <param name="BsonFieldName">
/// The actual BSON key used in serialized documents, as resolved by the source generator from
/// <c>[BsonProperty]</c>, <c>[JsonPropertyName]</c>, or <c>[Column]</c> attributes, falling back
/// to <c>PropertyName.ToLowerInvariant()</c>. <see langword="null"/> when this field was
/// produced by the reflection-fallback path without attribute information; in that case
/// <see cref="PayloadMask"/> falls back to <c>PropertyName.ToLowerInvariant()</c>.
/// </param>
public readonly record struct PersonalDataField(
    string PropertyName,
    DataSensitivity Sensitivity,
    bool IsTimestamp,
    string? BsonFieldName = null);
