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
public readonly record struct PersonalDataField(
    string PropertyName,
    DataSensitivity Sensitivity,
    bool IsTimestamp);
