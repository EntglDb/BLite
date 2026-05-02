namespace BLite.Core.GDPR;

/// <summary>
/// Marks a property as containing personal data subject to GDPR obligations.
/// Apply on entity-model properties to enable source-gen metadata emission and
/// runtime inspection via <see cref="PersonalDataMetadataCache"/>.
/// </summary>
/// <remarks>
/// The attribute is compile-time metadata only; it does not add runtime behaviour.
/// Actual GDPR enforcement (export, erasure) is performed by the host application
/// using BLite's GDPR primitives in <c>BLite.Core.GDPR</c>.
/// </remarks>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class PersonalDataAttribute : Attribute
{
    /// <summary>Sensitivity tier. Default: <see cref="DataSensitivity.Personal"/> (Art. 4(1)).</summary>
    public DataSensitivity Sensitivity { get; init; } = DataSensitivity.Personal;

    /// <summary>
    /// When <see langword="true"/>, this property holds the timestamp eligible for
    /// retention / MaxAge policies.  At most one property per entity should be marked
    /// <c>IsTimestamp = true</c>.
    /// </summary>
    public bool IsTimestamp { get; init; }
}
