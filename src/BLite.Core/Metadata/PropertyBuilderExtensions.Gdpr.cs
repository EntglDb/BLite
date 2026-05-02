using BLite.Core.GDPR;

namespace BLite.Core.Metadata;

/// <summary>
/// GDPR-related fluent extension for the model-builder property API.
/// This extension is recognised by the BLite source generator
/// (<c>PersonalDataAnalyzer</c>) when analysing <c>OnModelCreating</c>.
/// </summary>
public static class PropertyBuilderGdprExtensions
{
    /// <summary>
    /// Marks the property as containing personal data.
    /// The source generator records this annotation and emits a <c>PersonalDataFields</c>
    /// static member on the entity's generated mapper class.
    /// </summary>
    /// <param name="builder">The property builder returned by
    /// <c>EntityTypeBuilder&lt;T&gt;.Property(x => x.Prop)</c>.</param>
    /// <param name="sensitivity">
    /// Sensitivity tier.  Defaults to <see cref="DataSensitivity.Personal"/>.
    /// </param>
    /// <param name="isTimestamp">
    /// Set to <see langword="true"/> when this property holds the timestamp used by
    /// retention/MaxAge policies.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for fluent chaining.</returns>
    public static EntityTypeBuilder<T>.PropertyBuilder HasPersonalData<T>(
        this EntityTypeBuilder<T>.PropertyBuilder builder,
        DataSensitivity sensitivity = DataSensitivity.Personal,
        bool isTimestamp = false)
        where T : class
    {
        // The builder already holds the property name in its internal state
        // (via _propertyName). We record the personal-data intent on the parent
        // EntityTypeBuilder<T> so it is visible to the source generator at compile time.
        builder.SetPersonalData(sensitivity, isTimestamp);
        return builder;
    }
}
