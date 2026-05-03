using BLite.Core.GDPR;

namespace BLite.Core.Metadata;

/// <summary>
/// GDPR-mode fluent extension for <see cref="EntityTypeBuilder{T}"/>.
/// Mirrors the <c>HasRetentionPolicy</c> pattern.
/// </summary>
public static class EntityTypeBuilderGdprExtensions
{
    /// <summary>
    /// Configures the GDPR enforcement profile for this collection.
    /// The resolved mode is inspected by <c>GdprStrictValidator</c> at engine open.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="mode">The GDPR enforcement mode to apply.</param>
    /// <returns>The same <paramref name="builder"/> for fluent chaining.</returns>
    public static EntityTypeBuilder<T> HasGdprMode<T>(
        this EntityTypeBuilder<T> builder,
        GdprMode mode)
        where T : class
    {
        if (builder == null) throw new ArgumentNullException(nameof(builder));
        builder.GdprMode = mode;
        return builder;
    }
}
