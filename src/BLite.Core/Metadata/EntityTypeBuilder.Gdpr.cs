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
    /// When building a <see cref="DocumentDbContext"/>, the resolved mode is inspected by
    /// <c>GdprStrictValidator</c> at context construction time — the effective mode is the
    /// engine-wide <see cref="BLite.Core.KeyValue.BLiteKvOptions.DefaultGdprMode"/> escalated
    /// to <see cref="GdprMode.Strict"/> if any entity in the model has Strict configured.
    /// For <c>BLiteEngine</c> (dynamic path), only the engine-wide default is used.
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
