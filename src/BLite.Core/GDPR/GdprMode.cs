namespace BLite.Core.GDPR;

/// <summary>
/// Controls the GDPR enforcement profile for a collection or for the entire engine.
/// </summary>
/// <remarks>
/// Configure per-collection via <c>EntityTypeBuilder&lt;T&gt;.HasGdprMode(GdprMode)</c>
/// or the <c>[GdprMode(GdprMode.Strict)]</c> attribute.
/// Set an engine-wide default via <see cref="BLite.Core.KeyValue.BLiteKvOptions.DefaultGdprMode"/>.
/// </remarks>
public enum GdprMode : byte
{
    /// <summary>No GDPR enforcement — default behaviour (backwards-compatible).</summary>
    None = 0,

    /// <summary>
    /// Privacy-by-default (Art. 25).  At engine open the validator:
    /// <list type="bullet">
    ///   <item>Throws <see cref="InvalidOperationException"/> when encryption is absent.</item>
    ///   <item>Warns when no audit sink is registered.</item>
    ///   <item>Warns when a <c>[PersonalData]</c> collection has no retention policy.</item>
    ///   <item>Warns when <c>SecureEraseOnDelete</c> cannot be enforced.</item>
    /// </list>
    /// Strict mode never deletes data, never rotates keys, never modifies stored documents.
    /// </summary>
    Strict = 1,
}

/// <summary>
/// Declarative per-class GDPR mode annotation.
/// Place on an entity class to override the engine-wide
/// <see cref="BLite.Core.KeyValue.BLiteKvOptions.DefaultGdprMode"/>.
/// The attribute is read by <c>ModelBuilder.Entity&lt;T&gt;()</c> at model-building time;
/// the fluent <c>EntityTypeBuilder&lt;T&gt;.HasGdprMode()</c> call takes precedence when
/// called afterwards. Applies only to the <c>DocumentDbContext</c> (typed) path.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class GdprModeAttribute : Attribute
{
    /// <summary>The GDPR enforcement mode for the annotated entity type.</summary>
    public GdprMode Mode { get; }

    /// <param name="mode">The GDPR enforcement mode to apply.</param>
    public GdprModeAttribute(GdprMode mode) => Mode = mode;
}
