namespace BLite.Core.GDPR;

/// <summary>
/// Describes a single index on a BLite collection, as reported by
/// <see cref="GdprEngineExtensions.InspectDatabase"/>.
/// </summary>
/// <param name="Name">The index name as stored in the collection catalog.</param>
/// <param name="Fields">Ordered list of BSON field paths covered by the index.</param>
/// <param name="IsUnique">
/// <see langword="true"/> when the index enforces uniqueness across its key fields.
/// </param>
/// <param name="IsEncrypted">
/// <see langword="true"/> when the parent database is encrypted
/// (index pages share the same encryption as data pages).
/// </param>
public sealed record IndexInfo(
    string Name,
    IReadOnlyList<string> Fields,
    bool IsUnique,
    bool IsEncrypted);
