namespace BLite.Core.GDPR;

/// <summary>
/// Snapshot of a single BLite collection as reported by
/// <see cref="GdprEngineExtensions.InspectDatabase"/>.
/// </summary>
/// <param name="Name">Collection name.</param>
/// <param name="DocumentCount">
/// Approximate number of documents in the collection.
/// In single-file mode this is derived from a catalog estimate; in multi-file mode
/// it reflects the actual page count.  Use <c>BLiteEngine.FindAllAsync</c> for a precise count.
/// </param>
/// <param name="StorageSizeBytes">
/// Size of the collection's storage in bytes.
/// In multi-file mode this is the size of the dedicated collection file.
/// In single-file mode this is an estimate based on page count × page size.
/// </param>
/// <param name="Indexes">All secondary indexes registered on the collection.</param>
/// <param name="PersonalDataFields">
/// Names of fields annotated with <see cref="PersonalDataAttribute"/> (via the source generator
/// or reflection).  Empty when no personal-data annotations are present — never <see langword="null"/>.
/// </param>
/// <param name="RetentionPolicy">
/// Configured retention policy, or <see langword="null"/> if none is set.
/// </param>
public sealed record CollectionInfo(
    string Name,
    long DocumentCount,
    long StorageSizeBytes,
    IReadOnlyList<IndexInfo> Indexes,
    IReadOnlyList<string> PersonalDataFields,
    RetentionPolicyInfo? RetentionPolicy);
