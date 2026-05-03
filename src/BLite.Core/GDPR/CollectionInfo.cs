namespace BLite.Core.GDPR;

/// <summary>
/// Snapshot of a single BLite collection as reported by
/// <see cref="GdprEngineExtensions.InspectDatabase"/>.
/// </summary>
/// <param name="Name">Collection name.</param>
/// <param name="DocumentCount">
/// Always <c>0</c> in the current catalog-only snapshot — computing an exact count would
/// require a full collection scan.  Use <c>BLiteEngine.FindAllAsync</c> for a precise count.
/// </param>
/// <param name="StorageSizeBytes">
/// Size of the collection's storage in bytes.
/// In multi-file mode this is the size of the dedicated <c>&lt;collection&gt;.db</c> file
/// (obtained via a single <see cref="System.IO.FileInfo.Length"/> call).
/// In single-file mode this is always <c>0</c> because there is no dedicated file per collection.
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
