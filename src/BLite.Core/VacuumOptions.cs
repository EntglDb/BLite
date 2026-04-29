namespace BLite.Core;

/// <summary>
/// Options that control the behaviour of a <c>VacuumAsync</c> operation.
/// </summary>
public sealed class VacuumOptions
{
    /// <summary>
    /// When <c>true</c> (the default), every freed byte range on each page is
    /// overwritten with zeros after compaction. This prevents sensitive data
    /// (PII, credentials) from being recoverable via disk forensics and supports
    /// GDPR Art. 17 (right to erasure) compliance.
    /// </summary>
    public bool SecureErase { get; init; } = true;

    /// <summary>
    /// When <c>true</c> (the default), the database file is truncated after
    /// compaction to remove trailing free pages and shrink the file to its
    /// minimum required size.
    /// <para>
    /// Truncating a memory-mapped file requires unmapping, truncating, and
    /// remapping — the page-cache lifecycle is handled automatically.
    /// </para>
    /// <para>
    /// This option is silently ignored (treated as a no-op) for in-memory
    /// storage backends (e.g. <see cref="Storage.MemoryPageStorage"/>) which
    /// have no underlying file to truncate.
    /// </para>
    /// </summary>
    public bool TruncateFile { get; init; } = true;

    /// <summary>
    /// When <c>true</c>, all secondary indexes are rebuilt from scratch after
    /// the compaction pass.  This removes fragmentation from the index B-Trees
    /// that accumulates over time as documents are inserted, updated, and deleted.
    /// Defaults to <c>false</c> because index rebuilds are expensive for large
    /// collections; set to <c>true</c> only when index fragmentation is a concern.
    /// </summary>
    public bool RebuildIndexes { get; init; } = false;

    /// <summary>
    /// When non-<c>null</c>, VACUUM is applied to this collection only.
    /// When <c>null</c> (the default), all collections in the database are vacuumed.
    /// </summary>
    public string? CollectionName { get; init; }
}
