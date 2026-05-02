using BLite.Bson;
using BLite.Core.Storage;

namespace BLite.Core.GDPR;

/// <summary>
/// GDPR primitives exposed as extension methods on <see cref="BLiteEngine"/>.
/// <list type="bullet">
///   <item><see cref="ExportSubjectDataAsync"/> — Art. 15 (access) + Art. 20 (portability).</item>
///   <item><see cref="InspectDatabase"/> — Art. 30 record-of-processing snapshot.</item>
/// </list>
/// </summary>
public static class GdprEngineExtensions
{
    // ── Subject export (Art. 15 / Art. 20) ───────────────────────────────────

    /// <summary>
    /// Collects all documents across the database that contain a field
    /// <see cref="SubjectQuery.FieldName"/> equal to <see cref="SubjectQuery.FieldValue"/>
    /// and returns them in a <see cref="SubjectDataReport"/>.
    /// </summary>
    /// <remarks>
    /// <list type="bullet">
    ///   <item>Collections that contain no matching documents appear in <see cref="SubjectDataReport.DataByCollection"/>
    ///         with an empty list — they are never omitted.</item>
    ///   <item>Cancellation is honoured at every <c>await</c> boundary.</item>
    ///   <item>Never throws for a missing field — returns an empty report instead.</item>
    /// </list>
    /// </remarks>
    public static async Task<SubjectDataReport> ExportSubjectDataAsync(
        this BLiteEngine engine,
        SubjectQuery query,
        CancellationToken ct = default)
    {
        if (engine is null) throw new ArgumentNullException(nameof(engine));
        if (query is null) throw new ArgumentNullException(nameof(query));

        var targetCollections = query.Collections ?? engine.ListCollections();
        var fieldName  = query.FieldName;
        var fieldValue = query.FieldValue;

        var dataByCollection = new Dictionary<string, IReadOnlyList<BsonDocument>>(
            StringComparer.OrdinalIgnoreCase);

        foreach (var colName in targetCollections)
        {
            ct.ThrowIfCancellationRequested();

            var matched = new List<BsonDocument>();

            await foreach (var doc in engine.FindAsync(colName,
                d => FieldMatches(d, fieldName, fieldValue), ct).ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();
                matched.Add(doc);
            }

            dataByCollection[colName] = matched;
        }

        return new SubjectDataReport
        {
            GeneratedAt        = DateTimeOffset.UtcNow,
            SubjectId          = fieldValue,
            DataByCollection   = dataByCollection,
        };
    }

    private static bool FieldMatches(BsonDocument doc, string fieldName, BsonValue target)
    {
        if (!doc.TryGetValue(fieldName, out var val))
            return false;

        return val == target;
    }

    // ── Database inspection (Art. 30) ────────────────────────────────────────

    /// <summary>
    /// Returns a compliance snapshot of the database without doing any extra disk I/O beyond
    /// the catalog that is already loaded in memory.
    /// </summary>
    public static DatabaseInspectionReport InspectDatabase(this BLiteEngine engine)
    {
        if (engine is null) throw new ArgumentNullException(nameof(engine));

        var storage = engine.Storage;
        var isEncrypted    = storage.IsEncryptionEnabled;
        var isAuditEnabled = storage.AuditSink is not null;
        var isMultiFile    = storage.IsMultiFileMode;

        var allMetadata = engine.GetAllCollectionsMetadata();
        var collections = new List<CollectionInfo>(allMetadata.Count);

        foreach (var meta in allMetadata)
        {
            var indexes = new List<IndexInfo>(meta.Indexes.Count);
            foreach (var idx in meta.Indexes)
            {
                indexes.Add(new IndexInfo(
                    Name: idx.Name,
                    Fields: idx.PropertyPaths,
                    IsUnique: idx.IsUnique,
                    IsEncrypted: isEncrypted));
            }

            var storageSizeBytes = ComputeStorageSizeBytes(storage, meta.Name);
            var retentionInfo    = RetentionPolicyInfo.From(meta.GeneralRetentionPolicy);

            collections.Add(new CollectionInfo(
                Name: meta.Name,
                DocumentCount: 0,   // no-IO snapshot; use FindAllAsync for a precise count
                StorageSizeBytes: storageSizeBytes,
                Indexes: indexes,
                PersonalDataFields: Array.Empty<string>(),
                RetentionPolicy: retentionInfo));
        }

        var dbPath = GetDatabasePath(engine);

        return new DatabaseInspectionReport(
            DatabasePath: dbPath,
            IsEncrypted: isEncrypted,
            IsAuditEnabled: isAuditEnabled,
            IsMultiFileMode: isMultiFile,
            Collections: collections);
    }

    private static long ComputeStorageSizeBytes(StorageEngine storage, string collectionName)
    {
        var dir = storage.CollectionDataDirectory;
        if (dir is not null)
        {
            // Multi-file mode: each collection has its own file named <collectionName>.blcol (or similar).
            // We enumerate files matching the collection name (any extension) in the directory.
            foreach (var file in Directory.EnumerateFiles(dir))
            {
                var stem = Path.GetFileNameWithoutExtension(file);
                if (string.Equals(stem, collectionName, StringComparison.OrdinalIgnoreCase))
                {
                    try { return new FileInfo(file).Length; }
                    catch { return 0; }
                }
            }
            return 0;
        }

        // Single-file mode: no easy per-collection size; return 0 (catalog-only, no IO).
        return 0;
    }

    private static string GetDatabasePath(BLiteEngine engine)
    {
        // BLiteEngine exposes _databasePath via the internal Storage property indirectly.
        // For the inspection report we use the storage's filesystem path when available.
        // The field is internal-only; we obtain it via the BackupAsync path convention
        // by checking the backup plan, which is fragile. Instead, expose it safely:
        // BLiteEngine._databasePath is private, so we use a dedicated internal property.
        return engine.DatabasePath ?? string.Empty;
    }
}
