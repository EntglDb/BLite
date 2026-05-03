using System.Diagnostics.CodeAnalysis;
using BLite.Bson;
using BLite.Core.Storage;

namespace BLite.Core.GDPR;

/// <summary>
/// Shared, host-agnostic helpers used by both
/// <see cref="GdprEngineExtensions"/> (engine-first / dynamic) and
/// <see cref="GdprDocumentDbContextExtensions"/> (typed code-first).
/// Keeping the logic in a single internal helper guarantees that both surfaces
/// produce identical reports for the same database state.
/// </summary>
internal static class GdprPrimitives
{
    /// <summary>
    /// Delegate used by <see cref="ExportSubjectDataAsync"/> to scan a single collection
    /// for documents matching a predicate.  Implementations are free to pick the most
    /// efficient access path (e.g. a cached <c>DynamicCollection</c> on
    /// <see cref="BLiteEngine"/>, or a transient instance on a <see cref="DocumentDbContext"/>).
    /// </summary>
    internal delegate IAsyncEnumerable<BsonDocument> CollectionScanFunc(
        string collectionName,
        Func<BsonDocument, bool> predicate,
        CancellationToken ct);

    /// <summary>
    /// Core implementation of the subject-data export.  Iterates
    /// <paramref name="targetCollections"/>, runs <paramref name="scanFunc"/> per
    /// collection, buffers results in memory, and returns the assembled report.
    /// </summary>
    /// <remarks>
    /// <list type="bullet">
    ///   <item>Collections that contain no matching documents appear in the report
    ///         with an empty list — they are never omitted.</item>
    ///   <item>Cancellation is honoured at every <c>await</c> boundary.</item>
    ///   <item>Never throws for a missing field — returns an empty report instead.</item>
    /// </list>
    /// </remarks>
    internal static async Task<SubjectDataReport> ExportSubjectDataAsync(
        IEnumerable<string> targetCollections,
        CollectionScanFunc scanFunc,
        SubjectQuery query,
        CancellationToken ct)
    {
        var fieldName  = query.FieldName;
        var fieldValue = query.FieldValue;

        var dataByCollection = new Dictionary<string, IReadOnlyList<BsonDocument>>(
            StringComparer.OrdinalIgnoreCase);

        foreach (var colName in targetCollections)
        {
            ct.ThrowIfCancellationRequested();

            var matched = new List<BsonDocument>();

            await foreach (var doc in scanFunc(colName,
                d => FieldMatches(d, fieldName, fieldValue), ct).ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();
                matched.Add(doc);
            }

            dataByCollection[colName] = matched;
        }

        return new SubjectDataReport
        {
            GeneratedAt      = DateTimeOffset.UtcNow,
            SubjectId        = fieldValue,
            DataByCollection = dataByCollection,
        };
    }

    /// <summary>
    /// Core implementation of the database-inspection snapshot used by both
    /// <see cref="GdprEngineExtensions.InspectDatabase"/> and
    /// <see cref="GdprDocumentDbContextExtensions.InspectDatabase"/>.
    /// </summary>
    [RequiresUnreferencedCode("InspectDatabase uses PersonalDataResolver which scans loaded assemblies for generated mapper classes.")]
    internal static DatabaseInspectionReport InspectDatabase(StorageEngine storage, string? databasePath)
    {
        var isEncrypted    = storage.IsEncryptionEnabled;
        var isAuditEnabled = storage.AuditSink is not null;
        var isMultiFile    = storage.IsMultiFileMode;

        var allMetadata = storage.GetAllCollectionsMetadata();
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

            // Resolve personal-data field names via source-gen metadata (or reflection fallback).
            var pdFields = PersonalDataResolver.ResolveByCollectionName(meta.Name);
            var pdFieldNames = pdFields.Count == 0
                ? Array.Empty<string>()
                : pdFields.Select(f => f.PropertyName).ToArray();

            collections.Add(new CollectionInfo(
                Name: meta.Name,
                DocumentCount: 0,   // catalog-only snapshot: see CollectionInfo.DocumentCount XMLDoc.
                StorageSizeBytes: storageSizeBytes,
                Indexes: indexes,
                PersonalDataFields: pdFieldNames,
                RetentionPolicy: retentionInfo));
        }

        return new DatabaseInspectionReport(
            DatabasePath: databasePath ?? string.Empty,
            IsEncrypted: isEncrypted,
            IsAuditEnabled: isAuditEnabled,
            IsMultiFileMode: isMultiFile,
            Collections: collections);
    }

    /// <summary>
    /// Returns the on-disk size of a collection's storage in multi-file mode,
    /// or <c>0</c> in single-file mode (no per-collection accounting available).
    /// </summary>
    internal static long ComputeStorageSizeBytes(StorageEngine storage, string collectionName)
    {
        var dir = storage.CollectionDataDirectory;
        if (dir is not null)
        {
            // Multi-file mode: each collection has a deterministic filename
            // <collectionName.toLower()>.db in the collection-data directory.
            var filePath = Path.Combine(dir, collectionName.ToLowerInvariant() + ".db");
            try
            {
                var fi = new FileInfo(filePath);
                return fi.Exists ? fi.Length : 0;
            }
            catch { return 0; }
        }

        // Single-file mode: no easy per-collection size; return 0 (catalog-only, no IO).
        return 0;
    }

    /// <summary>
    /// Returns the names of every collection registered in the storage catalog.
    /// </summary>
    internal static IReadOnlyList<string> ListCollectionNames(StorageEngine storage)
    {
        var meta = storage.GetAllCollectionsMetadata();
        var names = new List<string>(meta.Count);
        foreach (var m in meta) names.Add(m.Name);
        return names;
    }

    private static bool FieldMatches(BsonDocument doc, string fieldName, BsonValue target)
    {
        if (!doc.TryGetValue(fieldName, out var val))
            return false;

        return val == target;
    }
}
