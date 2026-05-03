using System.Diagnostics.CodeAnalysis;
using BLite.Bson;

namespace BLite.Core.GDPR;

/// <summary>
/// GDPR primitives exposed as extension methods on <see cref="DocumentDbContext"/>,
/// providing the same surface as <see cref="GdprEngineExtensions"/> for code-first
/// (typed) consumers.  Both extension classes delegate to <see cref="GdprPrimitives"/>
/// so the resulting reports are byte-for-byte equivalent for the same database state.
/// </summary>
public static class GdprDocumentDbContextExtensions
{
    // ── Subject export (Art. 15 / Art. 20) ───────────────────────────────────

    /// <summary>
    /// Collects all documents across the database that contain a field
    /// <see cref="SubjectQuery.FieldName"/> equal to <see cref="SubjectQuery.FieldValue"/>
    /// and returns them in a <see cref="SubjectDataReport"/>.
    /// </summary>
    /// <remarks>
    /// Implemented by spinning up a transient <see cref="DynamicCollection"/> per
    /// scanned collection; the dynamic instance shares <see cref="DocumentDbContext"/>'s
    /// storage engine and transaction holder, and is disposed at the end of each scan.
    /// </remarks>
    public static Task<SubjectDataReport> ExportSubjectDataAsync(
        this DocumentDbContext context,
        SubjectQuery query,
        CancellationToken ct = default)
    {
        if (context is null) throw new ArgumentNullException(nameof(context));
        if (query is null) throw new ArgumentNullException(nameof(query));

        var storage = context.Storage;
        var targetCollections = query.Collections ?? GdprPrimitives.ListCollectionNames(storage);

        return GdprPrimitives.ExportSubjectDataAsync(
            targetCollections,
            scanFunc: (name, predicate, c) => ScanCollectionAsync(context, name, predicate, c),
            query,
            ct);
    }

    private static async IAsyncEnumerable<BsonDocument> ScanCollectionAsync(
        DocumentDbContext context,
        string collectionName,
        Func<BsonDocument, bool> predicate,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        using var col = new DynamicCollection(context.Storage, context, collectionName);
        await foreach (var doc in col.FindAsync(predicate, ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            yield return doc;
        }
    }

    // ── Database inspection (Art. 30) ────────────────────────────────────────

    /// <summary>
    /// Returns a compliance snapshot of the database accessed through this context.
    /// Equivalent to <see cref="GdprEngineExtensions.InspectDatabase"/> for the
    /// engine-first API.
    /// </summary>
    [RequiresUnreferencedCode("InspectDatabase uses PersonalDataResolver which scans loaded assemblies for generated mapper classes.")]
    public static DatabaseInspectionReport InspectDatabase(this DocumentDbContext context)
    {
        if (context is null) throw new ArgumentNullException(nameof(context));
        return GdprPrimitives.InspectDatabase(context.Storage, context.DatabaseFilePath);
    }
}
