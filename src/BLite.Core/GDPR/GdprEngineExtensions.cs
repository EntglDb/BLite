using System.Diagnostics.CodeAnalysis;

namespace BLite.Core.GDPR;

/// <summary>
/// GDPR primitives exposed as extension methods on <see cref="BLiteEngine"/>.
/// <list type="bullet">
///   <item><see cref="ExportSubjectDataAsync"/> — Art. 15 (access) + Art. 20 (portability).</item>
///   <item><see cref="InspectDatabase"/> — Art. 30 record-of-processing snapshot.</item>
/// </list>
/// A symmetric surface is provided on <see cref="DocumentDbContext"/> via
/// <see cref="GdprDocumentDbContextExtensions"/> so both engine-first (dynamic) and
/// code-first (typed) flows expose an identical GDPR API.
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
    ///   <item>Matched documents are buffered in memory per collection so that the returned
    ///         <see cref="SubjectDataReport"/> supports multiple export formats without re-scanning.</item>
    ///   <item>Collections that contain no matching documents appear in <see cref="SubjectDataReport.DataByCollection"/>
    ///         with an empty list — they are never omitted.</item>
    ///   <item>Cancellation is honoured at every <c>await</c> boundary.</item>
    ///   <item>Never throws for a missing field — returns an empty report instead.</item>
    /// </list>
    /// </remarks>
    public static Task<SubjectDataReport> ExportSubjectDataAsync(
        this BLiteEngine engine,
        SubjectQuery query,
        CancellationToken ct = default)
    {
        if (engine is null) throw new ArgumentNullException(nameof(engine));
        if (query is null) throw new ArgumentNullException(nameof(query));

        var targetCollections = query.Collections ?? engine.ListCollections();

        return GdprPrimitives.ExportSubjectDataAsync(
            targetCollections,
            scanFunc: (name, predicate, c) => engine.FindAsync(name, predicate, c),
            query,
            ct);
    }

    // ── Database inspection (Art. 30) ────────────────────────────────────────

    /// <summary>
    /// Returns a compliance snapshot of the database.
    /// The catalog is read from the in-memory structures already loaded by the engine.
    /// In multi-file mode, per-collection storage sizes are obtained with a lightweight
    /// <see cref="FileInfo.Length"/> lookup (one stat call per collection).
    /// </summary>
    [RequiresUnreferencedCode("InspectDatabase uses PersonalDataResolver which scans loaded assemblies for generated mapper classes.")]
    public static DatabaseInspectionReport InspectDatabase(this BLiteEngine engine)
    {
        if (engine is null) throw new ArgumentNullException(nameof(engine));

        return GdprPrimitives.InspectDatabase(engine.Storage, engine.DatabasePath);
    }
}
