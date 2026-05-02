using BLite.Bson;

namespace BLite.Core.GDPR;

/// <summary>
/// Describes a subject-data lookup for Art. 15 (access) and Art. 20 (portability) requests.
/// Pass an instance to <see cref="GdprEngineExtensions.ExportSubjectDataAsync"/>.
/// </summary>
/// <remarks>
/// Created by the host application; owned and disposed by the host application.
/// </remarks>
public sealed class SubjectQuery
{
    /// <summary>
    /// The BSON field name to match against (e.g. <c>"email"</c>, <c>"userId"</c>).
    /// An indexed lookup is attempted first; a full scan is the fallback.
    /// </summary>
    public required string FieldName { get; init; }

    /// <summary>
    /// The value the field must equal (e.g. <c>BsonValue.From("alice@example.com")</c>).
    /// </summary>
    public required BsonValue FieldValue { get; init; }

    /// <summary>
    /// Restricts the export to the named collections.
    /// <see langword="null"/> means <em>all</em> collections registered in the engine.
    /// </summary>
    public IReadOnlyList<string>? Collections { get; init; }

    /// <summary>
    /// Serialization format for <see cref="SubjectDataReport.WriteToFileAsync"/> and
    /// <see cref="SubjectDataReport.WriteToStreamAsync"/>.
    /// Defaults to <see cref="SubjectExportFormat.Json"/>.
    /// </summary>
    public SubjectExportFormat Format { get; init; } = SubjectExportFormat.Json;
}
