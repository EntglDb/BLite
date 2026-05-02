namespace BLite.Core.GDPR;

/// <summary>
/// Serialization format used when exporting subject data via
/// <see cref="SubjectDataReport.WriteToFileAsync"/> and
/// <see cref="SubjectDataReport.WriteToStreamAsync"/>.
/// </summary>
public enum SubjectExportFormat : byte
{
    /// <summary>
    /// UTF-8 JSON: <c>{ "generatedAt": "&lt;ISO-8601&gt;", "subjectId": …, "data": { "&lt;col&gt;": [ … ] } }</c>
    /// </summary>
    Json = 1,

    /// <summary>
    /// CSV: one header row + one data row per document.
    /// Nested BSON values are JSON-serialised into the cell.
    /// </summary>
    Csv = 2,

    /// <summary>
    /// Length-prefixed BSON stream.  Each document is written as
    /// <c>[int32 size][BSON bytes]</c>; the stream is re-importable via the BSON reader.
    /// </summary>
    Bson = 3,
}
