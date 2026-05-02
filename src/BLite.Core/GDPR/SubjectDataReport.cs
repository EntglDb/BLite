using System.Text;
using System.Text.Json;
using BLite.Bson;

namespace BLite.Core.GDPR;

/// <summary>
/// Contains the personal-data documents matching a <see cref="SubjectQuery"/>.
/// Produced by <see cref="GdprEngineExtensions.ExportSubjectDataAsync"/>; owned and
/// disposed by the host application.
/// </summary>
/// <remarks>
/// Implements <see cref="IAsyncDisposable"/> to accommodate future streaming variants;
/// the current in-memory implementation completes synchronously on <c>DisposeAsync</c>.
/// </remarks>
public sealed class SubjectDataReport : IAsyncDisposable
{
    /// <summary>UTC timestamp when this report was generated.</summary>
    public DateTimeOffset GeneratedAt { get; init; }

    /// <summary>The subject identifier used in the originating <see cref="SubjectQuery"/>.</summary>
    public BsonValue SubjectId { get; init; }

    /// <summary>
    /// Documents matched per collection.  Collections with no matching documents appear
    /// as empty lists — never <see langword="null"/> values.
    /// </summary>
    public IReadOnlyDictionary<string, IReadOnlyList<BsonDocument>> DataByCollection { get; init; }
        = new Dictionary<string, IReadOnlyList<BsonDocument>>();

    // ── Export ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Serializes this report to <paramref name="output"/> as UTF-8 JSON with the schema:
    /// <code>{ "generatedAt": "&lt;ISO-8601&gt;", "subjectId": …, "data": { "&lt;col&gt;": [ … ] } }</code>
    /// </summary>
    public async Task ExportAsJsonAsync(Stream output, CancellationToken ct = default)
    {
        var writerOpts = new JsonWriterOptions { Indented = true };
        await using var writer = new Utf8JsonWriter(output, writerOpts);

        writer.WriteStartObject();

        writer.WriteString("generatedAt", GeneratedAt.ToString("O"));
        writer.WritePropertyName("subjectId");
        WriteJsonBsonValue(writer, SubjectId);

        writer.WritePropertyName("data");
        writer.WriteStartObject();

        foreach (var (colName, docs) in DataByCollection)
        {
            ct.ThrowIfCancellationRequested();
            writer.WritePropertyName(colName);
            writer.WriteStartArray();
            foreach (var doc in docs)
            {
                WriteJsonDocument(writer, doc);
            }
            writer.WriteEndArray();
        }

        writer.WriteEndObject(); // data
        writer.WriteEndObject(); // root

        await writer.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Serializes the documents from <paramref name="collection"/> as a CSV stream
    /// (UTF-8 BOM-less).  The first row is the header; one row per document follows.
    /// Nested BSON values are JSON-serialised into the cell.
    /// </summary>
    public async Task ExportAsCsvAsync(Stream output, string collection, CancellationToken ct = default)
    {
        if (!DataByCollection.TryGetValue(collection, out var docs) || docs.Count == 0)
            return;

        // Collect all field names from all documents to form a stable header.
        var headerSet = new LinkedList<string>();
        var headerIndex = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var doc in docs)
        {
            foreach (var (name, _) in doc.EnumerateFields())
            {
                if (!headerIndex.ContainsKey(name))
                {
                    headerIndex[name] = headerSet.Count;
                    headerSet.AddLast(name);
                }
            }
        }

        var headers = new List<string>(headerSet);

        await using var sw = new StreamWriter(output, new UTF8Encoding(false), 4096, leaveOpen: true);

        // Header row
        await sw.WriteLineAsync(string.Join(",", headers.Select(CsvEscape))).ConfigureAwait(false);

        // Data rows
        foreach (var doc in docs)
        {
            ct.ThrowIfCancellationRequested();

            var cells = new string[headers.Count];
            for (int i = 0; i < headers.Count; i++)
                cells[i] = string.Empty;

            foreach (var (name, value) in doc.EnumerateFields())
            {
                if (headerIndex.TryGetValue(name, out var idx))
                    cells[idx] = CsvEscape(BsonValueToCsvCell(value));
            }

            await sw.WriteLineAsync(string.Join(",", cells)).ConfigureAwait(false);
        }

        await sw.FlushAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Serializes all matched documents to <paramref name="output"/> as a
    /// length-prefixed BSON stream.  Each document is written as
    /// <c>[int32 documentSize][BSON bytes]</c>.
    /// The stream can be read back by reading the 4-byte size prefix and then
    /// deserialising the raw BSON bytes.
    /// </summary>
    public async Task ExportAsBsonAsync(Stream output, CancellationToken ct = default)
    {
        var sizeBuffer = new byte[4];

        foreach (var (_, docs) in DataByCollection)
        {
            foreach (var doc in docs)
            {
                ct.ThrowIfCancellationRequested();

                var rawData = doc.RawData;
                // The raw data already contains the 4-byte BSON document size prefix.
                // We write it as-is so the stream is re-parseable by a BSON reader.
                System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(sizeBuffer, rawData.Length);
                await output.WriteAsync(sizeBuffer, 0, 4, ct).ConfigureAwait(false);
                var arr = rawData.ToArray();
                await output.WriteAsync(arr, 0, arr.Length, ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Writes this report to a file at <paramref name="path"/> using
    /// the format specified by the originating <see cref="SubjectQuery.Format"/>.
    /// The format is inferred from the file extension when <paramref name="format"/> is omitted.
    /// </summary>
    public async Task WriteToFileAsync(
        string path,
        SubjectExportFormat format = SubjectExportFormat.Json,
        CancellationToken ct = default)
    {
        await using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 65536, useAsync: true);
        await WriteToStreamAsync(fs, format, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes this report to <paramref name="stream"/> using the specified <paramref name="format"/>.
    /// For <see cref="SubjectExportFormat.Csv"/>, all collections are written sequentially.
    /// </summary>
    public async Task WriteToStreamAsync(
        Stream stream,
        SubjectExportFormat format = SubjectExportFormat.Json,
        CancellationToken ct = default)
    {
        switch (format)
        {
            case SubjectExportFormat.Json:
                await ExportAsJsonAsync(stream, ct).ConfigureAwait(false);
                break;
            case SubjectExportFormat.Csv:
                foreach (var colName in DataByCollection.Keys)
                {
                    ct.ThrowIfCancellationRequested();
                    await ExportAsCsvAsync(stream, colName, ct).ConfigureAwait(false);
                }
                break;
            case SubjectExportFormat.Bson:
                await ExportAsBsonAsync(stream, ct).ConfigureAwait(false);
                break;
            default:
                throw new ArgumentException($"Unknown SubjectExportFormat value: {format}", nameof(format));
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// <see cref="SubjectDataReport"/> does not own any unmanaged or async resources.
    /// <see cref="DataByCollection"/> holds only in-memory <see cref="BsonDocument"/> values
    /// produced by the engine scan; there are no streams or file handles to release.
    /// The pragma suppresses CA2215 because there is intentionally no base class to delegate to.
    /// </remarks>
#pragma warning disable CA2215
    public ValueTask DisposeAsync()
    {
#if NETSTANDARD2_1
        return new ValueTask(Task.CompletedTask);
#else
        return ValueTask.CompletedTask;
#endif
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private static void WriteJsonDocument(Utf8JsonWriter w, BsonDocument doc)
    {
        w.WriteStartObject();

        if (doc.TryGetId(out var id))
        {
            w.WritePropertyName("_id");
            WriteJsonBsonId(w, id);
        }

        foreach (var (name, value) in doc.EnumerateFields())
        {
            if (name == "_id") continue;
            w.WritePropertyName(name);
            WriteJsonBsonValue(w, value);
        }

        w.WriteEndObject();
    }

    private static void WriteJsonBsonId(Utf8JsonWriter w, BsonId id)
    {
        switch (id.Type)
        {
            case BsonIdType.ObjectId: w.WriteStringValue(id.AsObjectId().ToString()); break;
            case BsonIdType.Int32:    w.WriteNumberValue(id.AsInt32()); break;
            case BsonIdType.Int64:    w.WriteNumberValue(id.AsInt64()); break;
            case BsonIdType.Guid:     w.WriteStringValue(id.AsGuid().ToString("D")); break;
            case BsonIdType.String:   w.WriteStringValue(id.AsString()); break;
            default:                  w.WriteNullValue(); break;
        }
    }

    private static void WriteJsonBsonValue(Utf8JsonWriter w, BsonValue val)
    {
        if (val.IsNull)        { w.WriteNullValue();                                        return; }
        if (val.IsBoolean)     { w.WriteBooleanValue(val.AsBoolean);                        return; }
        if (val.IsInt32)       { w.WriteNumberValue(val.AsInt32);                           return; }
        if (val.IsInt64)       { w.WriteNumberValue(val.AsInt64);                           return; }
        if (val.IsDouble)      { w.WriteNumberValue(val.AsDouble);                          return; }
        if (val.IsDecimal)     { w.WriteNumberValue(val.AsDecimal);                         return; }
        if (val.IsString)      { w.WriteStringValue(val.AsString);                          return; }
        if (val.IsDateTime)    { w.WriteStringValue(val.AsDateTime.ToString("O"));          return; }
        if (val.IsGuid)        { w.WriteStringValue(val.AsGuid.ToString("D"));              return; }
        if (val.IsObjectId)    { w.WriteStringValue(val.AsObjectId.ToString());             return; }
        if (val.IsTimestamp)   { w.WriteNumberValue(val.AsTimestamp);                       return; }
        if (val.IsBinary)      { w.WriteStringValue(Convert.ToBase64String(val.AsBinary));  return; }

        if (val.IsArray)
        {
            w.WriteStartArray();
            foreach (var item in val.AsArray)
                WriteJsonBsonValue(w, item);
            w.WriteEndArray();
            return;
        }

        if (val.IsDocument)
        {
            WriteJsonDocument(w, val.AsDocument);
            return;
        }

        w.WriteNullValue();
    }

    private static string BsonValueToCsvCell(BsonValue value)
    {
        if (value.IsNull)     return string.Empty;
        if (value.IsBoolean)  return value.AsBoolean ? "true" : "false";
        if (value.IsInt32)    return value.AsInt32.ToString();
        if (value.IsInt64)    return value.AsInt64.ToString();
        if (value.IsDouble)   return value.AsDouble.ToString("G");
        if (value.IsDecimal)  return value.AsDecimal.ToString("G");
        if (value.IsString)   return value.AsString;
        if (value.IsDateTime) return value.AsDateTime.ToString("O");
        if (value.IsGuid)     return value.AsGuid.ToString("D");
        if (value.IsObjectId) return value.AsObjectId.ToString();

        // Complex values (arrays, nested documents, binary) → embedded JSON
        return BsonJsonConverter.ToJson(value, indented: false);
    }

    private static string CsvEscape(string value)
    {
        if (value.IndexOfAny(new[] { ',', '"', '\r', '\n' }) >= 0)
            return $"\"{value.Replace("\"", "\"\"")}\"";
        return value;
    }
}
