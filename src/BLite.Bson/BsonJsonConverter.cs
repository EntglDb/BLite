// BLite.Bson — JSON ↔ BSON conversion
// Copyright (C) 2026 BLite Team
// Licensed under MIT — See LICENSE in the repository root for full license text.

using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;

namespace BLite.Bson;

/// <summary>
/// Converts between UTF-8 JSON and BLite's native BSON representation.
/// Uses System.Text.Json (built into the .NET runtime — zero extra dependencies).
///
/// JSON → BSON type mapping:
///   null            → BsonValue.Null
///   bool            → BsonValue.Boolean
///   number (int32)  → BsonValue.Int32
///   number (int64)  → BsonValue.Int64
///   number (double) → BsonValue.Double
///   string          → BsonValue.String  (auto-detected: ISO-8601 → DateTime, UUID → Guid)
///   object          → nested BsonDocument (shares the engine key map)
///   array           → BsonValue.Array
///   "_id" field     → BsonId (24-hex ObjectId / int / long / Guid / string fallback)
///
/// BSON → JSON type mapping (for display / export):
///   Null        → null
///   Boolean     → true / false
///   Int32/Int64 → number
///   Double/Dec  → number
///   String      → string
///   DateTime    → ISO-8601 string ("2026-02-23T10:00:00Z")
///   Guid        → string ("xxxxxxxx-xxxx-...")
///   ObjectId    → string (24-char lowercase hex)
///   Timestamp   → number (Unix seconds)
///   Binary      → base64 string
///   Array       → JSON array
///   Document    → JSON object
///   Coordinates → [lon, lat] (GeoJSON order)
/// </summary>
public static class BsonJsonConverter
{
    // ── JSON → BsonDocument ───────────────────────────────────────────────────

    /// <summary>
    /// Parses a JSON string (root must be <c>{…}</c>) into a <see cref="BsonDocument"/>.
    /// The document shares the engine's key maps so field names are compressed consistently.
    /// </summary>
    /// <exception cref="JsonException">Root element is not a JSON object.</exception>
    public static BsonDocument FromJson(
        string json,
        ConcurrentDictionary<string, ushort> keyMap,
        ConcurrentDictionary<ushort, string> reverseKeyMap)
    {
        using var jdoc = JsonDocument.Parse(json);

        if (jdoc.RootElement.ValueKind != JsonValueKind.Object)
            throw new JsonException("Root JSON element must be an object ({…}).");

        return ObjectElementToDocument(jdoc.RootElement, keyMap, reverseKeyMap);
    }

    /// <summary>
    /// Parses a UTF-8 JSON byte buffer (root must be <c>{…}</c>) into a <see cref="BsonDocument"/>.
    /// </summary>
    public static BsonDocument FromJson(
        ReadOnlyMemory<byte> jsonUtf8,
        ConcurrentDictionary<string, ushort> keyMap,
        ConcurrentDictionary<ushort, string> reverseKeyMap)
    {
        using var jdoc = JsonDocument.Parse(jsonUtf8);

        if (jdoc.RootElement.ValueKind != JsonValueKind.Object)
            throw new JsonException("Root JSON element must be an object ({…}).");

        return ObjectElementToDocument(jdoc.RootElement, keyMap, reverseKeyMap);
    }

    // ── BsonDocument → JSON ───────────────────────────────────────────────────

    /// <summary>Serializes a <see cref="BsonDocument"/> to a JSON string.</summary>
    public static string ToJson(BsonDocument document, bool indented = true)
    {
        var opts = new JsonWriterOptions { Indented = indented };
        using var ms     = new System.IO.MemoryStream();
        using var writer = new Utf8JsonWriter(ms, opts);
        WriteDocument(writer, document);
        writer.Flush();
        return Encoding.UTF8.GetString(ms.ToArray());
    }

    /// <summary>Serializes a single <see cref="BsonValue"/> to a JSON string.</summary>
    public static string ToJson(BsonValue value, bool indented = false)
    {
        var opts = new JsonWriterOptions { Indented = indented };
        using var ms     = new System.IO.MemoryStream();
        using var writer = new Utf8JsonWriter(ms, opts);
        WriteValue(writer, value);
        writer.Flush();
        return Encoding.UTF8.GetString(ms.ToArray());
    }

    // ── JSON → BSON: private helpers ──────────────────────────────────────────

    private static BsonDocument ObjectElementToDocument(
        JsonElement obj,
        ConcurrentDictionary<string, ushort> keyMap,
        ConcurrentDictionary<ushort, string> reverseKeyMap)
    {
        return BsonDocument.Create(keyMap, reverseKeyMap, builder =>
        {
            // "_id" must be written first via AddId
            if (obj.TryGetProperty("_id", out var idEl))
                builder.AddId(JsonElementToBsonId(idEl));

            foreach (var prop in obj.EnumerateObject())
            {
                if (prop.Name == "_id") continue;
                builder.Add(prop.Name, JsonElementToBsonValue(prop.Value, keyMap, reverseKeyMap));
            }
        });
    }

    private static BsonId JsonElementToBsonId(JsonElement el)
    {
        switch (el.ValueKind)
        {
            case JsonValueKind.Number:
                if (el.TryGetInt32(out var i32)) return new BsonId(i32);
                if (el.TryGetInt64(out var i64)) return new BsonId(i64);
                break;

            case JsonValueKind.String:
                var s = el.GetString()!;
                // 24-char hex → ObjectId
                if (s.Length == 24 && IsAllHex(s))
                    return new BsonId(ParseObjectId(s));
                // UUID → Guid
                if (Guid.TryParseExact(s, "D", out var g))
                    return new BsonId(g);
                // Generic string id
                return new BsonId(s);
        }

        // Fallback: generate a new ObjectId
        return new BsonId(ObjectId.NewObjectId());
    }

    private static BsonValue JsonElementToBsonValue(
        JsonElement el,
        ConcurrentDictionary<string, ushort> keyMap,
        ConcurrentDictionary<ushort, string> reverseKeyMap)
    {
        return el.ValueKind switch
        {
            JsonValueKind.Null   => BsonValue.Null,
            JsonValueKind.True   => BsonValue.FromBoolean(true),
            JsonValueKind.False  => BsonValue.FromBoolean(false),
            JsonValueKind.Number => JsonNumberToBsonValue(el),
            JsonValueKind.String => JsonStringToBsonValue(el.GetString()!),
            JsonValueKind.Array  => JsonArrayToBsonValue(el, keyMap, reverseKeyMap),
            JsonValueKind.Object => BsonValue.FromDocument(
                                        ObjectElementToDocument(el, keyMap, reverseKeyMap)),
            _ => BsonValue.Null
        };
    }

    private static BsonValue JsonNumberToBsonValue(JsonElement el)
    {
        if (el.TryGetInt32(out var i32))  return BsonValue.FromInt32(i32);
        if (el.TryGetInt64(out var i64))  return BsonValue.FromInt64(i64);
        if (el.TryGetDouble(out var dbl)) return BsonValue.FromDouble(dbl);
        if (el.TryGetDecimal(out var dec)) return BsonValue.FromDecimal(dec);
        return BsonValue.Null;
    }

    private static BsonValue JsonStringToBsonValue(string s)
    {
        // ISO-8601 datetime (with Z or offset)
        if (s.Length >= 20 && DateTime.TryParseExact(s,
            ["yyyy-MM-ddTHH:mm:ssZ", "yyyy-MM-ddTHH:mm:ss.fffZ",
             "yyyy-MM-ddTHH:mm:ss.fffffffZ",
             "yyyy-MM-ddTHH:mm:sszzz", "yyyy-MM-ddTHH:mm:ss.fffzzz"],
            System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.RoundtripKind,
            out var dt))
        {
            return BsonValue.FromDateTime(dt.ToUniversalTime());
        }

        // UUID / Guid (standard 36-char "D" format)
        if (s.Length == 36 && Guid.TryParseExact(s, "D", out var g))
            return BsonValue.FromGuid(g);

        return BsonValue.FromString(s);
    }

    private static BsonValue JsonArrayToBsonValue(
        JsonElement el,
        ConcurrentDictionary<string, ushort> keyMap,
        ConcurrentDictionary<ushort, string> reverseKeyMap)
    {
        var items = new List<BsonValue>(el.GetArrayLength());
        foreach (var item in el.EnumerateArray())
            items.Add(JsonElementToBsonValue(item, keyMap, reverseKeyMap));
        return BsonValue.FromArray(items);
    }

    // ── BSON → JSON: private helpers ──────────────────────────────────────────

    private static void WriteDocument(Utf8JsonWriter w, BsonDocument doc)
    {
        w.WriteStartObject();

        if (doc.TryGetId(out var id))
        {
            w.WritePropertyName("_id");
            WriteBsonId(w, id);
        }

        foreach (var (name, value) in doc.EnumerateFields())
        {
            if (name == "_id") continue;
            w.WritePropertyName(name);
            WriteValue(w, value);
        }

        w.WriteEndObject();
    }

    private static void WriteBsonId(Utf8JsonWriter w, BsonId id)
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

    private static void WriteValue(Utf8JsonWriter w, BsonValue val)
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

        if (val.IsCoordinates)
        {
            var (lat, lon) = val.AsCoordinates;   // GeoJSON: [lon, lat]
            w.WriteStartArray();
            w.WriteNumberValue(lon);
            w.WriteNumberValue(lat);
            w.WriteEndArray();
            return;
        }

        if (val.IsArray)
        {
            w.WriteStartArray();
            foreach (var item in val.AsArray)
                WriteValue(w, item);
            w.WriteEndArray();
            return;
        }

        if (val.IsDocument)
        {
            WriteDocument(w, val.AsDocument);
            return;
        }

        w.WriteNullValue(); // unknown type fallback
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    /// <summary>Converts a 24-char hex string to an <see cref="ObjectId"/>.</summary>
    private static ObjectId ParseObjectId(string hex)
    {
        var bytes = Convert.FromHexString(hex);   // net5+ / net10
        return new ObjectId(bytes);
    }

    private static bool IsAllHex(string s)
    {
        foreach (var c in s)
            if (!Uri.IsHexDigit(c)) return false;
        return true;
    }
}
