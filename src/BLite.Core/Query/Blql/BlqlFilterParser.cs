using System;
using System.Collections.Generic;
using System.Text.Json;
using BLite.Bson;

namespace BLite.Core.Query.Blql;

/// <summary>
/// Parses MQL-style JSON filter strings into <see cref="BlqlFilter"/> instances.
/// 
/// Supported syntax:
/// <code>
/// // Equality
/// { "field": "value" }
/// { "field": 42 }
///
/// // Comparison operators
/// { "age": { "$gt": 18 } }
/// { "age": { "$gte": 18 } }
/// { "age": { "$lt": 100 } }
/// { "age": { "$lte": 100 } }
/// { "name": { "$ne": "Bob" } }
///
/// // Logical operators (top-level explicit)
/// { "$and": [ { "age": { "$gt": 18 } }, { "status": "active" } ] }
/// { "$or":  [ { "role": "admin" }, { "role": "superadmin" } ] }
/// { "$nor": [ { "deleted": true } ] }
/// { "$not": { "deleted": true } }
///
/// // Multiple top-level fields → implicit AND
/// { "status": "active", "age": { "$gt": 18 } }
///
/// // Set operators
/// { "role": { "$in":  ["admin", "mod"] } }
/// { "role": { "$nin": ["banned"] } }
///
/// // Field tests
/// { "email": { "$exists": true } }
/// { "email": { "$exists": false } }
/// { "age":   { "$type": 16 } }      // 16 = BsonType.Int32
///
/// // Regex
/// { "name": { "$regex": "^Al" } }
///
/// // String operators
/// { "name": { "$startsWith": "Al" } }
/// { "email": { "$endsWith": "@gmail.com" } }
/// { "bio": { "$contains": "engineer" } }
///
/// // Array operators
/// { "scores": { "$elemMatch": { "$gte": 80, "$lt": 90 } } }   // scalar array
/// { "results": { "$elemMatch": { "score": { "$gte": 80 } } } } // document array
/// { "tags": { "$size": 3 } }
/// { "tags": { "$all": ["urgent", "reviewed"] } }
///
/// // Arithmetic
/// { "qty": { "$mod": [4, 0] } }
///
/// // Field-level negation
/// { "age": { "$not": { "$gt": 30 } } }
///
/// // Geospatial
/// { "location": { "$geoWithin": { "$box": [[minLon, minLat], [maxLon, maxLat]] } } }
/// { "location": { "$geoNear": { "$center": [lon, lat], "$maxDistance": 5.0 } } }
///
/// // Vector search
/// { "embedding": { "$nearVector": { "$vector": [0.1, 0.2, ...], "$k": 10, "$metric": "cosine" } } }
///
/// // Null equality
/// { "field": null }
/// </code>
/// </summary>
public static class BlqlFilterParser
{
    /// <summary>
    /// Parses a JSON string into a <see cref="BlqlFilter"/>.
    /// Throws <see cref="FormatException"/> for unrecognised syntax.
    /// </summary>
    public static BlqlFilter Parse(string json)
    {
        if (string.IsNullOrWhiteSpace(json) || json.Trim() == "{}")
            return BlqlFilter.Empty;

        using var doc = JsonDocument.Parse(json, new JsonDocumentOptions { CommentHandling = JsonCommentHandling.Skip });
        return ParseObject(doc.RootElement);
    }

    // ── Core recursive parser ───────────────────────────────────────────────

    private static BlqlFilter ParseObject(JsonElement obj)
    {
        if (obj.ValueKind != JsonValueKind.Object)
            throw new FormatException($"Expected a JSON object, got {obj.ValueKind}");

        var filters = new List<BlqlFilter>();

        foreach (var prop in obj.EnumerateObject())
        {
            var key = prop.Name;
            var val = prop.Value;

            // Top-level logical operators
            if (key.Equals("$and", StringComparison.OrdinalIgnoreCase))
            {
                filters.Add(BlqlFilter.And(ParseArray(val)));
                continue;
            }
            if (key.Equals("$or", StringComparison.OrdinalIgnoreCase))
            {
                filters.Add(BlqlFilter.Or(ParseArray(val)));
                continue;
            }
            if (key.Equals("$nor", StringComparison.OrdinalIgnoreCase))
            {
                filters.Add(BlqlFilter.Nor(ParseArray(val)));
                continue;
            }
            if (key.Equals("$not", StringComparison.OrdinalIgnoreCase))
            {
                filters.Add(BlqlFilter.Not(ParseObject(val)));
                continue;
            }

            // Reject any other unknown top-level $ operator (e.g. $where, $expr, $function).
            // Passing them through as field names would silently match nothing and mislead callers.
            if (key.StartsWith('$'))
                throw new FormatException(
                    $"Unknown or unsupported BLQL top-level operator: '{key}'. " +
                    $"Supported logical operators: $and, $or, $nor, $not.");

            // Field-level condition
            filters.Add(ParseFieldCondition(key, val));
        }

        return filters.Count switch
        {
            0 => BlqlFilter.Empty,
            1 => filters[0],
            _ => BlqlFilter.And(filters)
        };
    }

    private static BlqlFilter ParseFieldCondition(string field, JsonElement val)
    {
        // Null literal → field must be absent/null
        if (val.ValueKind == JsonValueKind.Null)
            return BlqlFilter.IsNull(field);

        // Scalar value → equality
        if (val.ValueKind != JsonValueKind.Object)
            return BlqlFilter.Eq(field, ReadScalar(val));

        // Object → operator map { "$op": value }
        var opFilters = new List<BlqlFilter>();

        foreach (var op in val.EnumerateObject())
        {
            opFilters.Add(op.Name.ToLowerInvariant() switch
            {
                "$eq"     => BlqlFilter.Eq(field, ReadScalar(op.Value)),
                "$ne"     => BlqlFilter.Ne(field, ReadScalar(op.Value)),
                "$gt"     => BlqlFilter.Gt(field, ReadScalar(op.Value)),
                "$gte"    => BlqlFilter.Gte(field, ReadScalar(op.Value)),
                "$lt"     => BlqlFilter.Lt(field, ReadScalar(op.Value)),
                "$lte"    => BlqlFilter.Lte(field, ReadScalar(op.Value)),
                "$in"     => BlqlFilter.In(field, ReadValueArray(op.Value)),
                "$nin"    => BlqlFilter.Nin(field, ReadValueArray(op.Value)),
                "$exists" => op.Value.ValueKind is JsonValueKind.True or JsonValueKind.False
                    ? BlqlFilter.Exists(field, op.Value.GetBoolean())
                    : throw new FormatException($"$exists requires a boolean value (true/false), got {op.Value.ValueKind}"),
                "$type"   => BlqlFilter.Type(field, (BsonType)op.Value.GetByte()),
                "$regex"  => op.Value.ValueKind == System.Text.Json.JsonValueKind.String
                    ? BlqlFilter.Regex(
                         field,
                         op.Value.GetString()!,
                         System.Text.RegularExpressions.RegexOptions.NonBacktracking)
                    : throw new FormatException(
                         $"$regex requires a string value, got {op.Value.ValueKind}"),

                // String operators
                "$startswith" => op.Value.ValueKind == JsonValueKind.String
                    ? BlqlFilter.StartsWith(field, op.Value.GetString()!)
                    : throw new FormatException($"$startsWith requires a string value, got {op.Value.ValueKind}"),
                "$endswith"   => op.Value.ValueKind == JsonValueKind.String
                    ? BlqlFilter.EndsWith(field, op.Value.GetString()!)
                    : throw new FormatException($"$endsWith requires a string value, got {op.Value.ValueKind}"),
                "$contains"   => op.Value.ValueKind == JsonValueKind.String
                    ? BlqlFilter.Contains(field, op.Value.GetString()!)
                    : throw new FormatException($"$contains requires a string value, got {op.Value.ValueKind}"),

                // Array operators
                "$elemmatch"  => ParseElemMatch(field, op.Value),
                "$size"       => BlqlFilter.Size(field, op.Value.GetInt32()),
                "$all"        => BlqlFilter.All(field, ReadValueArray(op.Value)),

                // Arithmetic
                "$mod"        => ParseMod(field, op.Value),

                // Field-level negation
                "$not"        => op.Value.ValueKind == JsonValueKind.Object
                    ? BlqlFilter.Not(ParseFieldCondition(field, op.Value))
                    : throw new FormatException($"$not requires an object with operator conditions, got {op.Value.ValueKind}"),

                // Geospatial operators
                "$geowithin"  => ParseGeoWithin(field, op.Value),
                "$geonear"    => ParseGeoNear(field, op.Value),

                // Vector search
                "$nearvector" => ParseNearVector(field, op.Value),

                _ => throw new FormatException($"Unknown BLQL operator: {op.Name}")
            });
        }

        return opFilters.Count switch
        {
            0 => BlqlFilter.Empty,
            1 => opFilters[0],
            _ => BlqlFilter.And(opFilters)  // e.g. { "$gte": 0, "$lte": 100 }
        };
    }

    private static BlqlFilter[] ParseArray(JsonElement arr)
    {
        if (arr.ValueKind != JsonValueKind.Array)
            throw new FormatException($"Expected a JSON array, got {arr.ValueKind}");

        var result = new List<BlqlFilter>();
        foreach (var el in arr.EnumerateArray())
            result.Add(ParseObject(el));

        return result.ToArray();
    }

    // ── Value helpers ───────────────────────────────────────────────────────

    private static BsonValue ReadScalar(JsonElement el) => el.ValueKind switch
    {
        JsonValueKind.String  => BsonValue.FromString(el.GetString()!),
        JsonValueKind.True    => BsonValue.FromBoolean(true),
        JsonValueKind.False   => BsonValue.FromBoolean(false),
        JsonValueKind.Null    => BsonValue.Null,
        JsonValueKind.Number  => ReadNumber(el),
        _ => throw new FormatException($"Unsupported JSON value kind for scalar: {el.ValueKind}")
    };

    private static BsonValue ReadNumber(JsonElement el)
    {
        // Try int32, then int64, then double
        if (el.TryGetInt32(out var i32)) return BsonValue.FromInt32(i32);
        if (el.TryGetInt64(out var i64)) return BsonValue.FromInt64(i64);
        return BsonValue.FromDouble(el.GetDouble());
    }

    private static BsonValue[] ReadValueArray(JsonElement arr)
    {
        if (arr.ValueKind != JsonValueKind.Array)
            throw new FormatException($"Expected a JSON array for $in/$nin/$all, got {arr.ValueKind}");

        var result = new List<BsonValue>();
        foreach (var el in arr.EnumerateArray())
            result.Add(ReadScalar(el));

        return result.ToArray();
    }

    // ── Complex operator parsers ────────────────────────────────────────────

    /// <summary>
    /// Parses { "$elemMatch": { ... } }.
    /// Inner object can be an operator map (for scalar arrays) or a field condition map (for document arrays).
    /// For scalar arrays: { "$elemMatch": { "$gte": 80, "$lt": 90 } }
    /// For document arrays: { "$elemMatch": { "score": { "$gte": 80 } } }
    /// </summary>
    private static BlqlFilter ParseElemMatch(string field, JsonElement val)
    {
        if (val.ValueKind != JsonValueKind.Object)
            throw new FormatException($"$elemMatch requires an object, got {val.ValueKind}");

        // Detect whether inner keys are operators ($...) or field names.
        // If all keys start with $, treat as scalar element conditions using the parent field name.
        bool allOperators = true;
        foreach (var prop in val.EnumerateObject())
        {
            if (!prop.Name.StartsWith('$'))
            {
                allOperators = false;
                break;
            }
        }

        BlqlFilter condition = allOperators
            ? ParseFieldCondition(field, val)   // { "$gte": 80 } → compared using the parent field name
            : ParseObject(val);                  // { "score": { "$gte": 80 } } → sub-document filter

        return BlqlFilter.ElemMatch(field, condition);
    }

    /// <summary>
    /// Parses { "$mod": [divisor, remainder] }.
    /// </summary>
    private static BlqlFilter ParseMod(string field, JsonElement val)
    {
        if (val.ValueKind != JsonValueKind.Array)
            throw new FormatException("$mod requires an array [divisor, remainder]");

        var items = new List<long>();
        foreach (var el in val.EnumerateArray())
            items.Add(el.GetInt64());

        if (items.Count != 2)
            throw new FormatException("$mod requires exactly 2 elements: [divisor, remainder]");

        if (items[0] == 0)
            throw new FormatException("$mod divisor must not be zero");

        return BlqlFilter.Mod(field, items[0], items[1]);
    }

    /// <summary>
    /// Parses { "$geoWithin": { "$box": [[minLon, minLat], [maxLon, maxLat]] } }.
    /// </summary>
    private static BlqlFilter ParseGeoWithin(string field, JsonElement val)
    {
        if (val.ValueKind != JsonValueKind.Object)
            throw new FormatException("$geoWithin requires an object");

        JsonElement box = default;
        bool found = false;
        foreach (var prop in val.EnumerateObject())
        {
            if (prop.Name.Equals("$box", StringComparison.OrdinalIgnoreCase))
            {
                box = prop.Value;
                found = true;
                break;
            }
        }

        if (!found || box.ValueKind != JsonValueKind.Array || box.GetArrayLength() != 2)
            throw new FormatException("$geoWithin requires { \"$box\": [[minLon, minLat], [maxLon, maxLat]] }");

        var min = box[0];
        var max = box[1];
        if (min.ValueKind != JsonValueKind.Array || min.GetArrayLength() != 2 ||
            max.ValueKind != JsonValueKind.Array || max.GetArrayLength() != 2)
            throw new FormatException("$geoWithin.$box elements must be [lon, lat] pairs");

        return BlqlFilter.GeoWithin(field,
            min[0].GetDouble(), min[1].GetDouble(),
            max[0].GetDouble(), max[1].GetDouble());
    }

    /// <summary>
    /// Parses { "$geoNear": { "$center": [lon, lat], "$maxDistance": radiusKm } }.
    /// </summary>
    private static BlqlFilter ParseGeoNear(string field, JsonElement val)
    {
        if (val.ValueKind != JsonValueKind.Object)
            throw new FormatException("$geoNear requires an object");

        double? lon = null, lat = null, maxDistance = null;

        foreach (var prop in val.EnumerateObject())
        {
            switch (prop.Name.ToLowerInvariant())
            {
                case "$center":
                    if (prop.Value.ValueKind != JsonValueKind.Array || prop.Value.GetArrayLength() != 2)
                        throw new FormatException("$geoNear.$center must be a [lon, lat] array");
                    lon = prop.Value[0].GetDouble();
                    lat = prop.Value[1].GetDouble();
                    break;
                case "$maxdistance":
                    maxDistance = prop.Value.GetDouble();
                    break;
                default:
                    throw new FormatException($"Unknown $geoNear property: {prop.Name}");
            }
        }

        if (lon == null || lat == null || maxDistance == null)
            throw new FormatException("$geoNear requires $center and $maxDistance");

        return BlqlFilter.GeoNear(field, lon.Value, lat.Value, maxDistance.Value);
    }

    /// <summary>
    /// Parses { "$nearVector": { "$vector": [...], "$k": 10, "$metric": "cosine" } }.
    /// </summary>
    private static BlqlFilter ParseNearVector(string field, JsonElement val)
    {
        if (val.ValueKind != JsonValueKind.Object)
            throw new FormatException("$nearVector requires an object");

        float[]? vector = null;
        int k = 10;
        string metric = "cosine";

        foreach (var prop in val.EnumerateObject())
        {
            switch (prop.Name.ToLowerInvariant())
            {
                case "$vector":
                    if (prop.Value.ValueKind != JsonValueKind.Array)
                        throw new FormatException("$nearVector.$vector must be an array of numbers");
                    var floats = new List<float>();
                    foreach (var el in prop.Value.EnumerateArray())
                        floats.Add(el.GetSingle());
                    vector = floats.ToArray();
                    break;
                case "$k":
                    k = prop.Value.GetInt32();
                    break;
                case "$metric":
                    metric = prop.Value.GetString() ?? "cosine";
                    break;
                default:
                    throw new FormatException($"Unknown $nearVector property: {prop.Name}");
            }
        }

        if (vector == null)
            throw new FormatException("$nearVector requires $vector array");

        return BlqlFilter.NearVector(field, vector, k, metric);
    }
}
