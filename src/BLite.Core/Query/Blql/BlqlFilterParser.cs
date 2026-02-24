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
            throw new FormatException($"Expected a JSON array for $in/$nin, got {arr.ValueKind}");

        var result = new List<BsonValue>();
        foreach (var el in arr.EnumerateArray())
            result.Add(ReadScalar(el));

        return result.ToArray();
    }
}
