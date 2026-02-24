using System;
using System.Text.Json;

namespace BLite.Core.Query.Blql;

/// <summary>
/// Parses MQL-style JSON sort strings into <see cref="BlqlSort"/> instances.
/// 
/// Syntax: a JSON object whose keys are field names and values are
/// <c>1</c> (ascending) or <c>-1</c> (descending).
/// 
/// <code>
/// { "name": 1 }                  // ascending by name
/// { "age": -1 }                  // descending by age
/// { "status": 1, "createdAt": -1 }  // multi-key sort
/// </code>
/// </summary>
public static class BlqlSortParser
{
    /// <summary>
    /// Parses a JSON sort string into a <see cref="BlqlSort"/>.
    /// Returns <c>null</c> if the string is null, empty or <c>{}</c>.
    /// </summary>
    public static BlqlSort? Parse(string? json)
    {
        if (string.IsNullOrWhiteSpace(json) || json.Trim() == "{}")
            return null;

        using var doc = JsonDocument.Parse(json, new JsonDocumentOptions { CommentHandling = JsonCommentHandling.Skip });
        var root = doc.RootElement;

        if (root.ValueKind != JsonValueKind.Object)
            throw new FormatException($"Sort must be a JSON object, got {root.ValueKind}");

        BlqlSort? sort = null;

        foreach (var prop in root.EnumerateObject())
        {
            if (prop.Value.ValueKind != JsonValueKind.Number)
                throw new FormatException($"Sort direction for '{prop.Name}' must be 1 or -1");

            var dir = prop.Value.GetInt32();
            if (dir != 1 && dir != -1)
                throw new FormatException($"Sort direction for '{prop.Name}' must be 1 (asc) or -1 (desc), got {dir}");

            bool descending = dir == -1;

            sort = sort == null
                ? BlqlSort.By(prop.Name, descending)
                : sort.ThenBy(prop.Name, descending);
        }

        return sort;
    }
}
