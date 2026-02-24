using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using BLite.Bson;

namespace BLite.Core.Query.Blql;

/// <summary>
/// Defines which fields to include or exclude from query results.
/// Inspired by MQL projection semantics.
/// 
/// Usage:
/// <code>
/// // Include only specific fields (+ _id always included unless excluded):
/// var proj = BlqlProjection.Include("name", "age");
///
/// // Exclude specific fields:
/// var proj = BlqlProjection.Exclude("password", "internalField");
/// </code>
/// 
/// Note: Include and Exclude cannot be mixed (except for _id).
/// </summary>
public sealed class BlqlProjection
{
    private readonly HashSet<string> _fields;
    private readonly bool _isInclude; // true = include mode, false = exclude mode

    private BlqlProjection(HashSet<string> fields, bool isInclude)
    {
        _fields = fields;
        _isInclude = isInclude;
    }

    /// <summary>
    /// A no-op projection that returns the full document.
    /// </summary>
    public static readonly BlqlProjection All = new(new HashSet<string>(), isInclude: false);

    /// <summary>
    /// Returns only the specified fields. The <c>_id</c> field is always included unless explicitly excluded.
    /// </summary>
    public static BlqlProjection Include(params string[] fields)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        set.Add("_id"); // always include _id in include-mode unless overridden
        foreach (var f in fields) set.Add(f.ToLowerInvariant());
        return new BlqlProjection(set, isInclude: true);
    }

    /// <summary>
    /// Returns all fields except the specified ones.
    /// </summary>
    public static BlqlProjection Exclude(params string[] fields)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var f in fields) set.Add(f.ToLowerInvariant());
        return new BlqlProjection(set, isInclude: false);
    }

    /// <summary>
    /// Returns true if this is an all-fields (identity) projection.
    /// </summary>
    public bool IsIdentity => !_isInclude && _fields.Count == 0;

    /// <summary>
    /// Applies this projection to a document, returning a new BsonDocument with only the projected fields.
    /// Returns the original document if the projection is identity.
    /// </summary>
    public BsonDocument Apply(BsonDocument document, ConcurrentDictionary<string, ushort> keyMap, ConcurrentDictionary<ushort, string> reverseKeyMap)
    {
        if (IsIdentity) return document;

        var fields = document.EnumerateFields();
        var builder = new BsonDocumentBuilder(keyMap, reverseKeyMap);

        foreach (var (name, value) in fields)
        {
            bool include = _isInclude
                ? _fields.Contains(name)
                : !_fields.Contains(name);

            if (include)
                builder.Add(name, value);
        }

        return builder.Build();
    }

    public override string ToString()
    {
        if (IsIdentity) return "{}";
        var parts = new List<string>();
        foreach (var f in _fields) parts.Add($"\"{f}\": {(_isInclude ? 1 : 0)}");
        return "{ " + string.Join(", ", parts) + " }";
    }
}
