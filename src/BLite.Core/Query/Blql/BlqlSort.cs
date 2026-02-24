using System;
using System.Collections.Generic;
using BLite.Bson;

namespace BLite.Core.Query.Blql;

/// <summary>
/// Defines how query results should be sorted.
/// Supports multiple sort keys with independent ascending/descending direction per key.
/// 
/// Usage:
/// <code>
/// var sort = BlqlSort.By("age").ThenBy("name", descending: true);
/// </code>
/// </summary>
public sealed class BlqlSort
{
    private readonly List<SortKey> _keys = new();

    private BlqlSort() { }

    /// <summary>Creates a sort spec starting with the given field in ascending order.</summary>
    public static BlqlSort By(string field, bool descending = false)
    {
        var s = new BlqlSort();
        s._keys.Add(new SortKey(field.ToLowerInvariant(), descending));
        return s;
    }

    /// <summary>Creates a sort spec starting with the given field ascending.</summary>
    public static BlqlSort Ascending(string field) => By(field, descending: false);

    /// <summary>Creates a sort spec starting with the given field descending.</summary>
    public static BlqlSort Descending(string field) => By(field, descending: true);

    /// <summary>Adds an additional sort key.</summary>
    public BlqlSort ThenBy(string field, bool descending = false)
    {
        _keys.Add(new SortKey(field.ToLowerInvariant(), descending));
        return this;
    }

    /// <summary>Returns read-only access to the sort keys (in priority order).</summary>
    public IReadOnlyList<SortKey> Keys => _keys;

    /// <summary>Produces a Comparison delegate for use with List.Sort / OrderBy.</summary>
    public Comparison<BsonDocument> ToComparison()
    {
        return (a, b) =>
        {
            foreach (var key in _keys)
            {
                a.TryGetValue(key.Field, out var aVal);
                b.TryGetValue(key.Field, out var bVal);

                var cmp = BsonValueComparer.Compare(aVal, bVal);
                if (cmp != 0)
                    return key.Descending ? -cmp : cmp;
            }
            return 0;
        };
    }

    /// <summary>Represents a single sort field with its direction.</summary>
    public sealed class SortKey
    {
        public string Field { get; }
        public bool Descending { get; }

        internal SortKey(string field, bool descending)
        {
            Field = field;
            Descending = descending;
        }

        public override string ToString() => $"{Field}: {(Descending ? -1 : 1)}";
    }

    public override string ToString()
    {
        return "{ " + string.Join(", ", _keys) + " }";
    }
}
