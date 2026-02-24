using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using BLite.Bson;

namespace BLite.Core.Query.Blql;

/// <summary>
/// Represents a BLQL filter condition — the "where clause" of a query.
/// Inspired by MQL (MongoDB Query Language) semantics.
/// 
/// Usage examples:
/// <code>
/// // Equality: { "name": "Alice" }
/// var f = BlqlFilter.Eq("name", "Alice");
///
/// // Comparison: { "age": { "$gt": 25 } }
/// var f = BlqlFilter.Gt("age", 25);
///
/// // Compound AND: { "$and": [ { "age": { "$gt": 25 } }, { "status": "active" } ] }
/// var f = BlqlFilter.And(BlqlFilter.Gt("age", 25), BlqlFilter.Eq("status", "active"));
///
/// // IN operator: { "role": { "$in": ["admin", "moderator"] } }
/// var f = BlqlFilter.In("role", "admin", "moderator");
/// </code>
/// </summary>
public abstract class BlqlFilter
{
    /// <summary>A filter that matches every document (no-op).</summary>
    public static readonly BlqlFilter Empty = new EmptyFilter();

    // ── Factory: comparison ─────────────────────────────────────────────────

    /// <summary>Field equals value. { field: value }</summary>
    public static BlqlFilter Eq(string field, BsonValue value)
        => new CompareFilter(field, CompareOp.Eq, value);

    /// <summary>Field equals value (implicit conversion from primitives).</summary>
    public static BlqlFilter Eq(string field, int value) => Eq(field, BsonValue.FromInt32(value));
    public static BlqlFilter Eq(string field, long value) => Eq(field, BsonValue.FromInt64(value));
    public static BlqlFilter Eq(string field, double value) => Eq(field, BsonValue.FromDouble(value));
    public static BlqlFilter Eq(string field, string value) => Eq(field, BsonValue.FromString(value));
    public static BlqlFilter Eq(string field, bool value) => Eq(field, BsonValue.FromBoolean(value));
    public static BlqlFilter Eq(string field, ObjectId value) => Eq(field, BsonValue.FromObjectId(value));
    public static BlqlFilter Eq(string field, DateTime value) => Eq(field, BsonValue.FromDateTime(value));

    /// <summary>Field not equals value. { field: { "$ne": value } }</summary>
    public static BlqlFilter Ne(string field, BsonValue value)
        => new CompareFilter(field, CompareOp.Ne, value);

    /// <summary>Field greater than value. { field: { "$gt": value } }</summary>
    public static BlqlFilter Gt(string field, BsonValue value)
        => new CompareFilter(field, CompareOp.Gt, value);

    public static BlqlFilter Gt(string field, int value) => Gt(field, BsonValue.FromInt32(value));
    public static BlqlFilter Gt(string field, long value) => Gt(field, BsonValue.FromInt64(value));
    public static BlqlFilter Gt(string field, double value) => Gt(field, BsonValue.FromDouble(value));
    public static BlqlFilter Gt(string field, DateTime value) => Gt(field, BsonValue.FromDateTime(value));

    /// <summary>Field greater than or equal. { field: { "$gte": value } }</summary>
    public static BlqlFilter Gte(string field, BsonValue value)
        => new CompareFilter(field, CompareOp.Gte, value);

    public static BlqlFilter Gte(string field, int value) => Gte(field, BsonValue.FromInt32(value));
    public static BlqlFilter Gte(string field, long value) => Gte(field, BsonValue.FromInt64(value));
    public static BlqlFilter Gte(string field, double value) => Gte(field, BsonValue.FromDouble(value));
    public static BlqlFilter Gte(string field, DateTime value) => Gte(field, BsonValue.FromDateTime(value));

    /// <summary>Field less than value. { field: { "$lt": value } }</summary>
    public static BlqlFilter Lt(string field, BsonValue value)
        => new CompareFilter(field, CompareOp.Lt, value);

    public static BlqlFilter Lt(string field, int value) => Lt(field, BsonValue.FromInt32(value));
    public static BlqlFilter Lt(string field, long value) => Lt(field, BsonValue.FromInt64(value));
    public static BlqlFilter Lt(string field, double value) => Lt(field, BsonValue.FromDouble(value));
    public static BlqlFilter Lt(string field, DateTime value) => Lt(field, BsonValue.FromDateTime(value));

    /// <summary>Field less than or equal. { field: { "$lte": value } }</summary>
    public static BlqlFilter Lte(string field, BsonValue value)
        => new CompareFilter(field, CompareOp.Lte, value);

    public static BlqlFilter Lte(string field, int value) => Lte(field, BsonValue.FromInt32(value));
    public static BlqlFilter Lte(string field, long value) => Lte(field, BsonValue.FromInt64(value));
    public static BlqlFilter Lte(string field, double value) => Lte(field, BsonValue.FromDouble(value));
    public static BlqlFilter Lte(string field, DateTime value) => Lte(field, BsonValue.FromDateTime(value));

    // ── Factory: set operators ──────────────────────────────────────────────

    /// <summary>Field value is in the given set. { field: { "$in": [values] } }</summary>
    public static BlqlFilter In(string field, IEnumerable<BsonValue> values)
        => new InFilter(field, values.ToArray(), negate: false);

    /// <inheritdoc cref="In(string, IEnumerable{BsonValue})"/>
    public static BlqlFilter In(string field, params BsonValue[] values)
        => new InFilter(field, values, negate: false);

    public static BlqlFilter In(string field, params string[] values)
        => In(field, values.Select(BsonValue.FromString));

    public static BlqlFilter In(string field, params int[] values)
        => In(field, values.Select(BsonValue.FromInt32));

    /// <summary>Field value is NOT in the given set. { field: { "$nin": [values] } }</summary>
    public static BlqlFilter Nin(string field, IEnumerable<BsonValue> values)
        => new InFilter(field, values.ToArray(), negate: true);

    /// <inheritdoc cref="Nin(string, IEnumerable{BsonValue})"/>
    public static BlqlFilter Nin(string field, params BsonValue[] values)
        => new InFilter(field, values, negate: true);

    public static BlqlFilter Nin(string field, params string[] values)
        => Nin(field, values.Select(BsonValue.FromString));

    // ── Factory: logical operators ─────────────────────────────────────────

    /// <summary>All filters must match. { "$and": [...] }</summary>
    public static BlqlFilter And(params BlqlFilter[] filters)
        => new LogicalFilter(LogicalOp.And, filters);

    /// <inheritdoc cref="And(BlqlFilter[])"/>
    public static BlqlFilter And(IEnumerable<BlqlFilter> filters)
        => new LogicalFilter(LogicalOp.And, filters.ToArray());

    /// <summary>At least one filter must match. { "$or": [...] }</summary>
    public static BlqlFilter Or(params BlqlFilter[] filters)
        => new LogicalFilter(LogicalOp.Or, filters);

    /// <inheritdoc cref="Or(BlqlFilter[])"/>
    public static BlqlFilter Or(IEnumerable<BlqlFilter> filters)
        => new LogicalFilter(LogicalOp.Or, filters.ToArray());

    /// <summary>Negates a filter. { "$nor": [filter] }</summary>
    public static BlqlFilter Nor(params BlqlFilter[] filters)
        => new LogicalFilter(LogicalOp.Nor, filters);

    /// <summary>Inverts the result of any filter. { field: { "$not": {...} } }</summary>
    public static BlqlFilter Not(BlqlFilter inner)
        => new NotFilter(inner);

    // ── Factory: field tests ───────────────────────────────────────────────

    /// <summary>Field exists (or doesn't). { field: { "$exists": true/false } }</summary>
    public static BlqlFilter Exists(string field, bool shouldExist = true)
        => new ExistsFilter(field, shouldExist);

    /// <summary>Field type equals the given BsonType. { field: { "$type": type } }</summary>
    public static BlqlFilter Type(string field, BsonType bsonType)
        => new TypeFilter(field, bsonType);

    /// <summary>Field is null. { field: { "$eq": null } }</summary>
    public static BlqlFilter IsNull(string field)
        => new ExistsFilter(field, exists: false);

    // ── Factory: string operators ──────────────────────────────────────────

    /// <summary>Field matches a regular expression. { field: { "$regex": pattern } }</summary>
    public static BlqlFilter Regex(string field, string pattern, RegexOptions options = RegexOptions.None)
    {
        // NonBacktracking and Compiled are mutually exclusive in .NET
        var finalOptions = options.HasFlag(RegexOptions.NonBacktracking)
            ? options
            : options | RegexOptions.Compiled;
        return new RegexFilter(field, new Regex(pattern, finalOptions));
    }

    /// <inheritdoc cref="Regex(string, string, RegexOptions)"/>
    public static BlqlFilter Regex(string field, Regex regex)
        => new RegexFilter(field, regex);

    // ── Factory: range helpers ─────────────────────────────────────────────

    /// <summary>Combines Gte + Lte into a closed range.</summary>
    public static BlqlFilter Between(string field, BsonValue min, BsonValue max)
        => And(Gte(field, min), Lte(field, max));

    public static BlqlFilter Between(string field, int min, int max)
        => Between(field, BsonValue.FromInt32(min), BsonValue.FromInt32(max));

    public static BlqlFilter Between(string field, long min, long max)
        => Between(field, BsonValue.FromInt64(min), BsonValue.FromInt64(max));

    public static BlqlFilter Between(string field, double min, double max)
        => Between(field, BsonValue.FromDouble(min), BsonValue.FromDouble(max));

    public static BlqlFilter Between(string field, DateTime min, DateTime max)
        => Between(field, BsonValue.FromDateTime(min), BsonValue.FromDateTime(max));

    // ── Fluent AND/OR shortcuts on instances ───────────────────────────────

    /// <summary>Returns a new filter that is the logical AND of this and <paramref name="other"/>.</summary>
    public BlqlFilter AndAlso(BlqlFilter other) => And(this, other);

    /// <summary>Returns a new filter that is the logical OR of this and <paramref name="other"/>.</summary>
    public BlqlFilter OrElse(BlqlFilter other) => Or(this, other);

    // ── Evaluation ─────────────────────────────────────────────────────────

    /// <summary>
    /// Returns true if the given document satisfies this filter.
    /// </summary>
    public abstract bool Matches(BsonDocument document);

    // ── Internal node types ────────────────────────────────────────────────

    private enum CompareOp { Eq, Ne, Gt, Gte, Lt, Lte }
    private enum LogicalOp { And, Or, Nor }

    private sealed class EmptyFilter : BlqlFilter
    {
        public override bool Matches(BsonDocument document) => true;
        public override string ToString() => "{}";
    }

    private sealed class CompareFilter : BlqlFilter
    {
        private readonly string _field;
        private readonly CompareOp _op;
        private readonly BsonValue _value;

        public CompareFilter(string field, CompareOp op, BsonValue value)
        {
            _field = field.ToLowerInvariant();
            _op = op;
            _value = value;
        }

        public override bool Matches(BsonDocument document)
        {
            if (!document.TryGetValue(_field, out var docValue))
            {
                // Field not present: only $ne matches (null != anything)
                return _op == CompareOp.Ne;
            }

            var cmp = BsonValueComparer.Compare(docValue, _value);
            return _op switch
            {
                CompareOp.Eq  => cmp == 0,
                CompareOp.Ne  => cmp != 0,
                CompareOp.Gt  => cmp > 0,
                CompareOp.Gte => cmp >= 0,
                CompareOp.Lt  => cmp < 0,
                CompareOp.Lte => cmp <= 0,
                _ => false
            };
        }

        public override string ToString()
        {
            var opStr = _op switch
            {
                CompareOp.Eq  => "$eq",
                CompareOp.Ne  => "$ne",
                CompareOp.Gt  => "$gt",
                CompareOp.Gte => "$gte",
                CompareOp.Lt  => "$lt",
                CompareOp.Lte => "$lte",
                _ => "?"
            };
            return _op == CompareOp.Eq
                ? $"{{ \"{_field}\": {_value} }}"
                : $"{{ \"{_field}\": {{ \"{opStr}\": {_value} }} }}";
        }
    }

    private sealed class InFilter : BlqlFilter
    {
        private readonly string _field;
        private readonly BsonValue[] _values;
        private readonly bool _negate;

        public InFilter(string field, BsonValue[] values, bool negate)
        {
            _field = field.ToLowerInvariant();
            _values = values;
            _negate = negate;
        }

        public override bool Matches(BsonDocument document)
        {
            if (!document.TryGetValue(_field, out var docValue))
                return _negate; // field absent: $in=false, $nin=true

            var found = _values.Any(v => BsonValueComparer.Compare(docValue, v) == 0);
            return _negate ? !found : found;
        }

        public override string ToString()
        {
            var op = _negate ? "$nin" : "$in";
            var vals = string.Join(", ", _values.Select(v => v.ToString()));
            return $"{{ \"{_field}\": {{ \"{op}\": [{vals}] }} }}";
        }
    }

    private sealed class LogicalFilter : BlqlFilter
    {
        private readonly LogicalOp _op;
        private readonly BlqlFilter[] _filters;

        public LogicalFilter(LogicalOp op, BlqlFilter[] filters)
        {
            _op = op;
            _filters = filters;
        }

        public override bool Matches(BsonDocument document)
        {
            return _op switch
            {
                LogicalOp.And => _filters.All(f => f.Matches(document)),
                LogicalOp.Or  => _filters.Any(f => f.Matches(document)),
                LogicalOp.Nor => _filters.All(f => !f.Matches(document)),
                _ => false
            };
        }

        public override string ToString()
        {
            var op = _op switch { LogicalOp.And => "$and", LogicalOp.Or => "$or", LogicalOp.Nor => "$nor", _ => "?" };
            var parts = string.Join(", ", _filters.Select(f => f.ToString()));
            return $"{{ \"{op}\": [{parts}] }}";
        }
    }

    private sealed class NotFilter : BlqlFilter
    {
        private readonly BlqlFilter _inner;

        public NotFilter(BlqlFilter inner) { _inner = inner; }

        public override bool Matches(BsonDocument document) => !_inner.Matches(document);

        public override string ToString() => $"{{ \"$not\": {_inner} }}";
    }

    private sealed class ExistsFilter : BlqlFilter
    {
        private readonly string _field;
        private readonly bool _exists;

        public ExistsFilter(string field, bool exists)
        {
            _field = field.ToLowerInvariant();
            _exists = exists;
        }

        public override bool Matches(BsonDocument document)
        {
            var hasField = document.TryGetValue(_field, out var val) && !val.IsNull;
            return _exists ? hasField : !hasField;
        }

        public override string ToString()
            => $"{{ \"{_field}\": {{ \"$exists\": {(_exists ? "true" : "false")} }} }}";
    }

    private sealed class TypeFilter : BlqlFilter
    {
        private readonly string _field;
        private readonly BsonType _bsonType;

        public TypeFilter(string field, BsonType bsonType)
        {
            _field = field.ToLowerInvariant();
            _bsonType = bsonType;
        }

        public override bool Matches(BsonDocument document)
        {
            if (!document.TryGetValue(_field, out var val)) return false;
            return val.Type == _bsonType;
        }

        public override string ToString()
            => $"{{ \"{_field}\": {{ \"$type\": {(byte)_bsonType} }} }}";
    }

    private sealed class RegexFilter : BlqlFilter
    {
        private readonly string _field;
        private readonly Regex _regex;

        public RegexFilter(string field, Regex regex)
        {
            _field = field.ToLowerInvariant();
            _regex = regex;
        }

        public override bool Matches(BsonDocument document)
        {
            if (!document.TryGetValue(_field, out var val)) return false;
            if (val.Type != BsonType.String) return false;
            return _regex.IsMatch(val.AsString);
        }

        public override string ToString()
            => $"{{ \"{_field}\": {{ \"$regex\": \"{_regex}\" }} }}";
    }
}
