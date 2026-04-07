using BLite.Bson;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace BLite.Core.Query;

/// <summary>
/// AOT-safe, reflection-free factory for <see cref="BsonReaderPredicate"/> instances.
/// Every method builds a plan-time closure over the supplied value(s); the closure
/// is evaluated once per document during a BSON-level scan without deserialising
/// the whole document to a POCO.
///
/// Generated <c>{Entity}Filter</c> classes use this class to construct their
/// fallback predicates when no B-Tree index is available for a property.
/// </summary>
public static class BsonPredicateBuilder
{
    // ── Equality / comparison ─────────────────────────────────────────────────

    /// <summary>Returns a predicate that matches documents where <paramref name="field"/> equals <paramref name="value"/>.</summary>
    public static BsonReaderPredicate Eq<T>(string field, T value)
        => CreateCompare(field, (object?)value, ExpressionType.Equal)
           ?? (_ => false);

    /// <summary>Returns a predicate that matches documents where <paramref name="field"/> is greater than <paramref name="value"/>.</summary>
    public static BsonReaderPredicate Gt<T>(string field, T value)
        => CreateCompare(field, (object?)value, ExpressionType.GreaterThan)
           ?? (_ => false);

    /// <summary>Returns a predicate that matches documents where <paramref name="field"/> is greater than or equal to <paramref name="value"/>.</summary>
    public static BsonReaderPredicate Gte<T>(string field, T value)
        => CreateCompare(field, (object?)value, ExpressionType.GreaterThanOrEqual)
           ?? (_ => false);

    /// <summary>Returns a predicate that matches documents where <paramref name="field"/> is less than <paramref name="value"/>.</summary>
    public static BsonReaderPredicate Lt<T>(string field, T value)
        => CreateCompare(field, (object?)value, ExpressionType.LessThan)
           ?? (_ => false);

    /// <summary>Returns a predicate that matches documents where <paramref name="field"/> is less than or equal to <paramref name="value"/>.</summary>
    public static BsonReaderPredicate Lte<T>(string field, T value)
        => CreateCompare(field, (object?)value, ExpressionType.LessThanOrEqual)
           ?? (_ => false);

    /// <summary>
    /// Returns a predicate that matches documents where <paramref name="field"/> is
    /// between <paramref name="lo"/> and <paramref name="hi"/> (both inclusive).
    /// </summary>
    public static BsonReaderPredicate Between<T>(string field, T lo, T hi)
    {
        var gteP = Gte(field, lo);
        var lteP = Lte(field, hi);
        return reader => gteP(reader) && lteP(reader);
    }

    // ── String operations ─────────────────────────────────────────────────────

    /// <summary>Returns a predicate that matches documents where the string <paramref name="field"/> contains <paramref name="pattern"/>.</summary>
    public static BsonReaderPredicate Contains(string field, string pattern)
        => BsonExpressionEvaluator.CreateStringMethodPredicate(field, "Contains", pattern);

    /// <summary>Returns a predicate that matches documents where the string <paramref name="field"/> starts with <paramref name="pattern"/>.</summary>
    public static BsonReaderPredicate StartsWith(string field, string pattern)
        => BsonExpressionEvaluator.CreateStringMethodPredicate(field, "StartsWith", pattern);

    /// <summary>Returns a predicate that matches documents where the string <paramref name="field"/> ends with <paramref name="pattern"/>.</summary>
    public static BsonReaderPredicate EndsWith(string field, string pattern)
        => BsonExpressionEvaluator.CreateStringMethodPredicate(field, "EndsWith", pattern);

    // ── Null checks ───────────────────────────────────────────────────────────

    /// <summary>Returns a predicate that matches documents where <paramref name="field"/> is BSON Null or absent.</summary>
    public static BsonReaderPredicate IsNull(string field)
        => BsonExpressionEvaluator.CreateNullCheckPredicate(field, expectNull: true);

    /// <summary>Returns a predicate that matches documents where <paramref name="field"/> is not BSON Null and is present.</summary>
    public static BsonReaderPredicate IsNotNull(string field)
        => BsonExpressionEvaluator.CreateNullCheckPredicate(field, expectNull: false);

    // ── IN operator ───────────────────────────────────────────────────────────

    /// <summary>
    /// Returns a predicate that matches documents where <paramref name="field"/> equals any of
    /// <paramref name="values"/>. The set is built once at plan-time (O(1) lookup per document).
    /// </summary>
    public static BsonReaderPredicate In<T>(string field, IEnumerable<T> values)
    {
        var items = new HashSet<object?>();
        bool hasLong = false;

        foreach (var item in values)
        {
            if (item == null) { items.Add(null); continue; }

            var obj = (object)item;

            if (obj is Enum)
            {
                items.Add(Convert.ToInt64(obj));
                hasLong = true;
            }
            else if (obj is Guid g)
            {
                items.Add(g.ToString());
            }
            else if (obj is TimeSpan ts)
            {
                items.Add(ts.Ticks);
                hasLong = true;
            }
#if NET6_0_OR_GREATER
            else if (obj is DateOnly d)
            {
                items.Add(d.DayNumber);
            }
            else if (obj is TimeOnly to)
            {
                items.Add(to.Ticks);
                hasLong = true;
            }
#endif
            else
            {
                items.Add(obj);
            }
        }

        return BsonExpressionEvaluator.CreateInPredicateDirect(field, items, hasLong);
    }

    // ── Boolean combinators ───────────────────────────────────────────────────

    /// <summary>Returns a predicate that is true when both <paramref name="a"/> and <paramref name="b"/> are true.</summary>
    public static BsonReaderPredicate And(BsonReaderPredicate a, BsonReaderPredicate b)
        => reader => a(reader) && b(reader);

    /// <summary>Returns a predicate that is true when either <paramref name="a"/> or <paramref name="b"/> is true.</summary>
    public static BsonReaderPredicate Or(BsonReaderPredicate a, BsonReaderPredicate b)
        => reader => a(reader) || b(reader);

    /// <summary>Returns the logical negation of <paramref name="p"/>.</summary>
    public static BsonReaderPredicate Not(BsonReaderPredicate p)
        => reader => !p(reader);

    // ── Private helpers ───────────────────────────────────────────────────────

    private static BsonReaderPredicate? CreateCompare<T>(string field, T value, ExpressionType op)
        => BsonExpressionEvaluator.CreatePredicate(field, (object?)value, op);
}
