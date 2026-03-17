using System.Linq.Expressions;

namespace BLite.Core.Query;

internal class QueryModel
{
    public LambdaExpression? WhereClause { get; set; }
    public LambdaExpression? SelectClause { get; set; }
    public LambdaExpression? OrderByClause { get; set; }
    public int? Take { get; set; }
    public int? Skip { get; set; }
    public bool OrderDescending { get; set; }

    /// <summary>
    /// True when the expression tree contains operators that the direct pipeline cannot handle
    /// (GroupBy, Join, Sum/Average/Min/Max with selectors, etc.).
    /// The provider will fall back to <see cref="EnumerableRewriter"/> for these queries.
    /// </summary>
    public bool HasComplexOperators { get; set; }

    // ── Aggregate push-down ───────────────────────────────────────────────────

    /// <summary>
    /// Aggregate operator name ("Sum" or "Average") when detected as a terminal push-down candidate.
    /// Null means no aggregate push-down; in that case <see cref="HasComplexOperators"/> is set.
    /// </summary>
    public string? AggregateOp { get; set; }

    /// <summary>Selector lambda for the aggregate field, e.g. <c>o =&gt; o.Total</c>.</summary>
    public LambdaExpression? AggregateSelector { get; set; }

    /// <summary>
    /// True when the terminal operator is <c>Count()</c> or <c>LongCount()</c> with no predicate.
    /// Enables an O(n) primary-BTree key scan instead of full document deserialisation.
    /// </summary>
    public bool IsCountOnly { get; set; }
}
