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
}
