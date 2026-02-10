using System.Linq.Expressions;

namespace DocumentDb.Core.Query;

internal class QueryModel
{
    public LambdaExpression? WhereClause { get; set; }
    public LambdaExpression? SelectClause { get; set; }
    public LambdaExpression? OrderByClause { get; set; }
    public int? Take { get; set; }
    public int? Skip { get; set; }
    public bool OrderDescending { get; set; }
}
