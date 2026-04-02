using System.Linq.Expressions;

namespace BLite.Core.Query;

internal class BTreeExpressionVisitor : ExpressionVisitor
{
    private readonly QueryModel _model = new();

    public QueryModel GetModel() => _model;

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.DeclaringType == typeof(Queryable))
        {
            switch (node.Method.Name)
            {
                case "Where":
                    VisitWhere(node);
                    return node;
                case "Select":
                    VisitSelect(node);
                    return node;
                case "OrderBy":
                case "OrderByDescending":
                    VisitOrderBy(node);
                    return node;
                case "Take":
                    VisitTake(node);
                    return node;
                case "Skip":
                    VisitSkip(node);
                    return node;
                case "Sum":
                case "Average":
                    VisitAggregate(node);
                    return node;
                case "Count":
                case "LongCount":
                    VisitCount(node);
                    return node;
                default:
                    // GroupBy, Join, Min, Max, etc. — operators the direct pipeline cannot model.
                    _model.HasComplexOperators = true;
                    break;
            }
        }
        
        return base.VisitMethodCall(node);
    }

    private void VisitWhere(MethodCallExpression node)
    {
        // Recursively visit source first (to preserve order or chained calls)
        Visit(node.Arguments[0]);

        var predicate = (UnaryExpression)node.Arguments[1];
        var lambda = (LambdaExpression)predicate.Operand;

        if (_model.WhereClause == null)
        {
            _model.WhereClause = lambda;
        }
        else
        {
            // Combine predicates (AND)
            var parameter = Expression.Parameter(lambda.Parameters[0].Type, "x");
            var body = Expression.AndAlso(
                Expression.Invoke(_model.WhereClause, parameter),
                Expression.Invoke(lambda, parameter)
            );
            _model.WhereClause = Expression.Lambda(body, parameter);
        }
    }

    private void VisitSelect(MethodCallExpression node)
    {
        Visit(node.Arguments[0]);
        var selector = (UnaryExpression)node.Arguments[1];
        _model.SelectClause = (LambdaExpression)selector.Operand;
    }

    private void VisitOrderBy(MethodCallExpression node)
    {
        Visit(node.Arguments[0]);
        var keySelector = (UnaryExpression)node.Arguments[1];
        _model.OrderByClause = (LambdaExpression)keySelector.Operand;
        _model.OrderDescending = node.Method.Name == "OrderByDescending";
    }

    private void VisitTake(MethodCallExpression node)
    {
        Visit(node.Arguments[0]);
        var countExpression = (ConstantExpression)node.Arguments[1];
        if (countExpression.Value != null)
            _model.Take = (int)countExpression.Value;
    }

    private void VisitSkip(MethodCallExpression node)
    {
        Visit(node.Arguments[0]);
        var countExpression = (ConstantExpression)node.Arguments[1];
        if (countExpression.Value != null)
            _model.Skip = (int)countExpression.Value;
    }

    /// <summary>
    /// Handles <c>Sum(selector)</c> and <c>Average(selector)</c> as push-down aggregates.
    /// When a single-field selector is detected we store it for BSON-level evaluation;
    /// otherwise we fall back to the EnumerableRewriter path.
    /// </summary>
    private void VisitAggregate(MethodCallExpression node)
    {
        Visit(node.Arguments[0]); // recurse into source
        if (node.Arguments.Count >= 2 && node.Arguments[1] is UnaryExpression quote)
        {
            // Sum<T, TResult>(source, Expression<Func<T, TResult>> selector)
            _model.AggregateOp = node.Method.Name;
            _model.AggregateSelector = (LambdaExpression)quote.Operand;
            // Always mark complex so that Sum/Average WITH a WHERE clause falls back to
            // EnumerableRewriter. The Execute fast path fires before this check when
            // WhereClause == null, so the no-WHERE optimisation is still active.
            _model.HasComplexOperators = true;
        }
        else
        {
            // Sum() / Average() with no selector — fall back to enumerator rewriter.
            _model.HasComplexOperators = true;
        }
    }

    /// <summary>
    /// Handles <c>Count()</c> / <c>LongCount()</c> as a push-down scalar.
    /// When there is no predicate, the provider can use the primary BTree key count instead
    /// of materialising all documents.
    /// </summary>
    private void VisitCount(MethodCallExpression node)
    {
        Visit(node.Arguments[0]); // recurse into source
        if (node.Arguments.Count == 1)
        {
            // Count() with no predicate — fast O(log n) + k BTree scan.
            _model.IsCountOnly = true;
        }
        else
        {
            // Count(predicate) — treat as a complex operator.
            _model.HasComplexOperators = true;
        }
    }
}
