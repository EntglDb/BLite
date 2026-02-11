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
                    break;
                case "Select":
                    VisitSelect(node);
                    break;
                case "OrderBy":
                case "OrderByDescending":
                    VisitOrderBy(node);
                    break;
                case "Take":
                    VisitTake(node);
                    break;
                case "Skip":
                    VisitSkip(node);
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
}
