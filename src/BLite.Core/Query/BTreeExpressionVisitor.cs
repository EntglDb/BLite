// NOTE: BTreeExpressionVisitor builds expression trees and uses Expression.Lambda
// which requires dynamic code. Warnings are suppressed here because this class
// is used internally from annotated LINQ execution paths.
#pragma warning disable IL3050
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
                case "Min":
                case "Max":
                    VisitAggregate(node);
                    return node;
                case "Count":
                case "LongCount":
                    VisitCount(node);
                    return node;
                case "First":
                case "FirstOrDefault":
                case "Single":
                case "SingleOrDefault":
                case "Last":
                case "LastOrDefault":
                case "Any":
                    VisitElementTerminal(node);
                    return node;
                default:
                    // GroupBy, Join, etc. — operators the direct pipeline cannot model.
                    // Only the source (Arguments[0]) belongs to the chain this model describes.
                    // The other arguments are lambdas or other sequences (a Join inner, a Concat
                    // argument): a Where, OrderBy or Take found in them describes that sequence,
                    // not the root collection, so they are not visited (#186).
                    _model.HasComplexOperators = true;
                    if (node.Arguments.Count > 0)
                        Visit(node.Arguments[0]);
                    return node;
            }
        }
        
        return base.VisitMethodCall(node);
    }

    /// <summary>
    /// Folds <c>Where(p)</c> into the WHERE clause the provider fetches with, but only where
    /// <see cref="CanFoldPredicate"/> allows it: on the root chain, with nothing but plain
    /// <c>Where</c> calls and orderings below it, and with a predicate over the entities. Anywhere
    /// else, such as after <c>GroupBy</c>, <c>Take</c>, <c>SkipWhile</c> or <c>Select</c>, or with
    /// the indexed overload <c>Where((x, i) =&gt; ...)</c>, filtering the fetch would change the
    /// result or cannot be done at all, so the call is left to the in-memory replay (#186).
    /// </summary>
    private void VisitWhere(MethodCallExpression node)
    {
        // Recursively visit source first (to preserve order or chained calls)
        Visit(node.Arguments[0]);

        if (node.Arguments[1] is UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression predicate }
            && CanFoldPredicate(node.Arguments[0], predicate))
            AddPredicate(predicate);
        else
            _model.HasComplexOperators = true;
    }

    private void AddPredicate(LambdaExpression lambda)
    {
        if (_model.WhereClause == null)
        {
            _model.WhereClause = lambda;
        }
        else
        {
            // Combine predicates with AND, inlining both lambda bodies onto a single
            // shared parameter instead of wrapping them in Expression.Invoke.
            // Expression.Invoke produces an InvocationExpression that IndexOptimizer /
            // ParseSimplePredicate cannot traverse, causing every chained .Where() call
            // to fall back to a full scan even when the predicate targets an indexed field.
            // By rewriting both bodies to share one ParameterExpression the resulting
            // BinaryExpression.AndAlso stays fully transparent to the optimizer.
            var parameter = Expression.Parameter(lambda.Parameters[0].Type, "x");
            var leftBody  = new BsonProjectionCompiler.ParameterReplacer(
                                _model.WhereClause.Parameters[0], parameter)
                                .Visit(_model.WhereClause.Body);
            var rightBody = new BsonProjectionCompiler.ParameterReplacer(
                                lambda.Parameters[0], parameter)
                                .Visit(lambda.Body);
            _model.WhereClause = Expression.Lambda(
                Expression.AndAlso(leftBody, rightBody), parameter);
        }
    }

    private void VisitSelect(MethodCallExpression node)
    {
        Visit(node.Arguments[0]);
        var selector = (UnaryExpression)node.Arguments[1];
        _model.SelectClause = (LambdaExpression)selector.Operand;
    }

    /// <summary>
    /// Records an ordering. The direct pipeline sorts before it skips or takes, and sorts once, so
    /// an ordering after <c>Take</c> or <c>Skip</c> (which sorts only the window) or after another
    /// ordering (which keeps the first order among ties) is left to the in-memory replay. The model
    /// keeps the first ordering, so an index can still serve the window taken in that order (#230).
    /// </summary>
    private void VisitOrderBy(MethodCallExpression node)
    {
        Visit(node.Arguments[0]);
        if (_model.Take.HasValue || _model.Skip.HasValue || _model.OrderByClause != null)
        {
            _model.HasComplexOperators = true;
            return;
        }

        var keySelector = (UnaryExpression)node.Arguments[1];
        _model.OrderByClause = (LambdaExpression)keySelector.Operand;
        _model.OrderDescending = node.Method.Name == "OrderByDescending";
    }

    /// <summary>
    /// Records <c>Take(n)</c> as the window the direct pipeline takes after skipping. A later
    /// <c>Take</c> narrows that window: <c>Take(a).Take(b)</c> is <c>Take(min(a, b))</c>. A negative
    /// count takes nothing, as in LINQ. The <c>Take(Range)</c> overload is left to the replay (#230).
    /// </summary>
    private void VisitTake(MethodCallExpression node)
    {
        Visit(node.Arguments[0]);
        if (node.Arguments[1] is not ConstantExpression { Value: int count })
        {
            _model.HasComplexOperators = true;
            return;
        }

        count = Math.Max(count, 0);
        _model.Take = _model.Take.HasValue ? Math.Min(_model.Take.Value, count) : count;
    }

    /// <summary>
    /// Records <c>Skip(n)</c>. The direct pipeline skips before it takes, so a <c>Skip</c> after a
    /// window is folded into it exactly: <c>Skip(a).Skip(b)</c> is <c>Skip(a + b)</c> and
    /// <c>Take(a).Skip(b)</c> is <c>Skip(b).Take(max(a - b, 0))</c>. A negative count skips nothing,
    /// as in LINQ. A total beyond <see cref="int.MaxValue"/> is left to the replay (#230).
    /// </summary>
    private void VisitSkip(MethodCallExpression node)
    {
        Visit(node.Arguments[0]);
        if (node.Arguments[1] is not ConstantExpression { Value: int count })
        {
            _model.HasComplexOperators = true;
            return;
        }

        count = Math.Max(count, 0);
        long skip = (long)_model.Skip.GetValueOrDefault() + count;
        if (skip > int.MaxValue)
        {
            _model.HasComplexOperators = true;
            return;
        }

        _model.Skip = (int)skip;
        if (_model.Take.HasValue)
            _model.Take = Math.Max(_model.Take.Value - count, 0);
    }

    /// <summary>
    /// Handles <c>Sum(selector)</c>, <c>Average(selector)</c>, <c>Min(selector)</c>, and
    /// <c>Max(selector)</c> as push-down aggregates.
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
    /// Handles <c>First</c>, <c>FirstOrDefault</c>, <c>Single</c>, <c>SingleOrDefault</c>,
    /// <c>Last</c>, <c>LastOrDefault</c> and <c>Any</c>, with or without a predicate. A predicate
    /// on the entities themselves is folded into the WHERE clause (<c>First(p)</c> is
    /// <c>Where(p).First()</c>), so the index planner sees it, but only where
    /// <see cref="CanFoldPredicate"/> allows it. Overloads taking a default value are left to the
    /// fallback pipeline. <c>All(p)</c> is not handled here: it is not <c>Where(p).All()</c>.
    /// </summary>
    private void VisitElementTerminal(MethodCallExpression node)
    {
        Visit(node.Arguments[0]); // recurse into source

        if (node.Arguments.Count == 1)
        {
            _model.ElementTerminal = node.Method.Name;
        }
        else if (node.Arguments.Count == 2
                 && node.Arguments[1] is UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression predicate }
                 && CanFoldPredicate(node.Arguments[0], predicate))
        {
            AddPredicate(predicate);
            _model.ElementTerminal = node.Method.Name;
            _model.FoldedTerminal = node;
        }
        else
        {
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
        else if (node.Arguments[1] is UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression predicate }
                 && CanFoldPredicate(node.Arguments[0], predicate))
        {
            // Count(predicate) on the entities themselves is Where(predicate).Count().
            AddPredicate(predicate);
            _model.IsCountOnly = true;
            _model.FoldedTerminal = node;
        }
        else
        {
            // Count(predicate) after any other operator — treat as a complex operator.
            _model.HasComplexOperators = true;
        }
    }

    /// <summary>
    /// True when a terminal's <paramref name="predicate"/> may be folded into the WHERE clause,
    /// i.e. when <c>source.Terminal(p)</c> equals <c>source.Where(p).Terminal()</c> evaluated with
    /// the filter applied first. That holds only when every operator between the root and the
    /// terminal is a plain <c>Where</c> or an ordering (<c>OrderBy*</c> / <c>ThenBy*</c>): those keep
    /// the element type and commute with a filter. Anything else, such as <c>GroupBy</c>,
    /// <c>OfType</c>, <c>Select</c> or the order-dependent <c>Take</c>, <c>Skip</c>,
    /// <c>SkipWhile</c> and <c>TakeWhile</c>, either changes what the predicate is applied to or
    /// gives a different result when the predicate runs first.
    /// The provider relies on the same rule to know which <c>Where</c> calls the fetch applied.
    /// </summary>
    internal static bool CanFoldPredicate(Expression source, LambdaExpression predicate)
    {
        var current = source;
        while (current is MethodCallExpression call)
        {
            if (call.Method.DeclaringType != typeof(Queryable))
                return false;

            switch (call.Method.Name)
            {
                case "Where" when call.Arguments[1] is UnaryExpression { Operand: LambdaExpression { Parameters.Count: 1 } }:
                case "OrderBy":
                case "OrderByDescending":
                case "ThenBy":
                case "ThenByDescending":
                    current = call.Arguments[0];
                    break;
                default:
                    return false;
            }
        }

        // Only element-type-preserving operators were skipped, so the predicate must take the
        // root collection's own element type (the entity type).
        return current is ConstantExpression { Value: IQueryable root }
               && predicate.Parameters.Count == 1
               && predicate.Parameters[0].Type == root.ElementType;
    }
}
