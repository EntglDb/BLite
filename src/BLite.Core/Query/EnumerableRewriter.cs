using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace BLite.Core.Query;

internal class EnumerableRewriter : ExpressionVisitor
{
    private readonly IQueryable _source;
    private readonly object _target;

    public EnumerableRewriter(IQueryable source, object target)
    {
        _source = source;
        _target = target;
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        // Replace the IQueryable source with the materialized IEnumerable
        if (node.Value == _source)
        {
            return Expression.Constant(_target);
        }
        return base.VisitConstant(node);
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.DeclaringType == typeof(Queryable))
        {
            var methodName = node.Method.Name;
            var typeArgs = node.Method.GetGenericArguments();
            var args = new Expression[node.Arguments.Count];

            for (int i = 0; i < node.Arguments.Count; i++)
            {
                var arg = Visit(node.Arguments[i]);
                
                // Strip Quote from lambda arguments
                if (arg is UnaryExpression quote && quote.NodeType == ExpressionType.Quote)
                {
                    arg = quote.Operand;
                }
                args[i] = arg;
            }

            var enumerableMethods = typeof(Enumerable)
                .GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Where(m => m.Name == methodName && m.GetGenericArguments().Length == typeArgs.Length);

            foreach (var m in enumerableMethods)
            {
                var parameters = m.GetParameters();
                if (parameters.Length != args.Length) continue;

                // Simple check: create generic method and see if it works?
                // Or check parameter compatibility properly.
                // For now, assume single match for standard LINQ operators (simplified)
                try
                {
                    var genericMethod = m.MakeGenericMethod(typeArgs);
                    // Check if arguments are assignable (basic check)
                    // The first argument is usually "this IEnumerable<TSource>"
                    return Expression.Call(genericMethod, args);
                }
                catch
                {
                    // Ignore and try next overload
                }
            }
        }

        return base.VisitMethodCall(node);
    }
}
