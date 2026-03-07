using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace BLite.Core.Query;

internal class EnumerableRewriter : ExpressionVisitor
{
    // ── Static caches (per AppDomain) ───────────────────────────────────────────────────
    // All public static Enumerable methods grouped by name — built once, zero per-call GetMethods().
    private static readonly ILookup<string, MethodInfo> s_enumerableMethods =
        typeof(Enumerable)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .ToLookup(m => m.Name);

    // ── Instance ──────────────────────────────────────────────────────────────────
    private readonly IQueryable _source;
    private readonly object _target;

    public EnumerableRewriter(IQueryable source, object target)
    {
        _source = source;
        _target = target;
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        if (node.Value == _source)
            return Expression.Constant(_target);
        return base.VisitConstant(node);
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.DeclaringType == typeof(Queryable))
        {
            var methodName = node.Method.Name;
            var typeArgs   = node.Method.GetGenericArguments();
            var args       = new Expression[node.Arguments.Count];

            for (int i = 0; i < node.Arguments.Count; i++)
            {
                var arg = Visit(node.Arguments[i]);
                // Strip Quote wrapping around lambda arguments.
                if (arg is UnaryExpression { NodeType: ExpressionType.Quote } quote)
                {
                    var lambda = (LambdaExpression)quote.Operand;
                    arg = Expression.Constant(lambda.Compile());
                }
                args[i] = arg;
            }

            // Try every matching Enumerable overload until one accepts the
            // actual argument types.  .NET 6+ added defaultValue overloads for
            // First/FirstOrDefault/Single/SingleOrDefault/Last/LastOrDefault
            // that share the same generic-arg and param counts, so a name+arity
            // key is no longer sufficient for a single-entry cache.
            foreach (var candidate in s_enumerableMethods[methodName])
            {
                if (candidate.GetGenericArguments().Length != typeArgs.Length) continue;
                if (candidate.GetParameters().Length != args.Length) continue;
                MethodInfo closed;
                try { closed = candidate.MakeGenericMethod(typeArgs); }
                catch { continue; }
                try { return Expression.Call(closed, args); }
                catch { continue; }
            }
        }

        return base.VisitMethodCall(node);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────
}
