using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace BLite.Core.Query;

internal class EnumerableRewriter : ExpressionVisitor
{
    // ── Static caches (per AppDomain) ───────────────────────────────────────────────────
    // All public static Enumerable methods grouped by name — built once, zero per-call GetMethods().
#pragma warning disable IL2026
    private static readonly ILookup<string, MethodInfo> s_enumerableMethods =
        typeof(Enumerable)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .ToLookup(m => m.Name);
#pragma warning restore IL2026

    // ── Instance ──────────────────────────────────────────────────────────────────
    private readonly IQueryable _source;
    private readonly Expression _replacement;

    /// <summary>Standard constructor — the target IEnumerable is embedded as a constant.</summary>
    public EnumerableRewriter(IQueryable source, object target)
    {
        _source = source;
        _replacement = Expression.Constant(target);
    }

    /// <summary>
    /// Parameterised constructor: the target is a <see cref="ParameterExpression"/> that
    /// will be declared as a parameter on the outer lambda, enabling the compiled delegate
    /// to be cached and reused across calls with different source data.
    /// </summary>
    public EnumerableRewriter(IQueryable source, ParameterExpression targetParam)
    {
        _source = source;
        _replacement = targetParam;
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        if (node.Value == _source)
            return _replacement;
        return base.VisitConstant(node);
    }

    // NOTE: VisitMethodCall overrides ExpressionVisitor.VisitMethodCall which cannot be
    // annotated. The method uses reflection to find Enumerable methods at runtime.
    // Warnings are suppressed because this rewriter is only called from annotated paths.
#pragma warning disable IL3050, IL2026, IL2060, IL2072
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
                MethodInfo closed = null!;
                try { closed = candidate.MakeGenericMethod(typeArgs); }
                catch { continue; }
                try { return Expression.Call(closed, args); }
                catch { continue; }
            }
        }

        return base.VisitMethodCall(node);
    }
#pragma warning restore IL3050, IL2026, IL2060, IL2072

    // ── Helpers ──────────────────────────────────────────────────────────────────
}
