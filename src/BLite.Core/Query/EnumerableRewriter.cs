using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace BLite.Core.Query;

internal class EnumerableRewriter : ExpressionVisitor
{
    // ── Static caches (per AppDomain) ───────────────────────────────────────────────────
    // All public static Enumerable methods grouped by name — built once, zero per-call GetMethods().
    private static readonly ILookup<string, MethodInfo> s_enumerableMethods =
        typeof(Enumerable)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .ToLookup(m => m.Name);

    // Resolved + instantiated MethodInfo keyed by "Name|argCount|type1,type2,...".
    // MakeGenericMethod is called at most once per unique (operator, type-args) combination.
    private static readonly ConcurrentDictionary<string, MethodInfo?> s_resolvedCache = new();

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

            // Resolve the corresponding Enumerable method once per type combination.
            var cacheKey = BuildCacheKey(methodName, args.Length, typeArgs);
            var resolved = s_resolvedCache.GetOrAdd(
                cacheKey, _ => ResolveMethod(methodName, args.Length, typeArgs));

            if (resolved is not null)
            {
                try { return Expression.Call(resolved, args); }
                catch { /* arg types incompatible for this overload — fall through */ }
            }
        }

        return base.VisitMethodCall(node);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────

    private static MethodInfo? ResolveMethod(string name, int argCount, Type[] typeArgs)
    {
        foreach (var m in s_enumerableMethods[name])
        {
            if (m.GetGenericArguments().Length != typeArgs.Length) continue;
            if (m.GetParameters().Length != argCount) continue;
            try { return m.MakeGenericMethod(typeArgs); }
            catch { }
        }
        return null;
    }

    /// <summary>
    /// Builds a string cache key from the operator name, argument count, and closed type arguments.
    /// Example: <c>GroupBy|3|System.String,BLite.Tests.TestDocument</c>
    /// </summary>
    private static string BuildCacheKey(string name, int argCount, Type[] typeArgs)
    {
        var sb = new StringBuilder(name).Append('|').Append(argCount).Append('|');
        for (int i = 0; i < typeArgs.Length; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append(typeArgs[i].FullName ?? typeArgs[i].Name);
        }
        return sb.ToString();
    }
}
