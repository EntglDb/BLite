using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using static BLite.Core.Query.IndexOptimizer;

namespace BLite.Core.Query;

public class BTreeQueryProvider<TId, T> : IQueryProvider where T : class
{
    private readonly DocumentCollection<TId, T> _collection;
    private readonly ValueConverterRegistry _converterRegistry;

    // ── Reflection cache (computed once per TId+T combination) ────────────────
    // BsonProjectionCompiler.TryCompile<T, TResult> generic method definition.
    private static readonly MethodInfo s_tryCompileMethod =
        typeof(BsonProjectionCompiler)
            .GetMethod(nameof(BsonProjectionCompiler.TryCompile),
                       BindingFlags.Static | BindingFlags.Public)!;

    // DocumentCollection<TId,T>.Scan<TResult>(Func<BsonSpanReader, TResult?>) — generic overload.
    private static readonly MethodInfo s_scanProjectorMethod =
        typeof(DocumentCollection<TId, T>)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Single(m => m.Name == "Scan" && m.IsGenericMethodDefinition);

    // ── Per-projection-type MakeGenericMethod cache (Fase 3) ─────────────────
    // Keyed on TProj. Avoids calling MakeGenericMethod on every query execution.
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, MethodInfo>
        s_compiledSelectMethods = new();
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, MethodInfo>
        s_compiledScanMethods = new();

    public BTreeQueryProvider(DocumentCollection<TId, T> collection, ValueConverterRegistry? registry = null)
    {
        _collection = collection;
        _converterRegistry = registry ?? ValueConverterRegistry.Empty;
    }

    public IQueryable CreateQuery(Expression expression)
    {
        // IQueryProvider.CreateQuery(Expression) is called only for operators that produce a
        // different element type at runtime (e.g. GroupBy, Cast).  Delegate immediately to the
        // generic overload via a cached factory to avoid Activator.CreateInstance per call.
        var elementType = expression.Type.IsGenericType
            ? expression.Type.GetGenericArguments()[0]
            : typeof(T);

        if (elementType == typeof(T))
            return new BTreeQueryable<T>(this, expression);

        // Rare: element type differs from T — use cached factory method.
        var factory = s_createQueryFactories.GetOrAdd(elementType, t =>
        {
            var method = typeof(BTreeQueryProvider<TId, T>)
                .GetMethod(nameof(CreateQueryTyped), BindingFlags.NonPublic | BindingFlags.Instance)!
                .MakeGenericMethod(t);
            return (Func<Expression, IQueryable>)(expr =>
                (IQueryable)method.Invoke(this, [expr])!);
        });
        return factory(expression);
    }

    private BTreeQueryable<TElement> CreateQueryTyped<TElement>(Expression expression)
        => new(this, expression);

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, Func<Expression, IQueryable>>
        s_createQueryFactories = new();

    // Explicit implementation satisfies IQueryProvider contract.
    // The runtime object is always BTreeQueryable<TElement> : IBLiteQueryable<TElement>,
    // so callers can safely use AsAsyncEnumerable() or the typed LINQ extensions.
    IQueryable<TElement> IQueryProvider.CreateQuery<TElement>(Expression expression)
        => new BTreeQueryable<TElement>(this, expression);

    /// <summary>Typed overload — returns the richer <see cref="IBLiteQueryable{TElement}"/> interface.</summary>
    public IBLiteQueryable<TElement> CreateQuery<TElement>(Expression expression)
        => new BTreeQueryable<TElement>(this, expression);

    public object? Execute(Expression expression)
    {
        return Execute<object>(expression);
    }

    public TResult Execute<TResult>(Expression expression)
    {
        // 1. Parse the LINQ expression tree into a flat QueryModel.
        var visitor = new BTreeExpressionVisitor();
        visitor.Visit(expression);
        var model = visitor.GetModel();

        // ── Push-down SELECT (+ optional WHERE) ───────────────────────────────
        // Single-pass BSON projection: no T instantiation, no EnumerableRewriter.
        // Only applicable when there is no post-scan ordering or paging.
        if (model.SelectClause is not null
            && model.OrderByClause is null
            && !model.Take.HasValue
            && !model.Skip.HasValue)
        {
            var pushed = TryPushDownSelect<TResult>(model.SelectClause, model.WhereClause);
            if (pushed is not null) return pushed;
        }

        // 2. Data fetching — index / BSON-scan / full scan.
        IEnumerable<T> sourceData;
        bool whereAlreadyApplied = false;

        var indexOpt = IndexOptimizer.TryOptimize<T>(model, _collection.GetIndexes(), _converterRegistry);
        if (indexOpt != null)
        {
            if (indexOpt.IsVectorSearch)
            {
                sourceData = _collection.VectorSearch(indexOpt.IndexName, indexOpt.VectorQuery!, indexOpt.K);
            }
            else if (indexOpt.IsSpatialSearch)
            {
                sourceData = indexOpt.SpatialType == SpatialQueryType.Near
                    ? _collection.Near(indexOpt.IndexName, indexOpt.SpatialPoint, indexOpt.RadiusKm)
                    : _collection.Within(indexOpt.IndexName, indexOpt.SpatialMin, indexOpt.SpatialMax);
            }
            else
            {
                sourceData = _collection.QueryIndex(indexOpt.IndexName, indexOpt.MinValue, indexOpt.MaxValue);
            }
        }
        else if (model.WhereClause != null &&
                 BsonExpressionEvaluator.TryCompile<T>(model.WhereClause, _converterRegistry) is { } bsonPred)
        {
            sourceData = _collection.Scan(bsonPred);
            whereAlreadyApplied = true;
        }
        else
        {
            sourceData = _collection.FindAll();
        }

        // ── Complex-operator fallback ──────────────────────────────────────────
        // GroupBy, Join, Sum/Average/Min/Max with selectors — operators the direct
        // pipeline cannot model.  We still benefit from index / BSON-scan fetch,
        // then let EnumerableRewriter translate the remaining Queryable calls to
        // Enumerable equivalents and compile the residual expression once.
        if (model.HasComplexOperators)
            return ExecuteViaEnumerableRewriter<TResult>(expression, sourceData);

        // 3. Direct pipeline — no expression-tree rewrite, no EnumerableRewriter, no Compile().
        return ExecutePipeline<TResult>(model, sourceData, whereAlreadyApplied);
    }

    /// <summary>
    /// Applies WHERE / ORDER BY / SKIP / TAKE / SELECT directly on the materialized
    /// <see cref="IEnumerable{T}"/> source without rewriting the original expression tree.
    /// Terminal results (<c>IEnumerable&lt;T&gt;</c>, <c>List&lt;T&gt;</c>, scalar aggregates)
    /// are dispatched via a type-switch to avoid any reflection call per invocation.
    /// </summary>
    private TResult ExecutePipeline<TResult>(QueryModel model, IEnumerable<T> source, bool whereAlreadyApplied)
    {
        IEnumerable<T> data = source;

        // WHERE (residual — when the BSON scan didn't already filter)
        if (!whereAlreadyApplied && model.WhereClause != null)
        {
            var pred = model.WhereClause.Compile() as Func<T, bool>
                       ?? (Func<T, bool>)model.WhereClause.Compile();
            data = data.Where(pred);
        }

        // ORDER BY — compile key selector to Func<T, object> (boxing) to avoid
        // MakeGenericMethod for Enumerable.OrderBy<T,TKey> with runtime-only TKey.
        if (model.OrderByClause != null)
        {
            var param = model.OrderByClause.Parameters[0];
            var boxed = Expression.Lambda<Func<T, object>>(
                Expression.Convert(model.OrderByClause.Body, typeof(object)), param).Compile();
            data = model.OrderDescending
                ? data.OrderByDescending(x => (IComparable?)boxed(x))
                : data.OrderBy(x => (IComparable?)boxed(x));
        }

        // SKIP / TAKE
        if (model.Skip.HasValue) data = data.Skip(model.Skip.Value);
        if (model.Take.HasValue) data = data.Take(model.Take.Value);

        // SELECT (with ORDER BY / SKIP / TAKE — rare path, push-down already handled the common case)
        if (model.SelectClause != null)
            return ProjectEnumerable<TResult>(data, model.SelectClause);

        // Terminal dispatch — no reflection, no Compile(), no MakeGenericMethod.
        return TerminalReturn<TResult>(data);
    }

    /// <summary>
    /// Projects <paramref name="source"/> via the given SELECT lambda.
    /// The generic dispatch (<c>ProjectTyped&lt;TProj&gt;</c>) is resolved via a cached
    /// <see cref="MethodInfo"/>: <c>MakeGenericMethod</c> is called once per projection type.
    /// </summary>
    private TResult ProjectEnumerable<TResult>(IEnumerable<T> source, LambdaExpression selectLambda)
    {
        var resultType = typeof(TResult);
        if (!resultType.IsGenericType || resultType.GetGenericTypeDefinition() != typeof(IEnumerable<>))
            return default!;

        var projType = resultType.GetGenericArguments()[0];
        var method = s_projectMethodCache.GetOrAdd(
            projType,
            t => typeof(BTreeQueryProvider<TId, T>)
                .GetMethod(nameof(ProjectTyped), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(t));

        return (TResult)method.Invoke(null, [source, selectLambda])!;
    }

    /// <summary>Compiles <paramref name="selectLambda"/> as <c>Func&lt;T, TProj&gt;</c> and projects the source.</summary>
    private static IEnumerable<TProj> ProjectTyped<TProj>(IEnumerable<T> source, LambdaExpression selectLambda)
    {
        var selector = (Func<T, TProj>)selectLambda.Compile();
        return source.Select(selector);
    }

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, MethodInfo>
        s_projectMethodCache = new();

    /// <summary>
    /// Type-switch dispatch for terminal results — first-match, no reflection per call.
    /// Covers all standard LINQ terminal operators: First/Single/Count/Any/All/ToList/ToArray.
    /// </summary>
    private static TResult TerminalReturn<TResult>(IEnumerable<T> data)
    {
        if (typeof(TResult) == typeof(IEnumerable<T>)) return (TResult)data;
        if (typeof(TResult) == typeof(List<T>))        return (TResult)(object)data.ToList();
        if (typeof(TResult) == typeof(T[]))            return (TResult)(object)data.ToArray();
        if (typeof(TResult) == typeof(T))              return (TResult)(object)data.First();
        if (typeof(TResult) == typeof(int))            return (TResult)(object)data.Count();
        if (typeof(TResult) == typeof(long))           return (TResult)(object)(long)data.Count();
        if (typeof(TResult) == typeof(bool))           return (TResult)(object)data.Any();
        if (typeof(TResult) == typeof(object))         return (TResult)(object)(data.ToList());

        // Unknown terminal: materialise and attempt a cast — handles e.g. First<SubType>.
        var list = data.ToList();
        if (list.Count > 0 && list[0] is TResult first) return first;
        if (list is TResult asResult) return asResult;

        throw new NotSupportedException(
            $"BLite query pipeline: unsupported terminal result type '{typeof(TResult)}'.");
    }
    
    // ─── Push-down SELECT helper ──────────────────────────────────────────────

    /// <summary>
    /// Attempts to execute the query entirely via a single-pass BSON projection scan,
    /// bypassing full <typeparamref name="T"/> deserialisation.
    /// </summary>
    /// <returns>The projected <c>IEnumerable&lt;TProj&gt;</c>, or <c>null</c> if push-down is
    /// not applicable (caller should fall through to the standard path).</returns>
    private TResult? TryPushDownSelect<TResult>(LambdaExpression selectLambda,
                                                   LambdaExpression? whereLambda = null)
    {
        // We need TResult = IEnumerable<TProj> for some projection type TProj.
        var resultType = typeof(TResult);
        if (!resultType.IsGenericType) return default;
        if (resultType.GetGenericTypeDefinition() != typeof(IEnumerable<>)) return default;

        var projType = resultType.GetGenericArguments()[0];
        if (projType == typeof(T)) return default; // not a real projection

        // Compile the push-down projector via reflection (TProj is only known at runtime).
        // BsonProjectionCompiler.TryCompile<T, TProj>(selectLambda, whereLambda?) handles
        // both the pure-SELECT and the WHERE+SELECT cases in a single BSON pass.
        object? projector;
        try
        {
            var compileMethod = s_compiledSelectMethods.GetOrAdd(
                projType,
                t => s_tryCompileMethod.MakeGenericMethod(typeof(T), t));
            projector = compileMethod.Invoke(null, [selectLambda, whereLambda]);
        }
        catch { return default; }

        if (projector is null) return default; // compilation soft-failed

        // Invoke _collection.Scan<TProj>(projector) — MethodInfo cached per TProj.
        try
        {
            var scanMethod = s_compiledScanMethods.GetOrAdd(
                projType,
                t => s_scanProjectorMethod.MakeGenericMethod(t));
            var result = scanMethod.Invoke(_collection, [projector]);
            return (TResult?)result;
        }
        catch { return default; }
    }

    // ─── Expression-tree helpers ──────────────────────────────────────────────

    /// <summary>
    /// Fallback for queries containing operators the direct pipeline cannot handle
    /// (GroupBy, Join, Sum/Average/Min/Max with selectors, etc.).
    /// The source data has already been fetched (and filtered by index/BSON-scan),
    /// so we only need to rewrite the remaining <c>Queryable.*</c> calls to
    /// <c>Enumerable.*</c> equivalents before compiling and executing.
    /// </summary>
    private TResult ExecuteViaEnumerableRewriter<TResult>(Expression expression, IEnumerable<T> sourceData)
    {
        var rootFinder = new RootFinder();
        rootFinder.Visit(expression);
        var root = rootFinder.Root;
        if (root == null)
            throw new InvalidOperationException("Could not find root IQueryable in expression.");

        var rewriter = new EnumerableRewriter(root, sourceData);
        var rewritten = rewriter.Visit(expression);

        if (rewritten.Type != typeof(TResult))
            rewritten = Expression.Convert(rewritten, typeof(TResult));

        return Expression.Lambda<Func<TResult>>(rewritten).Compile()();
    }

    private sealed class RootFinder : ExpressionVisitor
    {
        public IQueryable? Root { get; private set; }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (Root == null && node.Value is IQueryable q)
                Root = q;
            return base.VisitConstant(node);
        }
    }
}
