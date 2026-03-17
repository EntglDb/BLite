using System.Collections.Concurrent;
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
        // IQueryProvider.CreateQuery(Expression) is called only for operators that change the element
        // type at runtime (e.g. GroupBy, Cast).  For the common case (same T) create directly.
        var elementType = expression.Type.IsGenericType
            ? expression.Type.GetGenericArguments()[0]
            : typeof(T);

        if (elementType == typeof(T))
            return new BTreeQueryable<T>(this, expression);

        // Rare path: element type differs from T.
        // Cache a compiled delegate Func<BTreeQueryProvider<TId,T>, Expression, IQueryable>.
        // Using a 2-param delegate (provider, expr) avoids capturing 'this' in the static cache
        // — which would be a bug: the cached lambda would call the FIRST provider instance forever.
        var del = s_createQueryDelegates.GetOrAdd(elementType, t =>
        {
            var method = typeof(BTreeQueryProvider<TId, T>)
                .GetMethod(nameof(CreateQueryTyped), BindingFlags.NonPublic | BindingFlags.Instance)!
                .MakeGenericMethod(t);
            var providerParam = Expression.Parameter(typeof(BTreeQueryProvider<TId, T>), "p");
            var exprParam     = Expression.Parameter(typeof(Expression), "e");
            var callExpr      = Expression.Call(providerParam, method, exprParam);
            var castResult    = Expression.Convert(callExpr, typeof(IQueryable));
            return Expression.Lambda<Func<BTreeQueryProvider<TId, T>, Expression, IQueryable>>(
                castResult, providerParam, exprParam).Compile();
        });
        return del(this, expression);
    }

    private BTreeQueryable<TElement> CreateQueryTyped<TElement>(Expression expression)
        => new(this, expression);

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<
        Type, Func<BTreeQueryProvider<TId, T>, Expression, IQueryable>>
        s_createQueryDelegates = new();

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

        // ── Fast path: Count() / LongCount() terminal with no WHERE ─────────────
        // Uses the primary BTree key scan (O(n) key reads, zero document reads).
        if (model.IsCountOnly && model.WhereClause == null && !model.HasComplexOperators)
        {
            var count = _collection.Count();
            if (typeof(TResult) == typeof(int))  return (TResult)(object)count;
            if (typeof(TResult) == typeof(long)) return (TResult)(object)(long)count;
        }

        // ── Fast path: Sum / Average via BSON field-projection scan ──────────────
        // Reads only the target field from raw BSON — T is never fully instantiated.
        // HasComplexOperators is intentionally NOT checked here: VisitAggregate always sets
        // it so that Sum/Average WITH a WHERE falls through to EnumerableRewriter, but we
        // still want the fast path to fire when there is no WHERE clause.
        if (model.AggregateOp is not null && model.AggregateSelector is not null
            && model.WhereClause == null)
        {
            if (TryBsonAggregate<TResult>(model.AggregateOp, model.AggregateSelector, out var aggResult))
                return aggResult;
        }

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
            // ── Fast path: OrderBy(indexedField).Take(N) via secondary BTree ────
            // Reads N entries directly from the index in the requested direction,
            // skipping the O(n log n) in-memory sort over all documents.
            if (model.OrderByClause != null && model.Take.HasValue && !model.Skip.HasValue)
            {
                var orderOpt = IndexOptimizer.TryOptimizeOrderBy(model, _collection.GetIndexes());
                if (orderOpt is not null)
                {
                    bool ascending = !model.OrderDescending;
                    var topN = _collection.QueryIndex(orderOpt.IndexName, null, null, ascending)
                                          .Take(model.Take.Value);
                    if (model.SelectClause != null)
                        return ProjectEnumerable<TResult>(topN, model.SelectClause);
                    return TerminalReturn<TResult>(topN);
                }
            }

            sourceData = _collection.FindAll();
        }

        // ── Complex-operator fallback ──────────────────────────────────────────
        // GroupBy, Join, Sum/Average/Min/Max with selectors — operators the direct
        // pipeline cannot model.  We still benefit from index / BSON-scan fetch,
        // then let EnumerableRewriter translate the remaining Queryable calls to
        // Enumerable equivalents and compile the residual expression once.
        if (model.HasComplexOperators)
            return ExecuteViaEnumerableRewriter<TResult>(expression, sourceData);

        // When OrderBy is chained after Select, the key-selector parameter type is the
        // projected type (e.g. DateTimeOffset), not T.  The direct pipeline builds a
        // Func<T, object> from that lambda, which throws ArgumentException at runtime.
        // Fall back to EnumerableRewriter which rewrites the full expression tree correctly.
        if (model.OrderByClause != null && model.OrderByClause.Parameters[0].Type != typeof(T))
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
    
    // ─── BSON-level aggregate push-down ──────────────────────────────────────

    /// <summary>
    /// Attempts to compute Sum or Average entirely by scanning raw BSON bytes,
    /// without deserialising the full entity <typeparamref name="T"/>.
    /// </summary>
    /// <returns>
    /// <c>true</c> and a populated <paramref name="result"/> on success;
    /// <c>false</c> to signal the caller should fall through to the standard path.
    /// </returns>
    private bool TryBsonAggregate<TResult>(
        string aggregateOp,
        LambdaExpression selector,
        out TResult result)
    {
        result = default!;
        var fieldType = selector.ReturnType;

        // Only numeric types are supported; others fall through to EnumerableRewriter.
        if (fieldType != typeof(decimal) && fieldType != typeof(double) &&
            fieldType != typeof(float)   && fieldType != typeof(int)    &&
            fieldType != typeof(long))
            return false;

        // Compile a BSON projector for the target field (result is cached per fieldType).
        object? projector;
        try
        {
            var compileMethod = s_compiledSelectMethods.GetOrAdd(
                fieldType, t => s_tryCompileMethod.MakeGenericMethod(typeof(T), t));
            projector = compileMethod.Invoke(null, [selector, null]);
        }
        catch { return false; }

        if (projector is null) return false; // field shape too complex for BSON push-down

        // Execute the BSON scan — yields only non-null field values.
        System.Collections.IEnumerable? values;
        try
        {
            var scanMethod = s_compiledScanMethods.GetOrAdd(
                fieldType, t => s_scanProjectorMethod.MakeGenericMethod(t));
            values = scanMethod.Invoke(_collection, [projector]) as System.Collections.IEnumerable;
        }
        catch { return false; }

        if (values is null) return false;

        try { return AggregateValues<TResult>(aggregateOp, values, fieldType, out result); }
        catch { return false; }
    }

    private static bool AggregateValues<TResult>(
        string op,
        System.Collections.IEnumerable values,
        Type fieldType,
        out TResult result)
    {
        result = default!;

        if (fieldType == typeof(decimal))
        {
            var typed = (IEnumerable<decimal>)values;
            decimal agg = op == "Sum" ? typed.Sum() : typed.DefaultIfEmpty().Average();
            if (typeof(TResult) == typeof(decimal)) { result = (TResult)(object)agg; return true; }
        }
        else if (fieldType == typeof(double))
        {
            var typed = (IEnumerable<double>)values;
            double agg = op == "Sum" ? typed.Sum() : typed.DefaultIfEmpty().Average();
            if (typeof(TResult) == typeof(double)) { result = (TResult)(object)agg; return true; }
        }
        else if (fieldType == typeof(float))
        {
            var typed = (IEnumerable<float>)values;
            float agg = op == "Sum" ? typed.Sum() : typed.DefaultIfEmpty().Average();
            if (typeof(TResult) == typeof(float)) { result = (TResult)(object)agg; return true; }
        }
        else if (fieldType == typeof(int))
        {
            var typed = (IEnumerable<int>)values;
            if (op == "Sum")
            {
                long sum = 0;
                foreach (var v in typed) sum += v;
                if (typeof(TResult) == typeof(long))   { result = (TResult)(object)sum;        return true; }
                if (typeof(TResult) == typeof(int))    { result = (TResult)(object)(int)sum;   return true; }
            }
            else
            {
                double avg = typed.DefaultIfEmpty().Average();
                if (typeof(TResult) == typeof(double)) { result = (TResult)(object)avg; return true; }
            }
        }
        else if (fieldType == typeof(long))
        {
            var typed = (IEnumerable<long>)values;
            if (op == "Sum")
            {
                long sum = 0;
                foreach (var v in typed) sum += v;
                if (typeof(TResult) == typeof(long)) { result = (TResult)(object)sum; return true; }
            }
            else
            {
                double avg = typed.DefaultIfEmpty().Select(x => (double)x).Average();
                if (typeof(TResult) == typeof(double)) { result = (TResult)(object)avg; return true; }
            }
        }
        return false;
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
    /// <remarks>
    /// The compiled <c>Func&lt;IEnumerable&lt;T&gt;, TResult&gt;</c> is cached per expression shape
    /// (keyed by <c>TResult.FullName + expression.ToString()</c>), so the expensive
    /// <see cref="Expression.Compile"/> call is paid only once per unique query shape.
    /// </remarks>
    private TResult ExecuteViaEnumerableRewriter<TResult>(Expression expression, IEnumerable<T> sourceData)
    {
        var rootFinder = new RootFinder();
        rootFinder.Visit(expression);
        var root = rootFinder.Root;
        if (root == null)
            throw new InvalidOperationException("Could not find root IQueryable in expression.");

        var cacheKey = typeof(TResult).FullName + "\x00" + expression.ToString();

        var cachedDelegate = s_enumRewriterCache.GetOrAdd(cacheKey, _ =>
        {
            var sourceParam = Expression.Parameter(typeof(IEnumerable<T>), "_src");
            var rewriter = new EnumerableRewriter(root, sourceParam);
            var rewritten = rewriter.Visit(expression);
            if (rewritten.Type != typeof(TResult))
                rewritten = Expression.Convert(rewritten, typeof(TResult));
            return Expression.Lambda<Func<IEnumerable<T>, TResult>>(rewritten, sourceParam).Compile();
        });

        return ((Func<IEnumerable<T>, TResult>)cachedDelegate)(sourceData);
    }

    /// <summary>Cache of compiled EnumerableRewriter delegates keyed by expression shape.</summary>
    private static readonly ConcurrentDictionary<string, Delegate> s_enumRewriterCache = new();

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
