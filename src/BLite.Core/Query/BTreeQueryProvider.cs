using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using BLite.Bson;
using BLite.Core.Audit;
using BLite.Core.Collections;
using BLite.Core.Indexing;
using BLite.Core.Metadata;
using BLite.Core.Metrics;
using static BLite.Core.Query.IndexOptimizer;

namespace BLite.Core.Query;

public class BTreeQueryProvider<TId, T> : IQueryProvider, IAsyncQueryProvider, IBTreeQueryCore<T> where T : class
{
    private readonly DocumentCollection<TId, T> _collection;
    private readonly ValueConverterRegistry _converterRegistry;

    public BTreeQueryProvider(DocumentCollection<TId, T> collection, ValueConverterRegistry? registry = null)
    {
        _collection = collection;
        _converterRegistry = registry ?? ValueConverterRegistry.Empty;
    }

    // NOTE: CreateQuery implements IQueryProvider.CreateQuery which cannot be annotated.
    // The body uses dynamic code (Compile, MakeGenericMethod) suppressed via pragma.
#pragma warning disable IL3050, IL2026, IL2075
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
            var exprParam = Expression.Parameter(typeof(Expression), "e");
            var callExpr = Expression.Call(providerParam, method, exprParam);
            var castResult = Expression.Convert(callExpr, typeof(IQueryable));
            return Expression.Lambda<Func<BTreeQueryProvider<TId, T>, Expression, IQueryable>>(
                castResult, providerParam, exprParam).Compile();
        });
        return del(this, expression);
    }
#pragma warning restore IL3050, IL2026, IL2075

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

    // NOTE: Execute/Execute<TResult> implement IQueryProvider methods which cannot be annotated.
    // Use pragma to suppress warnings about calling dynamic code methods within the body.
#pragma warning disable IL3050, IL2026
    public object? Execute(Expression expression)
    {
        return Execute<object>(expression);
    }

    public TResult Execute<TResult>(Expression expression)
    {
        // Run on the thread pool to avoid deadlocks when called from a thread that
        // has a SynchronizationContext (ASP.NET classic, WPF, Blazor).
        // Without Task.Run, ExecuteAsync awaits FindAllAsync which uses SemaphoreSlim.WaitAsync;
        // its continuation would try to resume on the captured context that is already blocked
        // by this .GetAwaiter().GetResult() call → deadlock.
        // BTreeQueryable.GetAsyncEnumerator applies the same pattern for the async path.
        return Task.Run(() => ExecuteAsync<TResult>(expression, CancellationToken.None))
                   .GetAwaiter().GetResult();
    }
#pragma warning restore IL3050, IL2026

    // Explicit IAsyncQueryProvider implementation — delegates to the private async path,
    // allowing BTreeQueryable<T> to call ExecuteAsync directly (no double Task.Run).
    [RequiresDynamicCode("BLite LINQ queries use Expression.Compile() and MakeGenericMethod which require dynamic code generation.")]
    [RequiresUnreferencedCode("BLite LINQ queries use reflection to resolve methods and types at runtime. Ensure all entity types and their members are preserved.")]
    Task<TResult> IAsyncQueryProvider.ExecuteAsync<TResult>(Expression expression, CancellationToken ct)
        => ExecuteAsync<TResult>(expression, ct);

    [RequiresDynamicCode("BLite LINQ queries use Expression.Compile() and MakeGenericMethod which require dynamic code generation.")]
    [RequiresUnreferencedCode("BLite LINQ queries use reflection to resolve methods and types at runtime. Ensure all entity types and their members are preserved.")]
    private async Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken)
    {
        // ── AUDIT: start ─────────────────────────────────────────────────────
        var auditOpts = _collection.Storage.AuditOptions;
        var auditVsw  = (auditOpts is not null && (auditOpts.Sink is not null || auditOpts.EnableMetrics))
            ? ValueStopwatch.StartNew()
            : default;

#if NET5_0_OR_GREATER
        Activity? activity = auditOpts?.EnableDiagnosticSource == true
            ? BLiteDiagnostics.ActivitySource.StartActivity(BLiteDiagnostics.QueryActivityName)
            : null;
        activity?.SetTag("db.system",           "blite");
        activity?.SetTag("db.collection.name",  _collection.CollectionName);
#endif
        // ────────────────────────────────────────────────────────────────────

        try
        {
            var result = await ExecuteAsyncCore<TResult>(expression, cancellationToken).ConfigureAwait(false);

            // ── AUDIT: emit on success ────────────────────────────────────────
            if (auditVsw.IsActive)
            {
                var elapsed = auditVsw.GetElapsed();
                var opts    = _collection.Storage.AuditOptions!;
                var userId  = (opts.ContextProvider ?? AmbientAuditContext.Instance).GetCurrentUserId();

                var evt = new QueryAuditEvent(
                    CollectionName: _collection.CollectionName,
                    // Phase 1: strategy resolution requires BTreeQueryVisitor integration
                    // (index vs full-scan detection). Deferred to a future release.
                    Strategy:       QueryStrategy.Unknown,
                    IndexName:      null,
                    ResultCount:    -1,
                    Elapsed:        elapsed,
                    UserId:         userId);

                _collection.Storage.AuditSink?.OnQuery(evt);
                _collection.Storage.AuditMetrics?.RecordQuery(QueryStrategy.Unknown, elapsed);

                // Slow-query detection
                if (opts.SlowOperationThreshold is { } threshold && elapsed > threshold)
                {
                    _collection.Storage.AuditSink?.OnSlowOperation(new SlowOperationEvent(
                        SlowOperationType.Query,
                        CollectionName: _collection.CollectionName,
                        Elapsed:        elapsed,
                        Detail:         null));
                }
            }

#if NET5_0_OR_GREATER
            // Always dispose the activity (even when metrics/sink not active) so ActivityStopped fires.
            activity?.SetTag("db.blite.query_strategy", QueryStrategy.Unknown.ToString());
            activity?.Dispose();
            activity = null;
#endif
            // ────────────────────────────────────────────────────────────────

            return result;
        }
        catch
        {
#if NET5_0_OR_GREATER
            activity?.SetStatus(ActivityStatusCode.Error);
            activity?.Dispose();
#endif
            throw;
        }
    }

    [RequiresDynamicCode("BLite LINQ queries use Expression.Compile() and MakeGenericMethod which require dynamic code generation.")]
    [RequiresUnreferencedCode("BLite LINQ queries use reflection to resolve methods and types at runtime. Ensure all entity types and their members are preserved.")]
    private async Task<TResult> ExecuteAsyncCore<TResult>(Expression expression, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // 1. Parse the LINQ expression tree into a flat QueryModel.
        var visitor = new BTreeExpressionVisitor();
        visitor.Visit(expression);
        var model = visitor.GetModel();

        // ── Element terminals: First / FirstOrDefault / Single / SingleOrDefault / Any ──
        // With only a WHERE in front, at most one (Single*: two) matching document decides the
        // result, so fetch just that many through the index-aware FetchAsync. Anything else in
        // front (OrderBy, Select, Take, ...), and Last / LastOrDefault, which need every match,
        // go through the general fallback.
        if (model.ElementTerminal is { } terminal)
        {
            if (terminal is not ("Last" or "LastOrDefault")
                && !model.HasComplexOperators && model.SelectClause == null && model.OrderByClause == null
                && !model.Take.HasValue && !model.Skip.HasValue && model.AggregateOp == null && !model.IsCountOnly)
            {
                bool single = terminal is "Single" or "SingleOrDefault";
                var found = new List<T>(single ? 2 : 1);
                await foreach (var item in _collection.FetchAsync(model.WhereClause, single ? 2 : 1, cancellationToken))
                    found.Add(item);

                return terminal switch
                {
                    "First"           => (TResult)(object)found.First()!,
                    "FirstOrDefault"  => (TResult)(object?)found.FirstOrDefault()!,
                    "Single"          => (TResult)(object)found.Single()!,
                    "SingleOrDefault" => (TResult)(object?)found.SingleOrDefault()!,
                    _                 => (TResult)(object)(found.Count > 0),
                };
            }

            model.HasComplexOperators = true;
        }

        // ── Fast path: Count() / LongCount() terminal with no WHERE ─────────────
        // Uses the primary BTree key scan (O(n) key reads, zero document reads).
        // Disabled when Take/Skip are present — those shaping operators reduce the
        // logical row-set before counting and must not be bypassed.
        if (model.IsCountOnly && model.WhereClause == null && !model.HasComplexOperators
            && !model.Take.HasValue && !model.Skip.HasValue)
        {
            var count = await _collection.CountAsync(cancellationToken);
            if (typeof(TResult) == typeof(int))    return (TResult)(object)count;
            if (typeof(TResult) == typeof(long))   return (TResult)(object)(long)count;
            if (typeof(TResult) == typeof(object)) return (TResult)(object)count;
        }

        // ── Fast path: Count() / LongCount() terminal with WHERE predicate ────────
        // Uses index key-only scan for indexed predicates (zero document reads) or a
        // streaming count for non-indexed predicates (no large List<T> accumulation).
        // Disabled when Take/Skip are present for the same reason as above.
        if (model.IsCountOnly && model.WhereClause != null && !model.HasComplexOperators
            && !model.Take.HasValue && !model.Skip.HasValue)
        {
            var count = await _collection.CountByPredicateAsync(model.WhereClause, cancellationToken);
            if (typeof(TResult) == typeof(int))    return (TResult)(object)count;
            if (typeof(TResult) == typeof(long))   return (TResult)(object)(long)count;
            if (typeof(TResult) == typeof(object)) return (TResult)(object)count;
        }

        // ── Fast path: Sum / Average / Min / Max of a property without materialising T ─
        // From index keys or raw BSON, with the result LINQ computes over the entities. Only for
        // an aggregate applied straight to Where clauses over the collection: any other operator
        // in between (Take, Skip, OrderBy, Select, Distinct, ...) changes which values are
        // aggregated or their order, so those queries run the usual way.
        if (model.AggregateOp is not null && model.AggregateSelector is not null
            && IsAggregateOverFilteredRoot(expression)
            && Enum.TryParse<AggregateOperator>(model.AggregateOp, out var aggregateOp))
        {
            var (handled, value) = await _collection.TryAggregateAsync(
                aggregateOp, model.AggregateSelector, model.WhereClause, typeof(TResult), cancellationToken).ConfigureAwait(false);
            if (handled) return (TResult)value!;
        }

        // ── Several query sources ─────────────────────────────────────────────
        // A query that reads a queryable outside its main chain (q.Where(...).Concat(q), a
        // self-join, a subquery inside a lambda) cannot be served by one filtered or paged
        // fetch: EnumerableRewriter replaces every use of the root with the same source. Replay
        // the whole query over the whole collection. Another sequence passed to an operator
        // (a Join inner, a Concat argument) is run as a query of its own first, so that its
        // clauses get the same planning as any query, VectorSearch included (#186).
        if (HasSecondaryQuery(expression))
        {
            var all = new List<T>();
            await foreach (var item in _collection.FetchAsync(null, int.MaxValue, cancellationToken))
                all.Add(item);
            return ExecuteViaEnumerableRewriter<TResult>(QueryOtherSequences(expression), all);
        }

        // 2. Data fetching.
        // When Take (+ optional Skip) is present without complex operators or ordering that
        // requires a full scan first, cap how many rows we need to materialise.
        int fetchLimit = (model.Take.HasValue && model.OrderByClause == null && !model.HasComplexOperators)
            ? (model.Skip.GetValueOrDefault() + model.Take.Value)
            : int.MaxValue;

        // ── Fast path: OrderBy(indexedField).Skip(S).Take(N) without WHERE ───────────
        // Reads only skip+take entries from the index in sorted order, skipping the
        // O(n log n) in-memory sort over all documents.  Skip is performed at the
        // index-entry level (no document reads for skipped positions), so only the
        // requested page window is ever deserialised.
        if (model.WhereClause == null && model.OrderByClause != null && model.Take.HasValue
            && TryGetIndexWindow(expression, model.OrderByClause, out int windowEnd, out bool plainWindow))
        {
            var orderOpt = IndexOptimizer.TryOptimizeOrderBy(model, _collection.GetIndexes());
            if (orderOpt is not null)
            {
                bool ascending = !model.OrderDescending;
                if (plainWindow && !model.HasComplexOperators
                    && (model.SelectClause == null || IsSequenceResult<TResult>()))
                {
                    int skip = model.Skip ?? 0;
                    int take = model.Take.Value;
                    var topN = new List<T>(take);
                    await foreach (var item in _collection.QueryIndexAsync(orderOpt.IndexName, null, null, ascending, skip, take, cancellationToken))
                        topN.Add(item);
                    if (model.SelectClause != null)
                        return ProjectEnumerable<TResult>(topN, model.SelectClause);
                    return TerminalReturn<TResult>(topN);
                }

                // Operators after the window (a terminal with a predicate or selector, a scalar
                // after Select, ...): read the first windowEnd entries in index order and replay the
                // whole query over them. OrderBy is a stable sort, so the replay keeps the index
                // order and its Skip/Take select the same window.
                // This needs the root queryable to be the query's only source: EnumerableRewriter
                // replaces every use of it with the prefix. A query that reads a queryable again
                // (Concat(q), Join(q, ...)) took the HasSecondaryQuery path above.
                var prefix = new List<T>(windowEnd);
                await foreach (var item in _collection.QueryIndexAsync(orderOpt.IndexName, null, null, ascending, 0, windowEnd, cancellationToken))
                    prefix.Add(item);
                return ExecuteViaEnumerableRewriter<TResult>(expression, prefix);
            }
        }

        // ── General path: FetchAsync picks index / BSON scan / full scan ─────
        // FetchAsync always applies the WHERE clause internally (all three strategies
        // filter before yielding), so no residual WHERE step is needed afterwards.
        var sourceList = new List<T>();
        bool whereAlreadyApplied = model.WhereClause != null;

        await foreach (var item in _collection.FetchAsync(model.WhereClause, fetchLimit, cancellationToken))
            sourceList.Add(item);

        IEnumerable<T> sourceData = sourceList;

        // ── Complex-operator fallback ──────────────────────────────────────────
        // GroupBy, Join, Sum/Average/Min/Max with selectors — operators the direct
        // pipeline cannot model.  We still benefit from index / BSON-scan fetch,
        // then let EnumerableRewriter translate the remaining Queryable calls to
        // Enumerable equivalents and compile the residual expression once.
        // FetchAsync applied the WHERE, so the replay drops the root-chain Where calls folded into
        // it. Re-evaluating them would change nothing except for VectorSearch, which only the index
        // can evaluate. Every other VectorSearch in the expression is still evaluated and throws.
        // The same holds for a terminal predicate folded into the WHERE (First(p) after OrderBy).
        if (whereAlreadyApplied)
            expression = DropAppliedWheres(DropFoldedTerminalPredicate(expression, model.FoldedTerminal));

        if (model.HasComplexOperators)
            return ExecuteViaEnumerableRewriter<TResult>(expression, sourceData);

        // When OrderBy is chained after Select, the key-selector parameter type is the
        // projected type (e.g. DateTimeOffset), not T.  The direct pipeline builds a
        // Func<T, object> from that lambda, which throws ArgumentException at runtime.
        // Fall back to EnumerableRewriter which rewrites the full expression tree correctly.
        if (model.OrderByClause != null && model.OrderByClause.Parameters[0].Type != typeof(T))
            return ExecuteViaEnumerableRewriter<TResult>(expression, sourceData);

        // The direct pipeline can only project into a sequence. A scalar terminal after a
        // Select (e.g. Take(n).Select(...).Count()) needs the full expression.
        if (model.SelectClause != null && !IsSequenceResult<TResult>())
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
    [RequiresDynamicCode("BLite LINQ queries use Expression.Compile() and MakeGenericMethod which require dynamic code generation.")]
    [RequiresUnreferencedCode("BLite LINQ queries use reflection to resolve methods and types at runtime. Ensure all entity types and their members are preserved.")]
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

        // SELECT
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
    [RequiresDynamicCode("BLite LINQ queries use Expression.Compile() and MakeGenericMethod which require dynamic code generation.")]
    [RequiresUnreferencedCode("BLite LINQ queries use reflection to resolve methods and types at runtime. Ensure all entity types and their members are preserved.")]
    private TResult ProjectEnumerable<TResult>(IEnumerable<T> source, LambdaExpression selectLambda)
    {
        if (!IsSequenceResult<TResult>())
            throw new NotSupportedException(
                $"BLite query pipeline: cannot project into result type '{typeof(TResult)}'.");

        var projType = typeof(TResult).GetGenericArguments()[0];
        var method = s_projectMethodCache.GetOrAdd(
            projType,
            t => typeof(BTreeQueryProvider<TId, T>)
                .GetMethod(nameof(ProjectTyped), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(t));

        return (TResult)method.Invoke(null, [source, selectLambda])!;
    }

    /// <summary>Compiles <paramref name="selectLambda"/> as <c>Func&lt;T, TProj&gt;</c> and projects the source.</summary>
    [RequiresDynamicCode("BLite LINQ queries use Expression.Compile() and MakeGenericMethod which require dynamic code generation.")]
    private static IEnumerable<TProj> ProjectTyped<TProj>(IEnumerable<T> source, LambdaExpression selectLambda)
    {
        var selector = (Func<T, TProj>)selectLambda.Compile();
        return source.Select(selector);
    }

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, MethodInfo>
        s_projectMethodCache = new();

    /// <summary>Whether <typeparamref name="TResult"/> is <c>IEnumerable&lt;TElement&gt;</c>, i.e. the query is enumerated rather than ended by a terminal.</summary>
    private static bool IsSequenceResult<TResult>()
        => typeof(TResult).IsGenericType && typeof(TResult).GetGenericTypeDefinition() == typeof(IEnumerable<>);

    /// <summary>
    /// Checks that <paramref name="expression"/> starts with <c>OrderBy</c>/<c>OrderByDescending</c>
    /// on the collection (the call whose key selector is <paramref name="orderBy"/>), followed by
    /// <c>Skip</c>/<c>Take</c> calls that bound it, so that the result only depends on the first
    /// <paramref name="windowEnd"/> documents in that order.
    /// </summary>
    /// <param name="plainWindow">
    /// <c>true</c> when the window is exactly <c>Take(n)</c> or <c>Skip(s).Take(n)</c>, as
    /// <see cref="QueryModel.Skip"/> and <see cref="QueryModel.Take"/> describe it, and only
    /// <c>Select</c> or a plain <c>Count()</c>/<c>LongCount()</c> follows it.
    /// </param>
    private static bool TryGetIndexWindow(Expression expression, LambdaExpression orderBy,
                                          out int windowEnd, out bool plainWindow)
    {
        windowEnd = 0;
        plainWindow = false;

        // Collect the Queryable calls from the root outwards.
        var calls = new List<MethodCallExpression>();
        var current = expression;
        while (current is MethodCallExpression call && call.Method.DeclaringType == typeof(Queryable))
        {
            calls.Add(call);
            current = call.Arguments[0];
        }
        if (current is not ConstantExpression { Value: IQueryable } || calls.Count < 2) return false;
        calls.Reverse();

        var first = calls[0];
        if (first.Method.Name is not ("OrderBy" or "OrderByDescending")
            || first.Arguments[1] is not UnaryExpression { Operand: var key }
            || !ReferenceEquals(key, orderBy))
            return false;

        long start = 0, length = long.MaxValue;
        int i = 1;
        for (; i < calls.Count && calls[i].Method.Name is "Skip" or "Take"; i++)
        {
            if (calls[i].Arguments[1] is not ConstantExpression { Value: int n }) return false;
            n = Math.Max(n, 0);
            if (calls[i].Method.Name == "Skip")
            {
                start += n;
                if (length != long.MaxValue) length = Math.Max(0, length - n);
            }
            else
            {
                length = Math.Min(length, n);
            }
        }

        // No Take bounds the window, or a ThenBy refines the order of the documents at its edge.
        if (length == long.MaxValue) return false;
        if (i < calls.Count && calls[i].Method.Name is "ThenBy" or "ThenByDescending") return false;

        long end = start + length;
        if (end > int.MaxValue) return false;
        windowEnd = (int)end;

        var window = calls.GetRange(1, i - 1).ConvertAll(c => c.Method.Name);
        plainWindow = (window.Count == 1 && window[0] == "Take"
                       || window.Count == 2 && window[0] == "Skip" && window[1] == "Take")
                      && calls.Skip(i).All(c => c.Method.Name == "Select"
                                                || c.Method.Name is "Count" or "LongCount" && c.Arguments.Count == 1);
        return true;
    }

    /// <summary>
    /// Whether an operator of the main chain (the <c>Queryable</c> calls from the result down
    /// to the root) has an argument other than its source that holds a queryable or calls a
    /// <c>Queryable</c> operator: a second use of the root (<c>Concat(q)</c>, <c>Join(q, ...)</c>,
    /// <c>Union(q.Select(...))</c>), another source, or a subquery inside a lambda.
    /// </summary>
    private static bool HasSecondaryQuery(Expression expression)
    {
        var finder = new SecondaryQueryFinder();
        for (var current = expression;
             current is MethodCallExpression call && call.Method.DeclaringType == typeof(Queryable);
             current = call.Arguments[0])
        {
            for (int i = 1; i < call.Arguments.Count && !finder.Found; i++)
                finder.Visit(call.Arguments[i]);
            if (finder.Found) return true;
        }
        return false;
    }

    /// <summary>
    /// Replaces each other sequence passed to an operator of the main chain (the <c>inner</c> of a
    /// <c>Join</c>, the second source of <c>Concat</c>, <c>Zip</c>, ...) that is a chain of
    /// <c>Queryable</c> calls with a queryable its own provider creates for it. The replay then
    /// enumerates that queryable, which runs it as a separate query, instead of rewriting its
    /// operators to <c>Enumerable</c> ones: its <c>Where</c> uses the indexes of its collection,
    /// and a <c>VectorSearch</c> in it, which only the index can answer, does not throw (#186).
    /// Lambdas are left alone: a subquery inside one may use the lambda's parameters.
    /// </summary>
    private static Expression QueryOtherSequences(Expression expression)
    {
        if (expression is not MethodCallExpression call
            || call.Method.DeclaringType != typeof(Queryable)
            || call.Arguments.Count == 0)
            return expression;

        var arguments = new Expression[call.Arguments.Count];
        arguments[0] = QueryOtherSequences(call.Arguments[0]);
        var parameters = call.Method.GetParameters();
        for (int i = 1; i < arguments.Length; i++)
        {
            var argument = call.Arguments[i];
            arguments[i] = argument is MethodCallExpression { Method.DeclaringType: var declaring } sequence
                           && declaring == typeof(Queryable)
                           && typeof(IQueryable).IsAssignableFrom(sequence.Type)
                           && FindChainRoot(sequence) is { } root
                ? Expression.Constant(root.Provider.CreateQuery(sequence), parameters[i].ParameterType)
                : argument;
        }

        return arguments.SequenceEqual(call.Arguments) ? call : call.Update(call.Object, arguments);
    }

    /// <summary>The queryable at the bottom of a chain of <c>Queryable</c> calls, or null.</summary>
    private static IQueryable? FindChainRoot(Expression expression)
    {
        while (expression is MethodCallExpression call
               && call.Method.DeclaringType == typeof(Queryable)
               && call.Arguments.Count > 0)
            expression = call.Arguments[0];
        return (expression as ConstantExpression)?.Value as IQueryable;
    }

    private sealed class SecondaryQueryFinder : ExpressionVisitor
    {
        public bool Found { get; private set; }

        public override Expression? Visit(Expression? node) => Found ? node : base.Visit(node);

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is IQueryable) Found = true;
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(Queryable))
            {
                Found = true;
                return node;
            }
            return base.VisitMethodCall(node);
        }
    }

    /// <summary>
    /// Type-switch dispatch for terminal results — first-match, no reflection per call.
    /// Covers all standard LINQ terminal operators: First/Single/Count/Any/All/ToList/ToArray.
    /// </summary>
    private static TResult TerminalReturn<TResult>(IEnumerable<T> data)
    {
        if (typeof(TResult) == typeof(IEnumerable<T>)) return (TResult)data;
        if (typeof(TResult) == typeof(List<T>)) return (TResult)(object)data.ToList();
        if (typeof(TResult) == typeof(T[])) return (TResult)(object)data.ToArray();
        if (typeof(TResult) == typeof(T)) return (TResult)(object)data.First();
        if (typeof(TResult) == typeof(int)) return (TResult)(object)data.Count();
        if (typeof(TResult) == typeof(long)) return (TResult)(object)(long)data.Count();
        if (typeof(TResult) == typeof(bool)) return (TResult)(object)data.Any();
        if (typeof(TResult) == typeof(object)) return (TResult)(object)(data.ToList());

        // Unknown terminal: materialise and attempt a cast — handles e.g. First<SubType>.
        var list = data.ToList();
        if (list.Count > 0 && list[0] is TResult first) return first;
        if (list is TResult asResult) return asResult;

        throw new NotSupportedException(
            $"BLite query pipeline: unsupported terminal result type '{typeof(TResult)}'.");
    }

    // ─── Aggregate push-down ─────────────────────────────────────────────────

    /// <summary>
    /// Whether <paramref name="expression"/> is <c>Sum</c>/<c>Average</c>/<c>Min</c>/<c>Max</c> with a
    /// selector, applied to zero or more <c>Where(x =&gt; ...)</c> calls over the collection itself.
    /// </summary>
    private static bool IsAggregateOverFilteredRoot(Expression expression)
    {
        if (expression is not MethodCallExpression call
            || call.Method.DeclaringType != typeof(Queryable)
            || call.Arguments.Count != 2)
            return false;

        var source = call.Arguments[0];
        while (source is MethodCallExpression where
               && where.Method.DeclaringType == typeof(Queryable)
               && where.Method.Name == nameof(Queryable.Where)
               && where.Arguments[1] is UnaryExpression { Operand: LambdaExpression { Parameters.Count: 1 } })
        {
            source = where.Arguments[0];
        }
        return source is ConstantExpression { Value: IQueryable };
    }

    // ─── Expression-tree helpers ──────────────────────────────────────────────

    /// <summary>
    /// Removes the <c>Queryable.Where(x =&gt; ...)</c> calls that <see cref="BTreeExpressionVisitor"/>
    /// folded into <see cref="QueryModel.WhereClause"/>: those on the root chain with only plain
    /// Where calls and orderings below them (<see cref="BTreeExpressionVisitor.CanFoldPredicate"/>).
    /// FetchAsync applied them, so every element reaching them already satisfies their predicate
    /// and dropping them leaves the result unchanged. Any other Where (after Take, SkipWhile,
    /// GroupBy, ...) was not folded and stays in the replay (#186).
    /// </summary>
    private static Expression DropAppliedWheres(Expression expression)
    {
        if (expression is not MethodCallExpression call
            || call.Method.DeclaringType != typeof(Queryable)
            || call.Arguments.Count == 0)
            return expression;

        var source = DropAppliedWheres(call.Arguments[0]);

        if (call.Method.Name == nameof(Queryable.Where)
            && call.Arguments[1] is UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression predicate }
            && BTreeExpressionVisitor.CanFoldPredicate(call.Arguments[0], predicate))
            return source;

        return source == call.Arguments[0]
            ? call
            : call.Update(call.Object, [source, .. call.Arguments.Skip(1)]);
    }

    /// <summary>
    /// Replaces the terminal whose predicate <see cref="BTreeExpressionVisitor"/> folded into
    /// <see cref="QueryModel.WhereClause"/> (<c>Count(p)</c>, <c>First(p)</c>, ...) with its
    /// parameterless overload. The visitor folds a predicate only when the terminal's source is the
    /// root followed by plain Where and ordering calls, so the elements reaching the terminal are
    /// fetched documents, which all satisfy the predicate already.
    /// </summary>
    [RequiresDynamicCode("BLite LINQ queries use Expression.Compile() and MakeGenericMethod which require dynamic code generation.")]
    [RequiresUnreferencedCode("BLite LINQ queries use reflection to resolve methods and types at runtime. Ensure all entity types and their members are preserved.")]
    private static Expression DropFoldedTerminalPredicate(Expression expression, MethodCallExpression? foldedTerminal)
    {
        if (foldedTerminal is null || !ReferenceEquals(expression, foldedTerminal))
            return expression;

        var name = foldedTerminal.Method.Name;
        if (!s_parameterlessTerminals.TryGetValue(name, out var definition))
        {
            definition = typeof(Queryable).GetMethods().Single(m => m.Name == name && m.GetParameters().Length == 1);
            s_parameterlessTerminals[name] = definition;
        }

        var parameterless = definition.MakeGenericMethod(foldedTerminal.Method.GetGenericArguments());
        return Expression.Call(parameterless, foldedTerminal.Arguments[0]);
    }

    /// <summary>Generic definitions of the parameterless Queryable terminals, by name.</summary>
    private static readonly ConcurrentDictionary<string, MethodInfo> s_parameterlessTerminals = new();

    /// <summary>
    /// Fallback for queries containing operators the direct pipeline cannot handle
    /// (GroupBy, Join, Sum/Average/Min/Max with selectors, etc.).
    /// The source data has already been fetched (and filtered by index/BSON-scan),
    /// so we only need to rewrite the remaining <c>Queryable.*</c> calls to
    /// <c>Enumerable.*</c> equivalents before compiling and executing.
    /// </summary>
    /// <remarks>
    /// The compiled <c>Func&lt;IEnumerable&lt;T&gt;, object?[], TResult&gt;</c> is cached per expression
    /// shape, so the expensive <see cref="Expression.Compile"/> call is paid only once per unique
    /// query shape. Every constant except the root (captured variables, inner sequences, literals)
    /// is lifted into the <c>object?[]</c> argument and passed in on each call: a constant compiled
    /// into the delegate would be reused by later calls with other values (#207).
    /// </remarks>
    [RequiresDynamicCode("BLite LINQ queries use Expression.Compile() and MakeGenericMethod which require dynamic code generation.")]
    private TResult ExecuteViaEnumerableRewriter<TResult>(Expression expression, IEnumerable<T> sourceData)
    {
        var rootFinder = new RootFinder();
        rootFinder.Visit(expression);
        var root = rootFinder.Root;
        if (root == null)
            throw new InvalidOperationException("Could not find root IQueryable in expression.");

        var argsParam = Expression.Parameter(typeof(object[]), "_args");
        var lifter = new ConstantLifter(root, argsParam);
        var shape = lifter.Visit(expression);

        // ToString() prints types by their short name only, so the lifted constants' full type
        // names are part of the key: two closure classes named <>c__DisplayClass0_0 in different
        // methods must not share a delegate.
        var cacheKey = typeof(TResult).FullName + "\x00" + shape.ToString() + "\x00"
            + string.Join("\x01", lifter.Types.Select(t => t.AssemblyQualifiedName));

        var cachedDelegate = s_enumRewriterCache.GetOrAdd(cacheKey, _ =>
        {
            var sourceParam = Expression.Parameter(typeof(IEnumerable<T>), "_src");
            var rewriter = new EnumerableRewriter(root, sourceParam);
            var rewritten = rewriter.Visit(shape);
            if (rewritten.Type != typeof(TResult))
                rewritten = Expression.Convert(rewritten, typeof(TResult));
            return Expression.Lambda<Func<IEnumerable<T>, object?[], TResult>>(rewritten, sourceParam, argsParam).Compile();
        });

        return ((Func<IEnumerable<T>, object?[], TResult>)cachedDelegate)(sourceData, lifter.Values.ToArray());
    }

    /// <summary>Cache of compiled EnumerableRewriter delegates keyed by expression shape.</summary>
    private static readonly ConcurrentDictionary<string, Delegate> s_enumRewriterCache = new();

    /// <summary>
    /// Replaces every constant except the root queryable with <c>(Type)_args[i]</c>, collecting
    /// the values and their types in order.
    /// </summary>
    private sealed class ConstantLifter : ExpressionVisitor
    {
        private readonly IQueryable _root;
        private readonly ParameterExpression _args;

        public ConstantLifter(IQueryable root, ParameterExpression args)
        {
            _root = root;
            _args = args;
        }

        public List<object?> Values { get; } = new();
        public List<Type> Types { get; } = new();

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (ReferenceEquals(node.Value, _root))
                return node;

            var index = Values.Count;
            Values.Add(node.Value);
            Types.Add(node.Type);
            return Expression.Convert(
                Expression.ArrayIndex(_args, Expression.Constant(index)), node.Type);
        }
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

    // ─── IBTreeQueryCore<T> implementation ───────────────────────────────────

    internal DocumentCollection<TId, T> Collection => _collection;

    IAsyncEnumerable<T> IBTreeQueryCore<T>.ScanAsync(BsonReaderPredicate predicate, CancellationToken ct)
        => _collection.ScanAsync(predicate, ct);

    IAsyncEnumerable<T> IBTreeQueryCore<T>.ScanAsync(IndexQueryPlan plan, CancellationToken ct)
        => _collection.ScanAsync(plan, ct);

    IEnumerable<CollectionIndexInfo> IBTreeQueryCore<T>.GetIndexes()
        => _collection.GetIndexes();

    Task<int> IBTreeQueryCore<T>.CountAsync(CancellationToken ct)
        => _collection.CountAsync(ct);

    ValueTask<TResult> IBTreeQueryCore<T>.MinBoundaryAsync<TResult>(IndexMinMax plan, CancellationToken ct)
        => _collection.MinBoundaryAsync<TResult>(plan, ct);

    ValueTask<TResult> IBTreeQueryCore<T>.MaxBoundaryAsync<TResult>(IndexMinMax plan, CancellationToken ct)
        => _collection.MaxBoundaryAsync<TResult>(plan, ct);
}
