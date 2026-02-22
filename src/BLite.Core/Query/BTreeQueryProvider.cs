using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using BLite.Core.Collections;
using static BLite.Core.Query.IndexOptimizer;

namespace BLite.Core.Query;

public class BTreeQueryProvider<TId, T> : IQueryProvider where T : class
{
    private readonly DocumentCollection<TId, T> _collection;

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

    public BTreeQueryProvider(DocumentCollection<TId, T> collection)
    {
        _collection = collection;
    }

    public IQueryable CreateQuery(Expression expression)
    {
        var elementType = expression.Type.GetGenericArguments()[0];
        try 
        {
            return (IQueryable)Activator.CreateInstance(
                typeof(BTreeQueryable<>).MakeGenericType(elementType), 
                new object[] { this, expression })!;
        }
        catch (TargetInvocationException ex)
        {
            throw ex.InnerException ?? ex;
        }
    }

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
        // 1. Visit to get model using strict BTreeExpressionVisitor (for optimization only)
        // We only care about WHERE clause for optimization.
        // GroupBy, Select, OrderBy, etc. are handled by EnumerableRewriter.
        
        var visitor = new BTreeExpressionVisitor();
        visitor.Visit(expression);
        var model = visitor.GetModel();

        // ── Push-down SELECT (+ optional WHERE)  ──────────────────────────────
        // When the query has a projection and no post-scan ordering/paging,
        // scan raw BSON bytes building TProj directly — T is never instantiated.
        // OrderBy / Take / Skip fall through to the standard path which pipes the
        // already-projected IEnumerable<TProj> through EnumerableRewriter.
        if (model.SelectClause is not null
            && model.OrderByClause is null
            && !model.Take.HasValue
            && !model.Skip.HasValue)
        {
            var pushed = TryPushDownSelect<TResult>(model.SelectClause, model.WhereClause);
            if (pushed is not null) return pushed;
        }
        // ─────────────────────────────────────────────────────────────────────
        
        // 2. Data Fetching Strategy (Optimized or Full Scan)
        IEnumerable<T> sourceData = null!;
        
        // A. Try Index Optimization (Only if Where clause exists)
        var indexOpt = IndexOptimizer.TryOptimize<T>(model, _collection.GetIndexes());
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
        
        // B. Try Scan Optimization (if no index used)
        if (sourceData == null)
        {
            Func<BLite.Bson.BsonSpanReader, bool>? bsonPredicate = null;
            if (model.WhereClause != null)
            {
                bsonPredicate = BsonExpressionEvaluator.TryCompile<T>(model.WhereClause);
            }

            if (bsonPredicate != null)
            {
                sourceData = _collection.Scan(bsonPredicate);
            }
        }

        // C. Fallback to Full Scan
        if (sourceData == null)
        {
            sourceData = _collection.FindAll();
        }
        
        // 3. Rewrite Expression Tree to use Enumerable
        // Replace the "Root" IQueryable with our sourceData IEnumerable
        
        // We need to find the root IQueryable in the expression to replace it.
        // It's likely the first argument of the first method call, or a constant.
        
        var rootFinder = new RootFinder();
        rootFinder.Visit(expression);
        var root = rootFinder.Root;
        
        if (root == null) throw new InvalidOperationException("Could not find root Queryable in expression");

        var rewriter = new EnumerableRewriter(root, sourceData);
        var rewrittenExpression = rewriter.Visit(expression);
        
        // 4. Compile and Execute
        // The rewritten expression is now a tree of IEnumerable calls returning TResult.
        // We need to turn it into a Func<TResult> and invoke it.
        
        if (rewrittenExpression.Type != typeof(TResult))
        {
            // If TResult is object (non-generic Execute), we need to cast
             rewrittenExpression = Expression.Convert(rewrittenExpression, typeof(TResult));
        }

        var lambda = Expression.Lambda<Func<TResult>>(rewrittenExpression);
        var compiled = lambda.Compile();
        return compiled();
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
            var compileMethod = s_tryCompileMethod.MakeGenericMethod(typeof(T), projType);
            projector = compileMethod.Invoke(null, [selectLambda, whereLambda]);
        }
        catch { return default; }

        if (projector is null) return default; // compilation soft-failed

        // Invoke _collection.Scan<TProj>(projector) via reflection.
        try
        {
            var scanMethod = s_scanProjectorMethod.MakeGenericMethod(projType);
            var result = scanMethod.Invoke(_collection, [projector]);
            return (TResult?)result;
        }
        catch { return default; }
    }

    // ─── Expression-tree helpers ──────────────────────────────────────────────

    private class RootFinder : ExpressionVisitor
    {
        public IQueryable? Root { get; private set; }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            // If we found a Queryable, that's our root source
            if (Root == null && node.Value is IQueryable q)
            {
                // We typically want the "base" queryable (the BTreeQueryable instance)
                // In a chain like Coll.Where.Select, the root is Coll.
                Root = q;
            }
            return base.VisitConstant(node);
        }
    }
}
