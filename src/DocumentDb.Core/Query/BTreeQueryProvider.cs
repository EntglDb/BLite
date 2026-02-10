using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using DocumentDb.Core.Collections;

namespace DocumentDb.Core.Query;

public class BTreeQueryProvider : IQueryProvider
{
    private readonly object _collection;

    public BTreeQueryProvider(object collection)
    {
        _collection = collection;
    }

    public IQueryable CreateQuery(Expression expression)
    {
        var elementType = expression.Type.GetGenericArguments()[0]; // Extract T from IQueryable<T> or IQueryable
        // However, Type might be IQueryable<T>, so we check for generic arguments.
        // If expression.Type is just IQueryable, we might have issues. 
        // Standard LINQ providers usually assume IQueryable<T>.
        
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

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        return new BTreeQueryable<TElement>(this, expression);
    }

    public object? Execute(Expression expression)
    {
        return Execute<object>(expression);
    }

    public TResult Execute<TResult>(Expression expression)
    {
        var rootFinder = new RootFinder();
        rootFinder.Visit(expression);
        
        if (rootFinder.Root == null) 
            throw new InvalidOperationException("Could not find root Queryable.");
        
        var elementType = rootFinder.Root.ElementType;
        
        // Use reflection to call generic ExecuteQuery
        var method = typeof(BTreeQueryProvider)
            .GetMethod(nameof(ExecuteQuery), BindingFlags.NonPublic | BindingFlags.Instance)!
            .MakeGenericMethod(elementType);
            
        try
        {
            var result = method.Invoke(this, new object[] { expression });
            return (TResult)result!;
        }
        catch (TargetInvocationException ex)
        {
            throw ex.InnerException ?? ex;
        }
    }

    private object ExecuteQuery<T>(Expression expression) where T : class
    {
        // 1. Visit to get model
        var visitor = new BTreeExpressionVisitor();
        visitor.Visit(expression);
        var model = visitor.GetModel();
        
        if (_collection is not DocumentCollection<T> collection)
            throw new InvalidOperationException($"Provider expects DocumentCollection<{typeof(T).Name}> but found {_collection.GetType().Name}");
        
        // 2. Execution Strategy
        IEnumerable<T> query = null!;
        
        // A. Try Index Optimization
        var indexOpt = IndexOptimizer.TryOptimize<T>(model, collection.GetIndexes());
        if (indexOpt != null)
        {
             query = collection.QueryIndex(indexOpt.IndexName, indexOpt.MinValue, indexOpt.MaxValue);
        }
        
        // B. Try Scan Optimization (if no index used)
        if (query == null)
        {
            Func<DocumentDb.Bson.BsonSpanReader, bool>? bsonPredicate = null;
            if (model.WhereClause != null)
            {
                bsonPredicate = BsonExpressionEvaluator.TryCompile<T>(model.WhereClause);
            }

            if (bsonPredicate != null)
            {
                query = collection.Scan(bsonPredicate);
            }
        }

        // C. Fallback to Full Scan
        if (query == null)
        {
            query = collection.FindAll();
        }

        // Apply Where in memory (always safe to re-apply, ensuring correctness)
        if (model.WhereClause != null)
        {
            var predicate = (Func<T, bool>)model.WhereClause.Compile();
            query = query.Where(predicate);
        }
        
        // Apply OrderBy
        if (model.OrderByClause != null)
        {
            // We have to use reflection to call OrderBy/OrderByDescending because TKey is unknown
            var keyType = model.OrderByClause.ReturnType;
            var method = typeof(Enumerable)
                .GetMethods()
                .First(m => m.Name == (model.OrderDescending ? "OrderByDescending" : "OrderBy") 
                            && m.GetParameters().Length == 2)
                .MakeGenericMethod(typeof(T), keyType);
                
            var compiledKeySelector = model.OrderByClause.Compile();
            query = (IEnumerable<T>)method.Invoke(null, new object[] { query, compiledKeySelector })!;
        }

        // Apply Skip
        if (model.Skip.HasValue)
        {
            query = query.Skip(model.Skip.Value);
        }

        // Apply Take
        if (model.Take.HasValue)
        {
            query = query.Take(model.Take.Value);
        }

        // Apply Select (Projection)
        if (model.SelectClause != null)
        {
            // Result is no longer IEnumerable<T>, but IEnumerable<TResult>
            var resultType = model.SelectClause.ReturnType;
            var selectMethod = typeof(Enumerable)
                .GetMethods()
                .First(m => m.Name == "Select" 
                            && m.GetParameters().Length == 2)
                .MakeGenericMethod(typeof(T), resultType);
            
            var compiledSelector = model.SelectClause.Compile();
            return selectMethod.Invoke(null, new object[] { query, compiledSelector })!;
        }

        return query;
    }

    private class RootFinder : ExpressionVisitor
    {
        public IQueryable? Root { get; private set; }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is IQueryable q)
            {
                Root = q;
            }
            return base.VisitConstant(node);
        }
    }
}
