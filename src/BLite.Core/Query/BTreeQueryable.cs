using System.Collections;
using System.Linq;
using System.Linq.Expressions;

namespace BLite.Core.Query;

internal class BTreeQueryable<T> : IOrderedQueryable<T>, IAsyncEnumerable<T>
{
    public BTreeQueryable(IQueryProvider provider, Expression expression)
    {
        Provider = provider;
        Expression = expression;
    }

    public BTreeQueryable(IQueryProvider provider)
    {
        Provider = provider;
        Expression = Expression.Constant(this);
    }

    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider { get; }

    public IEnumerator<T> GetEnumerator()
    {
        return Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    /// <summary>
    /// Async enumeration: offloads the (CPU-bound) query execution to the thread pool,
    /// then streams results back to the caller.
    /// This allows <c>await foreach</c> and async LINQ extensions on any BLite queryable.
    /// </summary>
    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken ct = default)
    {
        // Execute the full LINQ pipeline on the thread pool so the calling context is not blocked.
        // The BTreeQueryProvider resolves index optimizations synchronously (WAL cache / BTree pages).
        var captured = Expression; // capture for Task.Run closure
        var results = await Task.Run(() => Provider.Execute<IEnumerable<T>>(captured), ct)
                                .ConfigureAwait(false);

        foreach (var item in results)
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }
    }
}
