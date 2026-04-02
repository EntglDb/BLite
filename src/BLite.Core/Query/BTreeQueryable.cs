using System.Collections;
using System.Linq;
using System.Linq.Expressions;

namespace BLite.Core.Query;

internal class BTreeQueryable<T> : IBLiteQueryable<T>, IAsyncEnumerable<T>
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

    // ─── Async terminal operators (direct provider path, no IAsyncEnumerable overhead) ──

    /// <inheritdoc />
    public Task<T?> FirstOrDefaultAsync(CancellationToken ct)
    {
        // Inject Take(1) so BTreeQueryProvider.fetchLimit = 1 → single item fetched.
        var limited = Queryable.Take(this, 1);
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(limited.Expression);
            using var enumerator = results.GetEnumerator();
            return enumerator.MoveNext() ? enumerator.Current : default(T?);
        }, ct);
    }

    /// <inheritdoc />
    public Task<T?> FirstOrDefaultAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var filtered = Queryable.Where(this, predicate);
        var limited = Queryable.Take(filtered, 1);
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(limited.Expression);
            using var enumerator = results.GetEnumerator();
            return enumerator.MoveNext() ? enumerator.Current : default(T?);
        }, ct);
    }

    /// <inheritdoc />
    public Task<List<T>> ToListAsync(CancellationToken ct)
    {
        var expr = Expression;
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(expr);
            return results.ToList();
        }, ct);
    }

    /// <inheritdoc />
    public Task<T?> SingleOrDefaultAsync(CancellationToken ct)
    {
        // Take(2) to detect duplicates while limiting the scan.
        var limited = Queryable.Take(this, 2);
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(limited.Expression);
            using var enumerator = results.GetEnumerator();
            if (!enumerator.MoveNext()) return default(T?);
            var found = enumerator.Current;
            if (enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains more than one element.");
            return found;
        }, ct);
    }

    /// <inheritdoc />
    public Task<T?> SingleOrDefaultAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var filtered = Queryable.Where(this, predicate);
        var limited = Queryable.Take(filtered, 2);
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(limited.Expression);
            using var enumerator = results.GetEnumerator();
            if (!enumerator.MoveNext()) return default(T?);
            var found = enumerator.Current;
            if (enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains more than one element.");
            return found;
        }, ct);
    }

    /// <inheritdoc />
    public Task<T> FirstAsync(CancellationToken ct)
    {
        var limited = Queryable.Take(this, 1);
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(limited.Expression);
            using var enumerator = results.GetEnumerator();
            if (!enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains no elements.");
            return enumerator.Current;
        }, ct);
    }

    /// <inheritdoc />
    public Task<T> FirstAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var filtered = Queryable.Where(this, predicate);
        var limited = Queryable.Take(filtered, 1);
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(limited.Expression);
            using var enumerator = results.GetEnumerator();
            if (!enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains no elements.");
            return enumerator.Current;
        }, ct);
    }

    /// <inheritdoc />
    public Task<T> SingleAsync(CancellationToken ct)
    {
        var limited = Queryable.Take(this, 2);
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(limited.Expression);
            using var enumerator = results.GetEnumerator();
            if (!enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains no elements.");
            var found = enumerator.Current;
            if (enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains more than one element.");
            return found;
        }, ct);
    }

    /// <inheritdoc />
    public Task<T> SingleAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var filtered = Queryable.Where(this, predicate);
        var limited = Queryable.Take(filtered, 2);
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(limited.Expression);
            using var enumerator = results.GetEnumerator();
            if (!enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains no elements.");
            var found = enumerator.Current;
            if (enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains more than one element.");
            return found;
        }, ct);
    }

    /// <inheritdoc />
    public Task<bool> AllAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var expr = Expression;
        return Task.Run(() =>
        {
            var compiled = predicate.Compile();
            var results = Provider.Execute<IEnumerable<T>>(expr);
            foreach (var item in results)
            {
                ct.ThrowIfCancellationRequested();
                if (!compiled(item)) return false;
            }
            return true;
        }, ct);
    }

    /// <inheritdoc />
    public Task<T[]> ToArrayAsync(CancellationToken ct)
    {
        var expr = Expression;
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(expr);
            return results.ToArray();
        }, ct);
    }

    /// <inheritdoc />
    public Task<int> CountAsync(CancellationToken ct)
    {
        var expr = Expression;
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(expr);
            return results.Count();
        }, ct);
    }

    /// <inheritdoc />
    public Task<int> CountAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var filtered = Queryable.Where(this, predicate);
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(filtered.Expression);
            return results.Count();
        }, ct);
    }

    /// <inheritdoc />
    public Task<bool> AnyAsync(CancellationToken ct)
    {
        var limited = Queryable.Take(this, 1);
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(limited.Expression);
            using var enumerator = results.GetEnumerator();
            return enumerator.MoveNext();
        }, ct);
    }

    /// <inheritdoc />
    public Task<bool> AnyAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var filtered = Queryable.Where(this, predicate);
        var limited = Queryable.Take(filtered, 1);
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(limited.Expression);
            using var enumerator = results.GetEnumerator();
            return enumerator.MoveNext();
        }, ct);
    }

    /// <inheritdoc />
    public Task<T> LastAsync(CancellationToken ct)
    {
        var expr = Expression;
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(expr);
            T? found = default;
            bool seen = false;
            foreach (var item in results) { found = item; seen = true; }
            if (!seen) throw new InvalidOperationException("Sequence contains no elements.");
            return found!;
        }, ct);
    }

    /// <inheritdoc />
    public Task<T> LastAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var filtered = Queryable.Where(this, predicate);
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(filtered.Expression);
            T? found = default;
            bool seen = false;
            foreach (var item in results) { found = item; seen = true; }
            if (!seen) throw new InvalidOperationException("Sequence contains no elements.");
            return found!;
        }, ct);
    }

    /// <inheritdoc />
    public Task<T?> LastOrDefaultAsync(CancellationToken ct)
    {
        var expr = Expression;
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(expr);
            T? found = default;
            foreach (var item in results) found = item;
            return found;
        }, ct);
    }

    /// <inheritdoc />
    public Task<T?> LastOrDefaultAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var filtered = Queryable.Where(this, predicate);
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(filtered.Expression);
            T? found = default;
            foreach (var item in results) found = item;
            return found;
        }, ct);
    }

    /// <inheritdoc />
    public Task<T> ElementAtAsync(int index, CancellationToken ct)
    {
        if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));
        // Skip(index).Take(1) lets the provider apply fetchLimit.
        var limited = Queryable.Take(Queryable.Skip(this, index), 1);
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(limited.Expression);
            using var enumerator = results.GetEnumerator();
            if (!enumerator.MoveNext()) throw new ArgumentOutOfRangeException(nameof(index), "Index was out of range.");
            return enumerator.Current;
        }, ct);
    }

    /// <inheritdoc />
    public Task<T?> ElementAtOrDefaultAsync(int index, CancellationToken ct)
    {
        if (index < 0) return Task.FromResult(default(T?));
        var limited = Queryable.Take(Queryable.Skip(this, index), 1);
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(limited.Expression);
            using var enumerator = results.GetEnumerator();
            return enumerator.MoveNext() ? enumerator.Current : default(T?);
        }, ct);
    }

    /// <inheritdoc />
    public Task ForEachAsync(Action<T> action, CancellationToken ct)
    {
        var expr = Expression;
        return Task.Run(() =>
        {
            var results = Provider.Execute<IEnumerable<T>>(expr);
            foreach (var item in results)
            {
                ct.ThrowIfCancellationRequested();
                action(item);
            }
        }, ct);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<T> AsAsyncEnumerable() => this;

    // ─── OLAP aggregates (expression tree → Provider → TryBsonAggregate → BSON field scan) ──

    /// <inheritdoc />
    public Task<int> SumAsync(Expression<Func<T, int>> selector, CancellationToken ct)
    {
        var sumExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Sum), new[] { typeof(T) },
            Expression, Expression.Quote(selector));
        return Task.Run(() => Provider.Execute<int>(sumExpr), ct);
    }

    /// <inheritdoc />
    public Task<long> SumAsync(Expression<Func<T, long>> selector, CancellationToken ct)
    {
        var sumExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Sum), new[] { typeof(T) },
            Expression, Expression.Quote(selector));
        return Task.Run(() => Provider.Execute<long>(sumExpr), ct);
    }

    /// <inheritdoc />
    public Task<double> SumAsync(Expression<Func<T, double>> selector, CancellationToken ct)
    {
        var sumExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Sum), new[] { typeof(T) },
            Expression, Expression.Quote(selector));
        return Task.Run(() => Provider.Execute<double>(sumExpr), ct);
    }

    /// <inheritdoc />
    public Task<decimal> SumAsync(Expression<Func<T, decimal>> selector, CancellationToken ct)
    {
        var sumExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Sum), new[] { typeof(T) },
            Expression, Expression.Quote(selector));
        return Task.Run(() => Provider.Execute<decimal>(sumExpr), ct);
    }

    /// <inheritdoc />
    public Task<double> AverageAsync(Expression<Func<T, int>> selector, CancellationToken ct)
    {
        var avgExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Average), new[] { typeof(T) },
            Expression, Expression.Quote(selector));
        return Task.Run(() => Provider.Execute<double>(avgExpr), ct);
    }

    /// <inheritdoc />
    public Task<double> AverageAsync(Expression<Func<T, long>> selector, CancellationToken ct)
    {
        var avgExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Average), new[] { typeof(T) },
            Expression, Expression.Quote(selector));
        return Task.Run(() => Provider.Execute<double>(avgExpr), ct);
    }

    /// <inheritdoc />
    public Task<double> AverageAsync(Expression<Func<T, double>> selector, CancellationToken ct)
    {
        var avgExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Average), new[] { typeof(T) },
            Expression, Expression.Quote(selector));
        return Task.Run(() => Provider.Execute<double>(avgExpr), ct);
    }

    /// <inheritdoc />
    public Task<decimal> AverageAsync(Expression<Func<T, decimal>> selector, CancellationToken ct)
    {
        var avgExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Average), new[] { typeof(T) },
            Expression, Expression.Quote(selector));
        return Task.Run(() => Provider.Execute<decimal>(avgExpr), ct);
    }

    /// <inheritdoc />
    public Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken ct)
    {
        var minExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Min), new[] { typeof(T), typeof(TResult) },
            Expression, Expression.Quote(selector));
        return Task.Run(() => Provider.Execute<TResult>(minExpr), ct);
    }

    /// <inheritdoc />
    public Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken ct)
    {
        var maxExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Max), new[] { typeof(T), typeof(TResult) },
            Expression, Expression.Quote(selector));
        return Task.Run(() => Provider.Execute<TResult>(maxExpr), ct);
    }

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
