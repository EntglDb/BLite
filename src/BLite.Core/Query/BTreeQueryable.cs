// NOTE: BTreeQueryable is the LINQ queryable implementation for BLite collections.
// It extensively uses Expression.Call and IAsyncQueryProvider.ExecuteAsync (which requires
// dynamic code) to build and execute expression trees. All warnings are suppressed here
// because this class is inherently dynamic and only used via annotated APIs.
#pragma warning disable IL3050, IL2026
using BLite.Core.Indexing;
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

    private IAsyncQueryProvider AsyncProvider => (IAsyncQueryProvider)Provider;

    // ─── Async terminal operators (direct provider path, no IAsyncEnumerable overhead) ──

    /// <inheritdoc />
    public async Task<T?> FirstOrDefaultAsync(CancellationToken ct)
    {
        // Inject Take(1) so BTreeQueryProvider.fetchLimit = 1 → single item fetched.
        var limited = Queryable.Take(this, 1);
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(limited.Expression, ct).ConfigureAwait(false);
        using var enumerator = results.GetEnumerator();
        return enumerator.MoveNext() ? enumerator.Current : default(T?);
    }

    /// <inheritdoc />
    public async Task<T?> FirstOrDefaultAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var filtered = Queryable.Where(this, predicate);
        var limited = Queryable.Take(filtered, 1);
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(limited.Expression, ct).ConfigureAwait(false);
        using var enumerator = results.GetEnumerator();
        return enumerator.MoveNext() ? enumerator.Current : default(T?);
    }

    /// <inheritdoc />
    public async Task<List<T>> ToListAsync(CancellationToken ct)
    {
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(Expression, ct).ConfigureAwait(false);
        return results.ToList();
    }

    /// <inheritdoc />
    public async Task<T?> SingleOrDefaultAsync(CancellationToken ct)
    {
        // Take(2) to detect duplicates while limiting the scan.
        var limited = Queryable.Take(this, 2);
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(limited.Expression, ct).ConfigureAwait(false);
        using var enumerator = results.GetEnumerator();
        if (!enumerator.MoveNext()) return default(T?);
        var found = enumerator.Current;
        if (enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains more than one element.");
        return found;
    }

    /// <inheritdoc />
    public async Task<T?> SingleOrDefaultAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var filtered = Queryable.Where(this, predicate);
        var limited = Queryable.Take(filtered, 2);
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(limited.Expression, ct).ConfigureAwait(false);
        using var enumerator = results.GetEnumerator();
        if (!enumerator.MoveNext()) return default(T?);
        var found = enumerator.Current;
        if (enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains more than one element.");
        return found;
    }

    /// <inheritdoc />
    public async Task<T> FirstAsync(CancellationToken ct)
    {
        var limited = Queryable.Take(this, 1);
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(limited.Expression, ct).ConfigureAwait(false);
        using var enumerator = results.GetEnumerator();
        if (!enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains no elements.");
        return enumerator.Current;
    }

    /// <inheritdoc />
    public async Task<T> FirstAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var filtered = Queryable.Where(this, predicate);
        var limited = Queryable.Take(filtered, 1);
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(limited.Expression, ct).ConfigureAwait(false);
        using var enumerator = results.GetEnumerator();
        if (!enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains no elements.");
        return enumerator.Current;
    }

    /// <inheritdoc />
    public async Task<T> SingleAsync(CancellationToken ct)
    {
        var limited = Queryable.Take(this, 2);
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(limited.Expression, ct).ConfigureAwait(false);
        using var enumerator = results.GetEnumerator();
        if (!enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains no elements.");
        var found = enumerator.Current;
        if (enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains more than one element.");
        return found;
    }

    /// <inheritdoc />
    public async Task<T> SingleAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var filtered = Queryable.Where(this, predicate);
        var limited = Queryable.Take(filtered, 2);
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(limited.Expression, ct).ConfigureAwait(false);
        using var enumerator = results.GetEnumerator();
        if (!enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains no elements.");
        var found = enumerator.Current;
        if (enumerator.MoveNext()) throw new InvalidOperationException("Sequence contains more than one element.");
        return found;
    }

    /// <inheritdoc />
    public async Task<bool> AllAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var compiled = predicate.Compile();
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(Expression, ct).ConfigureAwait(false);
        foreach (var item in results)
        {
            ct.ThrowIfCancellationRequested();
            if (!compiled(item)) return false;
        }
        return true;
    }

    /// <inheritdoc />
    public async Task<T[]> ToArrayAsync(CancellationToken ct)
    {
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(Expression, ct).ConfigureAwait(false);
        return results.ToArray();
    }

    /// <inheritdoc />
    public Task<int> CountAsync(CancellationToken ct)
    {
        // Build Queryable.Count<T>(source) so the provider can push it down to the
        // native key-only scan, avoiding full document materialization.
        var countExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Count), new[] { typeof(T) },
            Expression);
        return AsyncProvider.ExecuteAsync<int>(countExpr, ct);
    }

    /// <inheritdoc />
    public Task<int> CountAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        // Build Queryable.Where(source, predicate).Count() so the provider can use an
        // index key-only scan or a streaming count without keeping all T objects in memory.
        var filtered = Queryable.Where(this, predicate);
        var countExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Count), new[] { typeof(T) },
            filtered.Expression);
        return AsyncProvider.ExecuteAsync<int>(countExpr, ct);
    }

    /// <inheritdoc />
    public async Task<bool> AnyAsync(CancellationToken ct)
    {
        var limited = Queryable.Take(this, 1);
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(limited.Expression, ct).ConfigureAwait(false);
        using var enumerator = results.GetEnumerator();
        return enumerator.MoveNext();
    }

    /// <inheritdoc />
    public async Task<bool> AnyAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var filtered = Queryable.Where(this, predicate);
        var limited = Queryable.Take(filtered, 1);
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(limited.Expression, ct).ConfigureAwait(false);
        using var enumerator = results.GetEnumerator();
        return enumerator.MoveNext();
    }

    /// <inheritdoc />
    public async Task<T> LastAsync(CancellationToken ct)
    {
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(Expression, ct).ConfigureAwait(false);
        T? found = default;
        bool seen = false;
        foreach (var item in results) { found = item; seen = true; }
        if (!seen) throw new InvalidOperationException("Sequence contains no elements.");
        return found!;
    }

    /// <inheritdoc />
    public async Task<T> LastAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var filtered = Queryable.Where(this, predicate);
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(filtered.Expression, ct).ConfigureAwait(false);
        T? found = default;
        bool seen = false;
        foreach (var item in results) { found = item; seen = true; }
        if (!seen) throw new InvalidOperationException("Sequence contains no elements.");
        return found!;
    }

    /// <inheritdoc />
    public async Task<T?> LastOrDefaultAsync(CancellationToken ct)
    {
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(Expression, ct).ConfigureAwait(false);
        T? found = default;
        foreach (var item in results) found = item;
        return found;
    }

    /// <inheritdoc />
    public async Task<T?> LastOrDefaultAsync(Expression<Func<T, bool>> predicate, CancellationToken ct)
    {
        var filtered = Queryable.Where(this, predicate);
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(filtered.Expression, ct).ConfigureAwait(false);
        T? found = default;
        foreach (var item in results) found = item;
        return found;
    }

    /// <inheritdoc />
    public async Task<T> ElementAtAsync(int index, CancellationToken ct)
    {
        if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));
        // Skip(index).Take(1) lets the provider apply fetchLimit.
        var limited = Queryable.Take(Queryable.Skip(this, index), 1);
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(limited.Expression, ct).ConfigureAwait(false);
        using var enumerator = results.GetEnumerator();
        if (!enumerator.MoveNext()) throw new ArgumentOutOfRangeException(nameof(index), "Index was out of range.");
        return enumerator.Current;
    }

    /// <inheritdoc />
    public async Task<T?> ElementAtOrDefaultAsync(int index, CancellationToken ct)
    {
        if (index < 0) return default(T?);
        var limited = Queryable.Take(Queryable.Skip(this, index), 1);
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(limited.Expression, ct).ConfigureAwait(false);
        using var enumerator = results.GetEnumerator();
        return enumerator.MoveNext() ? enumerator.Current : default(T?);
    }

    /// <inheritdoc />
    public async Task ForEachAsync(Action<T> action, CancellationToken ct)
    {
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(Expression, ct).ConfigureAwait(false);
        foreach (var item in results)
        {
            ct.ThrowIfCancellationRequested();
            action(item);
        }
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
        return AsyncProvider.ExecuteAsync<int>(sumExpr, ct);
    }

    /// <inheritdoc />
    public Task<long> SumAsync(Expression<Func<T, long>> selector, CancellationToken ct)
    {
        var sumExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Sum), new[] { typeof(T) },
            Expression, Expression.Quote(selector));
        return AsyncProvider.ExecuteAsync<long>(sumExpr, ct);
    }

    /// <inheritdoc />
    public Task<double> SumAsync(Expression<Func<T, double>> selector, CancellationToken ct)
    {
        var sumExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Sum), new[] { typeof(T) },
            Expression, Expression.Quote(selector));
        return AsyncProvider.ExecuteAsync<double>(sumExpr, ct);
    }

    /// <inheritdoc />
    public Task<decimal> SumAsync(Expression<Func<T, decimal>> selector, CancellationToken ct)
    {
        var sumExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Sum), new[] { typeof(T) },
            Expression, Expression.Quote(selector));
        return AsyncProvider.ExecuteAsync<decimal>(sumExpr, ct);
    }

    /// <inheritdoc />
    public Task<double> AverageAsync(Expression<Func<T, int>> selector, CancellationToken ct)
    {
        var avgExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Average), new[] { typeof(T) },
            Expression, Expression.Quote(selector));
        return AsyncProvider.ExecuteAsync<double>(avgExpr, ct);
    }

    /// <inheritdoc />
    public Task<double> AverageAsync(Expression<Func<T, long>> selector, CancellationToken ct)
    {
        var avgExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Average), new[] { typeof(T) },
            Expression, Expression.Quote(selector));
        return AsyncProvider.ExecuteAsync<double>(avgExpr, ct);
    }

    /// <inheritdoc />
    public Task<double> AverageAsync(Expression<Func<T, double>> selector, CancellationToken ct)
    {
        var avgExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Average), new[] { typeof(T) },
            Expression, Expression.Quote(selector));
        return AsyncProvider.ExecuteAsync<double>(avgExpr, ct);
    }

    /// <inheritdoc />
    public Task<decimal> AverageAsync(Expression<Func<T, decimal>> selector, CancellationToken ct)
    {
        var avgExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Average), new[] { typeof(T) },
            Expression, Expression.Quote(selector));
        return AsyncProvider.ExecuteAsync<decimal>(avgExpr, ct);
    }

    /// <inheritdoc />
    public Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken ct)
    {
        var minExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Min), new[] { typeof(T), typeof(TResult) },
            Expression, Expression.Quote(selector));
        return AsyncProvider.ExecuteAsync<TResult>(minExpr, ct);
    }

    /// <inheritdoc />
    public Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken ct)
    {
        var maxExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Max), new[] { typeof(T), typeof(TResult) },
            Expression, Expression.Quote(selector));
        return AsyncProvider.ExecuteAsync<TResult>(maxExpr, ct);
    }

    // ─── Phase 3: AOT-safe filter overloads (IndexQueryPlan / IndexMinMax) ─────

    /// <inheritdoc />
    public IEnumerable<CollectionIndexInfo> GetIndexes()
        => ((IBTreeQueryCore<T>)Provider).GetIndexes();

    /// <inheritdoc />
    public async Task<List<T>> ToListAsync(IndexQueryPlan plan, CancellationToken ct)
    {
        var core = (IBTreeQueryCore<T>)Provider;
        var list = new List<T>();
        await foreach (var item in core.ScanAsync(plan, ct).ConfigureAwait(false))
            list.Add(item);
        return list;
    }

    /// <inheritdoc />
    public async Task<T?> FirstOrDefaultAsync(IndexQueryPlan plan, CancellationToken ct)
    {
        var core = (IBTreeQueryCore<T>)Provider;
        await foreach (var item in core.ScanAsync(plan, ct).ConfigureAwait(false))
            return item;
        return default;
    }

    /// <inheritdoc />
    public async Task<T> FirstAsync(IndexQueryPlan plan, CancellationToken ct)
    {
        var core = (IBTreeQueryCore<T>)Provider;
        await foreach (var item in core.ScanAsync(plan, ct).ConfigureAwait(false))
            return item;
        throw new InvalidOperationException("Sequence contains no elements.");
    }

    /// <inheritdoc />
    public async Task<T?> SingleOrDefaultAsync(IndexQueryPlan plan, CancellationToken ct)
    {
        var core = (IBTreeQueryCore<T>)Provider;
        T? found = default;
        int count = 0;
        await foreach (var item in core.ScanAsync(plan, ct).ConfigureAwait(false))
        {
            if (count++ == 0) found = item;
            else throw new InvalidOperationException("Sequence contains more than one element.");
        }
        return found;
    }

    /// <inheritdoc />
    public async Task<T> SingleAsync(IndexQueryPlan plan, CancellationToken ct)
    {
        var core = (IBTreeQueryCore<T>)Provider;
        T? found = default;
        int count = 0;
        await foreach (var item in core.ScanAsync(plan, ct).ConfigureAwait(false))
        {
            if (count++ == 0) found = item;
            else throw new InvalidOperationException("Sequence contains more than one element.");
        }
        if (count == 0) throw new InvalidOperationException("Sequence contains no elements.");
        return found!;
    }

    /// <inheritdoc />
    public async Task<bool> AnyAsync(IndexQueryPlan plan, CancellationToken ct)
    {
        var core = (IBTreeQueryCore<T>)Provider;
        await foreach (var _ in core.ScanAsync(plan, ct).ConfigureAwait(false))
            return true;
        return false;
    }

    /// <inheritdoc />
    public async Task<int> CountAsync(IndexQueryPlan plan, CancellationToken ct)
    {
        var core = (IBTreeQueryCore<T>)Provider;
        int count = 0;
        await foreach (var _ in core.ScanAsync(plan, ct).ConfigureAwait(false))
            count++;
        return count;
    }

    /// <inheritdoc />
    public async Task<T[]> ToArrayAsync(IndexQueryPlan plan, CancellationToken ct)
    {
        var list = await ToListAsync(plan, ct).ConfigureAwait(false);
        return list.ToArray();
    }

    /// <inheritdoc />
    public async Task ForEachAsync(IndexQueryPlan plan, Action<T> action, CancellationToken ct)
    {
        var core = (IBTreeQueryCore<T>)Provider;
        await foreach (var item in core.ScanAsync(plan, ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            action(item);
        }
    }

    /// <inheritdoc />
    public async Task<TResult> MinAsync<TResult>(IndexMinMax plan, CancellationToken ct)
    {
        var core = (IBTreeQueryCore<T>)Provider;
        return await core.MinBoundaryAsync<TResult>(plan, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TResult> MaxAsync<TResult>(IndexMinMax plan, CancellationToken ct)
    {
        var core = (IBTreeQueryCore<T>)Provider;
        return await core.MaxBoundaryAsync<TResult>(plan, ct).ConfigureAwait(false);
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
    /// Async enumeration: executes the query via the provider's async path and streams results.
    /// This allows <c>await foreach</c> and async LINQ extensions on any BLite queryable.
    /// </summary>
    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken ct = default)
    {
        var results = await AsyncProvider.ExecuteAsync<IEnumerable<T>>(Expression, ct)
                                         .ConfigureAwait(false);
        foreach (var item in results)
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }
    }
}
