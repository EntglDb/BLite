using BLite.Core.Indexing;
using System.Linq;
using System.Linq.Expressions;

namespace BLite.Core.Query;

/// <summary>
/// Async LINQ extensions for BLite queryables.
/// Any <see cref="System.Linq.IQueryable{T}"/> returned by
/// <c>DocumentCollection.AsQueryable()</c> also implements
/// <see cref="IAsyncEnumerable{T}"/>, enabling these helpers.
/// </summary>
public static class BLiteQueryableExtensions
{
    // ─── IBLiteQueryable-preserving LINQ operators ────────────────────────────
    // Standard Queryable.Where/Select/etc. call IQueryProvider.CreateQuery<T> which
    // is declared as returning IQueryable<T>. The runtime object is always
    // BTreeQueryable<T> : IBLiteQueryable<T>, so the cast is safe and restores the
    // richer interface across the full query chain.

    /// <summary>Filters a sequence and preserves the <see cref="IBLiteQueryable{T}"/> contract.</summary>
    public static IBLiteQueryable<T> Where<T>(
        this IBLiteQueryable<T> source,
        Expression<Func<T, bool>> predicate)
        => (IBLiteQueryable<T>)Queryable.Where(source, predicate);

    /// <summary>Projects each element and preserves the <see cref="IBLiteQueryable{TResult}"/> contract.</summary>
    public static IBLiteQueryable<TResult> Select<T, TResult>(
        this IBLiteQueryable<T> source,
        Expression<Func<T, TResult>> selector)
        => (IBLiteQueryable<TResult>)Queryable.Select(source, selector);

    /// <summary>Sorts ascending and preserves the <see cref="IBLiteQueryable{T}"/> contract.</summary>
    public static IBLiteQueryable<T> OrderBy<T, TKey>(
        this IBLiteQueryable<T> source,
        Expression<Func<T, TKey>> keySelector)
        => (IBLiteQueryable<T>)Queryable.OrderBy(source, keySelector);

    /// <summary>Sorts descending and preserves the <see cref="IBLiteQueryable{T}"/> contract.</summary>
    public static IBLiteQueryable<T> OrderByDescending<T, TKey>(
        this IBLiteQueryable<T> source,
        Expression<Func<T, TKey>> keySelector)
        => (IBLiteQueryable<T>)Queryable.OrderByDescending(source, keySelector);

    /// <summary>Returns a specified number of elements and preserves the <see cref="IBLiteQueryable{T}"/> contract.</summary>
    public static IBLiteQueryable<T> Take<T>(
        this IBLiteQueryable<T> source,
        int count)
        => (IBLiteQueryable<T>)Queryable.Take(source, count);

    /// <summary>Skips a specified number of elements and preserves the <see cref="IBLiteQueryable{T}"/> contract.</summary>
    public static IBLiteQueryable<T> Skip<T>(
        this IBLiteQueryable<T> source,
        int count)
        => (IBLiteQueryable<T>)Queryable.Skip(source, count);

    // ─── Async terminal delegates ─────────────────────────────────────────────
    // Thin wrappers that route IQueryable<T> calls to the concrete
    // IBLiteQueryable<T> implementation — zero logic here.

    /// <summary>Returns the first element, or <c>default</c> if the sequence is empty.</summary>
    public static Task<T?> FirstOrDefaultAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).FirstOrDefaultAsync(ct);

    /// <summary>Returns the first element matching the predicate, or <c>default</c>.</summary>
    public static Task<T?> FirstOrDefaultAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).FirstOrDefaultAsync(predicate, ct);

    /// <summary>Executes the query asynchronously and returns all results as a list.</summary>
    public static Task<List<T>> ToListAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).ToListAsync(ct);

    /// <summary>Returns the single element, or <c>default</c> if empty. Throws if more than one.</summary>
    public static Task<T?> SingleOrDefaultAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).SingleOrDefaultAsync(ct);

    /// <summary>Returns the single element matching the predicate, or <c>default</c>.</summary>
    public static Task<T?> SingleOrDefaultAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).SingleOrDefaultAsync(predicate, ct);

    /// <summary>Returns the first element. Throws if empty.</summary>
    public static Task<T> FirstAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).FirstAsync(ct);

    /// <summary>Returns the first element matching the predicate. Throws if none matches.</summary>
    public static Task<T> FirstAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).FirstAsync(predicate, ct);

    /// <summary>Returns the single element. Throws if empty or more than one.</summary>
    public static Task<T> SingleAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).SingleAsync(ct);

    /// <summary>Returns the single element matching the predicate. Throws if none or more than one.</summary>
    public static Task<T> SingleAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).SingleAsync(predicate, ct);

    /// <summary>Returns <c>true</c> if all elements match the predicate.</summary>
    public static Task<bool> AllAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).AllAsync(predicate, ct);

    /// <summary>Executes the query asynchronously and returns results as an array.</summary>
    public static Task<T[]> ToArrayAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).ToArrayAsync(ct);

    /// <summary>Returns the number of elements asynchronously.</summary>
    public static Task<int> CountAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).CountAsync(ct);

    /// <summary>Returns the number of elements matching the predicate asynchronously.</summary>
    public static Task<int> CountAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).CountAsync(predicate, ct);

    /// <summary>Returns <c>true</c> if the sequence contains any elements.</summary>
    public static Task<bool> AnyAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).AnyAsync(ct);

    /// <summary>Returns <c>true</c> if any element matches the predicate.</summary>
    public static Task<bool> AnyAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).AnyAsync(predicate, ct);

    /// <summary>Returns the last element. Throws if empty.</summary>
    public static Task<T> LastAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).LastAsync(ct);

    /// <summary>Returns the last element matching the predicate. Throws if none.</summary>
    public static Task<T> LastAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).LastAsync(predicate, ct);

    /// <summary>Returns the last element, or <c>default</c> if empty.</summary>
    public static Task<T?> LastOrDefaultAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).LastOrDefaultAsync(ct);

    /// <summary>Returns the last element matching the predicate, or <c>default</c>.</summary>
    public static Task<T?> LastOrDefaultAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).LastOrDefaultAsync(predicate, ct);

    /// <summary>Returns the element at the specified index. Throws if out of range.</summary>
    public static Task<T> ElementAtAsync<T>(
        this IQueryable<T> source,
        int index,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).ElementAtAsync(index, ct);

    /// <summary>Returns the element at the specified index, or <c>default</c> if out of range.</summary>
    public static Task<T?> ElementAtOrDefaultAsync<T>(
        this IQueryable<T> source,
        int index,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).ElementAtOrDefaultAsync(index, ct);

    /// <summary>Executes the action for each element asynchronously.</summary>
    public static Task ForEachAsync<T>(
        this IQueryable<T> source,
        Action<T> action,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).ForEachAsync(action, ct);

    /// <summary>Returns the queryable as an <see cref="IAsyncEnumerable{T}"/>.</summary>
    public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IQueryable<T> source)
        => ((IBLiteQueryable<T>)source).AsAsyncEnumerable();

    // ─── OLAP aggregates ──────────────────────────────────────────────────────

    /// <summary>Returns the sum of a projected <see cref="int"/> field.</summary>
    public static Task<int> SumAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, int>> selector,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).SumAsync(selector, ct);

    /// <summary>Returns the sum of a projected <see cref="long"/> field.</summary>
    public static Task<long> SumAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, long>> selector,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).SumAsync(selector, ct);

    /// <summary>Returns the sum of a projected <see cref="double"/> field.</summary>
    public static Task<double> SumAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, double>> selector,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).SumAsync(selector, ct);

    /// <summary>Returns the sum of a projected <see cref="decimal"/> field.</summary>
    public static Task<decimal> SumAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, decimal>> selector,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).SumAsync(selector, ct);

    /// <summary>Returns the average of a projected <see cref="int"/> field.</summary>
    public static Task<double> AverageAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, int>> selector,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).AverageAsync(selector, ct);

    /// <summary>Returns the average of a projected <see cref="long"/> field.</summary>
    public static Task<double> AverageAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, long>> selector,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).AverageAsync(selector, ct);

    /// <summary>Returns the average of a projected <see cref="double"/> field.</summary>
    public static Task<double> AverageAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, double>> selector,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).AverageAsync(selector, ct);

    /// <summary>Returns the average of a projected <see cref="decimal"/> field.</summary>
    public static Task<decimal> AverageAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, decimal>> selector,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).AverageAsync(selector, ct);

    /// <summary>Returns the minimum projected value.</summary>
    public static Task<TResult> MinAsync<T, TResult>(
        this IQueryable<T> source,
        Expression<Func<T, TResult>> selector,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).MinAsync(selector, ct);

    /// <summary>Returns the maximum projected value.</summary>
    public static Task<TResult> MaxAsync<T, TResult>(
        this IQueryable<T> source,
        Expression<Func<T, TResult>> selector,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).MaxAsync(selector, ct);

    // ─── Phase 3: AOT-safe IndexQueryPlan / IndexMinMax overloads ────────────

    /// <summary>Returns the indexes of the underlying collection.</summary>
    public static IEnumerable<CollectionIndexInfo> GetIndexes<T>(this IQueryable<T> source)
        => ((IBLiteQueryable<T>)source).GetIndexes();

    /// <summary>Executes the query using a pre-built <see cref="IndexQueryPlan"/> and returns all results as a list.</summary>
    public static Task<List<T>> ToListAsync<T>(
        this IQueryable<T> source,
        IndexQueryPlan plan,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).ToListAsync(plan, ct);

    /// <summary>Returns the first element matching the plan, or <c>default</c> if none.</summary>
    public static Task<T?> FirstOrDefaultAsync<T>(
        this IQueryable<T> source,
        IndexQueryPlan plan,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).FirstOrDefaultAsync(plan, ct);

    /// <summary>Returns the first element matching the plan. Throws if none matches.</summary>
    public static Task<T> FirstAsync<T>(
        this IQueryable<T> source,
        IndexQueryPlan plan,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).FirstAsync(plan, ct);

    /// <summary>Returns the single element matching the plan, or <c>default</c>. Throws if more than one.</summary>
    public static Task<T?> SingleOrDefaultAsync<T>(
        this IQueryable<T> source,
        IndexQueryPlan plan,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).SingleOrDefaultAsync(plan, ct);

    /// <summary>Returns the single element matching the plan. Throws if none or more than one.</summary>
    public static Task<T> SingleAsync<T>(
        this IQueryable<T> source,
        IndexQueryPlan plan,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).SingleAsync(plan, ct);

    /// <summary>Returns <c>true</c> if any element matches the plan.</summary>
    public static Task<bool> AnyAsync<T>(
        this IQueryable<T> source,
        IndexQueryPlan plan,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).AnyAsync(plan, ct);

    /// <summary>Counts elements matching the plan.</summary>
    public static Task<int> CountAsync<T>(
        this IQueryable<T> source,
        IndexQueryPlan plan,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).CountAsync(plan, ct);

    /// <summary>Executes the query using a pre-built <see cref="IndexQueryPlan"/> and returns results as an array.</summary>
    public static Task<T[]> ToArrayAsync<T>(
        this IQueryable<T> source,
        IndexQueryPlan plan,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).ToArrayAsync(plan, ct);

    /// <summary>Executes <paramref name="action"/> for each element matching the plan.</summary>
    public static Task ForEachAsync<T>(
        this IQueryable<T> source,
        IndexQueryPlan plan,
        Action<T> action,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).ForEachAsync(plan, action, ct);

    /// <summary>Returns the minimum field value using the given <see cref="IndexMinMax"/> plan.</summary>
    public static Task<TResult> MinAsync<T, TResult>(
        this IQueryable<T> source,
        IndexMinMax plan,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).MinAsync<TResult>(plan, ct);

    /// <summary>Returns the maximum field value using the given <see cref="IndexMinMax"/> plan.</summary>
    public static Task<TResult> MaxAsync<T, TResult>(
        this IQueryable<T> source,
        IndexMinMax plan,
        CancellationToken ct = default)
        => ((IBLiteQueryable<T>)source).MaxAsync<TResult>(plan, ct);
}
