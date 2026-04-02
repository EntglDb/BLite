using System.Linq.Expressions;

namespace BLite.Core.Query;

/// <summary>
/// A BLite queryable that extends <see cref="IOrderedQueryable{T}"/> and preserves
/// async enumeration capability across LINQ chains (.Where / .Select / .OrderBy / .Take / .Skip).
/// </summary>
/// <remarks>
/// The runtime object (<see cref="BTreeQueryable{T}"/>) also implements
/// <see cref="IAsyncEnumerable{T}"/>, so async enumeration is always accessible:
/// <list type="bullet">
///   <item>Use <c>.AsAsyncEnumerable()</c> to get an <see cref="IAsyncEnumerable{T}"/> for <c>await foreach</c>.</item>
///   <item>Use <c>ToListAsync</c> / <c>ToArrayAsync</c> to materialise the full sequence.</item>
///   <item>Use <c>FirstAsync</c> / <c>FirstOrDefaultAsync</c> for the first matching element.</item>
///   <item>Use <c>SingleAsync</c> / <c>SingleOrDefaultAsync</c> for exactly one matching element.</item>
///   <item>Use <c>LastAsync</c> / <c>LastOrDefaultAsync</c> for the last matching element.</item>
///   <item>Use <c>ElementAtAsync</c> / <c>ElementAtOrDefaultAsync</c> for an element by index.</item>
///   <item>Use <c>AnyAsync</c> / <c>AllAsync</c> / <c>CountAsync</c> for predicates and aggregates.</item>
///   <item>Use <c>MinAsync</c> / <c>MaxAsync</c> / <c>SumAsync</c> / <c>AverageAsync</c> for numeric aggregates.</item>
///   <item>Use <c>ForEachAsync</c> to iterate with a side-effecting action.</item>
/// </list>
/// <para>
/// All async terminators are defined in <see cref="BLiteQueryableExtensions"/> and do
/// <b>not</b> block the calling thread. Prefer these over the inherited sync LINQ terminators
/// (e.g. <c>.First()</c>, <c>.ToList()</c>, <c>.Count()</c>) which block on a BLite queryable.
/// </para>
/// <para>
/// <see cref="IAsyncEnumerable{T}"/> is intentionally <b>not</b> part of this interface because
/// having both <see cref="IQueryable{T}"/> and <see cref="IAsyncEnumerable{T}"/> on the
/// same type causes compiler ambiguity when <c>System.Linq.Async</c> is also referenced.
/// </para>
/// </remarks>
public interface IBLiteQueryable<T> : IOrderedQueryable<T>
{
    /// <summary>Returns the first element, or <c>default</c> if the sequence is empty.</summary>
    Task<T?> FirstOrDefaultAsync(CancellationToken ct = default);

    /// <summary>Returns the first element matching <paramref name="predicate"/>, or <c>default</c>.</summary>
    Task<T?> FirstOrDefaultAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default);

    /// <summary>Executes the query asynchronously and returns all results as a list.</summary>
    Task<List<T>> ToListAsync(CancellationToken ct = default);

    /// <summary>Returns the single element of the sequence, or <c>default</c> if empty. Throws if more than one element.</summary>
    Task<T?> SingleOrDefaultAsync(CancellationToken ct = default);

    /// <summary>Returns the single element matching <paramref name="predicate"/>, or <c>default</c>. Throws if more than one.</summary>
    Task<T?> SingleOrDefaultAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default);

    /// <summary>Returns the first element. Throws if empty.</summary>
    Task<T> FirstAsync(CancellationToken ct = default);

    /// <summary>Returns the first element matching <paramref name="predicate"/>. Throws if none matches.</summary>
    Task<T> FirstAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default);

    /// <summary>Returns the single element. Throws if empty or more than one.</summary>
    Task<T> SingleAsync(CancellationToken ct = default);

    /// <summary>Returns the single element matching <paramref name="predicate"/>. Throws if none or more than one.</summary>
    Task<T> SingleAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default);

    /// <summary>Returns <c>true</c> if all elements match <paramref name="predicate"/>.</summary>
    Task<bool> AllAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default);

    /// <summary>Executes the query asynchronously and returns results as an array.</summary>
    Task<T[]> ToArrayAsync(CancellationToken ct = default);

    /// <summary>Returns the number of elements asynchronously.</summary>
    Task<int> CountAsync(CancellationToken ct = default);

    /// <summary>Returns the number of elements matching <paramref name="predicate"/> asynchronously.</summary>
    Task<int> CountAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default);

    /// <summary>Returns <c>true</c> if the sequence contains any elements.</summary>
    Task<bool> AnyAsync(CancellationToken ct = default);

    /// <summary>Returns <c>true</c> if any element matches <paramref name="predicate"/>.</summary>
    Task<bool> AnyAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default);

    /// <summary>Returns the last element. Throws if empty.</summary>
    Task<T> LastAsync(CancellationToken ct = default);

    /// <summary>Returns the last element matching <paramref name="predicate"/>. Throws if none.</summary>
    Task<T> LastAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default);

    /// <summary>Returns the last element, or <c>default</c> if empty.</summary>
    Task<T?> LastOrDefaultAsync(CancellationToken ct = default);

    /// <summary>Returns the last element matching <paramref name="predicate"/>, or <c>default</c>.</summary>
    Task<T?> LastOrDefaultAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default);

    /// <summary>Returns the element at <paramref name="index"/>. Throws if out of range.</summary>
    Task<T> ElementAtAsync(int index, CancellationToken ct = default);

    /// <summary>Returns the element at <paramref name="index"/>, or <c>default</c> if out of range.</summary>
    Task<T?> ElementAtOrDefaultAsync(int index, CancellationToken ct = default);

    /// <summary>Executes <paramref name="action"/> for each element asynchronously.</summary>
    Task ForEachAsync(Action<T> action, CancellationToken ct = default);

    /// <summary>Returns the queryable as an <see cref="IAsyncEnumerable{T}"/>.</summary>
    IAsyncEnumerable<T> AsAsyncEnumerable();

    // ─── OLAP aggregates (BSON field scan, T never materialised) ──────────────

    /// <summary>Returns the sum of a projected <see cref="int"/> field.</summary>
    Task<int> SumAsync(Expression<Func<T, int>> selector, CancellationToken ct = default);

    /// <summary>Returns the sum of a projected <see cref="long"/> field.</summary>
    Task<long> SumAsync(Expression<Func<T, long>> selector, CancellationToken ct = default);

    /// <summary>Returns the sum of a projected <see cref="double"/> field.</summary>
    Task<double> SumAsync(Expression<Func<T, double>> selector, CancellationToken ct = default);

    /// <summary>Returns the sum of a projected <see cref="decimal"/> field.</summary>
    Task<decimal> SumAsync(Expression<Func<T, decimal>> selector, CancellationToken ct = default);

    /// <summary>Returns the average of a projected <see cref="int"/> field.</summary>
    Task<double> AverageAsync(Expression<Func<T, int>> selector, CancellationToken ct = default);

    /// <summary>Returns the average of a projected <see cref="long"/> field.</summary>
    Task<double> AverageAsync(Expression<Func<T, long>> selector, CancellationToken ct = default);

    /// <summary>Returns the average of a projected <see cref="double"/> field.</summary>
    Task<double> AverageAsync(Expression<Func<T, double>> selector, CancellationToken ct = default);

    /// <summary>Returns the average of a projected <see cref="decimal"/> field.</summary>
    Task<decimal> AverageAsync(Expression<Func<T, decimal>> selector, CancellationToken ct = default);

    /// <summary>Returns the minimum projected value.</summary>
    Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken ct = default);

    /// <summary>Returns the maximum projected value.</summary>
    Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken ct = default);
}
