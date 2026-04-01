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
}
