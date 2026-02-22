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
///   <item>Use <c>ToListAsync</c>, <c>FirstOrDefaultAsync</c>, etc. directly — they resolve the async path at runtime.</item>
/// </list>
/// <para>
/// <see cref="IAsyncEnumerable{T}"/> is intentionally <b>not</b> part of this interface because
/// having both <see cref="IQueryable{T}"/> and <see cref="IAsyncEnumerable{T}"/> on the
/// same type causes compiler ambiguity when <c>System.Linq.Async</c> is also referenced.
/// </para>
/// </remarks>
public interface IBLiteQueryable<T> : IOrderedQueryable<T>
{
}
