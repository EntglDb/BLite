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

    // ─── Async bridge ─────────────────────────────────────────────────────────

    /// <summary>
    /// Exposes any <see cref="IQueryable{T}"/> returned by BLite as an
    /// <see cref="IAsyncEnumerable{T}"/>, enabling <c>await foreach</c> after
    /// standard LINQ operators (e.g. <c>query.Select(...).AsAsyncEnumerable()</c>).
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the queryable is not a BLite queryable (does not implement
    /// <see cref="IAsyncEnumerable{T}"/>).
    /// </exception>
    public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IQueryable<T> source)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
            return asyncEnum;

        throw new InvalidOperationException(
            $"The queryable of type '{source.GetType().Name}' does not implement " +
            $"IAsyncEnumerable<{typeof(T).Name}>. Use DocumentCollection.AsQueryable() " +
            "to obtain a BLite queryable that supports async enumeration.");
    }

    // ─── Materialisation ──────────────────────────────────────────────────────

    /// <summary>Executes the query asynchronously and returns all results as a list.</summary>
    public static async Task<List<T>> ToListAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            var list = new List<T>();
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
                list.Add(item);
            return list;
        }

        return await Task.Run(source.ToList, ct).ConfigureAwait(false);
    }

    /// <summary>Executes the query asynchronously and returns results as an array.</summary>
    public static async Task<T[]> ToArrayAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
    {
        var list = await source.ToListAsync(ct).ConfigureAwait(false);
        return list.ToArray();
    }

    // ─── Single-element ───────────────────────────────────────────────────────

    /// <summary>Returns the first element, or <c>default</c> if the sequence is empty.</summary>
    public static async Task<T?> FirstOrDefaultAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
                return item;
            return default;
        }

        return await Task.Run(source.FirstOrDefault, ct).ConfigureAwait(false);
    }

    /// <summary>Returns the first element matching the predicate, or <c>default</c>.</summary>
    public static Task<T?> FirstOrDefaultAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => source.Where(predicate).FirstOrDefaultAsync(ct);

    /// <summary>
    /// Returns the single element of the sequence, or <c>default</c> if empty.
    /// Throws <see cref="InvalidOperationException"/> if more than one element exists.
    /// </summary>
    public static async Task<T?> SingleOrDefaultAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            T? found = default;
            bool seen = false;
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
            {
                if (seen) throw new InvalidOperationException("Sequence contains more than one element.");
                found = item;
                seen = true;
            }
            return found;
        }

        return await Task.Run(source.SingleOrDefault, ct).ConfigureAwait(false);
    }

    /// <summary>Returns the single element matching the predicate, or <c>default</c>.</summary>
    public static Task<T?> SingleOrDefaultAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => source.Where(predicate).SingleOrDefaultAsync(ct);

    // ─── Aggregates ───────────────────────────────────────────────────────────

    /// <summary>Returns the number of elements in the sequence asynchronously.</summary>
    public static async Task<int> CountAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            int count = 0;
            await foreach (var _ in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
                count++;
            return count;
        }

        return await Task.Run(source.Count, ct).ConfigureAwait(false);
    }

    /// <summary>Returns the number of elements matching the predicate asynchronously.</summary>
    public static Task<int> CountAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => source.Where(predicate).CountAsync(ct);

    /// <summary>Returns <c>true</c> if any element satisfies the condition asynchronously.</summary>
    public static async Task<bool> AnyAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            await foreach (var _ in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
                return true;
            return false;
        }

        return await Task.Run(source.Any, ct).ConfigureAwait(false);
    }

    /// <summary>Returns <c>true</c> if any element matches the predicate asynchronously.</summary>
    public static Task<bool> AnyAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => source.Where(predicate).AnyAsync(ct);

    /// <summary>Returns <c>true</c> if all elements match the predicate asynchronously.</summary>
    public static async Task<bool> AllAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
    {
        // All(p) ≡ !Any(!p) but we keep it explicit for clarity
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            var compiled = predicate.Compile();
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
            {
                if (!compiled(item)) return false;
            }
            return true;
        }

        return await Task.Run(() => source.All(predicate), ct).ConfigureAwait(false);
    }
}
