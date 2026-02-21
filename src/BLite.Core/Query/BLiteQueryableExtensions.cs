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
