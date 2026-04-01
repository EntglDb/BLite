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

    // ─── Throwing single-element ──────────────────────────────────────────────

    /// <summary>
    /// Returns the first element of the sequence asynchronously.
    /// Throws <see cref="InvalidOperationException"/> if the sequence is empty.
    /// </summary>
    public static async Task<T> FirstAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
                return item;
            throw new InvalidOperationException("Sequence contains no elements.");
        }

        return await Task.Run(source.First, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the first element matching the predicate asynchronously.
    /// Throws <see cref="InvalidOperationException"/> if no element matches.
    /// </summary>
    public static Task<T> FirstAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => source.Where(predicate).FirstAsync(ct);

    /// <summary>
    /// Returns the single element of the sequence asynchronously.
    /// Throws <see cref="InvalidOperationException"/> if the sequence is empty or contains more than one element.
    /// </summary>
    public static async Task<T> SingleAsync<T>(
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
            if (!seen) throw new InvalidOperationException("Sequence contains no elements.");
            return found!;
        }

        return await Task.Run(source.Single, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the single element matching the predicate asynchronously.
    /// Throws <see cref="InvalidOperationException"/> if no element or more than one element matches.
    /// </summary>
    public static Task<T> SingleAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => source.Where(predicate).SingleAsync(ct);

    // ─── Last ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the last element of the sequence asynchronously.
    /// Throws <see cref="InvalidOperationException"/> if the sequence is empty.
    /// </summary>
    public static async Task<T> LastAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            T? found = default;
            bool seen = false;
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
            {
                found = item;
                seen = true;
            }
            if (!seen) throw new InvalidOperationException("Sequence contains no elements.");
            return found!;
        }

        return await Task.Run(source.Last, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the last element matching the predicate asynchronously.
    /// Throws <see cref="InvalidOperationException"/> if no element matches.
    /// </summary>
    public static Task<T> LastAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => source.Where(predicate).LastAsync(ct);

    /// <summary>Returns the last element, or <c>default</c> if the sequence is empty.</summary>
    public static async Task<T?> LastOrDefaultAsync<T>(
        this IQueryable<T> source,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            T? found = default;
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
                found = item;
            return found;
        }

        return await Task.Run(source.LastOrDefault, ct).ConfigureAwait(false);
    }

    /// <summary>Returns the last element matching the predicate, or <c>default</c>.</summary>
    public static Task<T?> LastOrDefaultAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => source.Where(predicate).LastOrDefaultAsync(ct);

    // ─── ElementAt ────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the element at the specified 0-based index asynchronously.
    /// Throws <see cref="ArgumentOutOfRangeException"/> if <paramref name="index"/> is out of range.
    /// </summary>
    public static async Task<T> ElementAtAsync<T>(
        this IQueryable<T> source,
        int index,
        CancellationToken ct = default)
    {
        if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            int current = 0;
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
            {
                if (current == index) return item;
                current++;
            }
            throw new ArgumentOutOfRangeException(nameof(index), "Index was out of range.");
        }

        return await Task.Run(() => source.ElementAt(index), ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the element at the specified 0-based index, or <c>default</c> if the index is out of range.
    /// </summary>
    public static async Task<T?> ElementAtOrDefaultAsync<T>(
        this IQueryable<T> source,
        int index,
        CancellationToken ct = default)
    {
        if (index < 0) return default;

        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            int current = 0;
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
            {
                if (current == index) return item;
                current++;
            }
            return default;
        }

        return await Task.Run(() => source.ElementAtOrDefault(index), ct).ConfigureAwait(false);
    }

    // ─── Min / Max ────────────────────────────────────────────────────────────

    /// <summary>Returns the minimum projected value asynchronously.</summary>
    public static async Task<TResult> MinAsync<T, TResult>(
        this IQueryable<T> source,
        Expression<Func<T, TResult>> selector,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            var compiled = selector.Compile();
            var comparer = Comparer<TResult>.Default;
            TResult? min = default;
            bool seen = false;
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
            {
                var val = compiled(item);
                if (!seen || comparer.Compare(val, min!) < 0)
                {
                    min = val;
                    seen = true;
                }
            }
            if (!seen) throw new InvalidOperationException("Sequence contains no elements.");
            return min!;
        }

        return await Task.Run(() => source.Min(selector)!, ct).ConfigureAwait(false);
    }

    /// <summary>Returns the maximum projected value asynchronously.</summary>
    public static async Task<TResult> MaxAsync<T, TResult>(
        this IQueryable<T> source,
        Expression<Func<T, TResult>> selector,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            var compiled = selector.Compile();
            var comparer = Comparer<TResult>.Default;
            TResult? max = default;
            bool seen = false;
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
            {
                var val = compiled(item);
                if (!seen || comparer.Compare(val, max!) > 0)
                {
                    max = val;
                    seen = true;
                }
            }
            if (!seen) throw new InvalidOperationException("Sequence contains no elements.");
            return max!;
        }

        return await Task.Run(() => source.Max(selector)!, ct).ConfigureAwait(false);
    }

    // ─── Sum ──────────────────────────────────────────────────────────────────

    /// <summary>Returns the sum of a projected <see cref="int"/> column asynchronously.</summary>
    public static async Task<int> SumAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, int>> selector,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            var compiled = selector.Compile();
            int sum = 0;
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
                sum += compiled(item);
            return sum;
        }

        return await Task.Run(() => source.Sum(selector), ct).ConfigureAwait(false);
    }

    /// <summary>Returns the sum of a projected <see cref="long"/> column asynchronously.</summary>
    public static async Task<long> SumAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, long>> selector,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            var compiled = selector.Compile();
            long sum = 0L;
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
                sum += compiled(item);
            return sum;
        }

        return await Task.Run(() => source.Sum(selector), ct).ConfigureAwait(false);
    }

    /// <summary>Returns the sum of a projected <see cref="double"/> column asynchronously.</summary>
    public static async Task<double> SumAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, double>> selector,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            var compiled = selector.Compile();
            double sum = 0.0;
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
                sum += compiled(item);
            return sum;
        }

        return await Task.Run(() => source.Sum(selector), ct).ConfigureAwait(false);
    }

    /// <summary>Returns the sum of a projected <see cref="decimal"/> column asynchronously.</summary>
    public static async Task<decimal> SumAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, decimal>> selector,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            var compiled = selector.Compile();
            decimal sum = 0m;
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
                sum += compiled(item);
            return sum;
        }

        return await Task.Run(() => source.Sum(selector), ct).ConfigureAwait(false);
    }

    // ─── Average ──────────────────────────────────────────────────────────────

    /// <summary>Returns the average of a projected <see cref="int"/> column asynchronously.</summary>
    public static async Task<double> AverageAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, int>> selector,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            var compiled = selector.Compile();
            long sum = 0L;
            long count = 0L;
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
            {
                sum += compiled(item);
                count++;
            }
            if (count == 0) throw new InvalidOperationException("Sequence contains no elements.");
            return (double)sum / count;
        }

        return await Task.Run(() => source.Average(selector), ct).ConfigureAwait(false);
    }

    /// <summary>Returns the average of a projected <see cref="long"/> column asynchronously.</summary>
    public static async Task<double> AverageAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, long>> selector,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            var compiled = selector.Compile();
            long sum = 0L;
            long count = 0L;
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
            {
                sum += compiled(item);
                count++;
            }
            if (count == 0) throw new InvalidOperationException("Sequence contains no elements.");
            return (double)sum / count;
        }

        return await Task.Run(() => source.Average(selector), ct).ConfigureAwait(false);
    }

    /// <summary>Returns the average of a projected <see cref="double"/> column asynchronously.</summary>
    public static async Task<double> AverageAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, double>> selector,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            var compiled = selector.Compile();
            double sum = 0.0;
            long count = 0L;
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
            {
                sum += compiled(item);
                count++;
            }
            if (count == 0) throw new InvalidOperationException("Sequence contains no elements.");
            return sum / count;
        }

        return await Task.Run(() => source.Average(selector), ct).ConfigureAwait(false);
    }

    /// <summary>Returns the average of a projected <see cref="decimal"/> column asynchronously.</summary>
    public static async Task<decimal> AverageAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, decimal>> selector,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            var compiled = selector.Compile();
            decimal sum = 0m;
            long count = 0L;
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
            {
                sum += compiled(item);
                count++;
            }
            if (count == 0) throw new InvalidOperationException("Sequence contains no elements.");
            return sum / count;
        }

        return await Task.Run(() => source.Average(selector), ct).ConfigureAwait(false);
    }

    // ─── ForEach ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Executes the given <paramref name="action"/> for each element of the sequence asynchronously.
    /// </summary>
    public static async Task ForEachAsync<T>(
        this IQueryable<T> source,
        Action<T> action,
        CancellationToken ct = default)
    {
        if (source is IAsyncEnumerable<T> asyncEnum)
        {
            await foreach (var item in asyncEnum.WithCancellation(ct).ConfigureAwait(false))
                action(item);
            return;
        }

        await Task.Run(() =>
        {
            foreach (var item in source)
            {
                ct.ThrowIfCancellationRequested();
                action(item);
            }
        }, ct).ConfigureAwait(false);
    }
}
