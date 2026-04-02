using System.Linq;
using System.Linq.Expressions;
using BLite.Core.Query;

namespace BLite.Core.Collections;

/// <summary>
/// Convenience async terminal operators on <see cref="IDocumentCollection{TId,T}"/>.
/// Each method builds an <see cref="IBLiteQueryable{T}"/> via <c>AsQueryable()</c>
/// and delegates to the <see cref="BTreeQueryable{T}"/> implementation —
/// index lookups, BSON scans, and <c>fetchLimit</c> all apply.
/// </summary>
public static class DocumentCollectionExtensions
{
    // ─── Where (returns IBLiteQueryable, keeps the expression tree alive) ─────

    /// <summary>Starts a queryable chain filtered by <paramref name="predicate"/>.</summary>
    public static IBLiteQueryable<T> Where<TId, T>(
        this IDocumentCollection<TId, T> col,
        Expression<Func<T, bool>> predicate) where T : class
        => col.AsQueryable().Where(predicate);

    // ─── Single-element terminals ─────────────────────────────────────────────

    /// <summary>Returns the first element, or <c>default</c> if the collection is empty.</summary>
    public static Task<T?> FirstOrDefaultAsync<TId, T>(
        this IDocumentCollection<TId, T> col,
        CancellationToken ct = default) where T : class
        => col.AsQueryable().FirstOrDefaultAsync(ct);

    /// <summary>Returns the first element matching <paramref name="predicate"/>, or <c>default</c>.</summary>
    public static Task<T?> FirstOrDefaultAsync<TId, T>(
        this IDocumentCollection<TId, T> col,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default) where T : class
        => col.AsQueryable().FirstOrDefaultAsync(predicate, ct);

    /// <summary>Returns the first element. Throws if empty.</summary>
    public static Task<T> FirstAsync<TId, T>(
        this IDocumentCollection<TId, T> col,
        CancellationToken ct = default) where T : class
        => col.AsQueryable().FirstAsync(ct);

    /// <summary>Returns the first element matching <paramref name="predicate"/>. Throws if none.</summary>
    public static Task<T> FirstAsync<TId, T>(
        this IDocumentCollection<TId, T> col,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default) where T : class
        => col.AsQueryable().FirstAsync(predicate, ct);

    /// <summary>Returns the single element, or <c>default</c> if empty. Throws if more than one.</summary>
    public static Task<T?> SingleOrDefaultAsync<TId, T>(
        this IDocumentCollection<TId, T> col,
        CancellationToken ct = default) where T : class
        => col.AsQueryable().SingleOrDefaultAsync(ct);

    /// <summary>Returns the single element matching <paramref name="predicate"/>, or <c>default</c>.</summary>
    public static Task<T?> SingleOrDefaultAsync<TId, T>(
        this IDocumentCollection<TId, T> col,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default) where T : class
        => col.AsQueryable().SingleOrDefaultAsync(predicate, ct);

    /// <summary>Returns the single element. Throws if empty or more than one.</summary>
    public static Task<T> SingleAsync<TId, T>(
        this IDocumentCollection<TId, T> col,
        CancellationToken ct = default) where T : class
        => col.AsQueryable().SingleAsync(ct);

    /// <summary>Returns the single element matching <paramref name="predicate"/>. Throws if none or >1.</summary>
    public static Task<T> SingleAsync<TId, T>(
        this IDocumentCollection<TId, T> col,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default) where T : class
        => col.AsQueryable().SingleAsync(predicate, ct);

    // ─── Aggregates ───────────────────────────────────────────────────────────

    /// <summary>Returns <c>true</c> if the collection contains any elements.</summary>
    public static Task<bool> AnyAsync<TId, T>(
        this IDocumentCollection<TId, T> col,
        CancellationToken ct = default) where T : class
        => col.AsQueryable().AnyAsync(ct);

    /// <summary>Returns <c>true</c> if any element matches <paramref name="predicate"/>.</summary>
    public static Task<bool> AnyAsync<TId, T>(
        this IDocumentCollection<TId, T> col,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default) where T : class
        => col.AsQueryable().AnyAsync(predicate, ct);

    /// <summary>Returns <c>true</c> if all elements match <paramref name="predicate"/>.</summary>
    public static Task<bool> AllAsync<TId, T>(
        this IDocumentCollection<TId, T> col,
        Expression<Func<T, bool>> predicate,
        CancellationToken ct = default) where T : class
        => col.AsQueryable().AllAsync(predicate, ct);

    // ─── Materialisation ──────────────────────────────────────────────────────

    /// <summary>Returns all elements as a list.</summary>
    public static Task<List<T>> ToListAsync<TId, T>(
        this IDocumentCollection<TId, T> col,
        CancellationToken ct = default) where T : class
        => col.AsQueryable().ToListAsync(ct);

    /// <summary>Returns all elements as an array.</summary>
    public static Task<T[]> ToArrayAsync<TId, T>(
        this IDocumentCollection<TId, T> col,
        CancellationToken ct = default) where T : class
        => col.AsQueryable().ToArrayAsync(ct);
}
