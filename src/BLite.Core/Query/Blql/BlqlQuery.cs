using System;
using System.Collections.Generic;
using System.Threading;
using BLite.Bson;

namespace BLite.Core.Query.Blql;

/// <summary>
/// Fluent query builder for BLQL — the BLite Query Language.
/// Operates on <see cref="DynamicCollection"/> and returns <see cref="BsonDocument"/> results.
/// 
/// Inspired by MQL (MongoDB Query Language) semantics, supporting:
/// filtering, sorting, field projection, skip and limit.
///
/// Usage:
/// <code>
/// // Simple query
/// var docs = collection.Query()
///     .Filter(BlqlFilter.And(
///         BlqlFilter.Eq("status", "active"),
///         BlqlFilter.Gt("age", 18)))
///     .OrderBy("name")
///     .Skip(0).Take(20)
///     .ToList();
///
/// // One-liner with shorthand
/// var docs = collection.Query(BlqlFilter.Eq("email", "alice@example.com"))
///     .First();
///
/// // With projection
/// var docs = collection.Query()
///     .Filter(BlqlFilter.Exists("email"))
///     .Project(BlqlProjection.Include("name", "email"))
///     .OrderByDescending("createdAt")
///     .ToList();
/// </code>
/// </summary>
public sealed class BlqlQuery
{
    private readonly DynamicCollection _collection;
    private BlqlFilter _filter = BlqlFilter.Empty;
    private BlqlSort? _sort;
    private BlqlProjection _projection = BlqlProjection.All;
    private int _skip = 0;
    private int? _take;

    internal BlqlQuery(DynamicCollection collection)
    {
        _collection = collection ?? throw new ArgumentNullException(nameof(collection));
    }

    // ── Filter ─────────────────────────────────────────────────────────────

    /// <summary>Sets the filter for this query, replacing any previously set filter.</summary>
    public BlqlQuery Filter(BlqlFilter filter)
    {
        _filter = filter ?? BlqlFilter.Empty;
        return this;
    }

    /// <summary>Adds an additional AND condition to the existing filter.</summary>
    public BlqlQuery And(BlqlFilter filter)
    {
        _filter = _filter == BlqlFilter.Empty ? filter : BlqlFilter.And(_filter, filter);
        return this;
    }

    /// <summary>Adds an additional OR condition to the existing filter.</summary>
    public BlqlQuery Or(BlqlFilter filter)
    {
        _filter = _filter == BlqlFilter.Empty ? filter : BlqlFilter.Or(_filter, filter);
        return this;
    }

    // ── Sort ───────────────────────────────────────────────────────────────

    /// <summary>Sorts results by the given field ascending.</summary>
    public BlqlQuery OrderBy(string field)
    {
        _sort = BlqlSort.Ascending(field);
        return this;
    }

    /// <summary>Sorts results by the given field descending.</summary>
    public BlqlQuery OrderByDescending(string field)
    {
        _sort = BlqlSort.Descending(field);
        return this;
    }

    /// <summary>Sorts results using the given sort specification.</summary>
    public BlqlQuery Sort(BlqlSort sort)
    {
        _sort = sort;
        return this;
    }

    /// <summary>
    /// Sorts results using a JSON sort string (MQL-style).
    /// Example: <c>"{ \"name\": 1, \"age\": -1 }"</c>
    /// </summary>
    public BlqlQuery Sort(string sortJson)
    {
        _sort = BlqlSortParser.Parse(sortJson);
        return this;
    }

    // ── Paging ─────────────────────────────────────────────────────────────

    /// <summary>Skips the first N matching documents.</summary>
    public BlqlQuery Skip(int count)
    {
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), "Skip must be >= 0");
        _skip = count;
        return this;
    }

    /// <summary>Returns at most N matching documents.</summary>
    public BlqlQuery Take(int count)
    {
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), "Take must be >= 0");
        _take = count;
        return this;
    }

    /// <summary>Equivalent to <see cref="Take(int)"/> — alias for familiarity with SQL/MQL.</summary>
    public BlqlQuery Limit(int count) => Take(count);

    // ── Projection ─────────────────────────────────────────────────────────

    /// <summary>Projects results using the given projection specification.</summary>
    public BlqlQuery Project(BlqlProjection projection)
    {
        _projection = projection ?? BlqlProjection.All;
        return this;
    }

    // ── Execution ──────────────────────────────────────────────────────────

    /// <summary>
    /// Executes the query and returns a lazy enumerable of matching <see cref="BsonDocument"/> instances.
    /// Sorting, if specified, materializes all results into a list first.
    /// </summary>
    public IEnumerable<BsonDocument> AsEnumerable()
    {
        return Task.Run(async () =>
        {
            var result = new List<BsonDocument>();
            await foreach(var item in AsAsyncEnumerable())
            {
                result.Add(item);
            }
            return result;
        }).GetAwaiter().GetResult();
    }

    /// <summary>Executes the query and returns all results as a <see cref="List{T}"/>.</summary>
    public List<BsonDocument> ToList()
    {
        var result = new List<BsonDocument>();
        foreach (var doc in AsEnumerable()) result.Add(doc);
        return result;
    }

    /// <summary>Returns the count of documents matching the filter (ignores skip/take).</summary>
    public int Count()
    {
        return CountAsync().GetAwaiter().GetResult();
    }

    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        int count = 0;
        await foreach (var doc in GetScanSource(_filter, cancellationToken).WithCancellation(cancellationToken))
            if (_filter.Matches(doc)) count++;
        return count;
    }

    /// <summary>Returns the first matching document, or <c>null</c> if none.</summary>
    public BsonDocument? FirstOrDefault()
    {
        return FirstOrDefaultAsync().GetAwaiter().GetResult();
    }

    public async Task<BsonDocument?> FirstOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        await foreach (var doc in GetScanSource(_filter, cancellationToken).WithCancellation(cancellationToken))
        {
            if (!_filter.Matches(doc)) continue;
            return _projection.IsIdentity
                ? doc
                : _collection.ProjectDocument(doc, _projection);
        }
        return null;
    }

    /// <summary>Returns the first matching document or throws <see cref="InvalidOperationException"/>.</summary>
    public BsonDocument First()
    {
        return FirstOrDefault()
            ?? throw new InvalidOperationException("Sequence contains no matching documents.");
    }

    public async Task<BsonDocument> FirstAsync(CancellationToken cancellationToken = default)
    {
        return (await FirstOrDefaultAsync(cancellationToken))
            ?? throw new InvalidOperationException("Sequence contains no matching documents.");
    }

    /// <summary>Returns true if any document matches the filter.</summary>
    public bool Any()
    {
        return AnyAsync().GetAwaiter().GetResult();
    }

    public async Task<bool> AnyAsync(CancellationToken cancellationToken = default)
    {
        await foreach (var doc in GetScanSource(_filter, cancellationToken).WithCancellation(cancellationToken))
            if (_filter.Matches(doc)) return true;
        return false;
    }

    /// <summary>Returns true if no document matches the filter.</summary>
    public bool None() => !Any();

    public async Task<bool> NoneAsync(CancellationToken cancellationToken = default) => !await AnyAsync(cancellationToken);

    /// <summary>Executes the query asynchronously and yields documents via async enumerable.</summary>
    public async IAsyncEnumerable<BsonDocument> AsAsyncEnumerable(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var filterRef = _filter;

        if (_sort != null)
        {
            // Optimization: single-key sort on an indexed field — use the B-tree for ordering
            // instead of materializing all documents ("top-N via index" pattern).
            if (_sort.Keys.Count == 1)
            {
                var sortKey = _sort.Keys[0];
                var sortIndexName = _collection.FindBTreeIndexForField(sortKey.Field);
                if (sortIndexName != null)
                {
                    bool ascending = !sortKey.Descending;
                    // If the filter also targets the sort field, use its bounds to further narrow
                    // the index scan; otherwise use open bounds.
                    object? min = null, max = null;
                    if (filterRef.TryGetIndexCandidate(out var fc) && fc.Field == sortKey.Field)
                        (min, max) = (fc.Min, fc.Max);

                    int sk = 0, tk = 0;
                    await foreach (var doc in _collection.QueryIndexAsync(sortIndexName, min, max, ascending).WithCancellation(ct).ConfigureAwait(false))
                    {
                        ct.ThrowIfCancellationRequested();
                        if (!filterRef.Matches(doc)) continue;
                        if (sk < _skip) { sk++; continue; }
                        if (_take.HasValue && tk >= _take.Value) yield break;
                        yield return _projection.IsIdentity ? doc : _collection.ProjectDocument(doc, _projection);
                        tk++;
                    }
                    yield break;
                }
            }

            // Fallback: full scan + in-memory sort (index narrowing still applies for the filter)
            var all = new List<BsonDocument>();
            await foreach (var doc in GetScanSource(filterRef, ct).WithCancellation(ct).ConfigureAwait(false))
                if (filterRef.Matches(doc)) all.Add(doc);

            all.Sort(new ComparisonComparer<BsonDocument>(_sort.ToComparison()));

            int s = 0, t = 0;
            foreach (var doc in all)
            {
                ct.ThrowIfCancellationRequested();
                if (s < _skip) { s++; continue; }
                if (_take.HasValue && t >= _take.Value) yield break;
                yield return _projection.IsIdentity ? doc : _collection.ProjectDocument(doc, _projection);
                t++;
            }
            yield break;
        }

        int skipped = 0;
        int taken = 0;

        await foreach (var doc in GetScanSource(filterRef, ct).WithCancellation(ct).ConfigureAwait(false))
        {
            if (!filterRef.Matches(doc)) continue;
            if (skipped < _skip) { skipped++; continue; }
            if (_take.HasValue && taken >= _take.Value) yield break;
            yield return _projection.IsIdentity ? doc : _collection.ProjectDocument(doc, _projection);
            taken++;
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────
    // Returns the narrowest possible document source for the given filter:
    // uses a B-tree secondary index range scan when one exists for the filter field,
    // otherwise falls back to a full collection scan. Callers must apply the filter
    // as a residual predicate because the index scan uses conservative (inclusive) bounds.
    private IAsyncEnumerable<BsonDocument> GetScanSource(BlqlFilter filter, CancellationToken ct)
    {
        if (filter.TryGetIndexCandidate(out var candidate))
        {
            var indexName = _collection.FindBTreeIndexForField(candidate.Field);
            if (indexName != null)
                return _collection.QueryIndexAsync(indexName, candidate.Min, candidate.Max);
        }
        return _collection.FindAllAsync(ct);
    }
    private sealed class ComparisonComparer<T> : IComparer<T>
    {
        private readonly Comparison<T> _comparison;
        public ComparisonComparer(Comparison<T> comparison) { _comparison = comparison; }
        public int Compare(T? x, T? y) => _comparison(x!, y!);
    }
}
