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
        var filterRef = _filter;
        var documents = _collection.Find(doc => filterRef.Matches(doc));

        // Sorting requires full materialization
        if (_sort != null)
        {
            var list = new List<BsonDocument>(documents);
            list.Sort(new ComparisonComparer<BsonDocument>(_sort.ToComparison()));
            documents = list;
        }

        int skipped = 0;
        int taken = 0;

        foreach (var doc in documents)
        {
            if (skipped < _skip) { skipped++; continue; }
            if (_take.HasValue && taken >= _take.Value) yield break;

            yield return _projection.IsIdentity
                ? doc
                : _collection.ProjectDocument(doc, _projection);

            taken++;
        }
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
        int count = 0;
        foreach (var doc in _collection.FindAll())
            if (_filter.Matches(doc)) count++;
        return count;
    }

    /// <summary>Returns the first matching document, or <c>null</c> if none.</summary>
    public BsonDocument? FirstOrDefault()
    {
        foreach (var doc in _collection.Find(d => _filter.Matches(d)))
        {
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

    /// <summary>Returns true if any document matches the filter.</summary>
    public bool Any()
    {
        foreach (var doc in _collection.FindAll())
            if (_filter.Matches(doc)) return true;
        return false;
    }

    /// <summary>Returns true if no document matches the filter.</summary>
    public bool None() => !Any();

    /// <summary>Executes the query asynchronously and yields documents via async enumerable.</summary>
    public async IAsyncEnumerable<BsonDocument> AsAsyncEnumerable(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var filterRef = _filter;

        if (_sort != null)
        {
            // Sort requires full materialization — run synchronously in one shot
            var all = ToList();
            foreach (var doc in all)
            {
                ct.ThrowIfCancellationRequested();
                yield return doc;
            }
            yield break;
        }

        int skipped = 0;
        int taken = 0;

        await foreach (var doc in _collection.FindAsync(d => filterRef.Matches(d), ct).ConfigureAwait(false))
        {
            if (skipped < _skip) { skipped++; continue; }
            if (_take.HasValue && taken >= _take.Value) yield break;

            yield return _projection.IsIdentity
                ? doc
                : _collection.ProjectDocument(doc, _projection);

            taken++;
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private sealed class ComparisonComparer<T> : IComparer<T>
    {
        private readonly Comparison<T> _comparison;
        public ComparisonComparer(Comparison<T> comparison) { _comparison = comparison; }
        public int Compare(T? x, T? y) => _comparison(x!, y!);
    }
}
