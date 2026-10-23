using System.Linq.Expressions;
using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Query;

namespace BLite.Tests;

/// <summary>
/// Regression tests for issue #247: typed indexes skip null keys, but the planner answered
/// <c>x.Prop == null</c> (alone, in AND/OR, in a <c>Contains</c> list) from the empty null bucket
/// with an exact plan, and served <c>OrderBy(nullable).Skip/Take</c> from the index, so the
/// documents whose key is null were missing. Each query must agree with LINQ-to-Objects over the
/// same documents, on a typed collection with and without indexes.
/// </summary>
public class NullKeyIndexPlanTests : IAsyncLifetime
{
    private readonly string _indexedPath = Path.Combine(Path.GetTempPath(), $"nk_indexed_{Guid.NewGuid():N}.db");
    private readonly string _plainPath = Path.Combine(Path.GetTempPath(), $"nk_plain_{Guid.NewGuid():N}.db");
    private NkIndexedContext _indexed = null!;
    private NkPlainContext _plain = null!;

    private static NkDoc[] Seed() =>
    [
        new NkDoc { Name = "d1", Nick = null, MaybeCount = null, Flag = true,  Rank = 3 },
        new NkDoc { Name = "d2", Nick = "a",  MaybeCount = 5,    Flag = false, Rank = 1 },
        new NkDoc { Name = "d3", Nick = "b",  MaybeCount = null, Flag = true,  Rank = 4 },
        new NkDoc { Name = "d4", Nick = "c",  MaybeCount = 2,    Flag = true,  Rank = 2 },
        new NkDoc { Name = "d5", Nick = null, MaybeCount = 9,    Flag = false, Rank = 5 },
        new NkDoc { Name = "d6", Nick = "a",  MaybeCount = 7,    Flag = true,  Rank = 6 },
    ];

    public async Task InitializeAsync()
    {
        _indexed = new NkIndexedContext(_indexedPath);
        _plain = new NkPlainContext(_plainPath);
        await _indexed.Docs.InsertBulkAsync(Seed());
        await _plain.Docs.InsertBulkAsync(Seed());
    }

    public Task DisposeAsync()
    {
        _indexed.Dispose();
        _plain.Dispose();
        foreach (var path in new[] { _indexedPath, _plainPath })
        {
            if (File.Exists(path)) File.Delete(path);
            var wal = Path.ChangeExtension(path, ".wal");
            if (File.Exists(wal)) File.Delete(wal);
        }
        return Task.CompletedTask;
    }

    private IEnumerable<(string Label, DocumentCollection<ObjectId, NkDoc> Docs)> Collections()
        => [("indexed", _indexed.Docs), ("plain", _plain.Docs)];

    // ─── Null equality ───────────────────────────────────────────────────────

    [Fact]
    public async Task Equality_With_Null_Finds_The_Null_Keyed_Documents()
    {
        int? none = null;
        await AssertAgreesAsync(x => x.Nick == null, "d1", "d5");
        await AssertAgreesAsync(x => x.MaybeCount == null, "d1", "d3");
        await AssertAgreesAsync(x => x.MaybeCount == none, "d1", "d3");
        await AssertAgreesAsync(x => null == x.Nick, "d1", "d5");
    }

    [Fact]
    public async Task Count_With_Null_Counts_The_Null_Keyed_Documents()
    {
        foreach (var (label, docs) in Collections())
        {
            Assert.True(2 == await docs.AsQueryable().CountAsync(x => x.Nick == null), label);
            Assert.True(2 == await docs.AsQueryable().CountAsync(x => x.MaybeCount == null), label);
            Assert.True(2 == await docs.AsQueryable().Where(x => x.MaybeCount == null).CountAsync(), label);
        }
    }

    [Fact]
    public async Task First_With_Null_Finds_A_Null_Keyed_Document()
    {
        foreach (var (label, docs) in Collections())
        {
            var first = await docs.AsQueryable().Where(x => x.MaybeCount == null).FirstAsync();
            Assert.True(first.MaybeCount == null && first.Name is "d1" or "d3", label);
            Assert.True(await docs.AsQueryable().AnyAsync(x => x.Nick == null), label);
        }
    }

    [Fact]
    public async Task Null_Equality_In_And_Or()
    {
        await AssertAgreesAsync(x => x.Nick == null && x.Flag, "d1");
        await AssertAgreesAsync(x => x.Flag && x.Nick == null, "d1");
        await AssertAgreesAsync(x => x.Nick == null && x.MaybeCount == 9, "d5");
        await AssertAgreesAsync(x => x.Nick == null || x.Nick == "a", "d1", "d2", "d5", "d6");
        await AssertAgreesAsync(x => x.MaybeCount == 5 || x.MaybeCount == null, "d1", "d2", "d3");
    }

    [Fact]
    public async Task Contains_With_Null_Finds_The_Null_Keyed_Documents()
    {
        var counts = new int?[] { null, 5 };
        var nicks = new List<string?> { "b", null };
        await AssertAgreesAsync(x => counts.Contains(x.MaybeCount), "d1", "d2", "d3");
        await AssertAgreesAsync(x => nicks.Contains(x.Nick), "d1", "d3", "d5");
        // Without a null the list is still served from the index.
        var plainNicks = new[] { "a", "c" };
        await AssertAgreesAsync(x => plainNicks.Contains(x.Nick), "d2", "d4", "d6");
    }

    // ─── Ordering windows ────────────────────────────────────────────────────

    [Fact]
    public async Task OrderBy_Nullable_Window_Includes_The_Null_Keys()
    {
        foreach (var (label, docs) in Collections())
        {
            var all = await docs.FindAllAsync().ToListAsync();
            var q = docs.AsQueryable();

            await AssertWindowAsync(label, all.OrderBy(x => x.MaybeCount).Take(3), q.OrderBy(x => x.MaybeCount).Take(3), x => x.MaybeCount);
            await AssertWindowAsync(label, all.OrderBy(x => x.MaybeCount).Skip(1).Take(2), q.OrderBy(x => x.MaybeCount).Skip(1).Take(2), x => x.MaybeCount);
            await AssertWindowAsync(label, all.OrderByDescending(x => x.MaybeCount).Take(6), q.OrderByDescending(x => x.MaybeCount).Take(6), x => x.MaybeCount);
            await AssertWindowAsync(label, all.OrderByDescending(x => x.MaybeCount).Skip(3).Take(3), q.OrderByDescending(x => x.MaybeCount).Skip(3).Take(3), x => x.MaybeCount);
            await AssertWindowAsync(label, all.OrderBy(x => x.Nick).Take(3), q.OrderBy(x => x.Nick).Take(3), x => x.Nick);
            await AssertWindowAsync(label, all.OrderByDescending(x => x.Nick).Skip(2).Take(4), q.OrderByDescending(x => x.Nick).Skip(2).Take(4), x => x.Nick);

            // Projection after the window, and an operator replayed over it.
            var projected = await q.OrderBy(x => x.MaybeCount).Take(3).Select(x => x.MaybeCount).ToListAsync();
            Assert.True(new int?[] { null, null, 2 }.SequenceEqual(projected), $"{label}: [{string.Join(", ", projected)}]");
            Assert.True(2 == await q.OrderBy(x => x.MaybeCount).Take(3).CountAsync(x => x.MaybeCount == null), label);
        }
    }

    [Fact]
    public async Task OrderBy_NonNullable_Key_Still_Uses_The_Index_Window()
    {
        var indexes = _indexed.Docs.GetIndexes().ToList();
        var q = _indexed.Docs.AsQueryable();

        Assert.NotNull(IndexOptimizer.TryOptimizeOrderBy(Model(q.OrderBy(x => x.Rank).Take(3).Expression), indexes));
        Assert.NotNull(IndexOptimizer.TryOptimizeOrderBy(Model(q.OrderByDescending(x => x.Rank).Skip(1).Take(2).Expression), indexes));
        Assert.NotNull(IndexOptimizer.TryOptimizeOrderBy(Model(q.OrderBy(x => x.Flag).Take(2).Expression), indexes));
        Assert.Null(IndexOptimizer.TryOptimizeOrderBy(Model(q.OrderBy(x => x.MaybeCount).Take(3).Expression), indexes));
        Assert.Null(IndexOptimizer.TryOptimizeOrderBy(Model(q.OrderBy(x => x.Nick).Take(3).Expression), indexes));

        foreach (var (label, docs) in Collections())
        {
            var all = await docs.FindAllAsync().ToListAsync();
            await AssertWindowAsync(label, all.OrderBy(x => x.Rank).Take(3), docs.AsQueryable().OrderBy(x => x.Rank).Take(3), x => x.Rank);
            await AssertWindowAsync(label, all.OrderByDescending(x => x.Rank).Skip(1).Take(2), docs.AsQueryable().OrderByDescending(x => x.Rank).Skip(1).Take(2), x => x.Rank);
        }
    }

    [Fact]
    public void Non_Null_Equality_Is_Still_Planned_From_The_Index()
    {
        var indexes = _indexed.Docs.GetIndexes().ToList();
        var nicks = new[] { "a", "c" };

        Assert.NotNull(IndexOptimizer.TryOptimize<NkDoc>((Expression<Func<NkDoc, bool>>)(x => x.Nick == "a"), indexes));
        Assert.NotNull(IndexOptimizer.TryOptimize<NkDoc>((Expression<Func<NkDoc, bool>>)(x => x.MaybeCount == 5), indexes));
        Assert.NotNull(IndexOptimizer.TryOptimize<NkDoc>((Expression<Func<NkDoc, bool>>)(x => nicks.Contains(x.Nick)), indexes));
        Assert.Null(IndexOptimizer.TryOptimize<NkDoc>((Expression<Func<NkDoc, bool>>)(x => x.Nick == null), indexes));
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static QueryModel Model(Expression expression)
    {
        var visitor = new BTreeExpressionVisitor();
        visitor.Visit(expression);
        return visitor.GetModel();
    }

    /// <summary>
    /// Checks a window against LINQ-to-Objects: the same sequence of keys, and the same documents
    /// (documents with equal keys may come in either order).
    /// </summary>
    private static async Task AssertWindowAsync<TKey>(string label, IEnumerable<NkDoc> expected, IQueryable<NkDoc> query, Func<NkDoc, TKey> key)
    {
        var expectedList = expected.ToList();
        var actual = await query.ToListAsync();
        var message = $"{label}: {query.Expression} returned [{string.Join(", ", actual.Select(d => $"{d.Name}:{key(d)}"))}], " +
                      $"LINQ-to-Objects [{string.Join(", ", expectedList.Select(d => $"{d.Name}:{key(d)}"))}]";
        Assert.True(expectedList.Select(key).SequenceEqual(actual.Select(key)), message);
        Assert.True(Names(expectedList).SequenceEqual(Names(actual)), message);
    }

    /// <summary>
    /// Runs <paramref name="predicate"/> on the indexed and the plain collection and checks both
    /// against LINQ-to-Objects over the documents as read back, and against <paramref name="expected"/>.
    /// </summary>
    private async Task AssertAgreesAsync(Expression<Func<NkDoc, bool>> predicate, params string[] expected)
    {
        foreach (var (label, docs) in Collections())
        {
            var all = await docs.FindAllAsync().ToListAsync();
            var reference = Names(all.Where(predicate.Compile()));
            Assert.Equal(expected, reference);

            var actual = Names(await docs.AsQueryable().Where(predicate).ToListAsync());
            Assert.True(reference.SequenceEqual(actual),
                $"{label}: {predicate} returned [{string.Join(", ", actual)}], LINQ-to-Objects [{string.Join(", ", reference)}]");
            Assert.True(expected.Length == await docs.AsQueryable().CountAsync(predicate), $"{label}: Count({predicate})");
        }
    }

    private static string[] Names(IEnumerable<NkDoc> docs)
        => docs.Select(d => d.Name).Order(StringComparer.Ordinal).ToArray();
}
