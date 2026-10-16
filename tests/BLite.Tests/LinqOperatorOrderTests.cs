using System.Linq.Expressions;
using BLite.Core;
using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// <c>Take</c>, <c>Skip</c> and the ordering apply in the order the query writes them. The direct
/// pipeline applies an ordering, then <c>Skip</c>, then <c>Take</c>, so it serves only queries whose
/// operators come in that order or combine into it exactly; any other query is replayed in memory
/// as written (#230).
/// </summary>
public class LinqOperatorOrderTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;
    private readonly List<PersonV2> _scan;

    public LinqOperatorOrderTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"operator_order_{Guid.NewGuid():N}.db");
        _db = new TestDbContext(_dbPath);

        // Unique ages (indexed) and names, inserted in a shuffled order so that neither the age
        // order nor the name order is the scan order.
        var rnd = new Random(230);
        var people = Enumerable.Range(0, 60)
            .Select(i => new PersonV2 { Name = $"p{i:00}", Age = i })
            .OrderBy(_ => rnd.Next())
            .ToList();
        _db.PeopleV2.InsertBulkAsync(people).GetAwaiter().GetResult();

        // Order-dependent operators (Take, Skip) see the documents in the order a full scan
        // returns them, which is not the insertion order.
        _scan = _db.PeopleV2.AsQueryable().ToList();
    }

    public void Dispose()
    {
        _db.Dispose();
        foreach (var f in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
            try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
    }

    private static QueryModel Model(Expression expression)
    {
        var visitor = new BTreeExpressionVisitor();
        visitor.Visit(expression);
        return visitor.GetModel();
    }

    private IQueryable<PersonV2> Q => _db.PeopleV2.AsQueryable();
    private IQueryable<PersonV2> L => _scan.AsQueryable();

    private static List<string> Names(IEnumerable<PersonV2> people) => people.Select(p => p.Name).ToList();

    /// <summary>Runs <paramref name="shape"/> over the collection and over its scan, sync and async.</summary>
    private void AssertSame(Func<IQueryable<PersonV2>, IQueryable<PersonV2>> shape)
    {
        var expected = Names(shape(L));
        Assert.Equal(expected, Names(shape(Q).ToList()));
        Assert.Equal(expected, Names(shape(Q).ToListAsync().GetAwaiter().GetResult()));
    }

    // ── Skip and Take in any order ────────────────────────────────────────────

    [Fact]
    public void SkipAfterTake_SkipsInsideTheTakenItems()
    {
        Assert.Equal(3, Q.Take(5).Skip(2).ToList().Count);
        AssertSame(q => q.Take(5).Skip(2));
        AssertSame(q => q.Take(2).Skip(5));
        AssertSame(q => q.Take(5).Skip(5));
        AssertSame(q => q.Skip(3).Take(10).Skip(4).Take(2));
    }

    [Fact]
    public void SkipBeforeTake_StillServedDirectly()
    {
        AssertSame(q => q.Skip(2).Take(5));
        AssertSame(q => q.Take(5));
        AssertSame(q => q.Skip(57));
    }

    [Fact]
    public void TwoTakes_KeepTheSmallerCount()
    {
        AssertSame(q => q.Take(3).Take(5));
        AssertSame(q => q.Take(5).Take(3));
    }

    [Fact]
    public void TwoSkips_AddUp()
    {
        AssertSame(q => q.Skip(2).Skip(3));
        AssertSame(q => q.Skip(2).Skip(3).Take(4));
        AssertSame(q => q.Skip(int.MaxValue).Skip(5));
        AssertSame(q => q.Skip(int.MaxValue).Take(5));
        AssertSame(q => q.Skip(5).Take(int.MaxValue));
    }

    [Fact]
    public void ZeroAndNegativeCounts_BehaveLikeLinq()
    {
        AssertSame(q => q.Take(0));
        AssertSame(q => q.Take(-1));
        AssertSame(q => q.Skip(-2).Take(3));
        AssertSame(q => q.Take(5).Skip(-2));
        AssertSame(q => q.OrderBy(p => p.Age).Take(0));
        AssertSame(q => q.OrderBy(p => p.Age).Take(-3));
        AssertSame(q => q.OrderBy(p => p.Age).Skip(-3).Take(2));
    }

    [Fact]
    public void TakeWithRange_BehavesLikeLinq()
    {
        AssertSame(q => q.Take(2..5));
        AssertSame(q => q.OrderBy(p => p.Age).Take(^3..));
    }

    // ── Orderings after Take or Skip ──────────────────────────────────────────

    [Fact]
    public void OrderingAfterTake_SortsOnlyTheTakenItems()
    {
        AssertSame(q => q.Take(10).OrderByDescending(p => p.Name));
        AssertSame(q => q.Take(10).OrderBy(p => p.Age));
        AssertSame(q => q.Take(10).OrderByDescending(p => p.Age).Take(3));
        AssertSame(q => q.Take(10).OrderBy(p => p.Name).ThenBy(p => p.Age));
    }

    [Fact]
    public void OrderingAfterSkip_SortsOnlyWhatSkipLeaves()
    {
        AssertSame(q => q.Skip(50).OrderBy(p => p.Name));
        AssertSame(q => q.Skip(50).OrderBy(p => p.Age).Take(3));
        AssertSame(q => q.Skip(2).Take(10).OrderBy(p => p.Age));
    }

    [Fact]
    public void OrderingAfterAnIndexedWindow_SortsTheWindow()
    {
        AssertSame(q => q.OrderBy(p => p.Age).Take(10).OrderByDescending(p => p.Name));
        AssertSame(q => q.OrderByDescending(p => p.Age).Skip(5).Take(10).OrderBy(p => p.Name).Take(3));
    }

    [Fact]
    public void SecondOrdering_SortsAgainKeepingTheFirstOrderForTies()
    {
        AssertSame(q => q.OrderBy(p => p.Name).OrderBy(p => p.Age % 3));
        AssertSame(q => q.OrderByDescending(p => p.Age).OrderBy(p => p.Age % 4).Take(7));
    }

    // ── Index-served orderings keep their window ──────────────────────────────

    [Fact]
    public void IndexedOrderingWindows_ReturnTheWindow()
    {
        AssertSame(q => q.OrderBy(p => p.Age).Take(5));
        AssertSame(q => q.OrderByDescending(p => p.Age).Skip(3).Take(5));
        AssertSame(q => q.OrderBy(p => p.Age).Take(10).Skip(4));
        AssertSame(q => q.OrderByDescending(p => p.Age).Take(3).Skip(1));
        AssertSame(q => q.OrderBy(p => p.Age).Take(10).Take(4));
        AssertSame(q => q.OrderBy(p => p.Age).Skip(2).Skip(3).Take(4));
    }

    [Fact]
    public void IndexFriendlyShapes_AreModelledDirectly()
    {
        var indexes = _db.PeopleV2.GetIndexes().ToList();

        var topN = Model(Q.OrderBy(p => p.Age).Take(5).Expression);
        Assert.False(topN.HasComplexOperators);
        Assert.Equal(5, topN.Take);
        Assert.Null(topN.Skip);
        Assert.NotNull(IndexOptimizer.TryOptimizeOrderBy(topN, indexes));

        var page = Model(Q.OrderByDescending(p => p.Age).Skip(3).Take(5).Expression);
        Assert.False(page.HasComplexOperators);
        Assert.Equal(3, page.Skip);
        Assert.Equal(5, page.Take);
        Assert.True(page.OrderDescending);
        Assert.NotNull(IndexOptimizer.TryOptimizeOrderBy(page, indexes));

        var takeSkip = Model(Q.OrderBy(p => p.Age).Take(10).Skip(4).Expression);
        Assert.False(takeSkip.HasComplexOperators);
        Assert.Equal(4, takeSkip.Skip);
        Assert.Equal(6, takeSkip.Take);
        Assert.NotNull(IndexOptimizer.TryOptimizeOrderBy(takeSkip, indexes));

        var whereTake = Model(Q.Where(p => p.Age > 3).OrderBy(p => p.Age).Skip(1).Take(2).Expression);
        Assert.False(whereTake.HasComplexOperators);
        Assert.Equal(1, whereTake.Skip);
        Assert.Equal(2, whereTake.Take);
    }

    [Fact]
    public void CombinedWindows_AreModelledExactly()
    {
        var takeSkip = Model(Q.Take(5).Skip(2).Expression);
        Assert.False(takeSkip.HasComplexOperators);
        Assert.Equal(2, takeSkip.Skip);
        Assert.Equal(3, takeSkip.Take);

        var takeTake = Model(Q.Take(3).Take(5).Expression);
        Assert.False(takeTake.HasComplexOperators);
        Assert.Equal(3, takeTake.Take);

        var skipSkip = Model(Q.Skip(2).Skip(3).Expression);
        Assert.False(skipSkip.HasComplexOperators);
        Assert.Equal(5, skipSkip.Skip);

        Assert.True(Model(Q.Skip(int.MaxValue).Skip(1).Expression).HasComplexOperators);
    }

    [Fact]
    public void OperatorsOutOfPipelineOrder_AreComplex()
    {
        Assert.True(Model(Q.Take(10).OrderBy(p => p.Name).Expression).HasComplexOperators);
        Assert.True(Model(Q.Skip(10).OrderByDescending(p => p.Age).Expression).HasComplexOperators);
        Assert.True(Model(Q.OrderBy(p => p.Name).OrderBy(p => p.Age).Expression).HasComplexOperators);

        // The ordering the window was taken in stays in the model, so the index can still serve it.
        var resorted = Model(Q.OrderBy(p => p.Age).Take(10).OrderBy(p => p.Name).Expression);
        Assert.True(resorted.HasComplexOperators);
        Assert.Equal("Age", ((MemberExpression)resorted.OrderByClause!.Body).Member.Name);
    }

    // ── Where after Take or Skip ──────────────────────────────────────────────

    [Fact]
    public void WhereAfterTakeOrSkip_FiltersTheWindow()
    {
        AssertSame(q => q.Take(10).Where(p => p.Age < 30));
        AssertSame(q => q.Skip(40).Where(p => p.Age < 30));
        AssertSame(q => q.Take(10).Skip(2).Where(p => p.Age < 30).OrderBy(p => p.Age));
        // A filter on the indexed Age returns the documents in index order, not scan order, so
        // this one filters on Name.
        AssertSame(q => q.Where(p => p.Name != "p05").Take(10).Skip(2));
        AssertSame(q => q.Where(p => p.Age < 30).OrderBy(p => p.Age).Take(10).Skip(2));
    }

    // ── Terminals after Take or Skip ──────────────────────────────────────────

    [Fact]
    public void SyncTerminals_AfterTakeAndSkip()
    {
        Assert.Equal(L.Take(5).Skip(2).Count(), Q.Take(5).Skip(2).Count());
        Assert.Equal(L.Take(5).Skip(2).LongCount(), Q.Take(5).Skip(2).LongCount());
        Assert.Equal(L.Take(5).Skip(2).Count(p => p.Age < 30), Q.Take(5).Skip(2).Count(p => p.Age < 30));
        Assert.Equal(L.Take(5).Skip(2).First().Name, Q.Take(5).Skip(2).First().Name);
        Assert.Equal(L.Take(5).Skip(2).FirstOrDefault()?.Name, Q.Take(5).Skip(2).FirstOrDefault()?.Name);
        Assert.Null(Q.Take(2).Skip(5).FirstOrDefault());
        Assert.Equal(L.Take(5).Skip(2).Last().Name, Q.Take(5).Skip(2).Last().Name);
        Assert.Equal(L.Take(5).Skip(4).Single().Name, Q.Take(5).Skip(4).Single().Name);
        Assert.False(Q.Take(2).Skip(5).Any());
        Assert.Equal(L.Take(5).Skip(2).Any(p => p.Age > 30), Q.Take(5).Skip(2).Any(p => p.Age > 30));
        Assert.Equal(L.Take(10).Skip(2).ElementAt(3).Name, Q.Take(10).Skip(2).ElementAt(3).Name);
        Assert.Throws<ArgumentOutOfRangeException>(() => Q.Take(5).Skip(2).ElementAt(3));
        Assert.Equal(L.Take(10).Skip(2).Sum(p => p.Age), Q.Take(10).Skip(2).Sum(p => p.Age));
        Assert.Equal(L.Take(10).Skip(2).Min(p => p.Age), Q.Take(10).Skip(2).Min(p => p.Age));
        Assert.Equal(L.Take(10).Skip(2).Max(p => p.Age), Q.Take(10).Skip(2).Max(p => p.Age));
        Assert.Equal(L.Take(10).OrderBy(p => p.Age).First().Name, Q.Take(10).OrderBy(p => p.Age).First().Name);
        Assert.Equal(L.Take(10).OrderBy(p => p.Age).Last().Name, Q.Take(10).OrderBy(p => p.Age).Last().Name);
        Assert.Equal(L.Take(5).Skip(2).Select(p => p.Name).Count(), Q.Take(5).Skip(2).Select(p => p.Name).Count());
        Assert.Equal(L.Take(5).Select(p => p.Name).Skip(2).ToList(), Q.Take(5).Select(p => p.Name).Skip(2).ToList());
    }

    [Fact]
    public async Task AsyncTerminals_AfterTakeAndSkip()
    {
        Assert.Equal(L.Take(5).Skip(2).Count(), await Q.Take(5).Skip(2).CountAsync());
        Assert.Equal(L.Take(5).Skip(2).LongCount(), await Q.Take(5).Skip(2).LongCountAsync());
        Assert.Equal(L.Take(5).Skip(2).Count(p => p.Age < 30), await Q.Take(5).Skip(2).CountAsync(p => p.Age < 30));
        Assert.Equal(L.Take(5).Skip(2).First().Name, (await Q.Take(5).Skip(2).FirstAsync()).Name);
        Assert.Null(await Q.Take(2).Skip(5).FirstOrDefaultAsync());
        Assert.Equal(L.Take(5).Skip(2).Last().Name, (await Q.Take(5).Skip(2).LastAsync()).Name);
        Assert.Equal(L.Take(5).Skip(4).Single().Name, (await Q.Take(5).Skip(4).SingleAsync()).Name);
        Assert.False(await Q.Take(2).Skip(5).AnyAsync());
        Assert.Equal(L.Take(5).Skip(2).Any(p => p.Age > 30), await Q.Take(5).Skip(2).AnyAsync(p => p.Age > 30));
        Assert.Equal(L.Take(5).All(p => p.Age < 59), await Q.Take(5).AllAsync(p => p.Age < 59));
        Assert.Equal(L.Take(10).Skip(2).ElementAt(3).Name, (await Q.Take(10).Skip(2).ElementAtAsync(3)).Name);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Q.Take(5).Skip(2).ElementAtAsync(3));
        Assert.Null(await Q.Take(2).ElementAtOrDefaultAsync(3));
        Assert.Equal(L.Take(10).Skip(2).Sum(p => p.Age), await Q.Take(10).Skip(2).SumAsync(p => p.Age));
        Assert.Equal(L.Take(10).Skip(2).Min(p => p.Age), await Q.Take(10).Skip(2).MinAsync(p => p.Age));
        Assert.Equal(Names(L.Take(5).Skip(2)), Names(await Q.Take(5).Skip(2).ToArrayAsync()));

        var streamed = new List<PersonV2>();
        await foreach (var p in Q.Take(10).OrderByDescending(p => p.Name).AsAsyncEnumerable())
            streamed.Add(p);
        Assert.Equal(Names(L.Take(10).OrderByDescending(p => p.Name)), Names(streamed));
    }

    [Fact]
    public async Task AllAsync_OnlyTestsTheQueriedItems()
    {
        // Only the first five of the scan are tested; the oldest person is not among them unless
        // the scan happens to start with them, which the expectation accounts for.
        var oldest = _scan.Max(p => p.Age);
        Assert.Equal(L.Take(5).All(p => p.Age < oldest), await Q.Take(5).AllAsync(p => p.Age < oldest));
        Assert.True(await Q.Where(p => p.Age < 10).AllAsync(p => p.Age < 10));
        Assert.Equal(L.Skip(55).All(p => p.Age > 0), await Q.Skip(55).AllAsync(p => p.Age > 0));
    }

    // ── Operators the pipeline cannot model ───────────────────────────────────

    [Fact]
    public void DistinctAndReverse_KeepTheirPlaceInTheQuery()
    {
        AssertSame(q => q.Take(10).Reverse().Take(3));
        AssertSame(q => q.Reverse().Take(3).Skip(1));
        AssertSame(q => q.Take(10).Skip(2).Distinct().Take(3));
        AssertSame(q => q.OrderBy(p => p.Age).Take(10).Reverse().Skip(2));
        AssertSame(q => q.Distinct().Take(10).OrderBy(p => p.Name));
    }
}
