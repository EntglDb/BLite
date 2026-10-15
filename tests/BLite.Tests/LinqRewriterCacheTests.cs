using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Queries that fall back to the in-memory rewriter reuse a compiled delegate per query shape.
/// The delegate must not keep the constants of the first call: captured variables, inner
/// sequences and the like are passed in on every call (#207).
/// </summary>
public class LinqRewriterCacheTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;
    private readonly List<PersonV2> _people;

    public LinqRewriterCacheTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"rewriter_cache_{Guid.NewGuid():N}.db");
        _db = new TestDbContext(_dbPath);
        _people = Enumerable.Range(0, 300).Select(i => new PersonV2 { Name = $"p{i:000}", Age = i % 50 }).ToList();
        _db.PeopleV2.InsertBulkAsync(_people).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _db.Dispose();
        foreach (var f in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
            try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
    }

    private IQueryable<PersonV2> Q => _db.PeopleV2.AsQueryable();

    private static readonly int[] s_thresholds = [5, 40, 1, 49, 0];

    [Fact]
    public void CountWithCapturedPredicate_UsesEachCallsValue()
    {
        static int F(IQueryable<PersonV2> q, int t) => q.Count(p => p.Age > t);

        foreach (var t in s_thresholds)
            Assert.Equal(F(_people.AsQueryable(), t), F(Q, t));
    }

    [Fact]
    public void WhereThenCountWithCapturedPredicate_UsesEachCallsValue()
    {
        static int F(IQueryable<PersonV2> q, int t) => q.Where(p => p.Name != "").Count(p => p.Age > t);

        foreach (var t in s_thresholds)
            Assert.Equal(F(_people.AsQueryable(), t), F(Q, t));
    }

    [Fact]
    public void SelectThenCountWithCapturedPredicate_UsesEachCallsValue()
    {
        static int F(IQueryable<PersonV2> q, int t) => q.Select(p => p.Age).Count(a => a > t);

        foreach (var t in s_thresholds)
            Assert.Equal(F(_people.AsQueryable(), t), F(Q, t));
    }

    [Fact]
    public void OrderByTakeThenCountWithCapturedPredicate_UsesEachCallsValue()
    {
        static int F(IQueryable<PersonV2> q, int t) => q.OrderBy(p => p.Name).Take(20).Count(p => p.Age < t);

        foreach (var t in s_thresholds)
            Assert.Equal(F(_people.AsQueryable(), t), F(Q, t));
    }

    [Fact]
    public void GroupByWithCapturedKey_UsesEachCallsValue()
    {
        static List<int> F(IQueryable<PersonV2> q, int m) =>
            q.GroupBy(p => p.Age % m).Select(g => g.Count()).ToList().OrderBy(c => c).ToList();

        foreach (var m in new[] { 7, 3, 50, 1 })
            Assert.Equal(F(_people.AsQueryable(), m), F(Q, m));
    }

    [Fact]
    public void JoinWithCapturedInnerSequence_UsesEachCallsSequence()
    {
        static List<string> F(IQueryable<PersonV2> q, List<int> ages) =>
            q.Join(ages, p => p.Age, a => a, (p, a) => p.Name).OrderBy(n => n).ToList();

        foreach (var ages in new List<int>[] { [1, 2], [10], [], [49, 0, 25] })
            Assert.Equal(F(_people.AsQueryable(), ages), F(Q, ages));
    }

    [Fact]
    public void SameShapeOverTwoDatabases_ReadsEachDatabase()
    {
        static int F(IQueryable<PersonV2> q, int t) => q.Select(p => p.Age).Count(a => a > t);

        var otherPath = Path.Combine(Path.GetTempPath(), $"rewriter_cache_other_{Guid.NewGuid():N}.db");
        try
        {
            using (var other = new TestDbContext(otherPath))
            {
                other.PeopleV2.InsertBulkAsync(
                    Enumerable.Range(0, 10).Select(i => new PersonV2 { Name = $"o{i}", Age = 100 + i })).GetAwaiter().GetResult();

                Assert.Equal(_people.Count(p => p.Age > 20), F(Q, 20));
                Assert.Equal(10, F(other.PeopleV2.AsQueryable(), 20));
                Assert.Equal(_people.Count(p => p.Age > 20), F(Q, 20));
            }
        }
        finally
        {
            foreach (var f in new[] { otherPath, Path.ChangeExtension(otherPath, ".wal") })
                try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
        }
    }
}
