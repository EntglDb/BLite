using System.Linq.Expressions;
using BLite.Bson;
using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// An AND of several conditions on one indexed property is planned as a single range only when
/// every bound is kept: where both sides bound the same end, the tighter bound wins (#223).
/// </summary>
public class IndexAndMergeTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;
    private readonly List<PersonV2> _people;
    private readonly List<TestDocument> _docs;

    public IndexAndMergeTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"index_and_merge_{Guid.NewGuid():N}.db");
        _db = new TestDbContext(_dbPath);
        _people = Enumerable.Range(0, 300).Select(i => new PersonV2 { Name = $"p{i:000}", Age = i % 50 }).ToList();
        _db.PeopleV2.InsertBulkAsync(_people).GetAwaiter().GetResult();
        _docs = Enumerable.Range(0, 200).Select(i => new TestDocument { Category = $"c{i % 40}", Amount = i, Name = $"d{i}" }).ToList();
        _db.TestDocuments.InsertBulkAsync(_docs).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _db.Dispose();
        foreach (var f in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
            try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
    }

    private static readonly int[] s_ages = [5, 10];

    public static TheoryData<string> AgePredicates => new(s_agePredicates.Keys);

    private static readonly Dictionary<string, Expression<Func<PersonV2, bool>>> s_agePredicates = new()
    {
        ["two lower, inclusive"] = p => p.Age >= 0 && p.Age >= 5,
        ["two lower, inclusive, tighter first"] = p => p.Age >= 5 && p.Age >= 0,
        ["two lower, strict right"] = p => p.Age >= 0 && p.Age > 5,
        ["two lower, same value, strict right"] = p => p.Age >= 5 && p.Age > 5,
        ["two upper, inclusive"] = p => p.Age <= 40 && p.Age <= 10,
        ["two upper, strict left"] = p => p.Age < 10 && p.Age <= 40,
        ["two upper, same value, strict left"] = p => p.Age < 10 && p.Age <= 10,
        ["equality and lower above it"] = p => p.Age == 5 && p.Age >= 10,
        ["equality and lower below it"] = p => p.Age == 5 && p.Age >= 1,
        ["equality and upper below it"] = p => p.Age == 30 && p.Age <= 10,
        ["two equalities, different"] = p => p.Age == 5 && p.Age == 6,
        ["two equalities, same"] = p => p.Age == 5 && p.Age == 5,
        ["crossing range"] = p => p.Age >= 30 && p.Age <= 10,
        ["empty range at one value"] = p => p.Age >= 10 && p.Age < 10,
        ["three conditions"] = p => p.Age > 3 && p.Age <= 40 && p.Age >= 20,
        ["range narrowed on both ends"] = p => p.Age >= 5 && p.Age <= 45 && p.Age >= 10 && p.Age < 40,
        ["In and range"] = p => s_ages.Contains(p.Age) && p.Age >= 6,
        ["range and In"] = p => p.Age <= 6 && s_ages.Contains(p.Age),
    };

    [Theory]
    [MemberData(nameof(AgePredicates))]
    public async Task Where_MatchesLinq(string name)
    {
        var predicate = s_agePredicates[name];
        var expected = _people.Where(predicate.Compile()).Select(p => p.Name).OrderBy(n => n).ToList();

        var actual = (await _db.PeopleV2.AsQueryable().Where(predicate).ToListAsync()).Select(p => p.Name).OrderBy(n => n).ToList();

        Assert.Equal(expected, actual);
    }

    [Theory]
    [MemberData(nameof(AgePredicates))]
    public async Task CountWithPredicate_MatchesLinq(string name)
    {
        var predicate = s_agePredicates[name];

        Assert.Equal(_people.Count(predicate.Compile()), await _db.PeopleV2.AsQueryable().CountAsync(predicate));
    }

    [Theory]
    [MemberData(nameof(AgePredicates))]
    public async Task WhereThenCount_MatchesLinq(string name)
    {
        var predicate = s_agePredicates[name];

        Assert.Equal(_people.Count(predicate.Compile()), await _db.PeopleV2.AsQueryable().Where(predicate).CountAsync());
    }

    [Fact]
    public async Task ChainedWhereAndCountPredicate_KeepEveryBound()
    {
        var q = _db.PeopleV2.AsQueryable();
        foreach (var t in new[] { 5, 40, 1, 49, 0 })
        {
            Assert.Equal(_people.Count(p => p.Age > t), await q.Where(p => p.Age >= 0).CountAsync(p => p.Age > t));
            Assert.Equal(_people.Count(p => p.Age >= t), (await q.Where(p => p.Age >= 0).Where(p => p.Age >= t).ToListAsync()).Count);
            Assert.Equal(_people.Count(p => p.Age <= 45 && p.Age <= t), (await q.Where(p => p.Age <= 45).Where(p => p.Age <= t).ToListAsync()).Count);
        }
    }

    [Fact]
    public async Task StringIndex_StartsWithAndUpperBound_KeepsTheTighterBound()
    {
        Expression<Func<TestDocument, bool>>[] predicates =
        [
            d => d.Category.StartsWith("c1") && string.Compare(d.Category, "c15") <= 0,
            d => d.Category.StartsWith("c1") && d.Category == "c12",
            d => d.Category == "c3" && d.Category.StartsWith("c1"),
        ];

        foreach (var predicate in predicates)
        {
            var expected = _docs.Where(predicate.Compile()).Select(d => d.Name).OrderBy(n => n).ToList();
            var actual = (await _db.TestDocuments.AsQueryable().Where(predicate).ToListAsync()).Select(d => d.Name).OrderBy(n => n).ToList();
            Assert.Equal(expected, actual);
            Assert.Equal(expected.Count, await _db.TestDocuments.AsQueryable().CountAsync(predicate));
        }
    }

    [Fact]
    public void Plan_TwoLowerBounds_KeepsTheLargerOne()
    {
        var plan = IndexOptimizer.TryOptimize<PersonV2>(
            (Expression<Func<PersonV2, bool>>)(p => p.Age >= 0 && p.Age > 5), _db.PeopleV2.GetIndexes());

        Assert.NotNull(plan);
        Assert.Equal(5, plan!.MinValue);
        Assert.False(plan.StartInclusive);
        Assert.Null(plan.MaxValue);
        Assert.False(plan.HasResiduePredicate);
    }

    [Fact]
    public void Plan_InValuesAndRange_KeepsInValuesAndPostFilters()
    {
        var plan = IndexOptimizer.TryOptimize<PersonV2>(
            (Expression<Func<PersonV2, bool>>)(p => s_ages.Contains(p.Age) && p.Age >= 6), _db.PeopleV2.GetIndexes());

        Assert.NotNull(plan);
        Assert.NotNull(plan!.InValues);
        Assert.True(plan.HasResiduePredicate);
    }
}
