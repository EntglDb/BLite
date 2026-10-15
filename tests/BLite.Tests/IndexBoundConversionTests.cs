using System.Linq.Expressions;
using BLite.Bson;
using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// A comparison between an indexed property and a constant of another type (<c>r.Count &lt; 9.5</c>,
/// <c>r.Count &gt;= 5L</c>) selects the keys LINQ selects: index keys carry no type tag, so the bound
/// must be expressed in the property's type before it is used as a key (#222).
/// </summary>
public class IndexBoundConversionTests : IDisposable
{
    private readonly string _dbPath;
    private readonly AggregateDbContext _db;
    private readonly List<Reading> _readings;

    public IndexBoundConversionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"index_bound_conversion_{Guid.NewGuid():N}.db");
        _db = new AggregateDbContext(_dbPath);
        // Count (int), Total (long) and MaybeCount (int?) are indexed. The extremes of each type
        // check that a bound outside the property's range keeps or drops every key.
        _readings = Enumerable.Range(0, 20)
            .Select(i => new Reading { Id = new ObjectId(i + 1, 0), Tag = $"r{i:00}", Count = i, Total = i, MaybeCount = i % 4 == 0 ? null : i })
            .Append(new Reading { Id = new ObjectId(21, 0), Tag = "min", Count = int.MinValue, Total = long.MinValue, MaybeCount = int.MinValue })
            .Append(new Reading { Id = new ObjectId(22, 0), Tag = "max", Count = int.MaxValue, Total = long.MaxValue, MaybeCount = int.MaxValue })
            .ToList();
        _db.Readings.InsertBulkAsync(_readings).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _db.Dispose();
        foreach (var f in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
            try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
    }

    private static double s_doubleBound = 9.5;
    private static long s_longBound = 5L;

    public static TheoryData<string> Predicates => new(s_predicates.Keys);

    private static readonly Dictionary<string, Expression<Func<Reading, bool>>> s_predicates = new()
    {
        // From the issue.
        ["int < double"] = r => r.Count < 9.5,
        ["int >= long"] = r => r.Count >= 5L,
        ["int <= long"] = r => r.Count <= 5L,
        ["int >= double"] = r => r.Count >= 2.5,

        // int property, double bound.
        ["int <= double"] = r => r.Count <= 9.5,
        ["int > double"] = r => r.Count > 9.5,
        ["int > integral double"] = r => r.Count > 9.0,
        ["int < integral double"] = r => r.Count < 9.0,
        ["int == integral double"] = r => r.Count == 5.0,
        ["int == fractional double"] = r => r.Count == 5.5,
        ["int < negative double"] = r => r.Count < -0.5,
        ["int >= negative double"] = r => r.Count >= -0.5,
        ["int < double above range"] = r => r.Count < 1e10,
        ["int > double above range"] = r => r.Count > 1e10,
        ["int >= double below range"] = r => r.Count >= -1e10,
        ["int <= double below range"] = r => r.Count <= -1e10,
        ["int == double above range"] = r => r.Count == 1e10,
        ["int < NaN"] = r => r.Count < double.NaN,
        ["int >= NaN"] = r => r.Count >= double.NaN,
        ["int < +infinity"] = r => r.Count < double.PositiveInfinity,
        ["int > -infinity"] = r => r.Count > double.NegativeInfinity,
        ["int >= +infinity"] = r => r.Count >= double.PositiveInfinity,
        ["int < captured double"] = r => r.Count < s_doubleBound,
        ["double > int"] = r => 9.5 > r.Count,

        // int property, long / float / decimal bound.
        ["int > long"] = r => r.Count > 5L,
        ["int < long"] = r => r.Count < 5L,
        ["int == long"] = r => r.Count == 5L,
        ["int >= captured long"] = r => r.Count >= s_longBound,
        ["int < long above range"] = r => r.Count < 5_000_000_000L,
        ["int >= long above range"] = r => r.Count >= 5_000_000_000L,
        ["int > long below range"] = r => r.Count > -5_000_000_000L,
        ["int <= int.MaxValue as long"] = r => r.Count <= (long)int.MaxValue,
        ["int > int.MaxValue as long"] = r => r.Count > (long)int.MaxValue,
        ["int < float"] = r => r.Count < 9.5f,
        ["int >= float"] = r => r.Count >= 2.5f,
        ["int >= float beyond exact range"] = r => r.Count >= 1e9f,
        ["int < decimal"] = r => r.Count < 9.5m,
        ["int >= decimal"] = r => r.Count >= 2.5m,
        ["int > decimal"] = r => r.Count > 9.5m,
        ["int <= decimal above range"] = r => r.Count <= 1e20m,

        // long property.
        ["long < double"] = r => r.Total < 9.5,
        ["long >= double"] = r => r.Total >= 2.5,
        ["long <= integral double"] = r => r.Total <= 5.0,
        ["long > double beyond exact range"] = r => r.Total > 1e17,
        ["long < double above range"] = r => r.Total < 1e19,
        ["long >= decimal"] = r => r.Total >= 2.5m,
        ["long < decimal above range"] = r => r.Total < 1e20m,
        ["long < float"] = r => r.Total < 9.5f,

        // int? property.
        ["int? < double"] = r => r.MaybeCount < 9.5,
        ["int? >= long"] = r => r.MaybeCount >= 5L,
        ["int? < double above range"] = r => r.MaybeCount < 1e10,
        ["int? == fractional double"] = r => r.MaybeCount == 5.5,

        // AND of two converted bounds on one index.
        ["double range"] = r => r.Count >= 2.5 && r.Count < 9.5,
        ["int and double bounds"] = r => r.Count >= 2 && r.Count < 9.5,
        ["double and long bounds"] = r => r.Count > 1.5 && r.Count <= 7L,
        ["converted bound and other property"] = r => r.Count < 9.5 && r.Total >= 3L,
    };

    [Theory]
    [MemberData(nameof(Predicates))]
    public async Task Where_MatchesLinq(string name)
    {
        var predicate = s_predicates[name];
        var expected = _readings.Where(predicate.Compile()).Select(r => r.Tag).OrderBy(t => t).ToList();

        var actual = (await _db.Readings.AsQueryable().Where(predicate).ToListAsync()).Select(r => r.Tag).OrderBy(t => t).ToList();

        Assert.Equal(expected, actual);
    }

    [Theory]
    [MemberData(nameof(Predicates))]
    public async Task CountWithPredicate_MatchesLinq(string name)
    {
        var predicate = s_predicates[name];

        Assert.Equal(_readings.Count(predicate.Compile()), await _db.Readings.AsQueryable().CountAsync(predicate));
    }

    [Theory]
    [MemberData(nameof(Predicates))]
    public void Max_MatchesLinq(string name)
    {
        var predicate = s_predicates[name];
        var q = _db.Readings.AsQueryable();

        Assert.Equal(Outcome(() => _readings.Where(predicate.Compile()).Max(r => r.Count)), Outcome(() => q.Where(predicate).Max(r => r.Count)));
        Assert.Equal(Outcome(() => _readings.Where(predicate.Compile()).Max(r => r.Total)), Outcome(() => q.Where(predicate).Max(r => r.Total)));
        Assert.Equal(Outcome(() => _readings.Where(predicate.Compile()).Min(r => r.MaybeCount)), Outcome(() => q.Where(predicate).Min(r => r.MaybeCount)));
    }

    /// <summary>The value, or the type of the exception (Max over no element throws).</summary>
    private static string Outcome<TValue>(Func<TValue> aggregate)
    {
        try { return $"value {aggregate()}"; }
        catch (Exception ex) { return ex.GetType().Name; }
    }

    [Fact]
    public void Plan_FractionalStrictUpperBound_BecomesAnInclusiveIntBound()
    {
        var plan = IndexOptimizer.TryOptimize<Reading>(
            (Expression<Func<Reading, bool>>)(r => r.Count < 9.5), _db.Readings.GetIndexes());

        Assert.NotNull(plan);
        Assert.Equal(9, plan!.MaxValue);
        Assert.True(plan.EndInclusive);
        Assert.Null(plan.MinValue);
        Assert.Equal(IndexOptimizer.FilterCompleteness.Exact, plan.FilterCompleteness);
        Assert.False(plan.HasResiduePredicate);
    }

    [Fact]
    public void Plan_LongBoundOnIntIndex_IsAnIntBound()
    {
        var plan = IndexOptimizer.TryOptimize<Reading>(
            (Expression<Func<Reading, bool>>)(r => r.Count >= 5L), _db.Readings.GetIndexes());

        Assert.NotNull(plan);
        Assert.Equal(5, plan!.MinValue);
        Assert.True(plan.StartInclusive);
    }

    [Fact]
    public void Plan_BoundThatCannotBeConverted_UsesNoIndex()
    {
        // Above 2^53 a long is not exactly a double: the bound has no exact long equivalent here.
        var plan = IndexOptimizer.TryOptimize<Reading>(
            (Expression<Func<Reading, bool>>)(r => r.Total > 1e17), _db.Readings.GetIndexes());

        Assert.Null(plan);
    }
}
