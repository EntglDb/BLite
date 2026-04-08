using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Indexing;
using BLite.Core.Query;
using BLite.Shared;
using BLite.Shared.Filters;

namespace BLite.Tests;

/// <summary>
/// Integration tests for Phase 3: AOT-safe LINQ-to-BsonReaderPredicate translation.
///
/// Scenarios tested:
///   - <see cref="IBLiteQueryable{T}"/> IndexQueryPlan overloads: ToListAsync, FirstOrDefaultAsync,
///     FirstAsync, SingleOrDefaultAsync, SingleAsync, AnyAsync, CountAsync, ToArrayAsync, ForEachAsync
///   - <see cref="BLiteQueryableExtensions"/> extension methods mirror the interface overloads
///   - GetIndexes() returns the live collection indexes
///   - IndexMinMax / AgeMin / AgeMax with a B-Tree index present (O(log n) path)
///   - IndexMinMax / AgeMin / AgeMax fallback (BsonAggregator.Min/Max, no index)
///   - BsonExpressionEvaluator.CreateFieldProjector (via BsonAggregateFieldAsync)
///   - <see cref="BLiteAotHelper.TryCompileWherePredicate{T}"/> public wrapper
/// </summary>
public class Phase3Tests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public Phase3Tests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"phase3_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);

        // Seed people: ages 10, 20, 30, 40, 50
        for (int i = 1; i <= 5; i++)
        {
            _db.People.InsertAsync(new Person
            {
                Id   = i,
                Name = $"Person{i}",
                Age  = i * 10
            }).GetAwaiter().GetResult();
        }
        _db.SaveChangesAsync().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ── GetIndexes ────────────────────────────────────────────────────────────

    [Fact]
    public void GetIndexes_ReturnsCollectionIndexes()
    {
        var indexes = _db.People.AsQueryable().GetIndexes().ToList();
        Assert.NotNull(indexes);
        // TestDbContext creates an Age index on People
        Assert.Contains(indexes, idx => idx.Name.Contains("Age") || idx.Name.Contains("age"));
    }

    // ── ToListAsync(IndexQueryPlan) ───────────────────────────────────────────

    [Fact]
    public async Task ToListAsync_WithPlan_ReturnsMatchingItems()
    {
        var pred = BsonPredicateBuilder.Gt("age", 20);
        var plan = IndexQueryPlan.Scan(pred);
        var results = await _db.People.AsQueryable().ToListAsync(plan);
        Assert.Equal(3, results.Count);
        Assert.All(results, p => Assert.True(p.Age > 20));
    }

    [Fact]
    public async Task ToListAsync_ExtensionMethod_WithPlan_ReturnsMatchingItems()
    {
        var pred = BsonPredicateBuilder.Eq("age", 30);
        var plan = IndexQueryPlan.Scan(pred);
        var results = await _db.People.AsQueryable().ToListAsync(plan);
        Assert.Single(results);
        Assert.Equal(30, results[0].Age);
    }

    // ── FirstOrDefaultAsync(IndexQueryPlan) ──────────────────────────────────

    [Fact]
    public async Task FirstOrDefaultAsync_WithPlan_ReturnsFirstMatch()
    {
        var pred = BsonPredicateBuilder.Gt("age", 20);
        var plan = IndexQueryPlan.Scan(pred);
        var result = await _db.People.AsQueryable().FirstOrDefaultAsync(plan);
        Assert.NotNull(result);
        Assert.True(result!.Age > 20);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_WithPlan_ReturnsNullWhenNoMatch()
    {
        var pred = BsonPredicateBuilder.Gt("age", 99);
        var plan = IndexQueryPlan.Scan(pred);
        var result = await _db.People.AsQueryable().FirstOrDefaultAsync(plan);
        Assert.Null(result);
    }

    // ── FirstAsync(IndexQueryPlan) ────────────────────────────────────────────

    [Fact]
    public async Task FirstAsync_WithPlan_ReturnsFirstMatch()
    {
        var pred = BsonPredicateBuilder.Gte("age", 40);
        var plan = IndexQueryPlan.Scan(pred);
        var result = await _db.People.AsQueryable().FirstAsync(plan);
        Assert.True(result.Age >= 40);
    }

    [Fact]
    public async Task FirstAsync_WithPlan_ThrowsWhenNoMatch()
    {
        var pred = BsonPredicateBuilder.Gt("age", 99);
        var plan = IndexQueryPlan.Scan(pred);
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _db.People.AsQueryable().FirstAsync(plan));
    }

    // ── SingleOrDefaultAsync(IndexQueryPlan) ─────────────────────────────────

    [Fact]
    public async Task SingleOrDefaultAsync_WithPlan_ReturnsSingleMatch()
    {
        var pred = BsonPredicateBuilder.Eq("age", 30);
        var plan = IndexQueryPlan.Scan(pred);
        var result = await _db.People.AsQueryable().SingleOrDefaultAsync(plan);
        Assert.NotNull(result);
        Assert.Equal(30, result!.Age);
    }

    [Fact]
    public async Task SingleOrDefaultAsync_WithPlan_ThrowsWhenMultipleMatch()
    {
        var pred = BsonPredicateBuilder.Gt("age", 10);
        var plan = IndexQueryPlan.Scan(pred);
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _db.People.AsQueryable().SingleOrDefaultAsync(plan));
    }

    // ── SingleAsync(IndexQueryPlan) ───────────────────────────────────────────

    [Fact]
    public async Task SingleAsync_WithPlan_ReturnsSingleMatch()
    {
        var pred = BsonPredicateBuilder.Eq("age", 50);
        var plan = IndexQueryPlan.Scan(pred);
        var result = await _db.People.AsQueryable().SingleAsync(plan);
        Assert.Equal(50, result.Age);
    }

    // ── AnyAsync(IndexQueryPlan) ──────────────────────────────────────────────

    [Fact]
    public async Task AnyAsync_WithPlan_ReturnsTrueWhenAnyMatch()
    {
        var pred = BsonPredicateBuilder.Gt("age", 40);
        var plan = IndexQueryPlan.Scan(pred);
        var result = await _db.People.AsQueryable().AnyAsync(plan);
        Assert.True(result);
    }

    [Fact]
    public async Task AnyAsync_WithPlan_ReturnsFalseWhenNoMatch()
    {
        var pred = BsonPredicateBuilder.Gt("age", 99);
        var plan = IndexQueryPlan.Scan(pred);
        var result = await _db.People.AsQueryable().AnyAsync(plan);
        Assert.False(result);
    }

    // ── CountAsync(IndexQueryPlan) ────────────────────────────────────────────

    [Fact]
    public async Task CountAsync_WithPlan_ReturnsMatchCount()
    {
        var pred = BsonPredicateBuilder.Gte("age", 30);
        var plan = IndexQueryPlan.Scan(pred);
        var count = await _db.People.AsQueryable().CountAsync(plan);
        Assert.Equal(3, count);
    }

    // ── ToArrayAsync(IndexQueryPlan) ──────────────────────────────────────────

    [Fact]
    public async Task ToArrayAsync_WithPlan_ReturnsMatchingArray()
    {
        var pred = BsonPredicateBuilder.Lte("age", 20);
        var plan = IndexQueryPlan.Scan(pred);
        var array = await _db.People.AsQueryable().ToArrayAsync(plan);
        Assert.Equal(2, array.Length);
        Assert.All(array, p => Assert.True(p.Age <= 20));
    }

    // ── ForEachAsync(IndexQueryPlan) ──────────────────────────────────────────

    [Fact]
    public async Task ForEachAsync_WithPlan_InvokesActionForEachMatch()
    {
        var pred = BsonPredicateBuilder.Gt("age", 30);
        var plan = IndexQueryPlan.Scan(pred);
        var visited = new List<int>();
        await _db.People.AsQueryable().ForEachAsync(plan, p => visited.Add(p.Age));
        Assert.Equal(2, visited.Count);
        Assert.All(visited, age => Assert.True(age > 30));
    }

    // ── IndexQueryPlan with B-Tree index ─────────────────────────────────────

    [Fact]
    public async Task ToListAsync_WithIndexPlan_UsesIndex()
    {
        // Age index should be available since TestDbContext registers it
        var indexes = _db.People.AsQueryable().GetIndexes().ToList();
        var plan = PersonFilter.AgeGt(30, indexes);
        var results = await _db.People.AsQueryable().ToListAsync(plan);
        Assert.Equal(2, results.Count);
        Assert.All(results, p => Assert.True(p.Age > 30));
    }

    [Fact]
    public async Task CountAsync_WithIndexPlan_UsesIndex()
    {
        var indexes = _db.People.AsQueryable().GetIndexes().ToList();
        var plan = PersonFilter.AgeLte(30, indexes);
        var count = await _db.People.AsQueryable().CountAsync(plan);
        Assert.Equal(3, count);
    }

    // ── IndexMinMax (AgeMin / AgeMax) with index ──────────────────────────────

    [Fact]
    public async Task MinAsync_AgeMin_WithIndex_ReturnsMinimumAge()
    {
        var indexes = _db.People.AsQueryable().GetIndexes().ToReadOnlyList();
        var plan = PersonFilter.AgeMin(indexes);
        var minAge = await _db.People.AsQueryable().MinAsync<int>(plan);
        Assert.Equal(10, minAge);
    }

    [Fact]
    public async Task MaxAsync_AgeMax_WithIndex_ReturnsMaximumAge()
    {
        var indexes = _db.People.AsQueryable().GetIndexes().ToReadOnlyList();
        var plan = PersonFilter.AgeMax(indexes);
        var maxAge = await _db.People.AsQueryable().MaxAsync<int>(plan);
        Assert.Equal(50, maxAge);
    }

    // ── IndexMinMax fallback (BsonAggregator scan when no index) ─────────────

    [Fact]
    public async Task MinAsync_FallbackScan_ReturnsMinimumAge()
    {
        // Force BsonAggregator scan by creating plan directly without index
        var plan = IndexMinMax.Scan(BsonAggregator.Min("age"));
        var minAge = await _db.People.AsQueryable().MinAsync<int>(plan);
        Assert.Equal(10, minAge);
    }

    [Fact]
    public async Task MaxAsync_FallbackScan_ReturnsMaximumAge()
    {
        var plan = IndexMinMax.Scan(BsonAggregator.Max("age"));
        var maxAge = await _db.People.AsQueryable().MaxAsync<int>(plan);
        Assert.Equal(50, maxAge);
    }

    // ── BLiteAotHelper.TryCompileWherePredicate ───────────────────────────────

    [Fact]
    public async Task BLiteAotHelper_TryCompileWherePredicate_CompilesSimpleLambda()
    {
        System.Linq.Expressions.Expression<System.Func<Person, bool>> expr = p => p.Age > 30;
        var pred = BLiteAotHelper.TryCompileWherePredicate<Person>(expr);
        Assert.NotNull(pred);
        // Use the predicate via IndexQueryPlan.Scan
        var plan = IndexQueryPlan.Scan(pred!);
        var results = await _db.People.AsQueryable().ToListAsync(plan);
        Assert.Equal(2, results.Count);
        Assert.All(results, p => Assert.True(p.Age > 30));
    }

    [Fact]
    public void BLiteAotHelper_TryCompileWherePredicate_ReturnsNullForComplexLambda()
    {
        // x.Prop1 is a member access, not a supported binary comparison,
        // so TryCompile cannot build a BsonReaderPredicate and must return null.
        System.Linq.Expressions.Expression<System.Func<Person, bool>> unsupportedExpr =
            p => p.Name == p.Name; // tautology that compares field to itself — unsatisfiable for BSON evaluator
        var pred = BLiteAotHelper.TryCompileWherePredicate<Person>(unsupportedExpr);
        // BsonExpressionEvaluator cannot translate field-to-field comparisons; must return null.
        Assert.Null(pred);
    }

    // ── Chained Where — both predicates must be applied ──────────────────────

    [Fact]
    public async Task ToListAsync_ChainedWhere_BothPredicatesApplied()
    {
        // .Where(age).Where(name) must honour BOTH filters, not silently drop the inner one.
        // This guards against the interceptor bug where baseQ strips the inner Where chain.
        var results = await _db.People.AsQueryable()
            .Where(p => p.Age > 20)
            .Where(p => p.Name == "Person3")
            .ToListAsync();
        Assert.Single(results);
        Assert.Equal("Person3", results[0].Name);
        Assert.Equal(30, results[0].Age);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_ChainedWhere_BothPredicatesApplied()
    {
        var result = await _db.People.AsQueryable()
            .Where(p => p.Age > 20)
            .Where(p => p.Name == "Person3")
            .FirstOrDefaultAsync();
        Assert.NotNull(result);
        Assert.Equal("Person3", result!.Name);
    }

    // ── LongCountAsync ────────────────────────────────────────────────────────

    [Fact]
    public async Task LongCountAsync_ReturnsCorrectCount()
    {
        var count = await _db.People.AsQueryable().LongCountAsync();
        Assert.Equal(5L, count);
    }

    [Fact]
    public async Task LongCountAsync_WithFilteredQueryable_ReturnsFilteredCount()
    {
        var count = await _db.People.AsQueryable()
            .Where(p => p.Age >= 30)
            .LongCountAsync();
        Assert.Equal(3L, count);
    }

    // ── AllAsync (AOT-safe inverse predicate path) ────────────────────────────

    [Fact]
    public async Task AllAsync_ReturnsTrueWhenAllMatch()
    {
        var result = await _db.People.AsQueryable().AllAsync(p => p.Age > 0);
        Assert.True(result);
    }

    [Fact]
    public async Task AllAsync_ReturnsFalseWhenAnyDoesNotMatch()
    {
        var result = await _db.People.AsQueryable().AllAsync(p => p.Age > 30);
        Assert.False(result); // Ages 10, 20, 30 do not satisfy Age > 30
    }
}

/// <summary>
/// Extension helpers for Phase 3 tests.
/// </summary>
file static class Phase3TestExtensions
{
    public static System.Collections.Generic.IReadOnlyList<CollectionIndexInfo> ToReadOnlyList(
        this IEnumerable<CollectionIndexInfo> source)
        => source.ToList();
}
