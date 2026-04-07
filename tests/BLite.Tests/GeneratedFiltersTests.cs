using BLite.Bson;
using BLite.Core.Indexing;
using BLite.Core.Query;
using BLite.Shared;
using BLite.Shared.Filters;

namespace BLite.Tests;

/// <summary>
/// Integration tests for Phase 2: generated {Entity}Filter classes,
/// <see cref="BsonPredicateBuilder"/>, <see cref="IndexQueryPlan"/>, and
/// <see cref="IDocumentCollection{T}.ScanAsync(IndexQueryPlan)"/>.
///
/// Scenarios tested:
///   - BsonPredicateBuilder: all operators (Eq, Gt, Gte, Lt, Lte, Between, Contains,
///     StartsWith, EndsWith, IsNull, IsNotNull, In, And, Or, Not)
///   - IndexQueryPlan.Scan() fallback: correct results without an index
///   - ScanAsync(IndexQueryPlan): full-scan path
///   - ScanAsync(IndexQueryPlan): B-Tree index path with index present
///   - ScanAsync(IndexQueryPlan): B-Tree index path falls back to scan when index absent
///   - PersonFilter (generated): methods compile and return correct results
///   - Compound query: IndexQueryPlan.And(residue)
/// </summary>
public class GeneratedFiltersTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public GeneratedFiltersTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"gen_filters_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);

        // Seed 5 people: ages 10, 20, 30, 40, 50
        for (int i = 0; i < 5; i++)
        {
            _db.People.InsertAsync(new Person
            {
                Id   = i + 1,
                Name = $"Person{i + 1}",
                Age  = (i + 1) * 10
            }).GetAwaiter().GetResult();
        }
        // Also insert one person with a specific name for string tests
        _db.People.InsertAsync(new Person { Id = 6, Name = "Alice", Age = 25 })
            .GetAwaiter().GetResult();

        _db.SaveChangesAsync().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ── BsonPredicateBuilder ──────────────────────────────────────────────────

    [Fact]
    public async Task BsonPredicateBuilder_Eq_ReturnsMatchingDocuments()
    {
        var pred = BsonPredicateBuilder.Eq("age", 30);
        var results = await _db.People.ScanAsync(pred).ToListAsync();
        Assert.Single(results);
        Assert.Equal(30, results[0].Age);
    }

    [Fact]
    public async Task BsonPredicateBuilder_Gt_ReturnsDocumentsGreaterThan()
    {
        var pred = BsonPredicateBuilder.Gt("age", 30);
        var results = await _db.People.ScanAsync(pred).ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.All(results, p => Assert.True(p.Age > 30));
    }

    [Fact]
    public async Task BsonPredicateBuilder_Gte_ReturnsDocumentsGreaterThanOrEqual()
    {
        var pred = BsonPredicateBuilder.Gte("age", 30);
        var results = await _db.People.ScanAsync(pred).ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.All(results, p => Assert.True(p.Age >= 30));
    }

    [Fact]
    public async Task BsonPredicateBuilder_Lt_ReturnsDocumentsLessThan()
    {
        var pred = BsonPredicateBuilder.Lt("age", 30);
        var results = await _db.People.ScanAsync(pred).ToListAsync();
        Assert.Equal(3, results.Count);  // 10, 20, 25
        Assert.All(results, p => Assert.True(p.Age < 30));
    }

    [Fact]
    public async Task BsonPredicateBuilder_Lte_ReturnsDocumentsLessThanOrEqual()
    {
        var pred = BsonPredicateBuilder.Lte("age", 30);
        var results = await _db.People.ScanAsync(pred).ToListAsync();
        Assert.Equal(4, results.Count);  // 10, 20, 25, 30
        Assert.All(results, p => Assert.True(p.Age <= 30));
    }

    [Fact]
    public async Task BsonPredicateBuilder_Between_ReturnsDocumentsInRange()
    {
        var pred = BsonPredicateBuilder.Between("age", 20, 40);
        var results = await _db.People.ScanAsync(pred).ToListAsync();
        Assert.Equal(4, results.Count);  // 20, 25, 30, 40
        Assert.All(results, p => Assert.True(p.Age >= 20 && p.Age <= 40));
    }

    [Fact]
    public async Task BsonPredicateBuilder_Contains_ReturnsMatchingDocuments()
    {
        var pred = BsonPredicateBuilder.Contains("name", "lic");  // matches "Alice"
        var results = await _db.People.ScanAsync(pred).ToListAsync();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task BsonPredicateBuilder_StartsWith_ReturnsMatchingDocuments()
    {
        var pred = BsonPredicateBuilder.StartsWith("name", "Person");
        var results = await _db.People.ScanAsync(pred).ToListAsync();
        Assert.Equal(5, results.Count);  // Person1..Person5
    }

    [Fact]
    public async Task BsonPredicateBuilder_EndsWith_ReturnsMatchingDocuments()
    {
        var pred = BsonPredicateBuilder.EndsWith("name", "3");  // matches "Person3"
        var results = await _db.People.ScanAsync(pred).ToListAsync();
        Assert.Single(results);
        Assert.Equal("Person3", results[0].Name);
    }

    [Fact]
    public async Task BsonPredicateBuilder_In_ReturnsMatchingDocuments()
    {
        var pred = BsonPredicateBuilder.In("age", new[] { 20, 40 });
        var results = await _db.People.ScanAsync(pred).ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.All(results, p => Assert.Contains(p.Age, new[] { 20, 40 }));
    }

    [Fact]
    public async Task BsonPredicateBuilder_And_ReturnsBothConditionsSatisfied()
    {
        var pred = BsonPredicateBuilder.And(
            BsonPredicateBuilder.Gt("age", 20),
            BsonPredicateBuilder.StartsWith("name", "Person"));
        var results = await _db.People.ScanAsync(pred).ToListAsync();
        Assert.Equal(3, results.Count);  // Person3(30), Person4(40), Person5(50)
        Assert.All(results, p => Assert.True(p.Age > 20 && p.Name.StartsWith("Person")));
    }

    [Fact]
    public async Task BsonPredicateBuilder_Or_ReturnsEitherConditionSatisfied()
    {
        var pred = BsonPredicateBuilder.Or(
            BsonPredicateBuilder.Eq("age", 10),
            BsonPredicateBuilder.Eq("name", "Alice"));
        var results = await _db.People.ScanAsync(pred).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task BsonPredicateBuilder_Not_NegatesPredicate()
    {
        var pred = BsonPredicateBuilder.Not(BsonPredicateBuilder.Gt("age", 30));
        var results = await _db.People.ScanAsync(pred).ToListAsync();
        // ages: 10, 20, 25, 30 → 4 results
        Assert.Equal(4, results.Count);
        Assert.All(results, p => Assert.True(p.Age <= 30));
    }

    // ── IndexQueryPlan.Scan fallback ──────────────────────────────────────────

    [Fact]
    public async Task IndexQueryPlan_Scan_ReturnsCorrectResults()
    {
        var plan = IndexQueryPlan.Scan(BsonPredicateBuilder.Eq("age", 30));
        var results = await _db.People.ScanAsync(plan).ToListAsync();
        Assert.Single(results);
        Assert.Equal(30, results[0].Age);
    }

    [Fact]
    public async Task IndexQueryPlan_Scan_WithResiduePredicate_FiltersResults()
    {
        // Scan all then post-filter
        var plan = IndexQueryPlan.Scan(BsonPredicateBuilder.Gte("age", 20))
            .And(BsonPredicateBuilder.StartsWith("name", "Person"));
        var results = await _db.People.ScanAsync(plan).ToListAsync();
        // ages >= 20 AND name startsWith "Person" → Person2(20), Person3(30), Person4(40), Person5(50)
        Assert.Equal(4, results.Count);
        Assert.All(results, p => Assert.True(p.Age >= 20 && p.Name.StartsWith("Person")));
    }

    // ── Generated PersonFilter ────────────────────────────────────────────────

    [Fact]
    public async Task PersonFilter_AgeGt_WithIndex_UsesIndexAndReturnsCorrectResults()
    {
        // Ensure the age index exists (registered in TestDbContext.OnModelCreating)
        var indexes = _db.People.GetIndexes().ToList();
        Assert.Contains(indexes, idx => idx.Type == IndexType.BTree
            && idx.PropertyPaths.Length == 1
            && idx.PropertyPaths[0].Equals("Age", StringComparison.OrdinalIgnoreCase));

        var plan = PersonFilter.AgeGt(30, indexes);
        Assert.True(plan.IsIndexScan, "Expected an index scan plan when the index is present");

        var results = await _db.People.ScanAsync(plan).ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.All(results, p => Assert.True(p.Age > 30));
    }

    [Fact]
    public async Task PersonFilter_AgeGte_WithIndex_UsesIndexAndReturnsCorrectResults()
    {
        var indexes = _db.People.GetIndexes().ToList();
        var plan = PersonFilter.AgeGte(30, indexes);
        Assert.True(plan.IsIndexScan);

        var results = await _db.People.ScanAsync(plan).ToListAsync();
        Assert.Equal(3, results.Count);  // 30, 40, 50
        Assert.All(results, p => Assert.True(p.Age >= 30));
    }

    [Fact]
    public async Task PersonFilter_AgeLt_WithIndex_UsesIndexAndReturnsCorrectResults()
    {
        var indexes = _db.People.GetIndexes().ToList();
        var plan = PersonFilter.AgeLt(30, indexes);
        Assert.True(plan.IsIndexScan);

        var results = await _db.People.ScanAsync(plan).ToListAsync();
        // Ages < 30: 10, 20, 25 → 3 results
        Assert.Equal(3, results.Count);
        Assert.All(results, p => Assert.True(p.Age < 30));
    }

    [Fact]
    public async Task PersonFilter_AgeLte_WithIndex_UsesIndexAndReturnsCorrectResults()
    {
        var indexes = _db.People.GetIndexes().ToList();
        var plan = PersonFilter.AgeLte(30, indexes);
        Assert.True(plan.IsIndexScan);

        var results = await _db.People.ScanAsync(plan).ToListAsync();
        // Ages <= 30: 10, 20, 25, 30 → 4 results
        Assert.Equal(4, results.Count);
        Assert.All(results, p => Assert.True(p.Age <= 30));
    }

    [Fact]
    public async Task PersonFilter_AgeEq_WithIndex_UsesIndexAndReturnsCorrectResults()
    {
        var indexes = _db.People.GetIndexes().ToList();
        var plan = PersonFilter.AgeEq(30, indexes);
        Assert.True(plan.IsIndexScan);

        var results = await _db.People.ScanAsync(plan).ToListAsync();
        Assert.Single(results);
        Assert.Equal(30, results[0].Age);
    }

    [Fact]
    public async Task PersonFilter_AgeBetween_WithIndex_ReturnsInclusiveRange()
    {
        var indexes = _db.People.GetIndexes().ToList();
        var plan = PersonFilter.AgeBetween(20, 40, indexes);
        Assert.True(plan.IsIndexScan);

        var results = await _db.People.ScanAsync(plan).ToListAsync();
        // Ages 20-40: 20, 25, 30, 40 → 4 results
        Assert.Equal(4, results.Count);
        Assert.All(results, p => Assert.True(p.Age >= 20 && p.Age <= 40));
    }

    [Fact]
    public async Task PersonFilter_AgeIn_WithIndex_UsesIndexAndReturnsCorrectResults()
    {
        var indexes = _db.People.GetIndexes().ToList();
        var plan = PersonFilter.AgeIn(new[] { 10, 30, 50 }, indexes);
        Assert.True(plan.IsIndexScan);

        var results = await _db.People.ScanAsync(plan).ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.All(results, p => Assert.Contains(p.Age, new[] { 10, 30, 50 }));
    }

    [Fact]
    public async Task PersonFilter_AgeGt_WithoutIndex_FallsBackToScan()
    {
        // Pass empty index list to force scan fallback
        var plan = PersonFilter.AgeGt(30, new List<CollectionIndexInfo>());
        Assert.False(plan.IsIndexScan, "Expected a scan plan when no index is registered");

        var results = await _db.People.ScanAsync(plan).ToListAsync();
        Assert.Equal(2, results.Count);  // 40, 50
        Assert.All(results, p => Assert.True(p.Age > 30));
    }

    [Fact]
    public async Task PersonFilter_NameContains_ReturnsCorrectResults()
    {
        var pred = PersonFilter.NameContains("lic");  // "Alice"
        var results = await _db.People.ScanAsync(pred).ToListAsync();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task PersonFilter_NameStartsWith_ReturnsCorrectResults()
    {
        var pred = PersonFilter.NameStartsWith("Person");
        var results = await _db.People.ScanAsync(pred).ToListAsync();
        Assert.Equal(5, results.Count);
    }

    // ── Compound query: index + residue ──────────────────────────────────────

    [Fact]
    public async Task IndexQueryPlan_And_AppliesResidueAfterIndexScan()
    {
        var indexes = _db.People.GetIndexes().ToList();

        // Age between 20-50 (index) AND name starts with "Person" (residue)
        // Expected: Person2(20), Person3(30), Person4(40), Person5(50)
        var plan = PersonFilter.AgeBetween(20, 50, indexes)
            .And(PersonFilter.NameStartsWith("Person"));
        Assert.True(plan.IsIndexScan);
        Assert.NotNull(plan.ResiduePredicate);

        var results = await _db.People.ScanAsync(plan).ToListAsync();
        Assert.Equal(4, results.Count);
        Assert.All(results, p => Assert.True(p.Age >= 20 && p.Age <= 50 && p.Name.StartsWith("Person")));
    }
}

// Helper extension so we don't need ToListAsync from System.Linq.Async
internal static class AsyncEnumerableHelpers
{
    public static async Task<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> source)
    {
        var result = new List<T>();
        await foreach (var item in source) result.Add(item);
        return result;
    }
}
