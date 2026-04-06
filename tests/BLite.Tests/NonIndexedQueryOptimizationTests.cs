using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Integration tests for the non-indexed query optimisation work (Phases 1-4).
///
/// Phase 2: CountScanAsync — counts via BSON predicates, no T materialisation.
/// Phase 3: OLAP (Sum/Avg/Min/Max) with WHERE; Min/Max detection.
/// Phase 4: ProjectionAnalyzer allows string-method WHERE in push-down SELECT.
/// </summary>
public class NonIndexedQueryOptimizationTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public NonIndexedQueryOptimizationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"niqo_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);

        // Seed: 5 TestDocuments  (Category A×2, B×2, C×1; Amount 10..50)
        _db.TestDocuments.InsertAsync(new TestDocument { Category = "A", Amount = 10, Name = "Item1" }).GetAwaiter().GetResult();
        _db.TestDocuments.InsertAsync(new TestDocument { Category = "A", Amount = 20, Name = "Item2" }).GetAwaiter().GetResult();
        _db.TestDocuments.InsertAsync(new TestDocument { Category = "B", Amount = 30, Name = "Item3" }).GetAwaiter().GetResult();
        _db.TestDocuments.InsertAsync(new TestDocument { Category = "B", Amount = 40, Name = "Item4" }).GetAwaiter().GetResult();
        _db.TestDocuments.InsertAsync(new TestDocument { Category = "C", Amount = 50, Name = "Item5" }).GetAwaiter().GetResult();
        _db.SaveChangesAsync().GetAwaiter().GetResult();

        // Seed: 4 Users for OrElse / string tests
        _db.Users.InsertAsync(new User { Name = "Alice",   Age = 30 }).GetAwaiter().GetResult();
        _db.Users.InsertAsync(new User { Name = "Bob",     Age = 25 }).GetAwaiter().GetResult();
        _db.Users.InsertAsync(new User { Name = "Charlie", Age = 35 }).GetAwaiter().GetResult();
        _db.Users.InsertAsync(new User { Name = "Dave",    Age = 20 }).GetAwaiter().GetResult();
        _db.SaveChangesAsync().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Phase 2 — CountScanAsync: BSON-level count, no T materialisation
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Count_WithBsonCompilablePredicate_ReturnsCorrectCount()
    {
        var count = _db.TestDocuments.AsQueryable().Count(x => x.Category == "A");
        Assert.Equal(2, count);
    }

    [Fact]
    public void Count_WithStringContainsPredicate_ReturnsCorrectCount()
    {
        // Phase 1 widening + Phase 2 count path
        // Ordinal comparison: "Charlie" and "Dave" contain lowercase 'a'; "Alice" has uppercase 'A'
        var count = _db.Users.AsQueryable().Count(x => x.Name.Contains("a"));
        Assert.Equal(2, count);
    }

    [Fact]
    public void Count_WithOrElsePredicate_ReturnsCorrectCount()
    {
        var count = _db.Users.AsQueryable().Count(x => x.Age == 25 || x.Age == 30);
        Assert.Equal(2, count);
    }

    [Fact]
    public void Count_WithAndAlsoPredicate_ReturnsCorrectCount()
    {
        var count = _db.TestDocuments.AsQueryable().Count(x => x.Category == "B" && x.Amount > 35);
        Assert.Equal(1, count); // Item4 (B, 40)
    }

    [Fact]
    public void Count_NoPredicate_MatchesWhereCount()
    {
        var totalCount   = _db.TestDocuments.AsQueryable().Count();
        var filteredCount = _db.TestDocuments.AsQueryable().Count(x => x.Amount >= 0);
        Assert.Equal(totalCount, filteredCount);
    }

    [Fact]
    public async Task CountAsync_WithBsonPredicate_ReturnsCorrectCount()
    {
        var count = await _db.TestDocuments.AsQueryable().CountAsync(x => x.Category == "B");
        Assert.Equal(2, count);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Phase 3 — Min/Max detection in BTreeExpressionVisitor
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Min_WithoutWhere_ReturnsMinValue()
    {
        var min = _db.TestDocuments.AsQueryable().Min(x => x.Amount);
        Assert.Equal(10, min);
    }

    [Fact]
    public void Max_WithoutWhere_ReturnsMaxValue()
    {
        var max = _db.TestDocuments.AsQueryable().Max(x => x.Amount);
        Assert.Equal(50, max);
    }

    [Fact]
    public void Min_WithWhere_ReturnsFilteredMin()
    {
        // Category B: amounts 30 and 40 → min is 30
        var min = _db.TestDocuments.AsQueryable().Where(x => x.Category == "B").Min(x => x.Amount);
        Assert.Equal(30, min);
    }

    [Fact]
    public void Max_WithWhere_ReturnsFilteredMax()
    {
        // Category A: amounts 10 and 20 → max is 20
        var max = _db.TestDocuments.AsQueryable().Where(x => x.Category == "A").Max(x => x.Amount);
        Assert.Equal(20, max);
    }

    [Fact]
    public async Task MinAsync_WithoutWhere_ReturnsMinValue()
    {
        var min = await _db.TestDocuments.AsQueryable().MinAsync(x => x.Amount);
        Assert.Equal(10, min);
    }

    [Fact]
    public async Task MaxAsync_WithoutWhere_ReturnsMaxValue()
    {
        var max = await _db.TestDocuments.AsQueryable().MaxAsync(x => x.Amount);
        Assert.Equal(50, max);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Phase 3 — Sum/Average with WHERE (BSON projection push-down)
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Sum_WithWhere_ReturnsFilteredSum()
    {
        // Category A: 10 + 20 = 30
        var sum = _db.TestDocuments.AsQueryable().Where(x => x.Category == "A").Sum(x => x.Amount);
        Assert.Equal(30, sum);
    }

    [Fact]
    public void Average_WithWhere_ReturnsFilteredAverage()
    {
        // Category B: (30 + 40) / 2 = 35
        var avg = _db.TestDocuments.AsQueryable().Where(x => x.Category == "B").Average(x => x.Amount);
        Assert.Equal(35.0, avg);
    }

    [Fact]
    public async Task SumAsync_WithWhere_ReturnsFilteredSum()
    {
        var sum = await _db.TestDocuments.AsQueryable()
            .Where(x => x.Category == "A")
            .SumAsync(x => x.Amount);
        Assert.Equal(30, sum);
    }

    [Fact]
    public async Task AverageAsync_WithWhere_ReturnsFilteredAverage()
    {
        var avg = await _db.TestDocuments.AsQueryable()
            .Where(x => x.Category == "B")
            .AverageAsync(x => x.Amount);
        Assert.Equal(35.0, avg);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Phase 4 — ProjectionAnalyzer: string method in WHERE keeps IsSimple=true
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Select_WithStringMethodWhere_PushesDownCorrectly()
    {
        // Before Phase 4 this fell back to full T materialisation; now the push-down
        // SELECT should still produce the correct projected result.
        var names = _db.Users.AsQueryable()
            .Where(x => x.Name.StartsWith("A"))
            .Select(x => x.Name)
            .ToList();

        Assert.Single(names);
        Assert.Equal("Alice", names[0]);
    }

    [Fact]
    public void Select_WithStringContainsWhere_FiltersAndProjects()
    {
        var ages = _db.Users.AsQueryable()
            .Where(x => x.Name.Contains("li"))  // Alice, Charlie
            .Select(x => x.Age)
            .OrderBy(a => a)
            .ToList();

        Assert.Equal(2, ages.Count);
        Assert.Equal(new[] { 30, 35 }, ages);
    }
}
