using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests.Query;

/// <summary>
/// Tests for the async LINQ materialiser extensions defined in
/// <see cref="BLiteQueryableExtensions"/>.
/// Covers: FirstAsync, SingleAsync, LastAsync/LastOrDefaultAsync,
/// ElementAtAsync/ElementAtOrDefaultAsync, MinAsync, MaxAsync,
/// SumAsync, AverageAsync and ForEachAsync.
/// </summary>
public class BLiteQueryableExtensionsTests : IDisposable
{
    private readonly string _dbPath;

    public BLiteQueryableExtensionsTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_ext_{Guid.NewGuid()}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    private async Task<TestDbContext> CreateAndSeed(int count = 10)
    {
        var db = new TestDbContext(_dbPath);
        for (int i = 1; i <= count; i++)
            await db.AsyncDocs.InsertAsync(new AsyncDoc { Id = i, Name = $"Doc{i}" });
        return db;
    }

    // ─── FirstAsync ───────────────────────────────────────────────────────────

    [Fact]
    public async Task FirstAsync_ReturnsFirstElement()
    {
        using var db = await CreateAndSeed(5);

        var doc = await db.AsyncDocs.AsQueryable()
            .OrderBy(d => d.Id)
            .FirstAsync();

        Assert.NotNull(doc);
        Assert.Equal(1, doc.Id);
    }

    [Fact]
    public async Task FirstAsync_WithPredicate_ReturnsMatchingElement()
    {
        using var db = await CreateAndSeed(5);

        var doc = await db.AsyncDocs.AsQueryable()
            .FirstAsync(d => d.Name == "Doc3");

        Assert.NotNull(doc);
        Assert.Equal(3, doc.Id);
    }

    [Fact]
    public async Task FirstAsync_Throws_WhenEmpty()
    {
        using var db = new TestDbContext(_dbPath);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => db.AsyncDocs.AsQueryable().FirstAsync());
    }

    [Fact]
    public async Task FirstAsync_WithPredicate_Throws_WhenNoMatch()
    {
        using var db = await CreateAndSeed(3);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => db.AsyncDocs.AsQueryable().FirstAsync(d => d.Name == "Ghost"));
    }

    // ─── SingleAsync ──────────────────────────────────────────────────────────

    [Fact]
    public async Task SingleAsync_ReturnsSingleMatchingElement()
    {
        using var db = await CreateAndSeed(5);

        var doc = await db.AsyncDocs.AsQueryable()
            .Where(d => d.Id == 2)
            .SingleAsync();

        Assert.NotNull(doc);
        Assert.Equal("Doc2", doc.Name);
    }

    [Fact]
    public async Task SingleAsync_WithPredicate_ReturnsSingleMatchingElement()
    {
        using var db = await CreateAndSeed(5);

        var doc = await db.AsyncDocs.AsQueryable()
            .SingleAsync(d => d.Id == 4);

        Assert.Equal("Doc4", doc.Name);
    }

    [Fact]
    public async Task SingleAsync_Throws_WhenEmpty()
    {
        using var db = new TestDbContext(_dbPath);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => db.AsyncDocs.AsQueryable().SingleAsync());
    }

    [Fact]
    public async Task SingleAsync_Throws_WhenMoreThanOneElement()
    {
        using var db = await CreateAndSeed(5);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => db.AsyncDocs.AsQueryable().SingleAsync());
    }

    [Fact]
    public async Task SingleAsync_WithPredicate_Throws_WhenNoMatch()
    {
        using var db = await CreateAndSeed(3);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => db.AsyncDocs.AsQueryable().SingleAsync(d => d.Name == "Ghost"));
    }

    // ─── LastAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task LastAsync_ReturnsLastElement()
    {
        using var db = await CreateAndSeed(5);

        var doc = await db.AsyncDocs.AsQueryable()
            .OrderBy(d => d.Id)
            .LastAsync();

        Assert.Equal(5, doc.Id);
    }

    [Fact]
    public async Task LastAsync_WithPredicate_ReturnsLastMatchingElement()
    {
        using var db = await CreateAndSeed(5);

        var doc = await db.AsyncDocs.AsQueryable()
            .OrderBy(d => d.Id)
            .LastAsync(d => d.Id < 4);

        Assert.Equal(3, doc.Id);
    }

    [Fact]
    public async Task LastAsync_Throws_WhenEmpty()
    {
        using var db = new TestDbContext(_dbPath);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => db.AsyncDocs.AsQueryable().LastAsync());
    }

    [Fact]
    public async Task LastAsync_WithPredicate_Throws_WhenNoMatch()
    {
        using var db = await CreateAndSeed(3);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => db.AsyncDocs.AsQueryable().LastAsync(d => d.Name == "Ghost"));
    }

    // ─── LastOrDefaultAsync ───────────────────────────────────────────────────

    [Fact]
    public async Task LastOrDefaultAsync_ReturnsLastElement()
    {
        using var db = await CreateAndSeed(5);

        var doc = await db.AsyncDocs.AsQueryable()
            .OrderBy(d => d.Id)
            .LastOrDefaultAsync();

        Assert.NotNull(doc);
        Assert.Equal(5, doc.Id);
    }

    [Fact]
    public async Task LastOrDefaultAsync_ReturnsNull_WhenEmpty()
    {
        using var db = new TestDbContext(_dbPath);

        var doc = await db.AsyncDocs.AsQueryable().LastOrDefaultAsync();

        Assert.Null(doc);
    }

    [Fact]
    public async Task LastOrDefaultAsync_WithPredicate_ReturnsNull_WhenNoMatch()
    {
        using var db = await CreateAndSeed(3);

        var doc = await db.AsyncDocs.AsQueryable()
            .LastOrDefaultAsync(d => d.Name == "Ghost");

        Assert.Null(doc);
    }

    // ─── ElementAtAsync ───────────────────────────────────────────────────────

    [Fact]
    public async Task ElementAtAsync_ReturnsElementAtIndex()
    {
        using var db = await CreateAndSeed(5);

        // OrderBy Id to get deterministic order: Doc1, Doc2, Doc3, Doc4, Doc5
        var doc = await db.AsyncDocs.AsQueryable()
            .OrderBy(d => d.Id)
            .ElementAtAsync(2);

        Assert.Equal("Doc3", doc.Name);
    }

    [Fact]
    public async Task ElementAtAsync_Throws_WhenIndexOutOfRange()
    {
        using var db = await CreateAndSeed(3);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => db.AsyncDocs.AsQueryable().ElementAtAsync(10));
    }

    [Fact]
    public async Task ElementAtAsync_Throws_WhenNegativeIndex()
    {
        using var db = await CreateAndSeed(3);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => db.AsyncDocs.AsQueryable().ElementAtAsync(-1));
    }

    // ─── ElementAtOrDefaultAsync ──────────────────────────────────────────────

    [Fact]
    public async Task ElementAtOrDefaultAsync_ReturnsElementAtIndex()
    {
        using var db = await CreateAndSeed(5);

        var doc = await db.AsyncDocs.AsQueryable()
            .OrderBy(d => d.Id)
            .ElementAtOrDefaultAsync(0);

        Assert.NotNull(doc);
        Assert.Equal("Doc1", doc.Name);
    }

    [Fact]
    public async Task ElementAtOrDefaultAsync_ReturnsDefault_WhenIndexOutOfRange()
    {
        using var db = await CreateAndSeed(3);

        var doc = await db.AsyncDocs.AsQueryable().ElementAtOrDefaultAsync(100);

        Assert.Null(doc);
    }

    [Fact]
    public async Task ElementAtOrDefaultAsync_ReturnsDefault_WhenNegativeIndex()
    {
        using var db = await CreateAndSeed(3);

        var doc = await db.AsyncDocs.AsQueryable().ElementAtOrDefaultAsync(-5);

        Assert.Null(doc);
    }

    // ─── MinAsync ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task MinAsync_ReturnsMinimumIntValue()
    {
        using var db = await CreateAndSeed(5);

        var min = await db.AsyncDocs.AsQueryable().MinAsync(d => d.Id);

        Assert.Equal(1, min);
    }

    [Fact]
    public async Task MinAsync_ReturnsMinimumStringValue()
    {
        using var db = await CreateAndSeed(5);

        var min = await db.AsyncDocs.AsQueryable().MinAsync(d => d.Name);

        // Alphabetically Doc1 < Doc2 < Doc3 < Doc4 < Doc5
        Assert.Equal("Doc1", min);
    }

    [Fact]
    public async Task MinAsync_Throws_WhenEmpty()
    {
        using var db = new TestDbContext(_dbPath);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => db.AsyncDocs.AsQueryable().MinAsync(d => d.Id));
    }

    // ─── MaxAsync ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task MaxAsync_ReturnsMaximumIntValue()
    {
        using var db = await CreateAndSeed(5);

        var max = await db.AsyncDocs.AsQueryable().MaxAsync(d => d.Id);

        Assert.Equal(5, max);
    }

    [Fact]
    public async Task MaxAsync_ReturnsMaximumStringValue()
    {
        using var db = await CreateAndSeed(5);

        var max = await db.AsyncDocs.AsQueryable().MaxAsync(d => d.Name);

        Assert.Equal("Doc5", max);
    }

    [Fact]
    public async Task MaxAsync_Throws_WhenEmpty()
    {
        using var db = new TestDbContext(_dbPath);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => db.AsyncDocs.AsQueryable().MaxAsync(d => d.Id));
    }

    // ─── SumAsync ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task SumAsync_Int_ReturnsTotalSum()
    {
        using var db = await CreateAndSeed(5); // Id 1..5

        var sum = await db.AsyncDocs.AsQueryable().SumAsync(d => d.Id);

        Assert.Equal(15, sum); // 1+2+3+4+5
    }

    [Fact]
    public async Task SumAsync_Int_ReturnsZero_WhenEmpty()
    {
        using var db = new TestDbContext(_dbPath);

        var sum = await db.AsyncDocs.AsQueryable().SumAsync(d => d.Id);

        Assert.Equal(0, sum);
    }

    [Fact]
    public async Task SumAsync_Long_ReturnsTotalSum()
    {
        using var db = await CreateAndSeed(5);

        var sum = await db.AsyncDocs.AsQueryable().SumAsync(d => (long)d.Id);

        Assert.Equal(15L, sum);
    }

    [Fact]
    public async Task SumAsync_Double_ReturnsTotalSum()
    {
        using var db = await CreateAndSeed(4); // 1+2+3+4 = 10

        var sum = await db.AsyncDocs.AsQueryable().SumAsync(d => (double)d.Id);

        Assert.Equal(10.0, sum, precision: 5);
    }

    [Fact]
    public async Task SumAsync_Decimal_ReturnsTotalSum()
    {
        using var db = await CreateAndSeed(3); // 1+2+3 = 6

        var sum = await db.AsyncDocs.AsQueryable().SumAsync(d => (decimal)d.Id);

        Assert.Equal(6m, sum);
    }

    // ─── AverageAsync ─────────────────────────────────────────────────────────

    [Fact]
    public async Task AverageAsync_Int_ReturnsCorrectAverage()
    {
        using var db = await CreateAndSeed(5); // avg(1,2,3,4,5) = 3.0

        var avg = await db.AsyncDocs.AsQueryable().AverageAsync(d => d.Id);

        Assert.Equal(3.0, avg, precision: 5);
    }

    [Fact]
    public async Task AverageAsync_Int_Throws_WhenEmpty()
    {
        using var db = new TestDbContext(_dbPath);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => db.AsyncDocs.AsQueryable().AverageAsync(d => d.Id));
    }

    [Fact]
    public async Task AverageAsync_Long_ReturnsCorrectAverage()
    {
        using var db = await CreateAndSeed(4); // avg(1,2,3,4) = 2.5

        var avg = await db.AsyncDocs.AsQueryable().AverageAsync(d => (long)d.Id);

        Assert.Equal(2.5, avg, precision: 5);
    }

    [Fact]
    public async Task AverageAsync_Double_ReturnsCorrectAverage()
    {
        using var db = await CreateAndSeed(4);

        var avg = await db.AsyncDocs.AsQueryable().AverageAsync(d => (double)d.Id);

        Assert.Equal(2.5, avg, precision: 5);
    }

    [Fact]
    public async Task AverageAsync_Decimal_ReturnsCorrectAverage()
    {
        using var db = await CreateAndSeed(4); // avg(1,2,3,4) = 2.5

        var avg = await db.AsyncDocs.AsQueryable().AverageAsync(d => (decimal)d.Id);

        Assert.Equal(2.5m, avg);
    }

    [Fact]
    public async Task AverageAsync_Decimal_Throws_WhenEmpty()
    {
        using var db = new TestDbContext(_dbPath);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => db.AsyncDocs.AsQueryable().AverageAsync(d => (decimal)d.Id));
    }

    // ─── ForEachAsync ─────────────────────────────────────────────────────────

    [Fact]
    public async Task ForEachAsync_InvokesActionForEachElement()
    {
        using var db = await CreateAndSeed(5);
        var collected = new List<int>();

        await db.AsyncDocs.AsQueryable()
            .OrderBy(d => d.Id)
            .ForEachAsync(d => collected.Add(d.Id));

        Assert.Equal(new[] { 1, 2, 3, 4, 5 }, collected);
    }

    [Fact]
    public async Task ForEachAsync_DoesNothing_WhenEmpty()
    {
        using var db = new TestDbContext(_dbPath);
        int callCount = 0;

        await db.AsyncDocs.AsQueryable().ForEachAsync(_ => callCount++);

        Assert.Equal(0, callCount);
    }

    [Fact]
    public async Task ForEachAsync_WithPredicate_InvokesActionForMatchingElements()
    {
        using var db = await CreateAndSeed(5);
        var collected = new List<int>();

        await db.AsyncDocs.AsQueryable()
            .Where(d => d.Id <= 3)
            .OrderBy(d => d.Id)
            .ForEachAsync(d => collected.Add(d.Id));

        Assert.Equal(new[] { 1, 2, 3 }, collected);
    }
}
