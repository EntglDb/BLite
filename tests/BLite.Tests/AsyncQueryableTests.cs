using BLite.Core.Query;
using BLite.Shared;
using Xunit;

namespace BLite.Tests;

/// <summary>
/// Tests for <see cref="BTreeQueryable{T}"/> implementing <see cref="IAsyncEnumerable{T}"/>
/// and for <see cref="BLiteQueryableExtensions"/> (ToListAsync, FirstOrDefaultAsync, etc.).
/// </summary>
public class AsyncQueryableTests : IDisposable
{
    private readonly string _dbPath;

    public AsyncQueryableTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_asyncq_{Guid.NewGuid()}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    private TestDbContext CreateAndSeed(int count = 10, int idOffset = 0)
    {
        var db = new TestDbContext(_dbPath);
        for (int i = 1; i <= count; i++)
            db.AsyncDocs.Insert(new AsyncDoc { Id = i + idOffset, Name = $"Doc{i}" });
        return db;
    }

    // ─── await foreach ────────────────────────────────────────────────────────

    [Fact]
    public async Task AwaitForeach_OnAsQueryable_YieldsAllDocuments()
    {
        using var db = CreateAndSeed(5);

        var results = new List<AsyncDoc>();
        await foreach (var doc in (IAsyncEnumerable<AsyncDoc>)db.AsyncDocs.AsQueryable())
            results.Add(doc);

        Assert.Equal(5, results.Count);
    }

    [Fact]
    public async Task AwaitForeach_OnFilteredQueryable_YieldsOnlyMatches()
    {
        using var db = CreateAndSeed(10);

        var results = new List<AsyncDoc>();
        await foreach (var doc in (IAsyncEnumerable<AsyncDoc>)db.AsyncDocs.AsQueryable().Where(d => d.Id > 5 + 0))
            results.Add(doc);

        Assert.Equal(5, results.Count);
        Assert.All(results, d => Assert.True(d.Id > 5));
    }

    // ─── ToListAsync ──────────────────────────────────────────────────────────

    [Fact]
    public async Task ToListAsync_ReturnsAllDocuments()
    {
        using var db = CreateAndSeed(7);

        var list = await db.AsyncDocs.AsQueryable().ToListAsync();

        Assert.Equal(7, list.Count);
    }

    [Fact]
    public async Task ToListAsync_WithWhere_ReturnsFilteredDocuments()
    {
        using var db = CreateAndSeed(10);

        var list = await db.AsyncDocs.AsQueryable()
            .Where(d => d.Name == "Doc3")
            .ToListAsync();

        Assert.Single(list);
        Assert.Equal("Doc3", list[0].Name);
    }

    [Fact]
    public async Task ToArrayAsync_ReturnsArray()
    {
        using var db = CreateAndSeed(4);

        var arr = await db.AsyncDocs.AsQueryable().ToArrayAsync();

        Assert.Equal(4, arr.Length);
    }

    // ─── FirstOrDefaultAsync ─────────────────────────────────────────────────

    [Fact]
    public async Task FirstOrDefaultAsync_ReturnsFirstDocument()
    {
        using var db = CreateAndSeed(5);

        var doc = await db.AsyncDocs.AsQueryable()
            .OrderBy(d => d.Id)
            .FirstOrDefaultAsync();

        Assert.NotNull(doc);
        Assert.Equal("Doc1", doc.Name);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_WithPredicate_ReturnsMatchingDocument()
    {
        using var db = CreateAndSeed(5);

        var doc = await db.AsyncDocs.AsQueryable()
            .FirstOrDefaultAsync(d => d.Name == "Doc4");

        Assert.NotNull(doc);
        Assert.Equal(4, doc.Id);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_ReturnsNull_WhenNoMatch()
    {
        using var db = CreateAndSeed(5);

        var doc = await db.AsyncDocs.AsQueryable()
            .FirstOrDefaultAsync(d => d.Name == "NonExistent");

        Assert.Null(doc);
    }

    // ─── SingleOrDefaultAsync ────────────────────────────────────────────────

    [Fact]
    public async Task SingleOrDefaultAsync_WithPredicate_ReturnsUniqueDocument()
    {
        using var db = CreateAndSeed(5);

        var doc = await db.AsyncDocs.AsQueryable()
            .Where(d => d.Id == 3)
            .SingleOrDefaultAsync();

        Assert.NotNull(doc);
        Assert.Equal("Doc3", doc.Name);
    }

    [Fact]
    public async Task SingleOrDefaultAsync_Throws_WhenMoreThanOne()
    {
        using var db = CreateAndSeed(5);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await db.AsyncDocs.AsQueryable().SingleOrDefaultAsync());
    }

    // ─── CountAsync ──────────────────────────────────────────────────────────

    [Fact]
    public async Task CountAsync_ReturnsCorrectCount()
    {
        using var db = CreateAndSeed(8);

        var count = await db.AsyncDocs.AsQueryable().CountAsync();

        Assert.Equal(8, count);
    }

    [Fact]
    public async Task CountAsync_WithPredicate_ReturnsFilteredCount()
    {
        using var db = CreateAndSeed(10);

        var count = await db.AsyncDocs.AsQueryable()
            .CountAsync(d => d.Id <= 5);

        Assert.Equal(5, count);
    }

    // ─── AnyAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task AnyAsync_ReturnsTrueWhenDocumentsExist()
    {
        using var db = CreateAndSeed(3);

        Assert.True(await db.AsyncDocs.AsQueryable().AnyAsync());
    }

    [Fact]
    public async Task AnyAsync_ReturnsFalse_WhenEmpty()
    {
        using var db = new TestDbContext(_dbPath);

        Assert.False(await db.AsyncDocs.AsQueryable().AnyAsync());
    }

    [Fact]
    public async Task AnyAsync_WithPredicate_ReturnsTrueOnMatch()
    {
        using var db = CreateAndSeed(5);

        Assert.True(await db.AsyncDocs.AsQueryable().AnyAsync(d => d.Name == "Doc2"));
        Assert.False(await db.AsyncDocs.AsQueryable().AnyAsync(d => d.Name == "Ghost"));
    }

    // ─── AllAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task AllAsync_ReturnsTrueWhenAllMatch()
    {
        using var db = CreateAndSeed(5);

        Assert.True(await db.AsyncDocs.AsQueryable().AllAsync(d => d.Id > 0));
    }

    [Fact]
    public async Task AllAsync_ReturnsFalseWhenSomeDoNotMatch()
    {
        using var db = CreateAndSeed(5);

        Assert.False(await db.AsyncDocs.AsQueryable().AllAsync(d => d.Id == 1));
    }

    // ─── Cancellation ────────────────────────────────────────────────────────

    [Fact]
    public async Task ToListAsync_WithCancelledToken_ThrowsOperationCanceled()
    {
        using var db = CreateAndSeed(10);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => db.AsyncDocs.AsQueryable().ToListAsync(cts.Token));
    }
}
