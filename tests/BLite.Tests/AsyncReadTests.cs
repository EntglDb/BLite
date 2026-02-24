using BLite.Bson;
using BLite.Core;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Tests for the async read path:
///   DocumentCollection.FindByIdAsync / FindAllAsync
///   DynamicCollection.FindByIdAsync / FindAllAsync
///   BLiteEngine.FindByIdAsync / FindAllAsync / InsertAsync / UpdateAsync / DeleteAsync
/// </summary>
public class AsyncReadTests : IDisposable
{
    private readonly string _dbPath;

    public AsyncReadTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_asyncread_{Guid.NewGuid()}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ─── DocumentCollection (via TestDbContext) ────────────────────────────────

    [Fact]
    public async Task DocumentCollection_FindByIdAsync_ReturnsInsertedDocument()
    {
        using var db = new TestDbContext(_dbPath);
        db.AsyncDocs.Insert(new AsyncDoc { Id = 1, Name = "Alpha" });

        var doc = await db.AsyncDocs.FindByIdAsync(1);

        Assert.NotNull(doc);
        Assert.Equal("Alpha", doc.Name);
    }

    [Fact]
    public async Task DocumentCollection_FindByIdAsync_ReturnsNull_WhenMissing()
    {
        using var db = new TestDbContext(_dbPath);

        var doc = await db.AsyncDocs.FindByIdAsync(9999);

        Assert.Null(doc);
    }

    [Fact]
    public async Task DocumentCollection_FindAllAsync_ReturnsAllDocuments()
    {
        using var db = new TestDbContext(_dbPath);
        for (int i = 1; i <= 5; i++)
            db.AsyncDocs.Insert(new AsyncDoc { Id = i + 100, Name = $"Doc{i}" });

        var results = new List<AsyncDoc>();
        await foreach (var doc in db.AsyncDocs.FindAllAsync())
            results.Add(doc);

        Assert.Equal(5, results.Count);
        Assert.Contains(results, d => d.Name == "Doc3");
    }

    [Fact]
    public async Task DocumentCollection_FindAllAsync_ReturnsEmpty_WhenNoDocuments()
    {
        using var db = new TestDbContext(_dbPath);

        var results = new List<AsyncDoc>();
        await foreach (var doc in db.AsyncDocs.FindAllAsync())
            results.Add(doc);

        Assert.Empty(results);
    }

    [Fact]
    public async Task DocumentCollection_FindByIdAsync_SeesUncommittedWrite_InSameTransaction()
    {
        using var db = new TestDbContext(_dbPath);
        using var txn = await db.BeginTransactionAsync();

        db.AsyncDocs.Insert(new AsyncDoc { Id = 200, Name = "Uncommitted" });

        // Should still be visible in the same transaction scope (RYOW)
        var doc = await db.AsyncDocs.FindByIdAsync(200);
        Assert.NotNull(doc);
        Assert.Equal("Uncommitted", doc.Name);
    }

    [Fact]
    public async Task DocumentCollection_FindAllAsync_WithCancellation_ThrowsOperationCanceled()
    {
        using var db = new TestDbContext(_dbPath);
        for (int i = 1; i <= 20; i++)
            db.AsyncDocs.Insert(new AsyncDoc { Id = i + 300, Name = $"CancelDoc{i}" });

        // Pre-cancel so the first MoveNextAsync after the first yield throws immediately.
        using var cts = new CancellationTokenSource();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var doc in db.AsyncDocs.FindAllAsync(cts.Token))
            {
                cts.Cancel(); // cancel after receiving the very first item
                // next MoveNextAsync will hit ct.ThrowIfCancellationRequested()
            }
        });
    }

    // ─── DynamicCollection (via BLiteEngine) ──────────────────────────────────

    [Fact]
    public async Task DynamicCollection_FindByIdAsync_ReturnsInsertedDocument()
    {
        using var engine = new BLiteEngine(_dbPath);
        var id = engine.Insert("items", engine.CreateDocument(["name"], b => b.AddString("name", "Widget")));
        engine.Commit();

        var doc = await engine.FindByIdAsync("items", id);

        Assert.NotNull(doc);
        Assert.True(doc.TryGetString("name", out var name));
        Assert.Equal("Widget", name);
    }

    [Fact]
    public async Task DynamicCollection_FindByIdAsync_ReturnsNull_WhenMissing()
    {
        using var engine = new BLiteEngine(_dbPath);
        engine.Insert("items", engine.CreateDocument(["name"], b => b.AddString("name", "X")));
        engine.Commit();

        var doc = await engine.FindByIdAsync("items", new BsonId(ObjectId.NewObjectId()));

        Assert.Null(doc);
    }

    [Fact]
    public async Task DynamicCollection_FindAllAsync_ReturnsAllDocuments()
    {
        using var engine = new BLiteEngine(_dbPath);
        for (int i = 1; i <= 4; i++)
            engine.Insert("things", engine.CreateDocument(["n"], b => b.AddInt32("n", i)));
        engine.Commit();

        var results = new List<BsonDocument>();
        await foreach (var doc in engine.FindAllAsync("things"))
            results.Add(doc);

        Assert.Equal(4, results.Count);
    }

    [Fact]
    public async Task DynamicCollection_FindAllAsync_ReturnsEmpty_WhenCollectionNotCreated()
    {
        using var engine = new BLiteEngine(_dbPath);

        var results = new List<BsonDocument>();
        await foreach (var doc in engine.FindAllAsync("nonexistent"))
            results.Add(doc);

        Assert.Empty(results);
    }

    // ─── BLiteEngine async write helpers ──────────────────────────────────────

    [Fact]
    public async Task BLiteEngine_InsertAsync_PersistsDocument()
    {
        using var engine = new BLiteEngine(_dbPath);
        var id = await engine.InsertAsync("catalog", engine.CreateDocument(["title"], b => b.AddString("title", "Book")));

        var doc = await engine.FindByIdAsync("catalog", id);
        Assert.NotNull(doc);
        Assert.True(doc.TryGetString("title", out var title));
        Assert.Equal("Book", title);
    }

    [Fact]
    public async Task BLiteEngine_UpdateAsync_ReturnsTrueAndPersistsChange()
    {
        using var engine = new BLiteEngine(_dbPath);
        var id = await engine.InsertAsync("catalog", engine.CreateDocument(["title"], b => b.AddString("title", "OldTitle")));

        var updated = await engine.UpdateAsync("catalog", id, engine.CreateDocument(["title"], b => b.AddString("title", "NewTitle")));

        Assert.True(updated);
        var doc = await engine.FindByIdAsync("catalog", id);
        Assert.NotNull(doc);
        Assert.True(doc.TryGetString("title", out var newTitle));
        Assert.Equal("NewTitle", newTitle);
    }

    [Fact]
    public async Task BLiteEngine_UpdateAsync_ReturnsFalse_WhenIdMissing()
    {
        using var engine = new BLiteEngine(_dbPath);

        var result = await engine.UpdateAsync(
            "catalog",
            new BsonId(ObjectId.NewObjectId()),
            engine.CreateDocument(["title"], b => b.AddString("title", "Ghost")));

        Assert.False(result);
    }

    [Fact]
    public async Task BLiteEngine_DeleteAsync_ReturnsTrueAndRemovesDocument()
    {
        using var engine = new BLiteEngine(_dbPath);
        var id = await engine.InsertAsync("catalog", engine.CreateDocument(["title"], b => b.AddString("title", "ToDelete")));

        var deleted = await engine.DeleteAsync("catalog", id);

        Assert.True(deleted);
        var doc = await engine.FindByIdAsync("catalog", id);
        Assert.Null(doc);
    }

    [Fact]
    public async Task BLiteEngine_DeleteAsync_ReturnsFalse_WhenIdMissing()
    {
        using var engine = new BLiteEngine(_dbPath);

        var result = await engine.DeleteAsync(
            "catalog",
            new BsonId(ObjectId.NewObjectId()));

        Assert.False(result);
    }

    [Fact]
    public async Task BLiteEngine_InsertAsync_ThenFindAllAsync_RoundTrip()
    {
        using var engine = new BLiteEngine(_dbPath);
        for (int i = 0; i < 10; i++)
            await engine.InsertAsync("log", engine.CreateDocument(["seq"], b => b.AddInt32("seq", i)));

        var results = new List<BsonDocument>();
        await foreach (var doc in engine.FindAllAsync("log"))
            results.Add(doc);

        Assert.Equal(10, results.Count);
    }
}
