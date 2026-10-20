using BLite.Bson;
using BLite.Core;
using BLite.Core.Query.Blql;
using BLite.Core.Storage;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// A typed <c>DocumentCollection</c> stores a document larger than one page across overflow
/// pages. The schema-less API (<see cref="DynamicCollection"/>, and through it BLQL, Studio and
/// <see cref="BLiteMigration"/>) used to hand the slot bytes to the BSON reader as if they were
/// a document and threw "Skipping type 69 not supported" (EntglDb/BLite#301). These tests write
/// through <see cref="TestDbContext"/> and read, query, delete and migrate through <see cref="BLiteEngine"/>.
/// </summary>
public class DynamicOverflowReadTests : IDisposable
{
    private const int LargeNameLength = 40_000;   // ~2.5 default pages
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"blite_overflow_{Guid.NewGuid():N}");
    private readonly string _dbPath;

    public DynamicOverflowReadTests()
    {
        Directory.CreateDirectory(_dir);
        _dbPath = Path.Combine(_dir, "app.db");
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private static string LargeName(char c = 'x') => new string(c, LargeNameLength);

    /// <summary>Writes one small and one large (overflow) user through the typed API; returns their ids.</summary>
    private async Task<(ObjectId small, ObjectId large)> WriteTypedAsync()
    {
        using var db = new TestDbContext(_dbPath);
        var small = new User { Name = "small", Age = 1 };
        var large = new User { Name = LargeName(), Age = 99 };
        await db.Users.InsertAsync(small);
        await db.Users.InsertAsync(large);
        await db.SaveChangesAsync();
        return (small.Id, large.Id);
    }

    private static string NameOf(BsonDocument d) { d.TryGetString("name", out var n); return n!; }

    [Fact]
    public async Task FindAllAsync_ReturnsTheOverflowDocument_Whole()
    {
        await WriteTypedAsync();

        using var engine = new BLiteEngine(_dbPath);
        var docs = await engine.GetOrCreateCollection("users").FindAllAsync().ToListAsync();

        Assert.Equal(2, docs.Count);
        var large = Assert.Single(docs, d => NameOf(d).Length > 10);
        Assert.Equal(LargeName(), NameOf(large));
        Assert.True(large.TryGetInt32("age", out var age));
        Assert.Equal(99, age);
        Assert.Equal(2, await engine.GetOrCreateCollection("users").CountAsync());
    }

    [Fact]
    public async Task FindByIdAsync_ReturnsTheOverflowDocument_Whole()
    {
        var (_, largeId) = await WriteTypedAsync();

        using var engine = new BLiteEngine(_dbPath);
        var doc = await engine.GetOrCreateCollection("users").FindByIdAsync(largeId);

        Assert.NotNull(doc);
        Assert.Equal(LargeName(), NameOf(doc!));
    }

    [Fact]
    public async Task Blql_FiltersAndSorts_OverflowDocuments()
    {
        await WriteTypedAsync();

        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("users");

        var byAge = col.Query(BlqlFilter.Eq("age", 99)).ToList();
        Assert.Equal(LargeName(), NameOf(Assert.Single(byAge)));

        var sorted = col.Query().OrderByDescending("age").ToList();
        Assert.Equal(2, sorted.Count);
        Assert.Equal(LargeName(), NameOf(sorted[0]));
        Assert.Equal("small", NameOf(sorted[1]));

        // An index on the field: the index-served path reads the same slots.
        await col.CreateIndexAsync("age");
        await engine.CommitAsync();
        var viaIndex = col.Query(BlqlFilter.Gte("age", 50)).ToList();
        Assert.Equal(LargeName(), NameOf(Assert.Single(viaIndex)));
    }

    [Fact]
    public async Task ScanAsync_PredicateSeesTheWholeDocument()
    {
        await WriteTypedAsync();

        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("users");

        // The predicate gets a reader over the reassembled document, so all of its bytes are
        // there; a slot alone holds at most one page of them.
        var matched = await col.ScanAsync(reader => reader.Remaining > LargeNameLength).ToListAsync();

        Assert.Equal(LargeName(), NameOf(Assert.Single(matched)));
    }

    [Fact]
    public async Task TypedDelete_OfTheOverflowDocument_IsSeenBySchemaLessApi()
    {
        var (_, largeId) = await WriteTypedAsync();
        using (var db = new TestDbContext(_dbPath))
        {
            Assert.True(await db.Users.DeleteAsync(largeId));
            await db.SaveChangesAsync();
        }

        using var engine = new BLiteEngine(_dbPath);
        var docs = await engine.GetOrCreateCollection("users").FindAllAsync().ToListAsync();
        Assert.Equal("small", NameOf(Assert.Single(docs)));
    }

    [Fact]
    public async Task SchemaLessDelete_OfTheOverflowDocument_IsSeenByTypedApi_AndTheSpaceIsReused()
    {
        var (_, largeId) = await WriteTypedAsync();
        long sizeBefore;
        using (var engine = new BLiteEngine(_dbPath))
        {
            var col = engine.GetOrCreateCollection("users");
            Assert.True(await col.DeleteAsync(largeId));
            await engine.CommitAsync();
            await engine.CheckpointAsync();
            Assert.Null(await col.FindByIdAsync(largeId));
            sizeBefore = new FileInfo(_dbPath).Length;
        }

        using (var db = new TestDbContext(_dbPath))
        {
            Assert.Null(await db.Users.FindByIdAsync(largeId));
            Assert.Equal(1, await db.Users.CountAsync());

            // The overflow pages were freed on commit: another document of the same size fits
            // into the recycled pages and the file does not grow by its size again.
            await db.Users.InsertAsync(new User { Name = LargeName('y'), Age = 7 });
            await db.SaveChangesAsync();
            db.ForceCheckpoint();
        }
        var sizeAfter = new FileInfo(_dbPath).Length;
        Assert.True(sizeAfter - sizeBefore < LargeNameLength,
            $"file grew by {sizeAfter - sizeBefore} bytes; the freed overflow pages were not reused");
    }

    [Fact]
    public async Task SchemaLessUpdate_OfTheOverflowDocument_ReplacesIt_ForBothApis()
    {
        var (_, largeId) = await WriteTypedAsync();
        using (var engine = new BLiteEngine(_dbPath))
        {
            var col = engine.GetOrCreateCollection("users");
            var replacement = col.CreateDocument(["_id", "name", "age"], b => b
                .AddId((BsonId)largeId)
                .AddString("name", "small now")
                .AddInt32("age", 42));
            Assert.True(await col.UpdateAsync(largeId, replacement));
            await engine.CommitAsync();

            var read = await col.FindByIdAsync(largeId);
            Assert.Equal("small now", NameOf(read!));
        }

        using var db = new TestDbContext(_dbPath);
        var typed = await db.Users.FindByIdAsync(largeId);
        Assert.NotNull(typed);
        Assert.Equal("small now", typed!.Name);
        Assert.Equal(42, typed.Age);
    }

    [Fact]
    public async Task SchemaLessInsert_LargerThanAPage_IsRejected_NamingTheLimit()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("users");
        var doc = col.CreateDocument(["name"], b => b.AddString("name", LargeName()));

        var ex = await Assert.ThrowsAsync<BLiteValidationException>(() => col.InsertAsync(doc).AsTask());
        Assert.Contains(col.MaxDocumentSize.ToString(), ex.Message);
        Assert.True(col.MaxDocumentSize > 0 && col.MaxDocumentSize < engine.Storage.PageSize);
    }

    [Fact]
    public async Task Migration_RefusesTheOverflowDocument_ByName_AndLeavesNothingBehind()
    {
        var (_, largeId) = await WriteTypedAsync();
        var config = PageFileConfig.Server(_dbPath);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => BLiteMigration.ToMultiFileAsync(_dbPath, config));

        Assert.Contains("'users'", ex.Message);
        Assert.Contains(largeId.ToString(), ex.Message);
        Assert.Contains("one page", ex.Message);

        // The source is intact and still readable by both APIs.
        using (var db = new TestDbContext(_dbPath))
        {
            Assert.Equal(2, await db.Users.CountAsync());
            Assert.Equal(LargeName(), (await db.Users.FindByIdAsync(largeId))!.Name);
        }

        // Nothing of the target is left: no temp file, no .idx, no WAL directory, no collection directory.
        Assert.False(File.Exists(_dbPath + ".migrating"));
        Assert.False(File.Exists(config.IndexFilePath!), config.IndexFilePath);
        Assert.False(File.Exists(config.WalPath!), config.WalPath);
        Assert.False(Directory.Exists(config.CollectionDataDirectory!), config.CollectionDataDirectory);
    }
}
