using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Tests for TruncateCollectionAsync and the fixed DropCollection.
/// </summary>
public class TruncateDropCollectionTests : IDisposable
{
    private readonly string _dbPath;

    public TruncateDropCollectionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_truncate_{Guid.NewGuid()}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var walPath = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(walPath)) File.Delete(walPath);
    }

    // ── BLiteEngine.TruncateCollectionAsync ───────────────────────────────────

    [Fact]
    public async Task Engine_TruncateCollection_DeletesAllDocuments()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("users");

        // Insert some documents
        for (int i = 0; i < 10; i++)
        {
            var doc = col.CreateDocument(["_id", "name"], b => b.AddString("name", $"User{i}"));
            await col.InsertAsync(doc);
        }

        Assert.Equal(10, (int)await col.CountAsync());

        // Truncate
        var deleted = await engine.TruncateCollectionAsync("users");

        Assert.Equal(10, deleted);
        Assert.Equal(0, (int)await col.CountAsync());
    }

    [Fact]
    public async Task Engine_TruncateCollection_CollectionRemainsUsable()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("items");

        // Insert 5 documents, truncate, then insert 3 more.
        for (int i = 0; i < 5; i++)
        {
            var doc = col.CreateDocument(["_id", "value"], b => b.AddInt32("value", i));
            await col.InsertAsync(doc);
        }

        await engine.TruncateCollectionAsync("items");
        Assert.Equal(0, (int)await col.CountAsync());

        for (int i = 0; i < 3; i++)
        {
            var doc = col.CreateDocument(["_id", "value"], b => b.AddInt32("value", i + 100));
            await col.InsertAsync(doc);
        }

        Assert.Equal(3, (int)await col.CountAsync());
    }

    [Fact]
    public async Task Engine_TruncateCollection_EmptyCollection_ReturnsZero()
    {
        using var engine = new BLiteEngine(_dbPath);
        engine.GetOrCreateCollection("empty");

        var deleted = await engine.TruncateCollectionAsync("empty");

        Assert.Equal(0, deleted);
    }

    // ── BLiteEngine.DropCollection (fixed) ────────────────────────────────────

    [Fact]
    public void Engine_DropCollection_FreesMetadata()
    {
        using var engine = new BLiteEngine(_dbPath);
        engine.GetOrCreateCollection("to_drop");

        Assert.True(engine.DropCollection("to_drop"));
        Assert.Null(engine.GetCollection("to_drop"));

        // Dropping again returns false
        Assert.False(engine.DropCollection("to_drop"));
    }

    [Fact]
    public async Task Engine_DropCollection_SingleFile_PagesReclaimedForReuse()
    {
        // Use an in-memory engine so there is no file-I/O during the background
        // checkpoint that would cause lock contention with the synchronous page-freeing
        // walk performed by FreeCollectionPages.
        using var engine = BLiteEngine.CreateInMemory();
        var col = engine.GetOrCreateCollection("temp_col");

        // Insert documents to allocate pages.
        for (int i = 0; i < 20; i++)
        {
            var doc = col.CreateDocument(["_id", "payload"],
                b => b.AddString("payload", new string('x', 200)));
            await col.InsertAsync(doc);
        }

        // Record the allocator high-watermark before drop.
        uint pageCountAfterInsert = engine.Storage.PageCount;

        engine.DropCollection("temp_col");

        // After dropping, the collection should no longer exist.
        Assert.Null(engine.GetCollection("temp_col"));

        // NextPageId is a monotonically increasing high-watermark; it doesn't shrink
        // when pages are freed — only AllocatePage can advance it.
        Assert.Equal(pageCountAfterInsert, engine.Storage.PageCount);

        // Re-insert the same volume of documents under a new collection.
        // The freed pages must be reused from the free list; the high-watermark
        // must not grow (within a small tolerance for new catalog/schema overhead).
        var col2 = engine.GetOrCreateCollection("new_col");
        for (int i = 0; i < 20; i++)
        {
            var doc = col2.CreateDocument(["_id", "payload"],
                b => b.AddString("payload", new string('x', 200)));
            await col2.InsertAsync(doc);
        }

        uint pageCountAfterReInsert = engine.Storage.PageCount;

        // Pages freed by DropCollection were reused; the allocator high-watermark
        // must not exceed the post-first-insert count by more than a small constant
        // (the new collection's metadata/schema pages cannot be reclaimed from the
        // same catalog slot as the old collection).
        Assert.True(pageCountAfterReInsert <= pageCountAfterInsert + 2,
            $"Expected page reuse: count after re-insert ({pageCountAfterReInsert}) " +
            $"should be <= count after first insert ({pageCountAfterInsert}) + 2.");

        Assert.Equal(20, (int)await col2.CountAsync());
    }

    // ── DocumentDbContext.TruncateCollectionAsync<T> ──────────────────────────

    [Fact]
    public async Task Context_TruncateCollectionAsync_DeletesAllDocuments()
    {
        using var db = new TestDbContext(_dbPath);

        for (int i = 0; i < 5; i++)
            await db.Users.InsertAsync(new User { Name = $"User{i}", Age = i + 20 });

        var count = await db.TruncateCollectionAsync<User>();

        Assert.Equal(5, count);

        int remaining = 0;
        await foreach (var _ in db.Users.FindAllAsync())
            remaining++;
        Assert.Equal(0, remaining);
    }

    [Fact]
    public async Task Context_TruncateCollectionAsync_CollectionRemainsUsable()
    {
        using var db = new TestDbContext(_dbPath);

        await db.Users.InsertAsync(new User { Name = "Before", Age = 10 });
        await db.TruncateCollectionAsync<User>();

        // Insert after truncate
        var newUser = new User { Name = "After", Age = 20 };
        await db.Users.InsertAsync(newUser);

        int count = 0;
        await foreach (var _ in db.Users.FindAllAsync())
            count++;
        Assert.Equal(1, count);
    }

    // ── DocumentDbContext.DropCollectionAsync<T> ──────────────────────────────

    [Fact]
    public async Task Context_DropCollectionAsync_ProxyThrowsOnAnyAccess()
    {
        using var db = new TestDbContext(_dbPath);

        await db.Users.InsertAsync(new User { Name = "WillBeDropped", Age = 42 });

        await db.DropCollectionAsync<User>();

        // After drop, Set<User>() should return a proxy that throws on access.
        var proxy = db.Set<User>();
        Assert.NotNull(proxy);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await proxy.InsertAsync(new User { Name = "Fail" }));
        Assert.Contains("dropped", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Context_DropCollectionAsync_TruncateAfterDropThrows()
    {
        using var db = new TestDbContext(_dbPath);
        await db.Users.InsertAsync(new User { Name = "WillBeDropped", Age = 42 });

        await db.DropCollectionAsync<User>();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => db.TruncateCollectionAsync<User>());
    }

    [Fact]
    public async Task Context_DropCollectionAsync_MetadataRemovedFromStorage()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            await db.Users.InsertAsync(new User { Name = "Temp", Age = 1 });
            await db.DropCollectionAsync<User>();
        }

        // Verify that after reopening the DB, the old data is gone.
        // (Users is re-created by InitializeCollections on open, but its data should be gone.)
        using var db2 = new TestDbContext(_dbPath);
        int count = 0;
        await foreach (var _ in db2.Users.FindAllAsync())
            count++;
        Assert.Equal(0, count);
    }

    // ── Orphan-collection pruning ──────────────────────────────────────────────

    [Fact]
    public async Task Context_Initialization_DropsOrphanCollections()
    {
        // Step 1: seed the database with a collection that is NOT registered
        // in TestDbContext ("legacy_col") alongside one that IS ("users").
        using (var engine = new BLiteEngine(_dbPath))
        {
            var legacy = engine.GetOrCreateCollection("legacy_col");
            var doc = legacy.CreateDocument(["_id", "v"], b => b.AddInt32("v", 1));
            await legacy.InsertAsync(doc);

            var users = engine.GetOrCreateCollection("users");
            var u = users.CreateDocument(["_id", "name"], b => b.AddString("name", "Alice"));
            await users.InsertAsync(u);
        }

        // Verify the orphan exists before we open the context.
        using (var engineCheck = new BLiteEngine(_dbPath))
        {
            Assert.Contains("legacy_col", engineCheck.ListCollections());
        }

        // Step 2: open TestDbContext (knows "users" and many others, but NOT "legacy_col").
        // The constructor must automatically drop "legacy_col" as an orphan.
        using (var db = new TestDbContext(_dbPath))
        {
            // The context's own collection must still be intact.
            int count = 0;
            await foreach (var _ in db.Users.FindAllAsync())
                count++;
            Assert.Equal(1, count);
        }

        // Step 3: reopen via raw engine and verify the orphan is gone.
        using (var engine2 = new BLiteEngine(_dbPath))
        {
            var collections = engine2.ListCollections();
            Assert.DoesNotContain("legacy_col", collections);
            Assert.Contains("users", collections);
        }
    }


    [Fact]
    public async Task Collection_TruncateAsync_DeletesAllDocuments()
    {
        using var db = new TestDbContext(_dbPath);

        for (int i = 0; i < 7; i++)
            await db.Users.InsertAsync(new User { Name = $"U{i}", Age = i });

        var deleted = await db.Users.TruncateAsync();

        Assert.Equal(7, deleted);

        int remaining = 0;
        await foreach (var _ in db.Users.FindAllAsync())
            remaining++;
        Assert.Equal(0, remaining);
    }

    [Fact]
    public async Task Collection_TruncateAsync_WithTransaction_DeletesAllDocuments()
    {
        using var db = new TestDbContext(_dbPath);

        for (int i = 0; i < 3; i++)
            await db.Users.InsertAsync(new User { Name = $"U{i}", Age = i });

        using var txn = await db.BeginTransactionAsync();
        var deleted = await db.Users.TruncateAsync(txn);
        await db.SaveChangesAsync(txn);

        Assert.Equal(3, deleted);

        int remaining = 0;
        await foreach (var _ in db.Users.FindAllAsync())
            remaining++;
        Assert.Equal(0, remaining);
    }
}
