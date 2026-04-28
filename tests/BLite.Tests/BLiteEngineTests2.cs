using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;
using System.Text.Json;

namespace BLite.Tests;

/// <summary>
/// Additional tests for <see cref="BLiteEngine"/> targeting mutation survivors not yet
/// covered by BLiteEngineTests / BLiteEngineAdditionalTests: async convenience CRUD,
/// bulk operations, predicate queries, BackupAsync, KvStore, CreateDocument,
/// and disposal guards for the new surface area.
/// </summary>
public class BLiteEngineTests2 : IDisposable
{
    private readonly string _dbPath;
    private BLiteEngine _engine;

    public BLiteEngineTests2()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"eng2_{Guid.NewGuid():N}.db");
        _engine = new BLiteEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    private BsonDocument MakeDoc(string name, int age)
    {
        return _engine.CreateDocument(["name", "age"], b => b
            .AddString("name", name)
            .AddInt32("age", age));
    }

    // ─── CreateDocument ───────────────────────────────────────────────────────

    [Fact]
    public void CreateDocument_ReturnsDocumentWithRegisteredFields()
    {
        var doc = _engine.CreateDocument(["product", "price"], b => b
            .AddString("product", "Widget")
            .AddInt32("price", 9));

        Assert.NotNull(doc);
        doc.TryGetString("product", out var name);
        Assert.Equal("Widget", name);
    }

    // ─── Async Insert + FindById ──────────────────────────────────────────────

    [Fact]
    public async Task InsertAsync_ThenFindByIdAsync_Works()
    {
        var doc = MakeDoc("Alice", 30);
        var id = await _engine.InsertAsync("col", doc);

        var found = await _engine.FindByIdAsync("col", id);

        Assert.NotNull(found);
        found.TryGetString("name", out var name);
        Assert.Equal("Alice", name);
    }

    [Fact]
    public async Task FindByIdAsync_NotFound_ReturnsNull()
    {
        var found = await _engine.FindByIdAsync("col", new BsonId(ObjectId.NewObjectId()));
        Assert.Null(found);
    }

    // ─── Async UpdateAsync ─────────────────────────────────────────────────────────

    [Fact]
    public async Task UpdateAsync_ReplacesDocument()
    {
        var id = await _engine.InsertAsync("col", MakeDoc("Alice", 30));
        var updated = MakeDoc("Alice", 99);

        var result = await _engine.UpdateAsync("col", id, updated);

        Assert.True(result);
        var found = await _engine.FindByIdAsync("col", id);
        found!.TryGetInt32("age", out int age);
        Assert.Equal(99, age);
    }

    [Fact]
    public async Task UpdateAsync_NonExistent_ReturnsFalse()
    {
        var doc = MakeDoc("Ghost", 0);
        var result = await _engine.UpdateAsync("col", new BsonId(ObjectId.NewObjectId()), doc);
        Assert.False(result);
    }

    // ─── Async Delete ─────────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteAsync_RemovesDocument()
    {
        var id = await _engine.InsertAsync("col", MakeDoc("Alice", 30));

        var removed = await _engine.DeleteAsync("col", id);

        Assert.True(removed);
        var found = await _engine.FindByIdAsync("col", id);
        Assert.Null(found);
    }

    [Fact]
    public async Task DeleteAsync_NonExistent_ReturnsFalse()
    {
        var removed = await _engine.DeleteAsync("col", new BsonId(ObjectId.NewObjectId()));
        Assert.False(removed);
    }

    // ─── FindAllAsync ─────────────────────────────────────────────────────────

    [Fact]
    public async Task FindAllAsync_ReturnsAllDocuments()
    {
        await _engine.InsertAsync("col", MakeDoc("Alice", 30));
        await _engine.InsertAsync("col", MakeDoc("Bob", 25));
        await _engine.InsertAsync("col", MakeDoc("Carol", 35));

        var results = new List<BsonDocument>();
        await foreach (var doc in _engine.FindAllAsync("col"))
            results.Add(doc);

        Assert.Equal(3, results.Count);
    }

    // ─── InsertBulk + InsertBulkAsync ─────────────────────────────────────────

    [Fact]
    public async Task InsertBulk_ReturnsAllIds()
    {
        var docs = Enumerable.Range(1, 4).Select(i => MakeDoc($"User{i}", i * 10)).ToList();

        var ids = await _engine.InsertBulkAsync("col", docs);
        Assert.Equal(4, ids.Count);
        Assert.All(ids, id => Assert.False(id.IsEmpty));
    }

    [Fact]
    public async Task InsertBulkAsync_ReturnsAllIds()
    {
        var docs = Enumerable.Range(1, 5).Select(i => MakeDoc($"Async{i}", i)).ToList();

        var ids = await _engine.InsertBulkAsync("col", docs);

        Assert.Equal(5, ids.Count);
        Assert.All(ids, id => Assert.False(id.IsEmpty));
    }

    // ─── Find + FindAsync (predicate) ─────────────────────────────────────────

    [Fact]
    public async Task Find_Predicate_FiltersCorrectly()
    {
        await _engine.InsertAsync("col", MakeDoc("Alice", 30));
        await _engine.InsertAsync("col", MakeDoc("Bob", 25));
        await _engine.InsertAsync("col", MakeDoc("Carol", 35));

        var results = await _engine.FindAsync("col", doc =>
        {
            doc.TryGetInt32("age", out int age);
            return age >= 30;
        }).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task FindAsync_Predicate_FiltersCorrectly()
    {
        await _engine.InsertAsync("col", MakeDoc("Alice", 30));
        await _engine.InsertAsync("col", MakeDoc("Bob", 25));

        var results = new List<BsonDocument>();
        await foreach (var doc in _engine.FindAsync("col", d =>
        {
            d.TryGetInt32("age", out int age);
            return age > 25;
        }))
        {
            results.Add(doc);
        }

        Assert.Single(results);
    }

    // ─── UpdateBulk + UpdateBulkAsync ─────────────────────────────────────────

    [Fact]
    public async Task UpdateBulk_UpdatesMultipleDocuments()
    {
        var id1 = await _engine.InsertAsync("col", MakeDoc("Alice", 30));
        var id2 = await _engine.InsertAsync("col", MakeDoc("Bob", 25));

        var updates = new[]
        {
            (id1, MakeDoc("Alice", 99)),
            (id2, MakeDoc("Bob", 88))
        };
        var count = await _engine.UpdateBulkAsync("col", updates);

        Assert.Equal(2, count);
        var updatedDoc = await _engine.FindByIdAsync("col", id1);
        updatedDoc!.TryGetInt32("age", out int age1);
        Assert.Equal(99, age1);
    }

    [Fact]
    public async Task UpdateBulkAsync_UpdatesMultipleDocuments()
    {
        var id1 = await _engine.InsertAsync("col", MakeDoc("Alice", 30));
        var id2 = await _engine.InsertAsync("col", MakeDoc("Bob", 25));

        var updates = new[] { (id1, MakeDoc("Alice", 77)), (id2, MakeDoc("Bob", 66)) };
        var count = await _engine.UpdateBulkAsync("col", updates);

        Assert.Equal(2, count);
    }

    // ─── DeleteBulk + DeleteBulkAsync ─────────────────────────────────────────

    [Fact]
    public async Task DeleteBulk_DeletesMultipleDocuments()
    {
        var id1 = await _engine.InsertAsync("col", MakeDoc("Alice", 30));
        var id2 = await _engine.InsertAsync("col", MakeDoc("Bob", 25));
        var id3 = await _engine.InsertAsync("col", MakeDoc("Carol", 35));
        var deleted = await _engine.DeleteBulkAsync("col", [id1, id2]);

        Assert.Equal(2, deleted);
        // Only Carol remains
        Assert.Single(await _engine.FindAllAsync("col").ToListAsync());
    }

    [Fact]
    public async Task DeleteBulkAsync_DeletesMultipleDocuments()
    {
        var id1 = await _engine.InsertAsync("col", MakeDoc("Alice", 30));
        var id2 = await _engine.InsertAsync("col", MakeDoc("Bob", 25));

        var deleted = await _engine.DeleteBulkAsync("col", [id1, id2]);

        Assert.Equal(2, deleted);
        Assert.Empty(await _engine.FindAllAsync("col").ToListAsync());
    }

    // ─── BackupAsync ──────────────────────────────────────────────────────────

    [Fact]
    public async Task BackupAsync_WithOptions_ReturnsResult_Manifest_AndEvents()
    {
        var backupDir = Path.Combine(Path.GetTempPath(), $"backup_{Guid.NewGuid():N}");
        var pattern = Path.Combine(backupDir, "{databaseName}-{timestampUtc}.db");
        BackupStartedEvent started = default;
        BackupCompletedEvent completed = default;
        var startedCount = 0;
        var completedCount = 0;
        Action<BackupStartedEvent> startedHandler = evt => { started = evt; startedCount++; };
        Action<BackupCompletedEvent> completedHandler = evt => { completed = evt; completedCount++; };

        _engine.BackupStarted += startedHandler;
        _engine.BackupCompleted += completedHandler;

        try
        {
            await _engine.InsertAsync("col", MakeDoc("Alice", 30));

            var result = await _engine.BackupAsync(new BackupOptions
            {
                DestinationPathPattern = pattern
            });

            var backupPath = result.DestinationPath;
            var manifestPath = result.ManifestPath;
            var backupWal = Path.ChangeExtension(backupPath, ".wal");

            Assert.True(File.Exists(backupPath));
            Assert.True(File.Exists(backupWal));
            Assert.True(File.Exists(manifestPath));
            Assert.Equal(2, result.FileCount);
            Assert.True(result.TotalBytes > 0);
            Assert.True(result.Duration >= TimeSpan.Zero);
            Assert.Equal(1, startedCount);
            Assert.Equal(1, completedCount);
            Assert.Equal(backupPath, started.DestinationPath);
            Assert.Equal(backupPath, completed.Result.DestinationPath);
            Assert.Equal(manifestPath, completed.Result.ManifestPath);

            using (var manifest = JsonDocument.Parse(await File.ReadAllTextAsync(manifestPath)))
            {
                var files = manifest.RootElement.GetProperty("files");
                Assert.Equal(result.FileCount, files.GetArrayLength());
            }

            // Open the backup and verify data
            using var backup = new BLiteEngine(backupPath);
            var all = await backup.FindAllAsync("col").ToListAsync();
            Assert.Single(all);
            all[0].TryGetString("name", out var name);
            Assert.Equal("Alice", name);
        }
        finally
        {
            _engine.BackupStarted -= startedHandler;
            _engine.BackupCompleted -= completedHandler;
            if (Directory.Exists(backupDir))
                Directory.Delete(backupDir, recursive: true);
        }
    }

    [Fact]
    public async Task BackupAsync_MultiFile_BackupsCollectionAndIndexFiles()
    {
        var rootDir = Path.Combine(Path.GetTempPath(), $"multibackup_{Guid.NewGuid():N}");
        var sourceDir = Path.Combine(rootDir, "source");
        var backupDir = Path.Combine(rootDir, "backup");
        Directory.CreateDirectory(sourceDir);
        Directory.CreateDirectory(backupDir);

        var sourcePath = Path.Combine(sourceDir, "multi.db");
        var backupPath = Path.Combine(backupDir, "multi.db");
        var config = PageFileConfig.Server(sourcePath);

        try
        {
            using (var engine = new BLiteEngine(sourcePath, config))
            {
                var col = engine.GetOrCreateCollection("catalog", BsonIdType.Int32);

                for (int i = 1; i <= 3; i++)
                {
                    var doc = col.CreateDocument(["_id", "name", "age"], b => b
                        .AddId((BsonId)i)
                        .AddString("name", $"Item{i}")
                        .AddInt32("age", 20 + i));
                    await col.InsertAsync(doc);
                }

                await engine.CommitAsync();
                await col.CreateIndexAsync("age", "idx_age");
                await engine.CommitAsync();

                var result = await engine.BackupAsync(new BackupOptions { DestinationPath = backupPath });
                var backupConfig = PageFileConfig.Server(backupPath);

                Assert.Equal(5, result.FileCount);
                Assert.True(File.Exists(backupPath));
                Assert.True(File.Exists(backupConfig.WalPath!));
                Assert.True(File.Exists(backupConfig.IndexFilePath!));
                Assert.True(File.Exists(Path.Combine(backupConfig.CollectionDataDirectory!, ".slots")));
                Assert.True(File.Exists(Path.Combine(backupConfig.CollectionDataDirectory!, "catalog.db")));
                Assert.True(File.Exists(result.ManifestPath));

                using var manifest = JsonDocument.Parse(await File.ReadAllTextAsync(result.ManifestPath));
                var fileNames = manifest.RootElement.GetProperty("files")
                    .EnumerateArray()
                    .Select(x => x.GetProperty("name").GetString())
                    .ToList();

                Assert.Contains("multi.db", fileNames);
                Assert.Contains(Path.Combine("wal", "multi.wal"), fileNames);
                Assert.Contains("multi.idx", fileNames);
                Assert.Contains(Path.Combine("collections", "multi", ".slots"), fileNames);
                Assert.Contains(Path.Combine("collections", "multi", "catalog.db"), fileNames);
            }

            using var backup = new BLiteEngine(backupPath);
            var all = await backup.FindAllAsync("catalog").ToListAsync();
            Assert.Equal(3, all.Count);

            var backupCollection = backup.GetOrCreateCollection("catalog", BsonIdType.Int32);
            var indexedResults = await backupCollection.QueryIndexAsync("idx_age", 23, 23).ToListAsync();
            Assert.Single(indexedResults);
        }
        finally
        {
            if (Directory.Exists(rootDir))
                Directory.Delete(rootDir, recursive: true);
        }
    }

    // ─── KvStore ──────────────────────────────────────────────────────────────

    [Fact]
    public void KvStore_BeforeDispose_ReturnsNonNull()
    {
        Assert.NotNull(_engine.KvStore);
    }

    [Fact]
    public void KvStore_AfterDispose_ThrowsObjectDisposedException()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _ = _engine.KvStore);
    }

    // ─── After-dispose guards for new operations ──────────────────────────────

    [Fact]
    public async Task AfterDispose_Insert_Convenience_Throws()
    {
        var doc = MakeDoc("x", 1);
        _engine.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await _engine.InsertAsync("col", doc));
    }

    [Fact]
    public async Task AfterDispose_FindById_Convenience_Throws()
    {
        _engine.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await _engine.FindByIdAsync("col", new BsonId(ObjectId.NewObjectId())));
    }

    [Fact]
    public async Task AfterDispose_Update_Convenience_Throws()
    {
        var doc = MakeDoc("x", 1);
        _engine.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await _engine.UpdateAsync("col", new BsonId(ObjectId.NewObjectId()), doc));
    }

    [Fact]
    public async Task AfterDispose_Delete_Convenience_Throws()
    {
        _engine.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await _engine.DeleteAsync("col", new BsonId(ObjectId.NewObjectId())));
    }

    [Fact]
    public async Task AfterDispose_FindAll_Convenience_Throws()
    {
        _engine.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await _engine.FindAllAsync("col").ToListAsync());
    }

    [Fact]
    public async Task AfterDispose_Find_Convenience_Throws()
    {
        _engine.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await _engine.FindAsync("col", _ => true).ToListAsync());
    }

    [Fact]
    public async Task AfterDispose_InsertBulk_Throws()
    {
        var doc = MakeDoc("x", 1);
        _engine.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await _engine.InsertBulkAsync("col", [doc]));
    }

    [Fact]
    public async Task AfterDispose_UpdateBulk_Throws()
    {
        var id = new BsonId(ObjectId.NewObjectId());
        var doc = MakeDoc("x", 1);
        _engine.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await _engine.UpdateBulkAsync("col", [(id, doc)]));
    }

    [Fact]
    public async Task AfterDispose_DeleteBulk_Throws()
    {
        var id = new BsonId(ObjectId.NewObjectId());
        _engine.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await _engine.DeleteBulkAsync("col", [id]));
    }

    [Fact]
    public async Task AfterDispose_InsertAsync_Throws()
    {
        var doc = MakeDoc("x", 1);
        _engine.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await _engine.InsertAsync("col", doc));
    }

    [Fact]
    public async Task AfterDispose_FindByIdAsync_Throws()
    {
        _engine.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await _engine.FindByIdAsync("col", new BsonId(ObjectId.NewObjectId())));
    }

    [Fact]
    public async Task AfterDispose_UpdateAsync_Throws()
    {
        var doc = MakeDoc("x", 1);
        _engine.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await _engine.UpdateAsync("col", new BsonId(ObjectId.NewObjectId()), doc));
    }

    [Fact]
    public async Task AfterDispose_DeleteAsync_Throws()
    {
        _engine.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await _engine.DeleteAsync("col", new BsonId(ObjectId.NewObjectId())));
    }

    [Fact]
    public async Task AfterDispose_InsertBulkAsync_Throws()
    {
        var doc = MakeDoc("x", 1);
        _engine.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await _engine.InsertBulkAsync("col", [doc]));
    }

    [Fact]
    public async Task AfterDispose_BackupAsync_Throws()
    {
        _engine.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await _engine.BackupAsync(Path.GetTempFileName()));
    }

    [Fact]
    public void AfterDispose_CreateDocument_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() =>
            _engine.CreateDocument(["x"], b => b.AddString("x", "v")));
    }
}
