using BLite.Bson;
using BLite.Core;

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

    // ─── Async Update ─────────────────────────────────────────────────────────

    [Fact]
    public async Task UpdateAsync_ReplacesDocument()
    {
        var id = _engine.Insert("col", MakeDoc("Alice", 30));
        var updated = MakeDoc("Alice", 99);

        var result = await _engine.UpdateAsync("col", id, updated);

        Assert.True(result);
        var found = _engine.FindById("col", id);
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
        var id = _engine.Insert("col", MakeDoc("Alice", 30));

        var removed = await _engine.DeleteAsync("col", id);

        Assert.True(removed);
        Assert.Null(_engine.FindById("col", id));
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
        _engine.Insert("col", MakeDoc("Alice", 30));
        _engine.Insert("col", MakeDoc("Bob", 25));
        _engine.Insert("col", MakeDoc("Carol", 35));

        var results = new List<BsonDocument>();
        await foreach (var doc in _engine.FindAllAsync("col"))
            results.Add(doc);

        Assert.Equal(3, results.Count);
    }

    // ─── InsertBulk + InsertBulkAsync ─────────────────────────────────────────

    [Fact]
    public void InsertBulk_ReturnsAllIds()
    {
        var docs = Enumerable.Range(1, 4).Select(i => MakeDoc($"User{i}", i * 10)).ToList();

        var ids = _engine.InsertBulk("col", docs);

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
    public void Find_Predicate_FiltersCorrectly()
    {
        _engine.Insert("col", MakeDoc("Alice", 30));
        _engine.Insert("col", MakeDoc("Bob", 25));
        _engine.Insert("col", MakeDoc("Carol", 35));

        var results = _engine.Find("col", doc =>
        {
            doc.TryGetInt32("age", out int age);
            return age >= 30;
        }).ToList();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task FindAsync_Predicate_FiltersCorrectly()
    {
        _engine.Insert("col", MakeDoc("Alice", 30));
        _engine.Insert("col", MakeDoc("Bob", 25));

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
    public void UpdateBulk_UpdatesMultipleDocuments()
    {
        var id1 = _engine.Insert("col", MakeDoc("Alice", 30));
        var id2 = _engine.Insert("col", MakeDoc("Bob", 25));

        var updates = new[]
        {
            (id1, MakeDoc("Alice", 99)),
            (id2, MakeDoc("Bob", 88))
        };
        var count = _engine.UpdateBulk("col", updates);

        Assert.Equal(2, count);
        _engine.FindById("col", id1)!.TryGetInt32("age", out int age1);
        Assert.Equal(99, age1);
    }

    [Fact]
    public async Task UpdateBulkAsync_UpdatesMultipleDocuments()
    {
        var id1 = _engine.Insert("col", MakeDoc("Alice", 30));
        var id2 = _engine.Insert("col", MakeDoc("Bob", 25));

        var updates = new[] { (id1, MakeDoc("Alice", 77)), (id2, MakeDoc("Bob", 66)) };
        var count = await _engine.UpdateBulkAsync("col", updates);

        Assert.Equal(2, count);
    }

    // ─── DeleteBulk + DeleteBulkAsync ─────────────────────────────────────────

    [Fact]
    public void DeleteBulk_DeletesMultipleDocuments()
    {
        var id1 = _engine.Insert("col", MakeDoc("Alice", 30));
        var id2 = _engine.Insert("col", MakeDoc("Bob", 25));
        var id3 = _engine.Insert("col", MakeDoc("Carol", 35));

        var deleted = _engine.DeleteBulk("col", [id1, id2]);

        Assert.Equal(2, deleted);
        // Only Carol remains
        Assert.Single(_engine.FindAll("col").ToList());
    }

    [Fact]
    public async Task DeleteBulkAsync_DeletesMultipleDocuments()
    {
        var id1 = _engine.Insert("col", MakeDoc("Alice", 30));
        var id2 = _engine.Insert("col", MakeDoc("Bob", 25));

        var deleted = await _engine.DeleteBulkAsync("col", [id1, id2]);

        Assert.Equal(2, deleted);
        Assert.Empty(_engine.FindAll("col").ToList());
    }

    // ─── BackupAsync ──────────────────────────────────────────────────────────

    [Fact]
    public async Task BackupAsync_CreatesRestorable_DatabaseFile()
    {
        var backupPath = Path.Combine(Path.GetTempPath(), $"backup_{Guid.NewGuid():N}.db");
        try
        {
            _engine.Insert("col", MakeDoc("Alice", 30));

            await _engine.BackupAsync(backupPath);

            Assert.True(File.Exists(backupPath));

            // Open the backup and verify data
            using var backup = new BLiteEngine(backupPath);
            var all = backup.FindAll("col").ToList();
            Assert.Single(all);
            all[0].TryGetString("name", out var name);
            Assert.Equal("Alice", name);
        }
        finally
        {
            if (File.Exists(backupPath)) File.Delete(backupPath);
            var backupWal = Path.ChangeExtension(backupPath, ".wal");
            if (File.Exists(backupWal)) File.Delete(backupWal);
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
    public void AfterDispose_Insert_Convenience_Throws()
    {
        var doc = MakeDoc("x", 1);
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.Insert("col", doc));
    }

    [Fact]
    public void AfterDispose_FindById_Convenience_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.FindById("col", new BsonId(ObjectId.NewObjectId())));
    }

    [Fact]
    public void AfterDispose_Update_Convenience_Throws()
    {
        var doc = MakeDoc("x", 1);
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.Update("col", new BsonId(ObjectId.NewObjectId()), doc));
    }

    [Fact]
    public void AfterDispose_Delete_Convenience_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.Delete("col", new BsonId(ObjectId.NewObjectId())));
    }

    [Fact]
    public void AfterDispose_FindAll_Convenience_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.FindAll("col").ToList());
    }

    [Fact]
    public void AfterDispose_Find_Convenience_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.Find("col", _ => true).ToList());
    }

    [Fact]
    public void AfterDispose_InsertBulk_Throws()
    {
        var doc = MakeDoc("x", 1);
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.InsertBulk("col", [doc]));
    }

    [Fact]
    public void AfterDispose_UpdateBulk_Throws()
    {
        var id = new BsonId(ObjectId.NewObjectId());
        var doc = MakeDoc("x", 1);
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.UpdateBulk("col", [(id, doc)]));
    }

    [Fact]
    public void AfterDispose_DeleteBulk_Throws()
    {
        var id = new BsonId(ObjectId.NewObjectId());
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.DeleteBulk("col", [id]));
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
