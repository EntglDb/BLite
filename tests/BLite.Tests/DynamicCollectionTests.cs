using BLite.Bson;
using BLite.Core;

namespace BLite.Tests;

/// <summary>
/// Tests for <see cref="DynamicCollection"/> targeting mutation survivors not yet
/// covered by existing tests: basic CRUD, bulk operations, predicate queries,
/// secondary indexes, TimeSeries API, and persistence.
/// </summary>
public class DynamicCollectionTests : IDisposable
{
    private readonly string _dbPath;
    private BLiteEngine _engine;

    public DynamicCollectionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_dyncol_{Guid.NewGuid():N}.db");
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
        var col = _engine.GetOrCreateCollection("tmp_schema");
        return col.CreateDocument(["name", "age"], b => b
            .AddString("name", name)
            .AddInt32("age", age));
    }

    private BsonDocument MakeDocWithId(BsonId id, string name, int age)
    {
        var col = _engine.GetOrCreateCollection("tmp_schema");
        return col.CreateDocument(["_id", "name", "age"], b => b
            .AddId(id)
            .AddString("name", name)
            .AddInt32("age", age));
    }

    // ─── Name / IdType / IsTimeSeries properties ──────────────────────────────

    [Fact]
    public async Task Name_ReturnsCollectionName()
    {
        var col = _engine.GetOrCreateCollection("mycol");
        Assert.Equal("mycol", col.Name);
    }

    [Fact]
    public async Task IdType_DefaultsToObjectId()
    {
        var col = _engine.GetOrCreateCollection("mycol");
        Assert.Equal(BsonIdType.ObjectId, col.IdType);
    }

    [Fact]
    public async Task IsTimeSeries_DefaultsFalse()
    {
        var col = _engine.GetOrCreateCollection("mycol");
        Assert.False(col.IsTimeSeries);
    }

    // ─── Insert + FindById ────────────────────────────────────────────────────

    [Fact]
    public async Task Insert_AutoGenerates_ObjectId()
    {
        var col = _engine.GetOrCreateCollection("users");
        var doc = MakeDoc("Alice", 30);

        var id = await col.InsertAsync(doc);
        await _engine.CommitAsync();

        Assert.False(id.IsEmpty);
        Assert.Equal(BsonIdType.ObjectId, id.Type);
    }

    [Fact]
    public async Task Insert_ExplicitId_PreservesId()
    {
        var col = _engine.GetOrCreateCollection("users");
        var explicitId = new BsonId(ObjectId.NewObjectId());
        var doc = MakeDocWithId(explicitId, "Bob", 25);

        var returned = await col.InsertAsync(doc);
        await _engine.CommitAsync();

        Assert.Equal(explicitId, returned);
    }

    [Fact]
    public async Task FindById_Found_ReturnsDocument()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id = await col.InsertAsync(MakeDoc("Carol", 28));
        await _engine.CommitAsync();

        var found = await col.FindByIdAsync(id);

        Assert.NotNull(found);
        found.TryGetString("name", out var name);
        Assert.Equal("Carol", name);
    }

    [Fact]
    public async Task FindById_NotFound_ReturnsNull()
    {
        var col = _engine.GetOrCreateCollection("users");

        var found = await col.FindByIdAsync(new BsonId(ObjectId.NewObjectId()));

        Assert.Null(found);
    }

    // ─── FindAll + Count ──────────────────────────────────────────────────────

    [Fact]
    public async Task FindAll_ReturnsAllInsertedDocuments()
    {
        var col = _engine.GetOrCreateCollection("users");
        await col.InsertAsync(MakeDoc("Alice", 30));
        await col.InsertAsync(MakeDoc("Bob", 25));
        await col.InsertAsync(MakeDoc("Carol", 35));
        await _engine.CommitAsync();

        IEnumerable<BsonDocument> all = [];
        await foreach (var doc in col.FindAllAsync())
        {
            all = all.Append(doc);
        }

        Assert.Equal(3, all.Count());
    }

    [Fact]
    public async Task Count_ReturnsCorrectCount()
    {
        var col = _engine.GetOrCreateCollection("users");
        await col.InsertAsync(MakeDoc("Alice", 30));
        await col.InsertAsync(MakeDoc("Bob", 25));
        await _engine.CommitAsync();

        Assert.Equal(2, await col.CountAsync());
    }

    [Fact]
    public async Task Count_EmptyCollection_ReturnsZero()
    {
        var col = _engine.GetOrCreateCollection("empty");
        Assert.Equal(0, await col.CountAsync());
    }

    // ─── UpdateAsync ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task Update_ReplacesDocument()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id = await col.InsertAsync(MakeDoc("Alice", 30));
        await _engine.CommitAsync();

        var updated = MakeDocWithId(id, "Alice", 31);
        var result = await col.UpdateAsync(id, updated);
        await _engine.CommitAsync();

        Assert.True(result);
        var found = await col.FindByIdAsync(id);
        Assert.NotNull(found);
        found.TryGetInt32("age", out int age);
        Assert.Equal(31, age);
    }

    [Fact]
    public async Task Update_NonExistent_ReturnsFalse()
    {
        var col = _engine.GetOrCreateCollection("users");
        var ghostId = new BsonId(ObjectId.NewObjectId());
        var doc = MakeDocWithId(ghostId, "Ghost", 99);

        var result = await col.UpdateAsync(ghostId, doc);

        Assert.False(result);
    }

    // ─── Delete ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task Delete_RemovesDocument()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id = await col.InsertAsync(MakeDoc("Alice", 30));
        await _engine.CommitAsync();

        var removed = await col.DeleteAsync(id);
        await _engine.CommitAsync();

        Assert.True(removed);
        Assert.Null(await col.FindByIdAsync(id));
    }

    [Fact]
    public async Task Delete_NonExistent_ReturnsFalse()
    {
        var col = _engine.GetOrCreateCollection("users");
        var ghostId = new BsonId(ObjectId.NewObjectId());

        var result = await col.DeleteAsync(ghostId);

        Assert.False(result);
    }

    // ─── InsertBulk ───────────────────────────────────────────────────────────

    [Fact]
    public async Task InsertBulk_ReturnsCorrectIdsAndAllDocumentsSaved()
    {
        var col = _engine.GetOrCreateCollection("users");
        var docs = Enumerable.Range(1, 5).Select(i => MakeDoc($"User{i}", i * 10)).ToList();

        var ids = await col.InsertBulkAsync(docs);
        await _engine.CommitAsync();

        Assert.Equal(5, ids.Count);
        Assert.All(ids, id => Assert.False(id.IsEmpty));
        Assert.Equal(5, await col.CountAsync());
    }

    [Fact]
    public async Task InsertBulkAsync_ReturnsCorrectIdsAndAllDocumentsSaved()
    {
        var col = _engine.GetOrCreateCollection("users");
        var docs = Enumerable.Range(1, 4).Select(i => MakeDoc($"Async{i}", i * 5)).ToList();

        var ids = await col.InsertBulkAsync(docs);
        await _engine.CommitAsync();

        Assert.Equal(4, ids.Count);
        Assert.Equal(4, await col.CountAsync());
    }

    // ─── UpdateBulk ───────────────────────────────────────────────────────────

    [Fact]
    public async Task UpdateBulk_UpdatesMultipleDocuments()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id1 = await col.InsertAsync(MakeDoc("Alice", 30));
        var id2 = await col.InsertAsync(MakeDoc("Bob", 25));
        await _engine.CommitAsync();

        var updates = new[]
        {
            (id1, MakeDocWithId(id1, "Alice", 31)),
            (id2, MakeDocWithId(id2, "Bob", 26))
        };
        var count = await col.UpdateBulkAsync(updates);
        await _engine.CommitAsync();

        Assert.Equal(2, count);
        var doc1 = await col.FindByIdAsync(id1);
        var doc2 = await col.FindByIdAsync(id2);
        doc1!.TryGetInt32("age", out int age1);
        doc2!.TryGetInt32("age", out int age2);
        Assert.Equal(31, age1);
        Assert.Equal(26, age2);
    }

    [Fact]
    public async Task UpdateBulkAsync_UpdatesMultipleDocuments()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id1 = await col.InsertAsync(MakeDoc("Alice", 30));
        var id2 = await col.InsertAsync(MakeDoc("Bob", 25));
        await _engine.CommitAsync();

        var updates = new[]
        {
            (id1, MakeDocWithId(id1, "Alice", 99)),
            (id2, MakeDocWithId(id2, "Bob", 88))
        };
        var count = await col.UpdateBulkAsync(updates);
        await _engine.CommitAsync();

        Assert.Equal(2, count);
        var doc1 = await col.FindByIdAsync(id1);
        doc1!.TryGetInt32("age", out int age1);
        Assert.Equal(99, age1);
    }

    // ─── DeleteBulk ───────────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteBulk_DeletesMultipleDocuments()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id1 = await col.InsertAsync(MakeDoc("Alice", 30));
        var id2 = await col.InsertAsync(MakeDoc("Bob", 25));
        var id3 = await col.InsertAsync(MakeDoc("Carol", 35));
        await _engine.CommitAsync();

        var deleted = await col.DeleteBulkAsync([id1, id2]);
        await _engine.CommitAsync();

        Assert.Equal(2, deleted);
        Assert.Equal(1, await col.CountAsync());
    }

    [Fact]
    public async Task DeleteBulkAsync_DeletesMultipleDocuments()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id1 = await col.InsertAsync(MakeDoc("Alice", 30));
        var id2 = await col.InsertAsync(MakeDoc("Bob", 25));
        await _engine.CommitAsync();

        var deleted = await col.DeleteBulkAsync([id1, id2]);
        await _engine.CommitAsync();

        Assert.Equal(2, deleted);
        Assert.Equal(0, await col.CountAsync());
    }

    // ─── Find + FindAsync predicate ───────────────────────────────────────────

    [Fact]
    public async Task Find_Predicate_FiltersCorrectly()
    {
        var col = _engine.GetOrCreateCollection("users");
        await col.InsertAsync(MakeDoc("Alice", 30));
        await col.InsertAsync(MakeDoc("Bob", 25));
        await col.InsertAsync(MakeDoc("Carol", 35));
        await _engine.CommitAsync();

        var results = new List<BsonDocument>();
        await foreach (var doc in col.FindAsync(d =>
        {
            d.TryGetInt32("age", out int age);
            return age >= 30;
        }))
            results.Add(doc);

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task FindAsync_Predicate_FiltersCorrectly()
    {
        var col = _engine.GetOrCreateCollection("users");
        await col.InsertAsync(MakeDoc("Alice", 30));
        await col.InsertAsync(MakeDoc("Bob", 25));
        await col.InsertAsync(MakeDoc("Carol", 35));
        await _engine.CommitAsync();

        var results = new List<BsonDocument>();
        await foreach (var doc in col.FindAsync(d =>
        {
            d.TryGetInt32("age", out int age);
            return age > 28;
        }))
        {
            results.Add(doc);
        }

        Assert.Equal(2, results.Count);
    }

    // ─── Scan predicate ───────────────────────────────────────────────────────

    [Fact]
    public async Task Scan_Predicate_ReturnsAllWhenAlwaysTrue()
    {
        var col = _engine.GetOrCreateCollection("users");
        await col.InsertAsync(MakeDoc("Alice", 30));
        await col.InsertAsync(MakeDoc("Bob", 25));
        await _engine.CommitAsync();

        var results = await col.ScanAsync((BsonReaderPredicate)(_ => true)).ToListAsync();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Scan_Predicate_ReturnsNoneWhenAlwaysFalse()
    {
        var col = _engine.GetOrCreateCollection("users");
        await col.InsertAsync(MakeDoc("Alice", 30));
        await _engine.CommitAsync();

        var results = await col.ScanAsync((BsonReaderPredicate)(_ => false)).ToListAsync();

        Assert.Empty(results);
    }

    // ─── CreateIndexAsync + QueryIndex ─────────────────────────────────────────────

    [Fact]
    public async Task CreateIndex_ThenQueryByRange_ReturnsMatchingDocuments()
    {
        var col = _engine.GetOrCreateCollection("users");
        await _engine.GetOrCreateCollection("users").CreateIndexAsync("age", name: "idx_age");
        await col.InsertAsync(MakeDoc("Alice", 30));
        await col.InsertAsync(MakeDoc("Bob", 25));
        await col.InsertAsync(MakeDoc("Carol", 35));
        await _engine.CommitAsync();

        var results = await col.QueryIndexAsync("idx_age", 25, 30).ToListAsync();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task QueryIndex_Nonexistent_ThrowsArgumentException()
    {
        var col = _engine.GetOrCreateCollection("users");

        await Assert.ThrowsAsync<ArgumentException>(async () => await col.QueryIndexAsync("ghost_idx", null, null).ToListAsync());
    }

    [Fact]
    public async Task QueryIndex_VectorIndex_ThrowsInvalidOperationException()
    {
        var col = _engine.GetOrCreateCollection("vectors");
        await col.CreateVectorIndexAsync("embedding", dimensions: 4, name: "idx_vec");

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await col.QueryIndexAsync("idx_vec", null, null).ToListAsync());
    }

    [Fact]
    public async Task ListIndexes_ContainsCreatedIndex()
    {
        var col = _engine.GetOrCreateCollection("users");
        await col.CreateIndexAsync("age", name: "idx_age");

        Assert.Contains("idx_age", col.ListIndexes());
    }

    [Fact]
    public async Task DropIndex_CreatedIndex_ReturnsTrue()
    {
        var col = _engine.GetOrCreateCollection("users");
        await col.CreateIndexAsync("age", name: "idx_age");

        var dropped = col.DropIndex("idx_age");

        Assert.True(dropped);
        Assert.DoesNotContain("idx_age", col.ListIndexes());
    }

    [Fact]
    public async Task DropIndex_NonExistent_ReturnsFalse()
    {
        var col = _engine.GetOrCreateCollection("users");

        var result = col.DropIndex("ghost_idx");

        Assert.False(result);
    }

    // ─── GetTimeSeriesConfig ──────────────────────────────────────────────────

    [Fact]
    public async Task GetTimeSeriesConfig_OnNonTimeSeries_ReturnsDefaults()
    {
        var col = _engine.GetOrCreateCollection("users");

        var (retention, ttlField) = col.GetTimeSeriesConfig();

        Assert.Equal(0L, retention);
        Assert.Null(ttlField);
    }

    // ─── SetTimeSeries ────────────────────────────────────────────────────────

    [Fact]
    public async Task SetTimeSeries_SetsIsTimeSeriesTrue()
    {
        var col = _engine.GetOrCreateCollection("sensors");
        col.SetTimeSeries("timestamp", TimeSpan.FromDays(7));

        Assert.True(col.IsTimeSeries);
    }

    [Fact]
    public async Task SetTimeSeries_GetTimeSeriesConfig_ReturnsConfiguredValues()
    {
        var col = _engine.GetOrCreateCollection("sensors");
        col.SetTimeSeries("ts", TimeSpan.FromHours(24));

        var (retention, ttlField) = col.GetTimeSeriesConfig();

        Assert.Equal("ts", ttlField);
        Assert.True(retention > 0);
    }

    // ─── ForcePruneAsync ───────────────────────────────────────────────────────────

    [Fact]
    public async Task ForcePrune_OnNonTimeSeries_ThrowsInvalidOperationException()
    {
        var col = _engine.GetOrCreateCollection("users");

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await col.ForcePruneAsync());
    }

    // ─── CreateDocument ───────────────────────────────────────────────────────

    [Fact]
    public async Task CreateDocument_RegistersKeys_CanBeInserted()
    {
        var col = _engine.GetOrCreateCollection("products");
        var doc = col.CreateDocument(["sku", "price"], b => b
            .AddString("sku", "ABC-123")
            .AddInt32("price", 99));

        var id = await col.InsertAsync(doc);
        await _engine.CommitAsync();

        Assert.False(id.IsEmpty);
        Assert.Equal(1, await col.CountAsync());
    }

    // ─── Persistence ─────────────────────────────────────────────────────────

    [Fact]
    public async Task Persistence_InsertThenReopen_FindByIdStillWorks()
    {
        BsonId savedId;
        {
            var col = _engine.GetOrCreateCollection("users");
            savedId = await col.InsertAsync(MakeDoc("Persistent", 42));
            await _engine.CommitAsync();
            _engine.Dispose();
        }

        using var engine2 = new BLiteEngine(_dbPath);
        var col2 = engine2.GetOrCreateCollection("users");
        var doc = await col2.FindByIdAsync(savedId);

        Assert.NotNull(doc);
        doc.TryGetString("name", out var name);
        Assert.Equal("Persistent", name);

        _engine = engine2; // prevent double-dispose in Dispose()
    }

    [Fact]
    public async Task Persistence_BulkInsert_DataSurvivesRestart()
    {
        {
            var col = _engine.GetOrCreateCollection("logs");
            await col.InsertBulkAsync(Enumerable.Range(1, 10).Select(i => MakeDoc($"log{i}", i)));
            await _engine.CommitAsync();
            _engine.Dispose();
        }

        using var engine2 = new BLiteEngine(_dbPath);
        var col2 = engine2.GetOrCreateCollection("logs");
        Assert.Equal(10, await col2.CountAsync());

        _engine = engine2;
    }

    // ─── Async FindAll ────────────────────────────────────────────────────────

    [Fact]
    public async Task FindAllAsync_ReturnsAllInsertedDocuments()
    {
        var col = _engine.GetOrCreateCollection("users");
        await col.InsertAsync(MakeDoc("A", 1));
        await col.InsertAsync(MakeDoc("B", 2));
        await col.InsertAsync(MakeDoc("C", 3));
        await _engine.CommitAsync();

        var results = new List<BsonDocument>();
        await foreach (var doc in col.FindAllAsync())
            results.Add(doc);

        Assert.Equal(3, results.Count);
    }

    // ─── Async UpdateAsync + Delete ────────────────────────────────────────────────

    [Fact]
    public async Task UpdateAsync_ReplacesDocument()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id = await col.InsertAsync(MakeDoc("Alice", 30));
        await _engine.CommitAsync();

        var updated = await col.UpdateAsync(id, MakeDocWithId(id, "Alice", 99));
        await _engine.CommitAsync();

        Assert.True(updated);
        var doc = await col.FindByIdAsync(id);
        doc!.TryGetInt32("age", out int age);
        Assert.Equal(99, age);
    }

    [Fact]
    public async Task DeleteAsync_RemovesDocument()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id = await col.InsertAsync(MakeDoc("Alice", 30));
        await _engine.CommitAsync();

        var removed = await col.DeleteAsync(id);
        await _engine.CommitAsync();

        Assert.True(removed);
        Assert.Null(await col.FindByIdAsync(id));
    }
}
