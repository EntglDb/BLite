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
    public void Name_ReturnsCollectionName()
    {
        var col = _engine.GetOrCreateCollection("mycol");
        Assert.Equal("mycol", col.Name);
    }

    [Fact]
    public void IdType_DefaultsToObjectId()
    {
        var col = _engine.GetOrCreateCollection("mycol");
        Assert.Equal(BsonIdType.ObjectId, col.IdType);
    }

    [Fact]
    public void IsTimeSeries_DefaultsFalse()
    {
        var col = _engine.GetOrCreateCollection("mycol");
        Assert.False(col.IsTimeSeries);
    }

    // ─── Insert + FindById ────────────────────────────────────────────────────

    [Fact]
    public void Insert_AutoGenerates_ObjectId()
    {
        var col = _engine.GetOrCreateCollection("users");
        var doc = MakeDoc("Alice", 30);

        var id = col.Insert(doc);
        _engine.Commit();

        Assert.False(id.IsEmpty);
        Assert.Equal(BsonIdType.ObjectId, id.Type);
    }

    [Fact]
    public void Insert_ExplicitId_PreservesId()
    {
        var col = _engine.GetOrCreateCollection("users");
        var explicitId = new BsonId(ObjectId.NewObjectId());
        var doc = MakeDocWithId(explicitId, "Bob", 25);

        var returned = col.Insert(doc);
        _engine.Commit();

        Assert.Equal(explicitId, returned);
    }

    [Fact]
    public void FindById_Found_ReturnsDocument()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id = col.Insert(MakeDoc("Carol", 28));
        _engine.Commit();

        var found = col.FindById(id);

        Assert.NotNull(found);
        found.TryGetString("name", out var name);
        Assert.Equal("Carol", name);
    }

    [Fact]
    public void FindById_NotFound_ReturnsNull()
    {
        var col = _engine.GetOrCreateCollection("users");

        var found = col.FindById(new BsonId(ObjectId.NewObjectId()));

        Assert.Null(found);
    }

    // ─── FindAll + Count ──────────────────────────────────────────────────────

    [Fact]
    public void FindAll_ReturnsAllInsertedDocuments()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.Insert(MakeDoc("Alice", 30));
        col.Insert(MakeDoc("Bob", 25));
        col.Insert(MakeDoc("Carol", 35));
        _engine.Commit();

        var all = col.FindAll().ToList();

        Assert.Equal(3, all.Count);
    }

    [Fact]
    public void Count_ReturnsCorrectCount()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.Insert(MakeDoc("Alice", 30));
        col.Insert(MakeDoc("Bob", 25));
        _engine.Commit();

        Assert.Equal(2, col.Count());
    }

    [Fact]
    public void Count_EmptyCollection_ReturnsZero()
    {
        var col = _engine.GetOrCreateCollection("empty");
        Assert.Equal(0, col.Count());
    }

    // ─── Update ───────────────────────────────────────────────────────────────

    [Fact]
    public void Update_ReplacesDocument()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id = col.Insert(MakeDoc("Alice", 30));
        _engine.Commit();

        var updated = MakeDocWithId(id, "Alice", 31);
        var result = col.Update(id, updated);
        _engine.Commit();

        Assert.True(result);
        var found = col.FindById(id);
        Assert.NotNull(found);
        found.TryGetInt32("age", out int age);
        Assert.Equal(31, age);
    }

    [Fact]
    public void Update_NonExistent_ReturnsFalse()
    {
        var col = _engine.GetOrCreateCollection("users");
        var ghostId = new BsonId(ObjectId.NewObjectId());
        var doc = MakeDocWithId(ghostId, "Ghost", 99);

        var result = col.Update(ghostId, doc);

        Assert.False(result);
    }

    // ─── Delete ───────────────────────────────────────────────────────────────

    [Fact]
    public void Delete_RemovesDocument()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id = col.Insert(MakeDoc("Alice", 30));
        _engine.Commit();

        var removed = col.Delete(id);
        _engine.Commit();

        Assert.True(removed);
        Assert.Null(col.FindById(id));
    }

    [Fact]
    public void Delete_NonExistent_ReturnsFalse()
    {
        var col = _engine.GetOrCreateCollection("users");
        var ghostId = new BsonId(ObjectId.NewObjectId());

        var result = col.Delete(ghostId);

        Assert.False(result);
    }

    // ─── InsertBulk ───────────────────────────────────────────────────────────

    [Fact]
    public void InsertBulk_ReturnsCorrectIdsAndAllDocumentsSaved()
    {
        var col = _engine.GetOrCreateCollection("users");
        var docs = Enumerable.Range(1, 5).Select(i => MakeDoc($"User{i}", i * 10)).ToList();

        var ids = col.InsertBulk(docs);
        _engine.Commit();

        Assert.Equal(5, ids.Count);
        Assert.All(ids, id => Assert.False(id.IsEmpty));
        Assert.Equal(5, col.Count());
    }

    [Fact]
    public async Task InsertBulkAsync_ReturnsCorrectIdsAndAllDocumentsSaved()
    {
        var col = _engine.GetOrCreateCollection("users");
        var docs = Enumerable.Range(1, 4).Select(i => MakeDoc($"Async{i}", i * 5)).ToList();

        var ids = await col.InsertBulkAsync(docs);
        _engine.Commit();

        Assert.Equal(4, ids.Count);
        Assert.Equal(4, col.Count());
    }

    // ─── UpdateBulk ───────────────────────────────────────────────────────────

    [Fact]
    public void UpdateBulk_UpdatesMultipleDocuments()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id1 = col.Insert(MakeDoc("Alice", 30));
        var id2 = col.Insert(MakeDoc("Bob", 25));
        _engine.Commit();

        var updates = new[]
        {
            (id1, MakeDocWithId(id1, "Alice", 31)),
            (id2, MakeDocWithId(id2, "Bob", 26))
        };
        var count = col.UpdateBulk(updates);
        _engine.Commit();

        Assert.Equal(2, count);
        col.FindById(id1)!.TryGetInt32("age", out int age1);
        col.FindById(id2)!.TryGetInt32("age", out int age2);
        Assert.Equal(31, age1);
        Assert.Equal(26, age2);
    }

    [Fact]
    public async Task UpdateBulkAsync_UpdatesMultipleDocuments()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id1 = col.Insert(MakeDoc("Alice", 30));
        var id2 = col.Insert(MakeDoc("Bob", 25));
        _engine.Commit();

        var updates = new[]
        {
            (id1, MakeDocWithId(id1, "Alice", 99)),
            (id2, MakeDocWithId(id2, "Bob", 88))
        };
        var count = await col.UpdateBulkAsync(updates);
        _engine.Commit();

        Assert.Equal(2, count);
        col.FindById(id1)!.TryGetInt32("age", out int age1);
        Assert.Equal(99, age1);
    }

    // ─── DeleteBulk ───────────────────────────────────────────────────────────

    [Fact]
    public void DeleteBulk_DeletesMultipleDocuments()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id1 = col.Insert(MakeDoc("Alice", 30));
        var id2 = col.Insert(MakeDoc("Bob", 25));
        var id3 = col.Insert(MakeDoc("Carol", 35));
        _engine.Commit();

        var deleted = col.DeleteBulk([id1, id2]);
        _engine.Commit();

        Assert.Equal(2, deleted);
        Assert.Equal(1, col.Count());
    }

    [Fact]
    public async Task DeleteBulkAsync_DeletesMultipleDocuments()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id1 = col.Insert(MakeDoc("Alice", 30));
        var id2 = col.Insert(MakeDoc("Bob", 25));
        _engine.Commit();

        var deleted = await col.DeleteBulkAsync([id1, id2]);
        _engine.Commit();

        Assert.Equal(2, deleted);
        Assert.Equal(0, col.Count());
    }

    // ─── Find + FindAsync predicate ───────────────────────────────────────────

    [Fact]
    public void Find_Predicate_FiltersCorrectly()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.Insert(MakeDoc("Alice", 30));
        col.Insert(MakeDoc("Bob", 25));
        col.Insert(MakeDoc("Carol", 35));
        _engine.Commit();

        var results = col.Find(doc =>
        {
            doc.TryGetInt32("age", out int age);
            return age >= 30;
        }).ToList();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task FindAsync_Predicate_FiltersCorrectly()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.Insert(MakeDoc("Alice", 30));
        col.Insert(MakeDoc("Bob", 25));
        col.Insert(MakeDoc("Carol", 35));
        _engine.Commit();

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
    public void Scan_Predicate_ReturnsAllWhenAlwaysTrue()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.Insert(MakeDoc("Alice", 30));
        col.Insert(MakeDoc("Bob", 25));
        _engine.Commit();

        var results = col.Scan((BsonReaderPredicate)(_ => true)).ToList();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public void Scan_Predicate_ReturnsNoneWhenAlwaysFalse()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.Insert(MakeDoc("Alice", 30));
        _engine.Commit();

        var results = col.Scan((BsonReaderPredicate)(_ => false)).ToList();

        Assert.Empty(results);
    }

    // ─── CreateIndex + QueryIndex ─────────────────────────────────────────────

    [Fact]
    public void CreateIndex_ThenQueryByRange_ReturnsMatchingDocuments()
    {
        var col = _engine.GetOrCreateCollection("users");
        _engine.GetOrCreateCollection("users").CreateIndex("age", name: "idx_age");
        col.Insert(MakeDoc("Alice", 30));
        col.Insert(MakeDoc("Bob", 25));
        col.Insert(MakeDoc("Carol", 35));
        _engine.Commit();

        var results = col.QueryIndex("idx_age", 25, 30).ToList();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public void QueryIndex_Nonexistent_ThrowsArgumentException()
    {
        var col = _engine.GetOrCreateCollection("users");

        Assert.Throws<ArgumentException>(() => col.QueryIndex("ghost_idx", null, null).ToList());
    }

    [Fact]
    public void QueryIndex_VectorIndex_ThrowsInvalidOperationException()
    {
        var col = _engine.GetOrCreateCollection("vectors");
        col.CreateVectorIndex("embedding", dimensions: 4, name: "idx_vec");

        Assert.Throws<InvalidOperationException>(() => col.QueryIndex("idx_vec", null, null).ToList());
    }

    [Fact]
    public void ListIndexes_ContainsCreatedIndex()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.CreateIndex("age", name: "idx_age");

        Assert.Contains("idx_age", col.ListIndexes());
    }

    [Fact]
    public void DropIndex_CreatedIndex_ReturnsTrue()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.CreateIndex("age", name: "idx_age");

        var dropped = col.DropIndex("idx_age");

        Assert.True(dropped);
        Assert.DoesNotContain("idx_age", col.ListIndexes());
    }

    [Fact]
    public void DropIndex_NonExistent_ReturnsFalse()
    {
        var col = _engine.GetOrCreateCollection("users");

        var result = col.DropIndex("ghost_idx");

        Assert.False(result);
    }

    // ─── GetTimeSeriesConfig ──────────────────────────────────────────────────

    [Fact]
    public void GetTimeSeriesConfig_OnNonTimeSeries_ReturnsDefaults()
    {
        var col = _engine.GetOrCreateCollection("users");

        var (retention, ttlField) = col.GetTimeSeriesConfig();

        Assert.Equal(0L, retention);
        Assert.Null(ttlField);
    }

    // ─── SetTimeSeries ────────────────────────────────────────────────────────

    [Fact]
    public void SetTimeSeries_SetsIsTimeSeriesTrue()
    {
        var col = _engine.GetOrCreateCollection("sensors");
        col.SetTimeSeries("timestamp", TimeSpan.FromDays(7));

        Assert.True(col.IsTimeSeries);
    }

    [Fact]
    public void SetTimeSeries_GetTimeSeriesConfig_ReturnsConfiguredValues()
    {
        var col = _engine.GetOrCreateCollection("sensors");
        col.SetTimeSeries("ts", TimeSpan.FromHours(24));

        var (retention, ttlField) = col.GetTimeSeriesConfig();

        Assert.Equal("ts", ttlField);
        Assert.True(retention > 0);
    }

    // ─── ForcePrune ───────────────────────────────────────────────────────────

    [Fact]
    public void ForcePrune_OnNonTimeSeries_ThrowsInvalidOperationException()
    {
        var col = _engine.GetOrCreateCollection("users");

        Assert.Throws<InvalidOperationException>(() => col.ForcePrune());
    }

    // ─── CreateDocument ───────────────────────────────────────────────────────

    [Fact]
    public void CreateDocument_RegistersKeys_CanBeInserted()
    {
        var col = _engine.GetOrCreateCollection("products");
        var doc = col.CreateDocument(["sku", "price"], b => b
            .AddString("sku", "ABC-123")
            .AddInt32("price", 99));

        var id = col.Insert(doc);
        _engine.Commit();

        Assert.False(id.IsEmpty);
        Assert.Equal(1, col.Count());
    }

    // ─── Persistence ─────────────────────────────────────────────────────────

    [Fact]
    public void Persistence_InsertThenReopen_FindByIdStillWorks()
    {
        BsonId savedId;
        {
            var col = _engine.GetOrCreateCollection("users");
            savedId = col.Insert(MakeDoc("Persistent", 42));
            _engine.Commit();
            _engine.Dispose();
        }

        using var engine2 = new BLiteEngine(_dbPath);
        var col2 = engine2.GetOrCreateCollection("users");
        var doc = col2.FindById(savedId);

        Assert.NotNull(doc);
        doc.TryGetString("name", out var name);
        Assert.Equal("Persistent", name);

        _engine = engine2; // prevent double-dispose in Dispose()
    }

    [Fact]
    public void Persistence_BulkInsert_DataSurvivesRestart()
    {
        {
            var col = _engine.GetOrCreateCollection("logs");
            col.InsertBulk(Enumerable.Range(1, 10).Select(i => MakeDoc($"log{i}", i)));
            _engine.Commit();
            _engine.Dispose();
        }

        using var engine2 = new BLiteEngine(_dbPath);
        var col2 = engine2.GetOrCreateCollection("logs");
        Assert.Equal(10, col2.Count());

        _engine = engine2;
    }

    // ─── Async FindAll ────────────────────────────────────────────────────────

    [Fact]
    public async Task FindAllAsync_ReturnsAllInsertedDocuments()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.Insert(MakeDoc("A", 1));
        col.Insert(MakeDoc("B", 2));
        col.Insert(MakeDoc("C", 3));
        _engine.Commit();

        var results = new List<BsonDocument>();
        await foreach (var doc in col.FindAllAsync())
            results.Add(doc);

        Assert.Equal(3, results.Count);
    }

    // ─── Async Update + Delete ────────────────────────────────────────────────

    [Fact]
    public async Task UpdateAsync_ReplacesDocument()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id = col.Insert(MakeDoc("Alice", 30));
        _engine.Commit();

        var updated = await col.UpdateAsync(id, MakeDocWithId(id, "Alice", 99));
        _engine.Commit();

        Assert.True(updated);
        col.FindById(id)!.TryGetInt32("age", out int age);
        Assert.Equal(99, age);
    }

    [Fact]
    public async Task DeleteAsync_RemovesDocument()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id = col.Insert(MakeDoc("Alice", 30));
        _engine.Commit();

        var removed = await col.DeleteAsync(id);
        _engine.Commit();

        Assert.True(removed);
        Assert.Null(col.FindById(id));
    }
}
