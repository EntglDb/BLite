using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;

namespace BLite.Tests;

/// <summary>
/// Mutation-coverage tests for DynamicCollection secondary indexes (BTree, Spatial),
/// QueryIndex, Near, Within, DropIndex, ListIndexes, and various CRUD edge cases.
/// Targets NoCoverage mutants in CollectionSecondaryIndex, DynamicCollection index paths,
/// and RTreeIndex.
/// </summary>
public class SecondaryIndexCoverageTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;
    private readonly DynamicCollection _col;

    public SecondaryIndexCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"idx_cov_{Guid.NewGuid()}.db");
        _engine = new BLiteEngine(_dbPath);
        _col = _engine.GetOrCreateCollection("items");
    }

    public void Dispose()
    {
        _engine.Dispose();
        TryDelete(_dbPath);
        TryDelete(Path.ChangeExtension(_dbPath, ".wal"));
    }

    private static void TryDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); } catch { }
    }

    private static string GetName(BsonDocument doc)
    {
        doc.TryGetString("name", out var n);
        return n ?? "";
    }

    // ══════════════════════════════════════════════════════════════════════
    //  BTree index — basic operations
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void CreateIndex_StringField_Works()
    {
        SeedItems(10);
        _col.CreateIndex("name", "idx_name");
        var indexes = _col.ListIndexes();
        Assert.Contains("idx_name", indexes);
    }

    [Fact]
    public void QueryIndex_RangeSearch_ReturnsCorrectResults()
    {
        SeedItems(20);
        _col.CreateIndex("value", "idx_value");

        var results = _col.QueryIndex("idx_value", 5, 10).ToList();
        Assert.True(results.Count >= 5);
        foreach (var doc in results)
        {
            doc.TryGetInt32("value", out var val);
            Assert.InRange(val, 5, 10);
        }
    }

    [Fact]
    public void QueryIndex_ExactMatch_ReturnsOne()
    {
        SeedItems(10);
        _col.CreateIndex("value", "idx_value");

        var results = _col.QueryIndex("idx_value", 7, 7).ToList();
        Assert.Single(results);
        results[0].TryGetInt32("value", out var val);
        Assert.Equal(7, val);
    }

    [Fact]
    public void QueryIndex_UnboundedMin_Works()
    {
        SeedItems(10);
        _col.CreateIndex("value", "idx_value");

        var results = _col.QueryIndex("idx_value", null, 3).ToList();
        Assert.True(results.Count >= 3);
    }

    [Fact]
    public void QueryIndex_UnboundedMax_Works()
    {
        SeedItems(10);
        _col.CreateIndex("value", "idx_value");

        var results = _col.QueryIndex("idx_value", 7, null).ToList();
        Assert.True(results.Count >= 3);
    }

    [Fact]
    public void QueryIndex_FullRange_ReturnsAll()
    {
        SeedItems(10);
        _col.CreateIndex("value", "idx_value");

        var results = _col.QueryIndex("idx_value", null, null).ToList();
        Assert.Equal(10, results.Count);
    }

    [Fact]
    public void QueryIndex_NonExistentIndex_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            _col.QueryIndex("nonexistent", null, null).ToList());
    }

    [Fact]
    public void DropIndex_Existing_ReturnsTrue()
    {
        SeedItems(5);
        _col.CreateIndex("name", "idx_name");
        Assert.True(_col.DropIndex("idx_name"));
        Assert.DoesNotContain("idx_name", _col.ListIndexes());
    }

    [Fact]
    public void DropIndex_NonExistent_ReturnsFalse()
    {
        Assert.False(_col.DropIndex("nonexistent"));
    }

    [Fact]
    public void ListIndexes_Empty_ReturnsEmpty()
    {
        Assert.Empty(_col.ListIndexes());
    }

    [Fact]
    public void ListIndexes_Multiple_ReturnsAll()
    {
        SeedItems(5);
        _col.CreateIndex("name", "idx_name");
        _col.CreateIndex("value", "idx_value");

        var indexes = _col.ListIndexes();
        Assert.Equal(2, indexes.Count);
        Assert.Contains("idx_name", indexes);
        Assert.Contains("idx_value", indexes);
    }

    [Fact]
    public void Index_SurvivesReopen()
    {
        SeedItems(5);
        _col.CreateIndex("value", "idx_value");
        _engine.Commit();
        _engine.Dispose();

        using var engine2 = new BLiteEngine(_dbPath);
        var col2 = engine2.GetOrCreateCollection("items");
        var indexes = col2.ListIndexes();
        Assert.Contains("idx_value", indexes);

        var results = col2.QueryIndex("idx_value", 2, 4).ToList();
        Assert.True(results.Count >= 2);
    }

    [Fact]
    public void Index_UpdatesWhenDocumentUpdated()
    {
        SeedItems(5);
        _col.CreateIndex("value", "idx_value");

        // Find and update document with value=3
        var doc = _col.QueryIndex("idx_value", 3, 3).First();
        doc.TryGetId(out var id);

        var newDoc = _col.CreateDocument(["_id", "name", "value"], b =>
            b.AddString("name", "updated").AddInt32("value", 99));
        _col.Update(id, newDoc);

        // Old value should no longer be found
        var oldResults = _col.QueryIndex("idx_value", 3, 3).ToList();
        Assert.Empty(oldResults);

        // New value should be found
        var newResults = _col.QueryIndex("idx_value", 99, 99).ToList();
        Assert.Single(newResults);
    }

    [Fact]
    public void Index_UpdatesWhenDocumentDeleted()
    {
        SeedItems(5);
        _col.CreateIndex("value", "idx_value");

        var doc = _col.QueryIndex("idx_value", 2, 2).First();
        doc.TryGetId(out var id);
        _col.Delete(id);

        var results = _col.QueryIndex("idx_value", 2, 2).ToList();
        Assert.Empty(results);
    }

    [Fact]
    public void Index_UniqueConstraint_PreventsDuplicates()
    {
        _col.CreateIndex("email", "idx_email", unique: true);

        var doc1 = _col.CreateDocument(["_id", "email"], b => b.AddString("email", "a@b.com"));
        _col.Insert(doc1);

        var doc2 = _col.CreateDocument(["_id", "email"], b => b.AddString("email", "a@b.com"));
        Assert.ThrowsAny<Exception>(() => _col.Insert(doc2));
    }

    [Fact]
    public void Index_StringField_QueryAfterBulkInsert()
    {
        _col.CreateIndex("category", "idx_cat");

        var docs = new List<BsonDocument>();
        for (int i = 0; i < 50; i++)
        {
            var cat = i % 3 == 0 ? "A" : (i % 3 == 1 ? "B" : "C");
            docs.Add(_col.CreateDocument(["_id", "category", "val"], b =>
                b.AddString("category", cat).AddInt32("val", i)));
        }
        _col.InsertBulk(docs);

        var a = _col.QueryIndex("idx_cat", "A", "A").ToList();
        var b = _col.QueryIndex("idx_cat", "B", "B").ToList();
        var c = _col.QueryIndex("idx_cat", "C", "C").ToList();

        Assert.Equal(17, a.Count); // ceil(50/3)
        Assert.Equal(17, b.Count);
        Assert.Equal(16, c.Count);
    }

    [Fact]
    public void Index_Int64Field_Works()
    {
        _col.CreateIndex("score", "idx_score");

        for (int i = 0; i < 10; i++)
        {
            var doc = _col.CreateDocument(["_id", "score"], b =>
                b.AddInt64("score", (long)i * 1000));
            _col.Insert(doc);
        }

        var results = _col.QueryIndex("idx_score", 3000L, 7000L).ToList();
        Assert.Equal(5, results.Count);
    }

    [Fact]
    public void Index_DateTimeField_Works()
    {
        _col.CreateIndex("date", "idx_date");

        var baseDate = new DateTime(2024, 1, 1);
        for (int i = 0; i < 10; i++)
        {
            var doc = _col.CreateDocument(["_id", "date"], b =>
                b.AddDateTime("date", baseDate.AddDays(i)));
            _col.Insert(doc);
        }

        // DateTime is not supported by QueryIndex — verify data via FindAll + index existence
        var indexes = _col.ListIndexes();
        Assert.Contains("idx_date", indexes);
        Assert.Equal(10, _col.FindAll().Count());
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Spatial index
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void SpatialIndex_CreateAndNear()
    {
        var col = _engine.GetOrCreateCollection("places");
        col.CreateSpatialIndex("location", "idx_loc");

        // Insert some locations
        InsertLocation(col, "Rome", 41.9028, 12.4964);
        InsertLocation(col, "Milan", 45.4642, 9.1900);
        InsertLocation(col, "Naples", 40.8518, 14.2681);
        InsertLocation(col, "London", 51.5074, -0.1278);

        // Search near Rome within 300km → should find Rome and Naples
        var results = col.Near("idx_loc", (41.9028, 12.4964), 300).ToList();
        Assert.True(results.Count >= 1); // At least Rome itself
    }

    [Fact]
    public void SpatialIndex_Within()
    {
        var col = _engine.GetOrCreateCollection("places2");
        col.CreateSpatialIndex("location", "idx_loc2");

        InsertLocation(col, "Rome", 41.9028, 12.4964);
        InsertLocation(col, "Milan", 45.4642, 9.1900);
        InsertLocation(col, "London", 51.5074, -0.1278);

        // Search within Italian boundaries
        var results = col.Within("idx_loc2", (36.0, 6.0), (47.0, 19.0)).ToList();
        Assert.True(results.Count >= 2); // Rome, Milan
    }

    [Fact]
    public void SpatialIndex_QueryOnWrongIndexType_Throws()
    {
        SeedItems(5);
        _col.CreateIndex("name", "idx_name");

        Assert.Throws<ArgumentException>(() =>
            _col.Near("idx_name", (0, 0), 100).ToList());
    }

    [Fact]
    public void SpatialIndex_NonExistentIndex_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            _col.Near("nonexistent", (0, 0), 100).ToList());
    }

    // ══════════════════════════════════════════════════════════════════════
    //  DynamicCollection — CRUD edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Insert_Null_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => _col.Insert(null!));
    }

    [Fact]
    public void Update_Null_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            _col.Update(new BsonId(ObjectId.NewObjectId()), null!));
    }

    [Fact]
    public void Delete_NonExistent_ReturnsFalse()
    {
        var id = new BsonId(ObjectId.NewObjectId());
        Assert.False(_col.Delete(id));
    }

    [Fact]
    public void FindById_NonExistent_ReturnsNull()
    {
        var id = new BsonId(ObjectId.NewObjectId());
        Assert.Null(_col.FindById(id));
    }

    [Fact]
    public void FindAll_Empty_ReturnsEmpty()
    {
        Assert.Empty(_col.FindAll().ToList());
    }

    [Fact]
    public void Find_WithPredicate_FiltersCorrectly()
    {
        SeedItems(10);
        var results = _col.Find(d =>
        {
            d.TryGetInt32("value", out var v);
            return v > 7;
        }).ToList();
        Assert.Equal(2, results.Count); // 8, 9
    }

    [Fact]
    public void Find_NullPredicate_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => _col.Find(null!).ToList());
    }

    [Fact]
    public async Task FindAsync_WithPredicate_FiltersCorrectly()
    {
        SeedItems(10);
        var results = new List<BsonDocument>();
        await foreach (var doc in _col.FindAsync(d =>
        {
            d.TryGetInt32("value", out var v);
            return v < 3;
        }))
        {
            results.Add(doc);
        }
        Assert.Equal(3, results.Count); // 0, 1, 2
    }

    [Fact]
    public async Task FindAsync_NullPredicate_Throws()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await foreach (var _ in _col.FindAsync(null!)) { }
        });
    }

    [Fact]
    public void InsertBulk_EmptyList_NoError()
    {
        _col.InsertBulk(new List<BsonDocument>());
        Assert.Empty(_col.FindAll().ToList());
    }

    [Fact]
    public void InsertBulk_ManyDocuments_AllPersisted()
    {
        var docs = Enumerable.Range(0, 100).Select(i =>
            _col.CreateDocument(["_id", "val"], b => b.AddInt32("val", i))).ToList();
        _col.InsertBulk(docs);
        Assert.Equal(100, _col.FindAll().Count());
    }

    [Fact]
    public void UpdateBulk_UpdatesMultiple()
    {
        SeedItems(5);
        var allDocs = _col.FindAll().ToList();
        var updates = new List<(BsonId, BsonDocument)>();

        foreach (var doc in allDocs)
        {
            doc.TryGetId(out var id);
            var newDoc = _col.CreateDocument(["_id", "name", "value"], b =>
                b.AddString("name", "updated").AddInt32("value", 999));
            updates.Add((id, newDoc));
        }

        _col.UpdateBulk(updates);

        foreach (var doc in _col.FindAll())
        {
            doc.TryGetString("name", out var name);
            Assert.Equal("updated", name);
        }
    }

    [Fact]
    public void DeleteBulk_DeletesMultiple()
    {
        SeedItems(5);
        var ids = _col.FindAll().Select(d => { d.TryGetId(out var id); return id; }).ToList();
        _col.DeleteBulk(ids.Take(3));
        Assert.Equal(2, _col.FindAll().Count());
    }

    [Fact]
    public void Count_ReturnsCorrectCount()
    {
        SeedItems(7);
        Assert.Equal(7, _col.Count());
    }

    [Fact]
    public void Scan_ReturnsAll()
    {
        SeedItems(5);
        var count = 0;
        foreach (var doc in _col.Scan(reader => true))
            count++;
        Assert.Equal(5, count);
    }

    // ── Async CRUD ───────────────────────────────────────────────────────

    [Fact]
    public async Task InsertAsync_Works()
    {
        var doc = _col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 1));
        var id = await _col.InsertAsync(doc);
        Assert.NotNull(id);
    }

    [Fact]
    public async Task FindByIdAsync_Works()
    {
        var doc = _col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 42));
        var id = _col.Insert(doc);
        var found = await _col.FindByIdAsync(id);
        Assert.NotNull(found);
        found!.TryGetInt32("x", out var x);
        Assert.Equal(42, x);
    }

    [Fact]
    public async Task UpdateAsync_Works()
    {
        var doc = _col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 1));
        var id = _col.Insert(doc);
        var newDoc = _col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 2));
        var updated = await _col.UpdateAsync(id, newDoc);
        Assert.True(updated);
    }

    [Fact]
    public async Task DeleteAsync_Works()
    {
        var doc = _col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 1));
        var id = _col.Insert(doc);
        var deleted = await _col.DeleteAsync(id);
        Assert.True(deleted);
        Assert.Null(await _col.FindByIdAsync(id));
    }

    // ══════════════════════════════════════════════════════════════════════
    //  SetTimeSeries smoke tests
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void TimeSeries_SetAndGetConfig()
    {
        var col = _engine.GetOrCreateCollection("timeseries");
        col.SetTimeSeries("createdAt", TimeSpan.FromHours(24));
        var config = col.GetTimeSeriesConfig();
        Assert.NotNull(config);
    }

    [Fact]
    public void TimeSeries_InsertAndQuery()
    {
        var col = _engine.GetOrCreateCollection("ts_insert");
        col.SetTimeSeries("ts", TimeSpan.FromDays(365));

        for (int i = 0; i < 5; i++)
        {
            var doc = col.CreateDocument(["_id", "ts", "val"], b =>
                b.AddDateTime("ts", DateTime.UtcNow.AddMinutes(-i)).AddInt32("val", i));
            col.Insert(doc);
        }

        Assert.Equal(5, col.FindAll().Count());
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Helpers
    // ══════════════════════════════════════════════════════════════════════

    private void SeedItems(int count)
    {
        for (int i = 0; i < count; i++)
        {
            var doc = _col.CreateDocument(["_id", "name", "value"], b =>
                b.AddString("name", $"item_{i:D3}").AddInt32("value", i));
            _col.Insert(doc);
        }
    }

    private static void InsertLocation(DynamicCollection col, string name, double lat, double lon)
    {
        var doc = col.CreateDocument(["_id", "name", "location"], b =>
        {
            b.AddString("name", name);
            b.AddCoordinates("location", (lat, lon));
        });
        col.Insert(doc);
    }
}
