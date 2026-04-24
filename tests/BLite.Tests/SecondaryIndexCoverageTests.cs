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
    public async Task CreateIndex_StringField_Works()
    {
        await SeedItems(10);
        await _col.CreateIndexAsync("name", "idx_name");
        var indexes = _col.ListIndexes();
        Assert.Contains("idx_name", indexes);
    }

    [Fact]
    public async Task QueryIndex_RangeSearch_ReturnsCorrectResults()
    {
        await SeedItems(20);
        await _col.CreateIndexAsync("value", "idx_value");

        var results = await _col.QueryIndexAsync("idx_value", 5, 10).ToListAsync();
        Assert.True(results.Count >= 5);
        foreach (var doc in results)
        {
            doc.TryGetInt32("value", out var val);
            Assert.InRange(val, 5, 10);
        }
    }

    [Fact]
    public async Task QueryIndex_ExactMatch_ReturnsOne()
    {
        await SeedItems(10);
        await _col.CreateIndexAsync("value", "idx_value");

        var results = await _col.QueryIndexAsync("idx_value", 7, 7).ToListAsync();
        Assert.Single(results);
        results[0].TryGetInt32("value", out var val);
        Assert.Equal(7, val);
    }

    [Fact]
    public async Task QueryIndex_UnboundedMin_Works()
    {
        await SeedItems(10);
        await _col.CreateIndexAsync("value", "idx_value");

        var results = await _col.QueryIndexAsync("idx_value", null, 3).ToListAsync();
        Assert.True(results.Count >= 3);
    }

    [Fact]
    public async Task QueryIndex_UnboundedMax_Works()
    {
        await SeedItems(10);
        await _col.CreateIndexAsync("value", "idx_value");

        var results = await _col.QueryIndexAsync("idx_value", 7, null).ToListAsync();
        Assert.True(results.Count >= 3);
    }

    [Fact]
    public async Task QueryIndex_FullRange_ReturnsAll()
    {
        await SeedItems(10);
        await _col.CreateIndexAsync("value", "idx_value");

        var results = await _col.QueryIndexAsync("idx_value", null, null).ToListAsync();
        Assert.Equal(10, results.Count);
    }

    [Fact]
    public async Task QueryIndex_NonExistentIndex_Throws()
    {
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _col.QueryIndexAsync("nonexistent", null, null).ToListAsync());
    }

    [Fact]
    public async Task DropIndex_Existing_ReturnsTrue()
    {
        await SeedItems(5);
        await _col.CreateIndexAsync("name", "idx_name");
        Assert.True(_col.DropIndex("idx_name"));
        Assert.DoesNotContain("idx_name", _col.ListIndexes());
    }

    [Fact]
    public async Task DropIndex_NonExistent_ReturnsFalse()
    {
        Assert.False(_col.DropIndex("nonexistent"));
    }

    [Fact]
    public void ListIndexes_Empty_ReturnsEmpty()
    {
        Assert.Empty(_col.ListIndexes());
    }

    [Fact]
    public async Task ListIndexes_Multiple_ReturnsAll()
    {
        await SeedItems(5);
        await _col.CreateIndexAsync("name", "idx_name");
        await _col.CreateIndexAsync("value", "idx_value");
        var indexes = _col.ListIndexes();
        Assert.Equal(2, indexes.Count);
        Assert.Contains("idx_name", indexes);
        Assert.Contains("idx_value", indexes);
    }

    [Fact]
    public async Task Index_SurvivesReopen()
    {
        await SeedItems(5);
        await _col.CreateIndexAsync("value", "idx_value");
        await _engine.CommitAsync();
        _engine.Dispose();

        using var engine2 = new BLiteEngine(_dbPath);
        var col2 = engine2.GetOrCreateCollection("items");
        var indexes = col2.ListIndexes();
        Assert.Contains("idx_value", indexes);

        var results = await col2.QueryIndexAsync("idx_value", 2, 4).ToListAsync();
        Assert.True(results.Count >= 2);
    }

    [Fact]
    public async Task Index_UpdatesWhenDocumentUpdated()
    {
        await SeedItems(5);
        await _col.CreateIndexAsync("value", "idx_value");

        // Find and update document with value=3
        var doc = (await _col.QueryIndexAsync("idx_value", 3, 3).ToListAsync()).First();
        doc.TryGetId(out var id);

        var newDoc = _col.CreateDocument(["_id", "name", "value"], b =>
            b.AddString("name", "updated").AddInt32("value", 99));
        await _col.UpdateAsync(id, newDoc);

        // Old value should no longer be found
        var oldResults = (await _col.QueryIndexAsync("idx_value", 3, 3).ToListAsync());
        Assert.Empty(oldResults);

        // New value should be found
        var newResults = (await _col.QueryIndexAsync("idx_value", 99, 99).ToListAsync());
        Assert.Single(newResults);
    }

    /// <summary>
    /// Regression test for non-in-place update dropping secondary index entries.
    /// When a document grows in size during an update, the engine relocates it by
    /// calling DeleteCore (which wipes all index entries) then re-writing to a new
    /// page. The index entry must be re-inserted even if the indexed field value
    /// has not changed. (Root cause of issue #21 / #28.)
    /// </summary>
    [Fact]
    public async Task Index_SurvivesNonInPlaceUpdate_WhenIndexedFieldUnchanged()
    {
        await _col.CreateIndexAsync("value", "idx_value");

        // Insert a small document so the slot size is small.
        var smallDoc = _col.CreateDocument(["_id", "name", "value", "payload"], b =>
            b.AddString("name", "original").AddInt32("value", 42).AddString("payload", "x"));
        var id = await _col.InsertAsync(smallDoc);

        // Verify the index finds it before the update.
        var before = await _col.QueryIndexAsync("idx_value", 42, 42).ToListAsync();
        Assert.Single(before);

        // Update the document: keep the indexed field ("value") the same but grow
        // the payload significantly to force a non-in-place (relocating) update.
        var largePayload = new string('A', 8192);
        var largeDoc = _col.CreateDocument(["_id", "name", "value", "payload"], b =>
            b.AddString("name", "updated").AddInt32("value", 42).AddString("payload", largePayload));
        var updated = await _col.UpdateAsync(id, largeDoc);
        Assert.True(updated);

        // The index must still find the document at its new physical location.
        var after = await _col.QueryIndexAsync("idx_value", 42, 42).ToListAsync();
        Assert.Single(after);
        after[0].TryGetString("name", out var name);
        Assert.Equal("updated", name);
    }

    [Fact]
    public async Task Index_UpdatesWhenDocumentDeleted()
    {
        await SeedItems(5);
        await _col.CreateIndexAsync("value", "idx_value");

        var doc = (await _col.QueryIndexAsync("idx_value", 2, 2).ToListAsync()).First();
        doc.TryGetId(out var id);
        await _col.DeleteAsync(id);
        var results = (await _col.QueryIndexAsync("idx_value", 2, 2).ToListAsync());
        Assert.Empty(results);
    }

    [Fact]
    public async Task Index_UniqueConstraint_PreventsDuplicates()
    {
        await _col.CreateIndexAsync("email", "idx_email", unique: true);

        var doc1 = _col.CreateDocument(["_id", "email"], b => b.AddString("email", "a@b.com"));
        await _col.InsertAsync(doc1);

        var doc2 = _col.CreateDocument(["_id", "email"], b => b.AddString("email", "a@b.com"));
        await Assert.ThrowsAnyAsync<Exception>(async () => await _col.InsertAsync(doc2));
    }

    [Fact]
    public async Task Index_StringField_QueryAfterBulkInsert()
    {
        await _col.CreateIndexAsync("category", "idx_cat");

        var docs = new List<BsonDocument>();
        for (int i = 0; i < 50; i++)
        {
            var cat = i % 3 == 0 ? "A" : (i % 3 == 1 ? "B" : "C");
            docs.Add(_col.CreateDocument(["_id", "category", "val"], b =>
                b.AddString("category", cat).AddInt32("val", i)));
        }
        
        await _col.InsertBulkAsync(docs);

        var a = (await _col.QueryIndexAsync("idx_cat", "A", "A").ToListAsync());
        var b = (await _col.QueryIndexAsync("idx_cat", "B", "B").ToListAsync());
        var c = (await _col.QueryIndexAsync("idx_cat", "C", "C").ToListAsync());
        Assert.Equal(17, a.Count); // ceil(50/3)
        Assert.Equal(17, b.Count);
        Assert.Equal(16, c.Count);
    }

    [Fact]
    public async Task Index_Int64Field_Works()
    {
        await _col.CreateIndexAsync("score", "idx_score");

        for (int i = 0; i < 10; i++)
        {
            var doc = _col.CreateDocument(["_id", "score"], b =>
                b.AddInt64("score", (long)i * 1000));
            await _col.InsertAsync(doc);
        }

        var results = (await _col.QueryIndexAsync("idx_score", 3000L, 7000L).ToListAsync());
        Assert.Equal(5, results.Count);
    }

    [Fact]
    public async Task Index_DateTimeField_Works()
    {
        await _col.CreateIndexAsync("date", "idx_date");

        var baseDate = new DateTime(2024, 1, 1);
        for (int i = 0; i < 10; i++)
        {
            var doc = _col.CreateDocument(["_id", "date"], b =>
                b.AddDateTime("date", baseDate.AddDays(i)));
            await _col.InsertAsync(doc);
        }

        // DateTime is not supported by QueryIndex — verify data via FindAll + index existence
        var indexes = _col.ListIndexes();
        Assert.Contains("idx_date", indexes);
        Assert.Equal(10, (await _col.FindAllAsync().ToListAsync()).Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Spatial index
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SpatialIndex_CreateAndNear()
    {
        var col = _engine.GetOrCreateCollection("places");
        await col.CreateSpatialIndexAsync("location", "idx_loc");

        // Insert some locations
        await InsertLocation(col, "Rome", 41.9028, 12.4964);
        await InsertLocation(col, "Milan", 45.4642, 9.1900);
        await InsertLocation(col, "Naples", 40.8518, 14.2681);
        await InsertLocation(col, "London", 51.5074, -0.1278);

        // Search near Rome within 300km → should find Rome and Naples
        var results = await col.NearAsync("idx_loc", (41.9028, 12.4964), 300).ToListAsync();
        Assert.True(results.Count >= 1); // At least Rome itself
    }

    [Fact]
    public async Task SpatialIndex_Within()
    {
        var col = _engine.GetOrCreateCollection("places2");
        await col.CreateSpatialIndexAsync("location", "idx_loc2");

        await InsertLocation(col, "Rome", 41.9028, 12.4964);
        await InsertLocation(col, "Milan", 45.4642, 9.1900);
        await InsertLocation(col, "London", 51.5074, -0.1278);

        // Search within Italian boundaries
        var results = await col.WithinAsync("idx_loc2", (36.0, 6.0), (47.0, 19.0)).ToListAsync();
        Assert.True(results.Count >= 2); // Rome, Milan
    }

    [Fact]
    public async Task SpatialIndex_QueryOnWrongIndexType_Throws()
    {
        await SeedItems(5);
        await _col.CreateIndexAsync("name", "idx_name");

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _col.NearAsync("idx_name", (0, 0), 100).ToListAsync());
    }

    [Fact]
    public async Task SpatialIndex_NonExistentIndex_Throws()
    {
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _col.NearAsync("nonexistent", (0, 0), 100).ToListAsync());
    }

    // ══════════════════════════════════════════════════════════════════════
    //  DynamicCollection — CRUD edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Insert_Null_Throws()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await _col.InsertAsync(null!));
    }

    [Fact]
    public async Task Update_Null_Throws()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await _col.UpdateAsync(new BsonId(ObjectId.NewObjectId()), null!));
    }

    [Fact]
    public async Task Delete_NonExistent_ReturnsFalse()
    {
        var id = new BsonId(ObjectId.NewObjectId());
        Assert.False(await _col.DeleteAsync(id));
    }

    [Fact]
    public async Task FindById_NonExistent_ReturnsNull()
    {
        var id = new BsonId(ObjectId.NewObjectId());
        Assert.Null(await _col.FindByIdAsync(id));
    }

    [Fact]
    public async Task FindAll_Empty_ReturnsEmpty()
    {
        Assert.Empty(await _col.FindAllAsync().ToListAsync());
    }

    [Fact]
    public async Task Find_WithPredicate_FiltersCorrectly()
    {
        await SeedItems(10);
        var results = await _col.FindAsync(d =>
        {
            d.TryGetInt32("value", out var v);
            return v > 7;
        }).ToListAsync();
        Assert.Equal(2, results.Count); // 8, 9
    }

    [Fact]
    public async Task Find_NullPredicate_Throws()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await _col.FindAsync(null!).ToListAsync();
        });
    }

    [Fact]
    public async Task FindAsync_WithPredicate_FiltersCorrectly()
    {
        await SeedItems(10);
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
    public async Task InsertBulk_EmptyList_NoError()
    {
        await _col.InsertBulkAsync(new List<BsonDocument>());
        var results = await _col.FindAllAsync().ToListAsync();
        Assert.Empty(results);
    }

    [Fact]
    public async Task InsertBulk_ManyDocuments_AllPersisted()
    {
        var docs = Enumerable.Range(0, 100).Select(i =>
            _col.CreateDocument(["_id", "val"], b => b.AddInt32("val", i))).ToList();
        await _col.InsertBulkAsync(docs);
        var results = await _col.FindAllAsync().ToListAsync();
        Assert.Equal(100, results.Count);
    }

    [Fact]
    public async Task UpdateBulk_UpdatesMultiple()
    {
        await SeedItems(5);
        var allDocs = (await _col.FindAllAsync().ToListAsync());
        var updates = new List<(BsonId, BsonDocument)>();

        foreach (var doc in allDocs)
        {
            doc.TryGetId(out var id);
            var newDoc = _col.CreateDocument(["_id", "name", "value"], b =>
                b.AddString("name", "updated").AddInt32("value", 999));
            updates.Add((id, newDoc));
        }

        await _col.UpdateBulkAsync(updates);

        var updatedDocs = await _col.FindAllAsync().ToListAsync();
        foreach (var doc in updatedDocs)    
        {
            doc.TryGetString("name", out var name);
            Assert.Equal("updated", name);
        }
    }

    [Fact]
    public async Task DeleteBulk_DeletesMultiple()
    {
        await SeedItems(5);
        var ids = (await _col.FindAllAsync().ToListAsync()).Select(d => { d.TryGetId(out var id); return id; }).ToList();
        await _col.DeleteBulkAsync(ids.Take(3));
        var remaining = await _col.FindAllAsync().ToListAsync();
        Assert.Equal(2, remaining.Count);
    }

    [Fact]
    public async Task Count_ReturnsCorrectCount()
    {
        await SeedItems(7);
        var count = await _col.CountAsync();
        Assert.Equal(7, count);
    }

    [Fact]
    public async Task Scan_ReturnsAll()
    {
        await SeedItems(5);
        var count = 0;
        await foreach (var doc in _col.ScanAsync(reader => true))
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
        var id = await _col.InsertAsync(doc);
        var found = await _col.FindByIdAsync(id);
        Assert.NotNull(found);
        found!.TryGetInt32("x", out var x);
        Assert.Equal(42, x);
    }

    [Fact]
    public async Task UpdateAsync_Works()
    {
        var doc = _col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 1));
        var id = await _col.InsertAsync(doc);
        var newDoc = _col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 2));
        var updated = await _col.UpdateAsync(id, newDoc);
        Assert.True(updated);
    }

    [Fact]
    public async Task DeleteAsync_Works()
    {
        var doc = _col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 1));
        var id = await _col.InsertAsync(doc);
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
    public async Task TimeSeries_InsertAndQuery()
    {
        var col = _engine.GetOrCreateCollection("ts_insert");
        col.SetTimeSeries("ts", TimeSpan.FromDays(365));

        for (int i = 0; i < 5; i++)
        {
            var doc = col.CreateDocument(["_id", "ts", "val"], b =>
                b.AddDateTime("ts", DateTime.UtcNow.AddMinutes(-i)).AddInt32("val", i));
            await col.InsertAsync(doc);
        }

        Assert.Equal(5, (await col.FindAllAsync().ToListAsync()).Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Helpers
    // ══════════════════════════════════════════════════════════════════════

    private async Task SeedItems(int count)
    {
        for (int i = 0; i < count; i++)
        {
            var doc = _col.CreateDocument(["_id", "name", "value"], b =>
                b.AddString("name", $"item_{i:D3}").AddInt32("value", i));
            await _col.InsertAsync(doc);
        }
    }

    private static async Task InsertLocation(DynamicCollection col, string name, double lat, double lon)
    {
        var doc = col.CreateDocument(["_id", "name", "location"], b =>
        {
            b.AddString("name", name);
            b.AddCoordinates("location", (lat, lon));
        });
        await col.InsertAsync(doc);
    }
}
