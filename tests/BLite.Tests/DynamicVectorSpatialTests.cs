using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;

namespace BLite.Tests;

/// <summary>
/// Tests for DynamicCollection vector and spatial index support.
/// All access goes through BLiteEngine.GetOrCreateCollection() as in production usage.
/// </summary>
public class DynamicVectorSpatialTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;

    public DynamicVectorSpatialTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_dyncol_{Guid.NewGuid()}.db");
        _engine = new BLiteEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ── Vector index ─────────────────────────────────────────────────────────

    [Fact]
    public void CreateVectorIndex_And_VectorSearch_ReturnsTopK()
    {
        var col = _engine.GetOrCreateCollection("vecs");
        col.CreateVectorIndex("embedding", dimensions: 3, metric: VectorMetric.L2);

        col.Insert(col.CreateDocument(["title", "embedding"], b => b
            .AddString("title", "Near")
            .AddFloatArray("embedding", [1.0f, 1.0f, 1.0f])));
        col.Insert(col.CreateDocument(["title", "embedding"], b => b
            .AddString("title", "Far")
            .AddFloatArray("embedding", [10.0f, 10.0f, 10.0f])));
        col.Insert(col.CreateDocument(["title", "embedding"], b => b
            .AddString("title", "Mid")
            .AddFloatArray("embedding", [2.0f, 2.0f, 2.0f])));
        _engine.Commit();

        var results = col.VectorSearch("idx_vector_embedding", [0.9f, 0.9f, 0.9f], k: 1).ToList();

        Assert.Single(results);
        Assert.True(results[0].TryGetString("title", out var title));
        Assert.Equal("Near", title);
    }

    [Fact]
    public void VectorSearch_TopK_Returns_K_Results()
    {
        var col = _engine.GetOrCreateCollection("vecs_k");
        col.CreateVectorIndex("embedding", dimensions: 2);

        for (int i = 1; i <= 5; i++)
        {
            col.Insert(col.CreateDocument(["val", "embedding"], b => b
                .AddInt32("val", i)
                .AddFloatArray("embedding", [(float)i, (float)i])));
        }
        _engine.Commit();

        var results = col.VectorSearch("idx_vector_embedding", [1.0f, 1.0f], k: 3).ToList();

        Assert.Equal(3, results.Count);
    }

    [Fact]
    public void VectorSearch_WithNamedIndex_Works()
    {
        var col = _engine.GetOrCreateCollection("vecs_named");
        col.CreateVectorIndex("emb", dimensions: 2, name: "my_vec_idx");

        col.Insert(col.CreateDocument(["emb"], b => b
            .AddFloatArray("emb", [1.0f, 0.0f])));
        _engine.Commit();

        var results = col.VectorSearch("my_vec_idx", [1.0f, 0.0f], k: 1).ToList();

        Assert.Single(results);
    }

    [Fact]
    public void CreateVectorIndex_Duplicate_Throws()
    {
        var col = _engine.GetOrCreateCollection("vecs_dup");
        col.CreateVectorIndex("emb", dimensions: 3);

        Assert.Throws<InvalidOperationException>(() =>
            col.CreateVectorIndex("emb", dimensions: 3));
    }

    [Fact]
    public void QueryIndex_OnVectorIndex_Throws()
    {
        var col = _engine.GetOrCreateCollection("vecs_qi");
        col.CreateVectorIndex("emb", dimensions: 2);

        Assert.Throws<InvalidOperationException>(() =>
            col.QueryIndex("idx_vector_emb", 1.0f, 1.0f).ToList());
    }

    // ── Spatial index ────────────────────────────────────────────────────────

    [Fact]
    public void CreateSpatialIndex_And_Near_ReturnsClosest()
    {
        var col = _engine.GetOrCreateCollection("places");
        col.CreateSpatialIndex("location");

        // Rome: 41.9, 12.5  |  Milan: 45.5, 9.2  |  Palermo: 38.1, 13.4
        col.Insert(col.CreateDocument(["city", "location"], b => b
            .AddString("city", "Rome")
            .AddCoordinates("location", (41.9, 12.5))));
        col.Insert(col.CreateDocument(["city", "location"], b => b
            .AddString("city", "Milan")
            .AddCoordinates("location", (45.5, 9.2))));
        col.Insert(col.CreateDocument(["city", "location"], b => b
            .AddString("city", "Palermo")
            .AddCoordinates("location", (38.1, 13.4))));
        _engine.Commit();

        var results = col.Near("idx_spatial_location", (41.9, 12.5), 100.0).ToList();

        Assert.Contains(results, d => d.TryGetString("city", out var c) && c == "Rome");
        Assert.DoesNotContain(results, d => d.TryGetString("city", out var c) && c == "Milan");
    }

    [Fact]
    public void CreateSpatialIndex_And_Within_ReturnsBbox()
    {
        var col = _engine.GetOrCreateCollection("geo_within");
        col.CreateSpatialIndex("pos");

        col.Insert(col.CreateDocument(["name", "pos"], b => b
            .AddString("name", "Inside")
            .AddCoordinates("pos", (43.0, 12.0))));
        col.Insert(col.CreateDocument(["name", "pos"], b => b
            .AddString("name", "Outside")
            .AddCoordinates("pos", (50.0, 20.0))));
        _engine.Commit();

        var results = col.Within("idx_spatial_pos", (40.0, 10.0), (46.0, 14.0)).ToList();

        Assert.Single(results);
        Assert.True(results[0].TryGetString("name", out var name));
        Assert.Equal("Inside", name);
    }

    [Fact]
    public void CreateSpatialIndex_Duplicate_Throws()
    {
        var col = _engine.GetOrCreateCollection("geo_dup");
        col.CreateSpatialIndex("location");

        Assert.Throws<InvalidOperationException>(() =>
            col.CreateSpatialIndex("location"));
    }

    // ── Persistence: reopen ──────────────────────────────────────────────────

    [Fact]
    public void VectorIndex_Persists_And_Restores_Across_Reopen()
    {
        var path = Path.Combine(Path.GetTempPath(), $"persist_vec_{Guid.NewGuid()}.db");
        try
        {
            using (var e1 = new BLiteEngine(path))
            {
                var col = e1.GetOrCreateCollection("items");
                col.CreateVectorIndex("emb", dimensions: 2);

                col.Insert(col.CreateDocument(["label", "emb"], b => b
                    .AddString("label", "A")
                    .AddFloatArray("emb", [1.0f, 0.0f])));
                col.Insert(col.CreateDocument(["label", "emb"], b => b
                    .AddString("label", "B")
                    .AddFloatArray("emb", [0.0f, 1.0f])));
                e1.Commit();
            }

            using (var e2 = new BLiteEngine(path))
            {
                var col = e2.GetOrCreateCollection("items");

                // Index restored: VectorSearch works without calling CreateVectorIndex again
                var results = col.VectorSearch("idx_vector_emb", [1.0f, 0.0f], k: 1).ToList();

                Assert.Single(results);
                Assert.True(results[0].TryGetString("label", out var lbl));
                Assert.Equal("A", lbl);
            }
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
            var w = Path.ChangeExtension(path, ".wal");
            if (File.Exists(w)) File.Delete(w);
        }
    }

    [Fact]
    public void SpatialIndex_Persists_And_Restores_Across_Reopen()
    {
        var path = Path.Combine(Path.GetTempPath(), $"persist_geo_{Guid.NewGuid()}.db");
        try
        {
            using (var e1 = new BLiteEngine(path))
            {
                var col = e1.GetOrCreateCollection("cities");
                col.CreateSpatialIndex("loc");

                col.Insert(col.CreateDocument(["name", "loc"], b => b
                    .AddString("name", "Rome")
                    .AddCoordinates("loc", (41.9, 12.5))));
                e1.Commit();
            }

            using (var e2 = new BLiteEngine(path))
            {
                var col = e2.GetOrCreateCollection("cities");

                // Spatial index restored: Within works without re-creating the index
                var results = col.Within("idx_spatial_loc", (40.0, 10.0), (44.0, 14.0)).ToList();

                Assert.Single(results);
                Assert.True(results[0].TryGetString("name", out var name));
                Assert.Equal("Rome", name);
            }
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
            var w = Path.ChangeExtension(path, ".wal");
            if (File.Exists(w)) File.Delete(w);
        }
    }

    // ── BTree coexistence ────────────────────────────────────────────────────

    [Fact]
    public void BTree_And_Vector_Indexes_Coexist()
    {
        var col = _engine.GetOrCreateCollection("mixed");
        col.CreateIndex("category");
        col.CreateVectorIndex("emb", dimensions: 2);

        col.Insert(col.CreateDocument(["category", "emb"], b => b
            .AddString("category", "tech")
            .AddFloatArray("emb", [1.0f, 0.0f])));
        col.Insert(col.CreateDocument(["category", "emb"], b => b
            .AddString("category", "food")
            .AddFloatArray("emb", [0.0f, 1.0f])));
        _engine.Commit();

        var byBTree = col.QueryIndex("idx_category", "tech", "tech").ToList();
        Assert.Single(byBTree);

        var byVec = col.VectorSearch("idx_vector_emb", [1.0f, 0.0f], k: 1).ToList();
        Assert.Single(byVec);
    }
}
