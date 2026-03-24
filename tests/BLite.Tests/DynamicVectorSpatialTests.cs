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
    public async Task CreateVectorIndex_And_VectorSearch_ReturnsTopK()
    {
        var col = _engine.GetOrCreateCollection("vecs");
        await col.CreateVectorIndexAsync("embedding", dimensions: 3, metric: VectorMetric.L2);

        await col.InsertAsync(col.CreateDocument(["title", "embedding"], b => b
            .AddString("title", "Near")
            .AddFloatArray("embedding", [1.0f, 1.0f, 1.0f])));
        await col.InsertAsync(col.CreateDocument(["title", "embedding"], b => b
            .AddString("title", "Far")
            .AddFloatArray("embedding", [10.0f, 10.0f, 10.0f])));
        await col.InsertAsync(col.CreateDocument(["title", "embedding"], b => b
            .AddString("title", "Mid")
            .AddFloatArray("embedding", [2.0f, 2.0f, 2.0f])));
        await _engine.CommitAsync();

        var results = await col.VectorSearchAsync("idx_vector_embedding", [0.9f, 0.9f, 0.9f], k: 1).ToListAsync();

        Assert.Single(results);
        Assert.True(results[0].TryGetString("title", out var title));
        Assert.Equal("Near", title);
    }

    [Fact]
    public async Task VectorSearch_TopK_Returns_K_Results()
    {
        var col = _engine.GetOrCreateCollection("vecs_k");
        await col.CreateVectorIndexAsync("embedding", dimensions: 2);

        for (int i = 1; i <= 5; i++)
        {
            await col.InsertAsync(col.CreateDocument(["val", "embedding"], b => b
                .AddInt32("val", i)
                .AddFloatArray("embedding", [(float)i, (float)i])));
        }
        await _engine.CommitAsync();
        var results = await col.VectorSearchAsync("idx_vector_embedding", [1.0f, 1.0f], k: 3).ToListAsync();

        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task VectorSearch_WithNamedIndex_Works()
    {
        var col = _engine.GetOrCreateCollection("vecs_named");
        await col.CreateVectorIndexAsync("emb", dimensions: 2, name: "my_vec_idx");

        await col.InsertAsync(col.CreateDocument(["emb"], b => b
            .AddFloatArray("emb", [1.0f, 0.0f])));
        await _engine.CommitAsync();

        var results = await col.VectorSearchAsync("my_vec_idx", [1.0f, 0.0f], k: 1).ToListAsync();
        Assert.Single(results);
    }

    [Fact]
    public async Task CreateVectorIndex_Duplicate_Throws()
    {
        var col = _engine.GetOrCreateCollection("vecs_dup");
        await col.CreateVectorIndexAsync("emb", dimensions: 3);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await col.CreateVectorIndexAsync("emb", dimensions: 3));
    }

    [Fact]
    public async Task QueryIndex_OnVectorIndex_Throws()
    {
        var col = _engine.GetOrCreateCollection("vecs_qi");
        await col.CreateVectorIndexAsync("emb", dimensions: 2);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await col.QueryIndexAsync("idx_vector_emb", 1.0f, 1.0f).ToListAsync());
    }

    // ── Spatial index ────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateSpatialIndex_And_Near_ReturnsClosest()
    {
        var col = _engine.GetOrCreateCollection("places");
        await col.CreateSpatialIndexAsync("location");

        // Rome: 41.9, 12.5  |  Milan: 45.5, 9.2  |  Palermo: 38.1, 13.4
        await col.InsertAsync(col.CreateDocument(["city", "location"], b => b
            .AddString("city", "Rome")
            .AddCoordinates("location", (41.9, 12.5))));
        await col.InsertAsync(col.CreateDocument(["city", "location"], b => b
            .AddString("city", "Milan")
            .AddCoordinates("location", (45.5, 9.2))));
        await col.InsertAsync(col.CreateDocument(["city", "location"], b => b
            .AddString("city", "Palermo")
            .AddCoordinates("location", (38.1, 13.4))));
        await _engine.CommitAsync();

        var results = await col.NearAsync("idx_spatial_location", (41.9, 12.5), 100.0).ToListAsync();

        Assert.Contains(results, d => d.TryGetString("city", out var c) && c == "Rome");
        Assert.DoesNotContain(results, d => d.TryGetString("city", out var c) && c == "Milan");
    }

    [Fact]
    public async Task CreateSpatialIndex_And_Within_ReturnsBbox()
    {
        var col = _engine.GetOrCreateCollection("geo_within");
        await col.CreateSpatialIndexAsync("pos");

        await col.InsertAsync(col.CreateDocument(["name", "pos"], b => b
            .AddString("name", "Inside")
            .AddCoordinates("pos", (43.0, 12.0))));
        await col.InsertAsync(col.CreateDocument(["name", "pos"], b => b
            .AddString("name", "Outside")
            .AddCoordinates("pos", (50.0, 20.0))));
        await _engine.CommitAsync();

        var results = await col.WithinAsync("idx_spatial_pos", (40.0, 10.0), (46.0, 14.0)).ToListAsync();

        Assert.Single(results);
        Assert.True(results[0].TryGetString("name", out var name));
        Assert.Equal("Inside", name);
    }

    [Fact]
    public async Task CreateSpatialIndex_Duplicate_Throws()
    {
        var col = _engine.GetOrCreateCollection("geo_dup");
        await col.CreateSpatialIndexAsync("location");

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await col.CreateSpatialIndexAsync("location"));
    }

    // ── Persistence: reopen ──────────────────────────────────────────────────

    [Fact]
    public async Task VectorIndex_Persists_And_Restores_Across_Reopen()
    {
        var path = Path.Combine(Path.GetTempPath(), $"persist_vec_{Guid.NewGuid()}.db");
        try
        {
            using (var e1 = new BLiteEngine(path))
            {
                var col = e1.GetOrCreateCollection("items");
                await col.CreateVectorIndexAsync("emb", dimensions: 2);

                await col.InsertAsync(col.CreateDocument(["label", "emb"], b => b
                    .AddString("label", "A")
                    .AddFloatArray("emb", [1.0f, 0.0f])));
                await col.InsertAsync(col.CreateDocument(["label", "emb"], b => b
                    .AddString("label", "B")
                    .AddFloatArray("emb", [0.0f, 1.0f])));
                await e1.CommitAsync();
            }

            using (var e2 = new BLiteEngine(path))
            {
                var col = e2.GetOrCreateCollection("items");

                // Index restored: VectorSearch works without calling CreateVectorIndexAsync again
                var results = await col.VectorSearchAsync("idx_vector_emb", [1.0f, 0.0f], k: 1).ToListAsync();

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
    public async Task SpatialIndex_Persists_And_Restores_Across_Reopen()
    {
        var path = Path.Combine(Path.GetTempPath(), $"persist_geo_{Guid.NewGuid()}.db");
        try
        {
            using (var e1 = new BLiteEngine(path))
            {
                var col = e1.GetOrCreateCollection("cities");
                await col.CreateSpatialIndexAsync("loc");

                await col.InsertAsync(col.CreateDocument(["name", "loc"], b => b
                    .AddString("name", "Rome")
                    .AddCoordinates("loc", (41.9, 12.5))));
                await e1.CommitAsync();
            }

            using (var e2 = new BLiteEngine(path))
            {
                var col = e2.GetOrCreateCollection("cities");

                // Spatial index restored: Within works without re-creating the index
                var results = await col.WithinAsync("idx_spatial_loc", (40.0, 10.0), (44.0, 14.0)).ToListAsync();

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
    public async Task BTree_And_Vector_Indexes_Coexist()
    {
        var col = _engine.GetOrCreateCollection("mixed");
        await col.CreateIndexAsync("category");
        await col.CreateVectorIndexAsync("emb", dimensions: 2);
        await col.InsertAsync(col.CreateDocument(["category", "emb"], b => b
            .AddString("category", "tech")
            .AddFloatArray("emb", [1.0f, 0.0f])));
        await col.InsertAsync(col.CreateDocument(["category", "emb"], b => b
            .AddString("category", "food")
            .AddFloatArray("emb", [0.0f, 1.0f])));
        await _engine.CommitAsync();

        var byBTree = await col.QueryIndexAsync("idx_category", "tech", "tech").ToListAsync();
        Assert.Single(byBTree);

        var byVec = await col.VectorSearchAsync("idx_vector_emb", [1.0f, 0.0f], k: 1).ToListAsync();
        Assert.Single(byVec);
    }
}
