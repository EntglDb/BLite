using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;

namespace BLite.Tests;

/// <summary>
/// Regression tests for the HNSW graph construction in
/// <see cref="VectorSearchIndex"/>.
///
/// These tests were added to lock down a class of bugs that produced a
/// structurally disconnected level-0 graph: under those bugs, repeated calls
/// to <c>VectorSearchAsync</c> would return only a small connected component
/// (≈ 15–20 % of the inserted nodes) regardless of the query, because the
/// search starts from a single entry point and cannot reach the rest of the
/// graph.
///
/// The two failure modes the tests cover:
///   1. <see cref="HnswSearch_LargeK_ReturnsAllInsertedNodes"/> — pure
///      reachability: with <c>k = N</c> and a large <c>efSearch</c>, the index
///      must return every inserted document. Pre-fix this returned ≈ 18 %.
///   2. <see cref="HnswSearch_RecallAtTen_OnRandomVectors_IsHigh"/> — quality
///      against a brute-force baseline. Pre-fix average recall@10 collapsed
///      well below 0.5; the canonical HNSW (M=16, ef_construction=200) easily
///      reaches ≥ 0.9 on this synthetic workload.
/// </summary>
public sealed class HnswGraphQualityTests : IDisposable
{
    private const int    Dimensions      = 32;
    private const int    NodeCount       = 250;
    private const int    QueryCount      = 20;
    private const int    K               = 10;
    private const double MinRecallAtK    = 0.85;
    private const int    Seed            = 20260511;

    private readonly string      _dbPath;
    private readonly BLiteEngine _engine;

    public HnswGraphQualityTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"hnsw_quality_{Guid.NewGuid():N}.db");
        _engine = new BLiteEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        TryDelete(_dbPath);
        TryDelete(Path.ChangeExtension(_dbPath, ".wal"));
    }

    /// <summary>
    /// Inserts <see cref="NodeCount"/> normalized random vectors and then
    /// queries the index asking for k = NodeCount. Every inserted document
    /// must come back: if the level-0 graph is disconnected from the entry
    /// point, only the reachable component is returned and this assertion
    /// fails. This is the strongest possible reachability check.
    /// </summary>
    [Fact]
    public async Task HnswSearch_LargeK_ReturnsAllInsertedNodes()
    {
        var col = _engine.GetOrCreateCollection("vectors");
        await col.CreateVectorIndexAsync(
            "embedding",
            dimensions: Dimensions,
            metric: VectorMetric.Cosine,
            name: "idx_v");

        var rng     = new Random(Seed);
        var vectors = GenerateNormalizedVectors(NodeCount, Dimensions, rng);
        await InsertAllAsync(col, vectors);

        // Use a deliberately large efSearch so that the test isolates *graph
        // reachability* from search-time pruning quality.
        var query   = vectors[0];
        var results = await col
            .VectorSearchAsync("idx_v", query, k: NodeCount, efSearch: NodeCount * 4)
            .ToListAsync();

        Assert.True(
            results.Count == NodeCount,
            $"""
            HNSW level-0 graph is not fully reachable from the entry point.
            Inserted : {NodeCount}
            Returned : {results.Count}
            This indicates a disconnected level-0 graph (regression of the
            pre-fix construction bug).
            """);
    }

    /// <summary>
    /// Builds a brute-force cosine baseline and checks that the HNSW recall@K
    /// averaged over <see cref="QueryCount"/> random queries is at least
    /// <see cref="MinRecallAtK"/>. With the canonical construction
    /// (Mmax0 = 2·M, full SelectNeighbors-based shrinking, keepPrunedConnections)
    /// this stays well above 0.9 on 250 random points in dim 32.
    /// </summary>
    [Fact]
    public async Task HnswSearch_RecallAtTen_OnRandomVectors_IsHigh()
    {
        var col = _engine.GetOrCreateCollection("vectors");
        await col.CreateVectorIndexAsync(
            "embedding",
            dimensions: Dimensions,
            metric: VectorMetric.Cosine,
            name: "idx_v");

        var rng     = new Random(Seed);
        var vectors = GenerateNormalizedVectors(NodeCount, Dimensions, rng);
        await InsertAllAsync(col, vectors);

        double recallSum = 0;
        for (int q = 0; q < QueryCount; q++)
        {
            var query = GenerateNormalizedVector(Dimensions, rng);

            var expected = BruteForceTopK(vectors, query, K);
            var actual   = await col
                .VectorSearchAsync("idx_v", query, k: K, efSearch: 100)
                .ToListAsync();

            var actualIndexes = new HashSet<int>(actual.Select(GetVectorIndex));
            int hits = 0;
            foreach (var expectedIdx in expected)
                if (actualIndexes.Contains(expectedIdx)) hits++;

            recallSum += hits / (double)K;
        }

        double avgRecall = recallSum / QueryCount;
        Assert.True(
            avgRecall >= MinRecallAtK,
            $"""
            HNSW recall@{K} too low.
            Average recall : {avgRecall:F3}
            Minimum required : {MinRecallAtK:F3}
            Nodes : {NodeCount}, Queries : {QueryCount}, Dim : {Dimensions}
            A low recall typically indicates that the SelectNeighbors heuristic
            or the bidirectional-link shrinking is pruning too aggressively or
            that the level-0 graph is partially disconnected.
            """);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Inserts each vector as a document with two fields: an integer "id" that
    /// records the original index in <paramref name="vectors"/> and the
    /// "embedding" float array. The "id" field is what we use to compare HNSW
    /// hits to the brute-force baseline.
    /// </summary>
    private async Task InsertAllAsync(DynamicCollection col, float[][] vectors)
    {
        for (int i = 0; i < vectors.Length; i++)
        {
            int idx = i;
            await col.InsertAsync(col.CreateDocument(["id", "embedding"], b => b
                .AddInt32("id", idx)
                .AddFloatArray("embedding", vectors[idx])));
        }
        await _engine.CommitAsync();
    }

    /// <summary>Reads the "id" field written by <see cref="InsertAllAsync"/>.</summary>
    private static int GetVectorIndex(BsonDocument doc)
    {
        if (!doc.TryGetInt32("id", out int id))
            throw new InvalidOperationException("Document is missing the 'id' field.");
        return id;
    }

    /// <summary>
    /// Computes the K nearest neighbours of <paramref name="query"/> over
    /// <paramref name="vectors"/> with a plain O(N·D) cosine scan, returning
    /// the indices into <paramref name="vectors"/>.
    /// </summary>
    private static int[] BruteForceTopK(float[][] vectors, float[] query, int k)
    {
        var scored = new (int Index, float Score)[vectors.Length];
        for (int i = 0; i < vectors.Length; i++)
        {
            float dot = 0f;
            var   v   = vectors[i];
            for (int d = 0; d < query.Length; d++) dot += query[d] * v[d];
            // Inputs are normalized, so cosine similarity == dot product.
            scored[i] = (i, dot);
        }
        Array.Sort(scored, (a, b) => b.Score.CompareTo(a.Score));

        var top = new int[k];
        for (int i = 0; i < k; i++) top[i] = scored[i].Index;
        return top;
    }

    private static float[][] GenerateNormalizedVectors(int count, int dim, Random rng)
    {
        var vectors = new float[count][];
        for (int i = 0; i < count; i++) vectors[i] = GenerateNormalizedVector(dim, rng);
        return vectors;
    }

    /// <summary>
    /// Samples a vector from the unit sphere using Box–Muller for each
    /// component and L2-normalizing. This produces a workload that is roughly
    /// isotropic and stresses the HNSW long-range edges (no obvious clusters,
    /// every node is plausibly a neighbour of many others).
    /// </summary>
    private static float[] GenerateNormalizedVector(int dim, Random rng)
    {
        var v = new float[dim];
        double sumSquares = 0;
        for (int i = 0; i < dim; i++)
        {
            double u1 = 1.0 - rng.NextDouble();
            double u2 = 1.0 - rng.NextDouble();
            double g  = Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2.0 * Math.PI * u2);
            v[i]      = (float)g;
            sumSquares += g * g;
        }
        float invNorm = (float)(1.0 / Math.Sqrt(sumSquares));
        for (int i = 0; i < dim; i++) v[i] *= invNorm;
        return v;
    }

    private static void TryDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch { /* best effort */ }
    }
}
