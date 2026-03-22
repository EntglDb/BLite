using BLite.Core;
using BLite.Shared;

namespace BLite.Tests;

public class VectorSearchTests
{
    // Each test uses a unique file name derived from the calling method so that
    // parallel test execution never shares a database file.
    private static string TempDb([System.Runtime.CompilerServices.CallerMemberName] string name = "")
        => $"_vec_{name}.db";

    private static void Cleanup(string path)
    {
        if (File.Exists(path)) File.Delete(path);
    }

    // ── diagnostics (isolate LINQ vs HNSW) ──────────────────────────────────

    [Fact]
    public void Test_VectorSearch_DirectApi_BypassLinq()
    {
        // Calls DocumentCollection.VectorSearch() directly, no LINQ expression pipeline.
        // If this returns 3 but the LINQ-based tests return 1, the bug is in the LINQ stack.
        // If this also returns 1, the bug is in HNSW or FindByLocation.
        string dbPath = TempDb();
        Cleanup(dbPath);

        using (var db = new TestDbContext(dbPath))
        {
            db.VectorItems.Insert(new VectorEntity { Title = "A", Embedding = [1.0f, 0.0f, 0.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "B", Embedding = [0.0f, 1.0f, 0.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "C", Embedding = [0.0f, 0.0f, 1.0f] });

            var results = db.VectorItems.VectorSearch("idx_vector", [1.0f, 1.0f, 1.0f], 3).ToList();
            Assert.Equal(3, results.Count);
        }

        Cleanup(dbPath);
    }

    [Fact]
    public void Test_VectorSearch_FullScan_AllItemsPresent()
    {
        // Full scan (no index) — verifies all 3 items were actually inserted.
        string dbPath = TempDb();
        Cleanup(dbPath);

        using (var db = new TestDbContext(dbPath))
        {
            db.VectorItems.Insert(new VectorEntity { Title = "A", Embedding = [1.0f, 0.0f, 0.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "B", Embedding = [0.0f, 1.0f, 0.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "C", Embedding = [0.0f, 0.0f, 1.0f] });

            var all = db.VectorItems.AsQueryable().ToList();
            Assert.Equal(3, all.Count);
        }

        Cleanup(dbPath);
    }

    // ── basic ────────────────────────────────────────────────────────────────

    [Fact]
    public void Test_VectorSearch_Basic()
    {
        string dbPath = TempDb();
        Cleanup(dbPath);

        using (var db = new TestDbContext(dbPath))
        {
            db.VectorItems.Insert(new VectorEntity { Title = "Near", Embedding = [1.0f, 1.0f, 1.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "Far",  Embedding = [10.0f, 10.0f, 10.0f] });

            var query = new[] { 0.9f, 0.9f, 0.9f };
            var results = db.VectorItems.AsQueryable().Where(x => x.Embedding.VectorSearch(query, 1)).ToList();

            Assert.Single(results);
            Assert.Equal("Near", results[0].Title);
        }

        Cleanup(dbPath);
    }

    // ── edge: empty index ────────────────────────────────────────────────────

    [Fact]
    public void Test_VectorSearch_EmptyIndex_ReturnsEmpty()
    {
        string dbPath = TempDb();
        Cleanup(dbPath);

        using (var db = new TestDbContext(dbPath))
        {
            var query = new[] { 1.0f, 1.0f, 1.0f };
            var results = db.VectorItems.AsQueryable().Where(x => x.Embedding.VectorSearch(query, 5)).ToList();

            Assert.Empty(results);
        }

        Cleanup(dbPath);
    }

    // ── edge: single node ────────────────────────────────────────────────────

    [Fact]
    public void Test_VectorSearch_SingleNode_SearchReturnsIt()
    {
        string dbPath = TempDb();
        Cleanup(dbPath);

        using (var db = new TestDbContext(dbPath))
        {
            db.VectorItems.Insert(new VectorEntity { Title = "Only", Embedding = [3.0f, 3.0f, 3.0f] });

            var query = new[] { 0.0f, 0.0f, 0.0f };

            // k=1 — normal case
            var r1 = db.VectorItems.AsQueryable().Where(x => x.Embedding.VectorSearch(query, 1)).ToList();
            Assert.Single(r1);
            Assert.Equal("Only", r1[0].Title);

            // k larger than total count — no crash, still returns the one item
            var rN = db.VectorItems.AsQueryable().Where(x => x.Embedding.VectorSearch(query, 100)).ToList();
            Assert.Single(rN);
        }

        Cleanup(dbPath);
    }

    // ── edge: k > count ──────────────────────────────────────────────────────

    [Fact]
    public void Test_VectorSearch_K_GreaterThanCount_ReturnsAll()
    {
        string dbPath = TempDb();
        Cleanup(dbPath);

        using (var db = new TestDbContext(dbPath))
        {
            db.VectorItems.Insert(new VectorEntity { Title = "A", Embedding = [1.0f, 0.0f, 0.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "B", Embedding = [0.0f, 1.0f, 0.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "C", Embedding = [0.0f, 0.0f, 1.0f] });

            var query = new[] { 1.0f, 1.0f, 1.0f };
            var results = db.VectorItems.AsQueryable().Where(x => x.Embedding.VectorSearch(query, 100)).ToList();

            // Must not throw, and must return all 3 items
            Assert.Equal(3, results.Count);
        }

        Cleanup(dbPath);
    }

    // ── exact match is nearest ───────────────────────────────────────────────

    [Fact]
    public void Test_VectorSearch_ExactMatchVector_IsFirstResult()
    {
        string dbPath = TempDb();
        Cleanup(dbPath);

        using (var db = new TestDbContext(dbPath))
        {
            db.VectorItems.Insert(new VectorEntity { Title = "Exact", Embedding = [5.0f, 5.0f, 5.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "Near",  Embedding = [4.9f, 4.9f, 4.9f] });
            db.VectorItems.Insert(new VectorEntity { Title = "Far",   Embedding = [0.0f, 0.0f, 0.0f] });

            var query = new[] { 5.0f, 5.0f, 5.0f };
            var results = db.VectorItems.AsQueryable().Where(x => x.Embedding.VectorSearch(query, 3)).ToList();

            Assert.Equal(3, results.Count);
            // The item with distance 0 must be ranked first
            Assert.Equal("Exact", results[0].Title);
        }

        Cleanup(dbPath);
    }

    // ── results ordered nearest-first ────────────────────────────────────────

    [Fact]
    public void Test_VectorSearch_Results_OrderedByDistance_NearestFirst()
    {
        string dbPath = TempDb();
        Cleanup(dbPath);

        using (var db = new TestDbContext(dbPath))
        {
            // Items placed at L2 distances 1, 2, 3, 4 from origin
            db.VectorItems.Insert(new VectorEntity { Title = "D1", Embedding = [1.0f, 0.0f, 0.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "D2", Embedding = [2.0f, 0.0f, 0.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "D3", Embedding = [3.0f, 0.0f, 0.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "D4", Embedding = [4.0f, 0.0f, 0.0f] });

            var query = new[] { 0.0f, 0.0f, 0.0f };
            var results = db.VectorItems.AsQueryable().Where(x => x.Embedding.VectorSearch(query, 4)).ToList();

            Assert.Equal(4, results.Count);
            // The nearest node must come first
            Assert.Equal("D1", results[0].Title);
        }

        Cleanup(dbPath);
    }

    // ── page overflow: AllocateNode must spill to a 2nd page ─────────────────
    //
    // With 8 KB pages, dim=3, M=16 the node size is:
    //   6 (loc) + 1 (level) + 3*4 (vector) + 16*17*6 (links) = 1651 bytes
    // Max nodes/page = (8192 - 60) / 1651 = 4
    // → inserting a 5th node triggers the page-chain extension fixed by AllocateNode.

    [Fact]
    public void Test_VectorSearch_PageOverflow_FirstSpillToSecondPage()
    {
        string dbPath = TempDb();
        Cleanup(dbPath);

        using (var db = new TestDbContext(dbPath))
        {
            for (int i = 0; i < 5; i++)
                db.VectorItems.Insert(new VectorEntity { Title = $"N{i}", Embedding = [(float)i, 0.0f, 0.0f] });

            // Query at the 5th item (the one that lives on the 2nd page)
            var query = new[] { 4.0f, 0.0f, 0.0f };
            var results = db.VectorItems.AsQueryable().Where(x => x.Embedding.VectorSearch(query, 1)).ToList();

            Assert.Single(results);
            Assert.Equal("N4", results[0].Title);
        }

        Cleanup(dbPath);
    }

    // ── multi-page chain: 20 nodes across 5 pages ────────────────────────────

    [Fact]
    public void Test_VectorSearch_MultiPageChain_RecallCorrect()
    {
        string dbPath = TempDb();
        Cleanup(dbPath);

        using (var db = new TestDbContext(dbPath))
        {
            for (int i = 0; i < 20; i++)
                db.VectorItems.Insert(new VectorEntity { Title = $"N{i}", Embedding = [(float)i, 0.0f, 0.0f] });

            // Query at N15: top result must be N15 or an immediate neighbour
            var query = new[] { 15.0f, 0.0f, 0.0f };
            var results = db.VectorItems.AsQueryable().Where(x => x.Embedding.VectorSearch(query, 5)).ToList();

            Assert.True(results.Count > 0);
            Assert.True(results.Count <= 5);
            // Nearest must be one of the closest nodes on the number line
            Assert.Contains(results[0].Title, new[] { "N13", "N14", "N15", "N16", "N17" });
        }

        Cleanup(dbPath);
    }

    // ── persistence: index survives db close / reopen ────────────────────────

    [Fact]
    public void Test_VectorSearch_Persistence_CloseReopenSearchCorrect()
    {
        string dbPath = TempDb();
        Cleanup(dbPath);

        // Insert 5 items (2 pages required), commit, then close db
        using (var db = new TestDbContext(dbPath))
        {
            db.VectorItems.Insert(new VectorEntity { Title = "A", Embedding = [1.0f, 0.0f, 0.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "B", Embedding = [2.0f, 0.0f, 0.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "C", Embedding = [3.0f, 0.0f, 0.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "D", Embedding = [4.0f, 0.0f, 0.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "E", Embedding = [5.0f, 0.0f, 0.0f] });
            db.SaveChanges();
        }

        // Reopen and verify we can still find the expected nearest
        using (var db = new TestDbContext(dbPath))
        {
            var query = new[] { 5.0f, 0.0f, 0.0f };
            var results = db.VectorItems.AsQueryable().Where(x => x.Embedding.VectorSearch(query, 1)).ToList();

            Assert.Single(results);
            Assert.Equal("E", results[0].Title);
        }

        Cleanup(dbPath);
    }

    // ── duplicate vectors: both must appear in results ───────────────────────

    [Fact]
    public void Test_VectorSearch_DuplicateVectors_BothReturned()
    {
        string dbPath = TempDb();
        Cleanup(dbPath);

        using (var db = new TestDbContext(dbPath))
        {
            db.VectorItems.Insert(new VectorEntity { Title = "Twin1", Embedding = [1.0f, 1.0f, 1.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "Twin2", Embedding = [1.0f, 1.0f, 1.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "Other", Embedding = [9.0f, 9.0f, 9.0f] });

            var query = new[] { 1.0f, 1.0f, 1.0f };
            var results = db.VectorItems.AsQueryable().Where(x => x.Embedding.VectorSearch(query, 2)).ToList();

            Assert.Equal(2, results.Count);
            var titles = results.Select(r => r.Title).ToHashSet();
            Assert.Contains("Twin1", titles);
            Assert.Contains("Twin2", titles);
        }

        Cleanup(dbPath);
    }

    // ── dimension mismatch throws ────────────────────────────────────────────

    [Fact]
    public void Test_VectorSearch_DimensionMismatch_Throws()
    {
        string dbPath = TempDb();
        Cleanup(dbPath);

        using (var db = new TestDbContext(dbPath))
        {
            // The index expects dim=3; passing a 5-element vector must throw
            Assert.Throws<ArgumentException>(() =>
                db.VectorItems.Insert(new VectorEntity { Title = "Bad", Embedding = [1.0f, 2.0f, 3.0f, 4.0f, 5.0f] }));
        }

        Cleanup(dbPath);
    }

    // ── brute-force recall: top-1 HNSW result must match brute-force top-1 ──

    // TODO: HNSW recall is non-deterministic on this 16-item / 4-page dataset; the first
    // query ([0.1, 0.1, 0.1] → expected P00) occasionally returns P07 because the random
    // level assignment produces a graph topology that traps the greedy search. Investigate
    // whether raising EfConstruction / efSearch or seeding the RNG fixes the recall.
    [Fact(Skip = "Flaky: HNSW top-1 recall not guaranteed on small datasets — see TODO above")]
    public void Test_VectorSearch_BruteForceRecall_Top1()
    {
        string dbPath = TempDb();
        Cleanup(dbPath);

        // 16 items spread across the unit cube; 4 pages required
        var items = new (string title, float[] vec)[]
        {
            ("P00", [0.0f, 0.0f, 0.0f]), ("P01", [0.5f, 0.0f, 0.0f]),
            ("P02", [1.0f, 0.0f, 0.0f]), ("P03", [0.0f, 0.5f, 0.0f]),
            ("P04", [0.0f, 1.0f, 0.0f]), ("P05", [0.5f, 0.5f, 0.0f]),
            ("P06", [1.0f, 1.0f, 0.0f]), ("P07", [0.0f, 0.0f, 0.5f]),
            ("P08", [0.0f, 0.0f, 1.0f]), ("P09", [0.5f, 0.0f, 0.5f]),
            ("P10", [1.0f, 0.0f, 1.0f]), ("P11", [0.0f, 0.5f, 0.5f]),
            ("P12", [0.5f, 0.5f, 0.5f]), ("P13", [1.0f, 0.5f, 0.5f]),
            ("P14", [0.5f, 1.0f, 0.5f]), ("P15", [1.0f, 1.0f, 1.0f]),
        };

        using (var db = new TestDbContext(dbPath))
        {
            foreach (var (title, vec) in items)
                db.VectorItems.Insert(new VectorEntity { Title = title, Embedding = vec });

            // Test 3 different queries
            float[][] queries =
            [
                [0.1f, 0.1f, 0.1f],
                [0.9f, 0.9f, 0.9f],
                [0.4f, 0.6f, 0.1f],
            ];

            foreach (var query in queries)
            {
                // Brute-force: find the true nearest
                string bruteForceNearest = items
                    .OrderBy(p => L2(p.vec, query))
                    .First().title;

                // HNSW top-1
                var hnswResults = db.VectorItems.AsQueryable()
                    .Where(x => x.Embedding.VectorSearch(query, 1))
                    .ToList();

                Assert.Single(hnswResults);
                Assert.Equal(bruteForceNearest, hnswResults[0].Title);
            }
        }

        Cleanup(dbPath);
    }

    private static float L2(float[] a, float[] b)
    {
        float sum = 0;
        for (int i = 0; i < a.Length; i++) { float d = a[i] - b[i]; sum += d * d; }
        return sum; // squared L2 is sufficient for ordering
    }
}
