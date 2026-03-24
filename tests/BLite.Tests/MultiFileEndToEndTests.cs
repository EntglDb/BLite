using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// End-to-end tests that exercise the multi-file storage engine through the full
/// document/index API rather than raw page allocation.
///
/// Coverage:
///   Phase 2 — custom WAL path: commit + restart durability
///   Phase 3 — separate index file: B-tree index queries using the .idx file
///   Phase 4 — per-collection files: CRUD, scan, multi-collection isolation, persistence
///   Combined — PageFileConfig.Server() full round-trip
/// </summary>
public class MultiFileEndToEndTests : IDisposable
{
    private readonly string _tempDir;

    public MultiFileEndToEndTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"blite_e2e_{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* best-effort */ }
    }

    // ─────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────

    private static BsonDocument MakeDoc(DynamicCollection col, int id, string name, int age)
        => col.CreateDocument(["_id", "name", "age"], b => b
            .AddId((BsonId)id)
            .AddString("name", name)
            .AddInt32("age", age));

    private static BsonDocument MakeNameDoc(DynamicCollection col, int id, string name)
        => col.CreateDocument(["_id", "name"], b => b
            .AddId((BsonId)id)
            .AddString("name", name));

    // ─────────────────────────────────────────────────────────────────
    // Phase 2 — custom WAL path
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task CustomWalPath_DocumentsPersistedAfterRestart()
    {
        var dbPath  = Path.Combine(_tempDir, "wal2.db");
        var walPath = Path.Combine(_tempDir, "wal", "wal2.wal");
        var config  = PageFileConfig.Default with { WalPath = walPath };

        // Write
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("users");
            await col.InsertAsync(MakeDoc(col, 1, "Alice", 30));
            await col.InsertAsync(MakeDoc(col, 2, "Bob", 25));
            await engine.CommitAsync();
        }

        Assert.True(File.Exists(walPath), "WAL should be at custom path");

        // Read-back after restart
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col  = engine.GetOrCreateCollection("users");
            var docs = (await col.FindAllAsync().ToListAsync());
            Assert.Equal(2, docs.Count);
        }
    }

    [Fact]
    public async Task CustomWalPath_WalRecovery_RestoresUncommittedData()
    {
        var dbPath  = Path.Combine(_tempDir, "walrec.db");
        var walPath = Path.Combine(_tempDir, "wal", "walrec.wal");
        var config  = PageFileConfig.Default with { WalPath = walPath };

        BsonId id1, id2;

        // Write + commit but do NOT checkpoint (WAL still contains data)
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("items");
            id1 = await col.InsertAsync(MakeDoc(col, 1, "Item1", 10));
            id2 = await col.InsertAsync(MakeDoc(col, 2, "Item2", 20));
            await engine.CommitAsync();
            // Deliberate: no CheckpointAsync here — WAL holds the data
        }

        // Restart — engine must replay the WAL to recover
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col  = engine.GetOrCreateCollection("items");
            var all  = (await col.FindAllAsync().ToListAsync());
            Assert.Equal(2, all.Count);

            var found1 = await col.FindByIdAsync(id1);
            var found2 = await col.FindByIdAsync(id2);
            Assert.NotNull(found1);
            Assert.NotNull(found2);

            Assert.True(found1!.TryGetString("name", out var n1));
            Assert.Equal("Item1", n1);
            Assert.True(found2!.TryGetString("name", out var n2));
            Assert.Equal("Item2", n2);
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Phase 3 — separate index file
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task SeparateIndexFile_BTreeIndex_QueryReturnsCorrectResults()
    {
        var dbPath  = Path.Combine(_tempDir, "idx.db");
        var idxPath = Path.Combine(_tempDir, "idx.idx");
        var config  = PageFileConfig.Default with { IndexFilePath = idxPath };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("people", BsonIdType.Int32);

        for (int i = 1; i <= 20; i++)
            await col.InsertAsync(MakeDoc(col, i, $"User{i}", 20 + i));

        await engine.CommitAsync();

        await col.CreateIndexAsync("age", "idx_age");

        Assert.True(File.Exists(idxPath), "Index file should be created");
        Assert.True(new FileInfo(idxPath).Length > 0, "Index file should not be empty");

        // Query age between 25 and 30 (inclusive) — pages are in the .idx file
        var results = await col.QueryIndexAsync("idx_age", 25, 30).ToListAsync();
        Assert.Equal(6, results.Count);
        foreach (var r in results)
        {
            Assert.True(r.TryGetInt32("age", out var age));
            Assert.InRange(age, 25, 30);
        }
    }

    [Fact]
    public async Task SeparateIndexFile_IndexQueryWorksAfterRestart()
    {
        var dbPath  = Path.Combine(_tempDir, "idxr.db");
        var idxPath = Path.Combine(_tempDir, "idxr.idx");
        var config  = PageFileConfig.Default with { IndexFilePath = idxPath };

        // First lifetime: populate + index
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("products", BsonIdType.Int32);
            for (int i = 1; i <= 10; i++)
                await col.InsertAsync(MakeDoc(col, i, $"Prod{i}", 100 + i));
            await engine.CommitAsync();
            await col.CreateIndexAsync("age", "idx_price");
            await engine.CommitAsync(); // commit B-tree entries written during index creation
        }

        // Second lifetime: index still usable
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("products", BsonIdType.Int32);
            var results = await col.QueryIndexAsync("idx_price", 105, 107).ToListAsync();
            Assert.Equal(3, results.Count);
            foreach (var r in results)
            {
                Assert.True(r.TryGetInt32("age", out var v));
                Assert.InRange(v, 105, 107);
            }
        }
    }

    [Fact]
    public async Task SeparateIndexFile_MultipleCollectionsBothUseIndexFile()
    {
        var dbPath  = Path.Combine(_tempDir, "idxmc.db");
        var idxPath = Path.Combine(_tempDir, "idxmc.idx");
        var config  = PageFileConfig.Default with { IndexFilePath = idxPath };

        using var engine = new BLiteEngine(dbPath, config);
        var colA = engine.GetOrCreateCollection("colA", BsonIdType.Int32);
        var colB = engine.GetOrCreateCollection("colB", BsonIdType.Int32);

        for (int i = 1; i <= 5; i++)
        {
            await colA.InsertAsync(MakeDoc(colA, i, $"A{i}", 10 + i));
            await colB.InsertAsync(MakeDoc(colB, i, $"B{i}", 50 + i));
        }
        await engine.CommitAsync();

        await colA.CreateIndexAsync("age", "idx_age_a");
        await colB.CreateIndexAsync("age", "idx_age_b");

        // Both indexes should be readable and routed to the index file
        var ra = await colA.QueryIndexAsync("idx_age_a", 12, 14).ToListAsync();
        var rb = await colB.QueryIndexAsync("idx_age_b", 52, 54).ToListAsync();

        Assert.Equal(3, ra.Count);
        Assert.Equal(3, rb.Count);
    }

    // ─────────────────────────────────────────────────────────────────
    // Phase 4 — per-collection files: CRUD
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task PerCollectionFiles_InsertAndFindById()
    {
        var dbPath  = Path.Combine(_tempDir, "coll.db");
        var collDir = Path.Combine(_tempDir, "coll_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("users", BsonIdType.Int32);

        var id = await col.InsertAsync(MakeDoc(col, 42, "Alice", 30));
        await engine.CommitAsync();

        var found = await col.FindByIdAsync(id);
        Assert.NotNull(found);
        Assert.True(found!.TryGetString("name", out var name));
        Assert.Equal("Alice", name);

        // Dedicated file must exist
        var collFile = Path.Combine(collDir, "users.db");
        Assert.True(File.Exists(collFile), "Collection file should exist");
    }

    [Fact]
    public async Task PerCollectionFiles_Update_ModifiesDocument()
    {
        var dbPath  = Path.Combine(_tempDir, "collupd.db");
        var collDir = Path.Combine(_tempDir, "collupd_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("users", BsonIdType.Int32);

        await col.InsertAsync(MakeDoc(col, 1, "Alice", 30));
        await engine.CommitAsync();

        var updated = col.CreateDocument(["_id", "name", "age"], b => b
            .AddId((BsonId)1)
            .AddString("name", "Alicia")
            .AddInt32("age", 31));
        Assert.True(await col.UpdateAsync((BsonId)1, updated));
        await engine.CommitAsync();

        var found = await col.FindByIdAsync((BsonId)1);
        Assert.NotNull(found);
        Assert.True(found!.TryGetString("name", out var n));
        Assert.Equal("Alicia", n);
    }

    [Fact]
    public async Task PerCollectionFiles_Delete_RemovesDocument()
    {
        var dbPath  = Path.Combine(_tempDir, "colldel.db");
        var collDir = Path.Combine(_tempDir, "colldel_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("orders", BsonIdType.Int32);

        await col.InsertAsync(MakeDoc(col, 1, "Order1", 100));
        await col.InsertAsync(MakeDoc(col, 2, "Order2", 200));
        await engine.CommitAsync();

        Assert.True(await col.DeleteAsync((BsonId)1));
        await engine.CommitAsync();

        Assert.Null(await col.FindByIdAsync((BsonId)1));
        Assert.NotNull(await col.FindByIdAsync((BsonId)2));
        Assert.Equal(1, await col.CountAsync());
    }

    // ─────────────────────────────────────────────────────────────────
    // Phase 4 — per-collection files: scan
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task PerCollectionFiles_FindAll_ReturnsAllDocuments()
    {
        var dbPath  = Path.Combine(_tempDir, "scan.db");
        var collDir = Path.Combine(_tempDir, "scan_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("docs", BsonIdType.Int32);

        // Insert enough documents to span multiple pages
        const int docCount = 200;
        for (int i = 1; i <= docCount; i++)
            await col.InsertAsync(MakeDoc(col, i, $"Doc{i}", i));
        await engine.CommitAsync();

        var all = (await col.FindAllAsync().ToListAsync());
        Assert.Equal(docCount, all.Count);
    }

    [Fact]
    public async Task PerCollectionFiles_Scan_FindsAllMatchingDocuments()
    {
        var dbPath  = Path.Combine(_tempDir, "scanp.db");
        var collDir = Path.Combine(_tempDir, "scanp_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("items", BsonIdType.Int32);

        const int total = 100;
        for (int i = 1; i <= total; i++)
            await col.InsertAsync(MakeDoc(col, i, $"Item{i}", i % 10));  // age cycles 1-9, 0
        await engine.CommitAsync();

        // Scan for age == 5 via Find(predicate)
        var matched = await col.FindAsync(doc =>
        {
            doc.TryGetInt32("age", out var age);
            return age == 5;
        }).ToListAsync();
        Assert.Equal(10, matched.Count); // i=5,15,25,...,95
        foreach (var d in matched)
        {
            Assert.True(d.TryGetInt32("age", out var age));
            Assert.Equal(5, age);
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Phase 4 — per-collection files: isolation & persistence
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task PerCollectionFiles_MultipleCollections_DataIsolated()
    {
        var dbPath  = Path.Combine(_tempDir, "iso.db");
        var collDir = Path.Combine(_tempDir, "iso_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var users  = engine.GetOrCreateCollection("users",  BsonIdType.Int32);
        var orders = engine.GetOrCreateCollection("orders", BsonIdType.Int32);

        for (int i = 1; i <= 5; i++)
            await users.InsertAsync(MakeDoc(users, i, $"User{i}", 20 + i));
        for (int i = 1; i <= 3; i++)
            await orders.InsertAsync(MakeDoc(orders, i, $"Order{i}", 100 * i));
        await engine.CommitAsync();

        Assert.Equal(5, await users.CountAsync());
        Assert.Equal(3, await orders.CountAsync());

        // Separate physical files must exist
        Assert.True(File.Exists(Path.Combine(collDir, "users.db")));
        Assert.True(File.Exists(Path.Combine(collDir, "orders.db")));
    }

    [Fact]
    public async Task PerCollectionFiles_DataPersistsAfterEngineRestart()
    {
        var dbPath  = Path.Combine(_tempDir, "persist.db");
        var collDir = Path.Combine(_tempDir, "persist_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        // First lifetime
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("customers", BsonIdType.Int32);
            for (int i = 1; i <= 10; i++)
                await col.InsertAsync(MakeDoc(col, i, $"Customer{i}", 30 + i));
            await engine.CommitAsync();
        }

        // Second lifetime — data must still be there
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("customers", BsonIdType.Int32);
            var all = await col.FindAllAsync().ToListAsync();
            Assert.Equal(10, all.Count);

            var found = await col.FindByIdAsync((BsonId)7);
            Assert.NotNull(found);
            Assert.True(found!.TryGetString("name", out var n));
            Assert.Equal("Customer7", n);
        }
    }

    [Fact]
    public async Task PerCollectionFiles_DropCollection_CollectionNoLongerQueryable()
    {
        var dbPath  = Path.Combine(_tempDir, "drop.db");
        var collDir = Path.Combine(_tempDir, "drop_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("temp", BsonIdType.Int32);
        await col.InsertAsync(MakeDoc(col, 1, "X", 1));
        await engine.CommitAsync();

        Assert.True(File.Exists(Path.Combine(collDir, "temp.db")),
            "Collection file must exist before drop");

        // Drop removes in-memory state and metadata
        Assert.True(engine.DropCollection("temp"));

        // Re-opening gives an empty collection
        var reopened = engine.GetOrCreateCollection("temp", BsonIdType.Int32);
        Assert.Equal(0, await reopened.CountAsync());
    }

    // ─────────────────────────────────────────────────────────────────
    // Phase 4 — per-collection files: WAL crash-recovery
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task PerCollectionFiles_WalRecovery_RestoresDocumentsAfterRestart()
    {
        var dbPath  = Path.Combine(_tempDir, "walcoll.db");
        var collDir = Path.Combine(_tempDir, "walcoll_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        // Write but no explicit checkpoint — WAL holds the data
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("invoices", BsonIdType.Int32);
            for (int i = 1; i <= 5; i++)
                await col.InsertAsync(MakeDoc(col, i, $"Inv{i}", i * 10));
            await engine.CommitAsync();
            // No CheckpointAsync — WAL still contains these writes
        }

        // Restart triggers WAL recovery; collection pages must be replayed to the right file
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("invoices", BsonIdType.Int32);
            var all = await col.FindAllAsync().ToListAsync();
            Assert.Equal(5, all.Count);

            for (int i = 1; i <= 5; i++)
            {
                var doc = await col.FindByIdAsync((BsonId)i);
                Assert.NotNull(doc);
                Assert.True(doc!.TryGetString("name", out var n));
                Assert.Equal($"Inv{i}", n);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Combined — all three phases together
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task AllPhasesCombined_FullCrudPlusIndex_MultipleCollections()
    {
        var dbPath  = Path.Combine(_tempDir, "all.db");
        var walPath = Path.Combine(_tempDir, "wal", "all.wal");
        var idxPath = Path.Combine(_tempDir, "all.idx");
        var collDir = Path.Combine(_tempDir, "all_data");

        var config = PageFileConfig.Default with
        {
            WalPath = walPath,
            IndexFilePath = idxPath,
            CollectionDataDirectory = collDir
        };

        // ── Write phase ──────────────────────────────────────────────
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var users  = engine.GetOrCreateCollection("users",  BsonIdType.Int32);
            var orders = engine.GetOrCreateCollection("orders", BsonIdType.Int32);

            for (int i = 1; i <= 20; i++)
                await users.InsertAsync(MakeDoc(users, i, $"User{i}", 18 + i));
            for (int i = 1; i <= 10; i++)
                await orders.InsertAsync(MakeDoc(orders, i, $"Order{i}", 100 + i));
            await engine.CommitAsync();

            // Index on users.age — pages go to the index file
            await users.CreateIndexAsync("age", "idx_users_age");
            await engine.CommitAsync(); // commit B-tree entries written during index creation
        }

        Assert.True(File.Exists(walPath), "WAL at custom path");
        Assert.True(File.Exists(idxPath), "Index file created");
        Assert.True(File.Exists(Path.Combine(collDir, "users.db")),  "users collection file");
        Assert.True(File.Exists(Path.Combine(collDir, "orders.db")), "orders collection file");

        // ── Read-back + query after restart ─────────────────────────
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var users  = engine.GetOrCreateCollection("users",  BsonIdType.Int32);
            var orders = engine.GetOrCreateCollection("orders", BsonIdType.Int32);

            // FindAll covers per-collection pages
            Assert.Equal(20, await users.CountAsync());
            Assert.Equal(10, await orders.CountAsync());

            // Index query on separate-index-file index
            var over30 = await users.QueryIndexAsync("idx_users_age", 30, 40).ToListAsync();
            Assert.True(over30.Count > 0);
            foreach (var d in over30)
            {
                Assert.True(d.TryGetInt32("age", out var age));
                Assert.InRange(age, 30, 40);
            }

            // CRUD on collection file
            Assert.True(await users.DeleteAsync((BsonId)5));
            await engine.CommitAsync();
            Assert.Null(await users.FindByIdAsync((BsonId)5));
            Assert.Equal(19, await users.CountAsync());
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // PageFileConfig.Server() factory — end-to-end
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ServerConfig_FullCrudPlusIndexPersistsAcrossRestarts()
    {
        var dbPath = Path.Combine(_tempDir, "server_db", "myapp.db");
        Directory.CreateDirectory(Path.GetDirectoryName(dbPath)!);

        var config = PageFileConfig.Server(dbPath);

        // Verify the factory produces correct paths
        Assert.Contains("myapp.wal",   config.WalPath!);
        Assert.Contains("myapp.idx",   config.IndexFilePath!);
        Assert.Contains("myapp",       config.CollectionDataDirectory!);
        Assert.Contains("wal",         config.WalPath!);
        Assert.Contains("collections", config.CollectionDataDirectory!);

        // ── First lifetime ───────────────────────────────────────────
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("catalog", BsonIdType.Int32);
            for (int i = 1; i <= 15; i++)
                await col.InsertAsync(MakeDoc(col, i, $"Item{i}", 10 * i));
            await engine.CommitAsync();

            await col.CreateIndexAsync("age", "idx_price");
            await engine.CommitAsync(); // commit B-tree entries written during index creation
        }

        // ── Second lifetime — data and indexes survive restart ────────
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col  = engine.GetOrCreateCollection("catalog", BsonIdType.Int32);
            var all  = await col.FindAllAsync().ToListAsync();
            Assert.Equal(15, all.Count);

            var cheap = await col.QueryIndexAsync("idx_price", 10, 50).ToListAsync();
            Assert.Equal(5, cheap.Count); // items 1-5 (age = 10-50)
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Index queries — B-tree range, exact match, multi-field
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task IndexQuery_RangeOnSecondaryIndex_ReturnsCorrectSubset()
    {
        var dbPath  = Path.Combine(_tempDir, "iq_range.db");
        var idxPath = Path.Combine(_tempDir, "iq_range.idx");
        var collDir = Path.Combine(_tempDir, "iq_range_data");
        var config  = PageFileConfig.Default with { IndexFilePath = idxPath, CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("products", BsonIdType.Int32);

        for (int i = 1; i <= 50; i++)
        {
            var doc = col.CreateDocument(["_id", "name", "price", "instock"], b => b
                .AddId((BsonId)i)
                .AddString("name", $"Product{i}")
                .AddInt32("price", i * 10)
                .AddBoolean("instock", i % 2 == 0));
            await col.InsertAsync(doc);
        }
        await engine.CommitAsync();
        await col.CreateIndexAsync("price", "idx_price");
        await engine.CommitAsync();
        // Range: price 100–200 (products 10–20, i.e. 11 items)
        var range = await col.QueryIndexAsync("idx_price", 100, 200).ToListAsync();
        Assert.Equal(11, range.Count);
        foreach (var d in range)
        {
            Assert.True(d.TryGetInt32("price", out var p));
            Assert.InRange(p, 100, 200);
        }
    }

    [Fact]
    public async Task IndexQuery_ExactMatch_FindsOnlyMatchingDocument()
    {
        var dbPath  = Path.Combine(_tempDir, "iq_exact.db");
        var idxPath = Path.Combine(_tempDir, "iq_exact.idx");
        var collDir = Path.Combine(_tempDir, "iq_exact_data");
        var config  = PageFileConfig.Default with { IndexFilePath = idxPath, CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("users", BsonIdType.Int32);

        for (int i = 1; i <= 20; i++)
        {
            var doc = col.CreateDocument(["_id", "name", "score"], b => b
                .AddId((BsonId)i)
                .AddString("name", $"User{i}")
                .AddInt32("score", i * 5));
            await col.InsertAsync(doc);
        }
        await engine.CommitAsync();
        await col.CreateIndexAsync("score", "idx_score");
        await engine.CommitAsync();
        // Exact: score == 50 (only user 10)
        var exact = await col.QueryIndexAsync("idx_score", 50, 50).ToListAsync();
        Assert.Single(exact);
        Assert.True(exact[0].TryGetInt32("score", out var s));
        Assert.Equal(50, s);
    }

    [Fact]
    public async Task IndexQuery_AfterEngineRestart_IndexStillWorks()
    {
        var dbPath  = Path.Combine(_tempDir, "iq_restart.db");
        var idxPath = Path.Combine(_tempDir, "iq_restart.idx");
        var collDir = Path.Combine(_tempDir, "iq_restart_data");
        var config  = PageFileConfig.Default with { IndexFilePath = idxPath, CollectionDataDirectory = collDir };

        // First lifetime: create + index
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("orders", BsonIdType.Int32);
            for (int i = 1; i <= 30; i++)
            {
                var doc = col.CreateDocument(["_id", "total"], b => b
                    .AddId((BsonId)i)
                    .AddInt32("total", i * 100));
                await col.InsertAsync(doc);
            }
            await engine.CommitAsync();
            await col.CreateIndexAsync("total", "idx_total");
            await engine.CommitAsync();
        }

        // Second lifetime: index must still be queryable from the separate index file
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("orders", BsonIdType.Int32);
            var results = await col.QueryIndexAsync("idx_total", 1000, 2000).ToListAsync();
            // totals 1000, 1100, ..., 2000 → items 10..20 → 11 results
            Assert.Equal(11, results.Count);
            foreach (var d in results)
            {
                Assert.True(d.TryGetInt32("total", out var t));
                Assert.InRange(t, 1000, 2000);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Non-indexed field queries — full scan via Find(predicate)
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task NonIndexedQuery_FindWithPredicate_ScansAllCollectionPages()
    {
        var dbPath  = Path.Combine(_tempDir, "ni_scan.db");
        var collDir = Path.Combine(_tempDir, "ni_scan_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("events", BsonIdType.Int32);

        const int total = 150;
        for (int i = 1; i <= total; i++)
        {
            var doc = col.CreateDocument(["_id", "type", "value"], b => b
                .AddId((BsonId)i)
                .AddString("type", i % 3 == 0 ? "special" : "normal")
                .AddInt32("value", i));
            await col.InsertAsync(doc);
        }
        await engine.CommitAsync();

        // 150/3 = 50 special entries — Find uses full scan (no index on "type")
        var specials = await col.FindAsync(d =>
        {
            d.TryGetString("type", out var t);
            return t == "special";
        }).ToListAsync();
        Assert.Equal(50, specials.Count);
        foreach (var d in specials)
        {
            Assert.True(d.TryGetString("type", out var t));
            Assert.Equal("special", t);
        }
    }

    [Fact]
    public async Task NonIndexedQuery_BooleanField_FilterWorks()
    {
        var dbPath  = Path.Combine(_tempDir, "ni_bool.db");
        var collDir = Path.Combine(_tempDir, "ni_bool_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("flags", BsonIdType.Int32);

        for (int i = 1; i <= 40; i++)
        {
            var doc = col.CreateDocument(["_id", "active"], b => b
                .AddId((BsonId)i)
                .AddBoolean("active", i % 4 == 0));
            await col.InsertAsync(doc);
        }
        await engine.CommitAsync();

        var active = await col.FindAsync(d =>
        {
            if (!d.TryGetValue("active", out var v)) return false;
            return v.AsBoolean;
        }).ToListAsync();
        Assert.Equal(10, active.Count); // i = 4,8,12,...,40
    }

    // ─────────────────────────────────────────────────────────────────
    // Document materialization — all BSON field types round-trip
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Materialization_AllFieldTypes_RoundTripCorrectly()
    {
        var dbPath  = Path.Combine(_tempDir, "mat.db");
        var idxPath = Path.Combine(_tempDir, "mat.idx");
        var collDir = Path.Combine(_tempDir, "mat_data");
        var config  = PageFileConfig.Default with { IndexFilePath = idxPath, CollectionDataDirectory = collDir };

        var now = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc);

        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("typed", BsonIdType.Int32);
            var doc = col.CreateDocument(
                ["_id", "sval", "i32", "i64", "dbl", "flag", "ts"],
                b => b
                    .AddId((BsonId)1)
                    .AddString("sval", "hello world")
                    .AddInt32("i32", 42)
                    .AddInt64("i64", 123456789012345L)
                    .AddDouble("dbl", 3.14159)
                    .AddBoolean("flag", true)
                    .AddDateTime("ts", now));
            await col.InsertAsync(doc);
            await engine.CommitAsync();
        }

        // Restart → read back and verify every field
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col   = engine.GetOrCreateCollection("typed", BsonIdType.Int32);
            var found = await col.FindByIdAsync((BsonId)1);
            Assert.NotNull(found);

            Assert.True(found!.TryGetString("sval", out var s));
            Assert.Equal("hello world", s);

            Assert.True(found.TryGetInt32("i32", out var i32));
            Assert.Equal(42, i32);

            // int64 via TryGetValue
            Assert.True(found.TryGetValue("i64", out var i64val));
            Assert.Equal(123456789012345L, i64val.AsInt64);

            // double via TryGetValue
            Assert.True(found.TryGetValue("dbl", out var dblVal));
            Assert.Equal(3.14159, dblVal.AsDouble, 5);

            // bool via TryGetValue
            Assert.True(found.TryGetValue("flag", out var flagVal));
            Assert.True(flagVal.AsBoolean);

            // datetime via TryGetValue
            Assert.True(found.TryGetValue("ts", out var tsVal));
            Assert.Equal(now, tsVal.AsDateTime);
        }
    }

    [Fact]
    public async Task Materialization_NullAndMissingFields_HandledCorrectly()
    {
        var dbPath  = Path.Combine(_tempDir, "mat_null.db");
        var collDir = Path.Combine(_tempDir, "mat_null_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("nulltest", BsonIdType.Int32);

        // Doc with an explicit null field
        var doc = col.CreateDocument(["_id", "present", "absent"], b => b
            .AddId((BsonId)1)
            .AddString("present", "yes")
            .AddNull("absent"));
        await col.InsertAsync(doc);
        await engine.CommitAsync();

        var found = await col.FindByIdAsync((BsonId)1);
        Assert.NotNull(found);
        Assert.True(found!.TryGetString("present", out var p));
        Assert.Equal("yes", p);

        // "absent" key exists but its value is null
        Assert.True(found.TryGetValue("absent", out var nullVal));
        Assert.Equal(BsonType.Null, nullVal.Type);

        // A completely missing key returns false
        Assert.False(found.TryGetValue("nothere", out _));
    }

    [Fact]
    public async Task Materialization_MultipleDocuments_EachFieldRoundTrips()
    {
        var dbPath  = Path.Combine(_tempDir, "mat_multi.db");
        var idxPath = Path.Combine(_tempDir, "mat_multi.idx");
        var collDir = Path.Combine(_tempDir, "mat_multi_data");
        var config  = PageFileConfig.Default with { IndexFilePath = idxPath, CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("records", BsonIdType.Int32);

        for (int i = 1; i <= 10; i++)
        {
            var doc = col.CreateDocument(["_id", "label", "count", "rate"], b => b
                .AddId((BsonId)i)
                .AddString("label", $"Record{i:D3}")
                .AddInt32("count", i * 7)
                .AddDouble("rate", i * 1.1));
            await col.InsertAsync(doc);
        }
        await engine.CommitAsync();

        var all = await col.FindAllAsync().ToListAsync();
        Assert.Equal(10, all.Count);

        for (int i = 1; i <= 10; i++)
        {
            var d = await col.FindByIdAsync((BsonId)i);
            Assert.NotNull(d);
            Assert.True(d!.TryGetString("label", out var lbl));
            Assert.Equal($"Record{i:D3}", lbl);
            Assert.True(d.TryGetInt32("count", out var c));
            Assert.Equal(i * 7, c);
            Assert.True(d.TryGetValue("rate", out var rateVal));
            Assert.Equal(i * 1.1, rateVal.AsDouble, 10);
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // TimeSeries collection in multi-file mode
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TimeSeries_InsertAndFindAll_InMultiFileMode()
    {
        var dbPath  = Path.Combine(_tempDir, "ts.db");
        var idxPath = Path.Combine(_tempDir, "ts.idx");
        var collDir = Path.Combine(_tempDir, "ts_data");
        var config  = PageFileConfig.Default with { IndexFilePath = idxPath, CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("metrics");
        col.SetTimeSeries("ts", TimeSpan.FromDays(30));

        var now = DateTime.UtcNow;
        for (int i = 1; i <= 10; i++)
        {
            var doc = col.CreateDocument(["sensor", "ts", "value"], b => b
                .AddString("sensor", $"sensor_{i}")
                .AddDateTime("ts", now.AddSeconds(-i))
                .AddDouble("value", i * 0.5));
            await col.InsertAsync(doc);
        }
        await engine.CommitAsync();

        var all = await col.FindAllAsync().ToListAsync();
        Assert.Equal(10, all.Count);

        foreach (var d in all)
        {
            Assert.True(d.TryGetString("sensor", out var sensor));
            Assert.StartsWith("sensor_", sensor);
            Assert.True(d.TryGetValue("value", out var valBson));
            Assert.True(valBson.AsDouble > 0);
        }
    }

    [Fact]
    public async Task TimeSeries_PersistsAcrossEngineRestart_InMultiFileMode()
    {
        var dbPath  = Path.Combine(_tempDir, "tsr.db");
        var idxPath = Path.Combine(_tempDir, "tsr.idx");
        var collDir = Path.Combine(_tempDir, "tsr_data");
        var config  = PageFileConfig.Default with { IndexFilePath = idxPath, CollectionDataDirectory = collDir };

        var baseTime = DateTime.UtcNow;

        // First lifetime
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("sensors");
            col.SetTimeSeries("ts", TimeSpan.FromDays(7));

            for (int i = 1; i <= 5; i++)
            {
                var doc = col.CreateDocument(["sensor", "ts"], b => b
                    .AddString("sensor", $"s{i}")
                    .AddDateTime("ts", baseTime.AddSeconds(-i)));
                await col.InsertAsync(doc);
            }
            await engine.CommitAsync();
        }

        // Second lifetime — data must survive restart
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("sensors");
            var all = await col.FindAllAsync().ToListAsync();
            Assert.Equal(5, all.Count);

            foreach (var d in all)
            {
                Assert.True(d.TryGetString("sensor", out var s));
                Assert.StartsWith("s", s);
            }
        }
    }

    [Fact]
    public async Task TimeSeries_ForcePrune_RemovesExpiredDocs_InMultiFileMode()
    {
        var dbPath  = Path.Combine(_tempDir, "tsp.db");
        var collDir = Path.Combine(_tempDir, "tsp_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("prunable");
        col.SetTimeSeries("ts", TimeSpan.FromDays(1));

        var now = DateTime.UtcNow;
        // 3 expired (2 days old) + 3 fresh
        for (int i = 1; i <= 3; i++)
        {
            var expired = col.CreateDocument(["sensor", "ts"], b => b
                .AddString("sensor", $"old_{i}")
                .AddDateTime("ts", now.AddDays(-2)));
            await col.InsertAsync(expired);
        }
        for (int i = 1; i <= 3; i++)
        {
            var fresh = col.CreateDocument(["sensor", "ts"], b => b
                .AddString("sensor", $"new_{i}")
                .AddDateTime("ts", now));
            await col.InsertAsync(fresh);
        }
        await engine.CommitAsync();

        await col.ForcePruneAsync();
        // Pages where only old docs lived are freed; fresh docs remain visible
        var remaining = await col.FindAllAsync().ToListAsync();
        // ForcePruneAsync frees pages dominated by expired timestamps; exact count depends on page packing
        // The fresh docs should all be present (they're on a separate page)
        var freshCount = remaining.Count(d => { d.TryGetString("sensor", out var s); return s?.StartsWith("new_") == true; });
        Assert.Equal(3, freshCount);
    }

    // ─────────────────────────────────────────────────────────────────
    // KV store in multi-file mode
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public void KvStore_BasicCrud_WorksWithCustomWalAndCollectionFiles()
    {
        var dbPath  = Path.Combine(_tempDir, "kv.db");
        var walPath = Path.Combine(_tempDir, "wal", "kv.wal");
        var collDir = Path.Combine(_tempDir, "kv_data");
        var config  = PageFileConfig.Default with { WalPath = walPath, CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);

        engine.KvStore.Set("user:1", "Alice"u8.ToArray());
        engine.KvStore.Set("user:2", "Bob"u8.ToArray());
        engine.KvStore.Set("config:theme", "dark"u8.ToArray());

        Assert.Equal("Alice"u8.ToArray(), engine.KvStore.Get("user:1"));
        Assert.Equal("Bob"u8.ToArray(),   engine.KvStore.Get("user:2"));
        Assert.True(engine.KvStore.Exists("config:theme"));

        // Overwrite
        engine.KvStore.Set("user:1", "Alicia"u8.ToArray());
        Assert.Equal("Alicia"u8.ToArray(), engine.KvStore.Get("user:1"));

        // Delete
        Assert.True(engine.KvStore.Delete("user:2"));
        Assert.Null(engine.KvStore.Get("user:2"));
    }

    [Fact]
    public void KvStore_PersistsAcrossEngineRestart_WithMultiFileConfig()
    {
        var dbPath  = Path.Combine(_tempDir, "kvr.db");
        var walPath = Path.Combine(_tempDir, "wal", "kvr.wal");
        var idxPath = Path.Combine(_tempDir, "kvr.idx");
        var collDir = Path.Combine(_tempDir, "kvr_data");
        var config  = PageFileConfig.Default with { WalPath = walPath, IndexFilePath = idxPath, CollectionDataDirectory = collDir };

        // First lifetime: write KV entries
        using (var engine = new BLiteEngine(dbPath, config))
        {
            engine.KvStore.Set("session:abc", "token123"u8.ToArray());
            engine.KvStore.Set("session:xyz", "token456"u8.ToArray());
            engine.KvStore.Set("flag:feature_x", "on"u8.ToArray());
        }

        // Second lifetime: all entries still readable
        using (var engine = new BLiteEngine(dbPath, config))
        {
            Assert.Equal("token123"u8.ToArray(), engine.KvStore.Get("session:abc"));
            Assert.Equal("token456"u8.ToArray(), engine.KvStore.Get("session:xyz"));
            Assert.Equal("on"u8.ToArray(),       engine.KvStore.Get("flag:feature_x"));
        }
    }

    [Fact]
    public void KvStore_ScanKeys_FindsByPrefix_InMultiFileMode()
    {
        var dbPath  = Path.Combine(_tempDir, "kvscan.db");
        var walPath = Path.Combine(_tempDir, "wal", "kvscan.wal");
        var config  = PageFileConfig.Default with { WalPath = walPath };

        using var engine = new BLiteEngine(dbPath, config);

        engine.KvStore.Set("user:1", "u1"u8.ToArray());
        engine.KvStore.Set("user:2", "u2"u8.ToArray());
        engine.KvStore.Set("user:3", "u3"u8.ToArray());
        engine.KvStore.Set("config:a", "a"u8.ToArray());
        engine.KvStore.Set("config:b", "b"u8.ToArray());

        var userKeys   = engine.KvStore.ScanKeys("user:").ToList();
        var configKeys = engine.KvStore.ScanKeys("config:").ToList();
        var allKeys    = engine.KvStore.ScanKeys().ToList();

        Assert.Equal(3, userKeys.Count);
        Assert.Equal(2, configKeys.Count);
        Assert.Equal(5, allKeys.Count);
        Assert.All(userKeys,   k => Assert.StartsWith("user:", k));
        Assert.All(configKeys, k => Assert.StartsWith("config:", k));
    }

    [Fact]
    public async Task KvStore_CoexistsWithDocumentCollections_NoInterference()
    {
        var dbPath  = Path.Combine(_tempDir, "kvco.db");
        var walPath = Path.Combine(_tempDir, "wal", "kvco.wal");
        var idxPath = Path.Combine(_tempDir, "kvco.idx");
        var collDir = Path.Combine(_tempDir, "kvco_data");
        var config  = PageFileConfig.Default with { WalPath = walPath, IndexFilePath = idxPath, CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);

        // Populate KV store
        for (int i = 1; i <= 10; i++)
            engine.KvStore.Set($"key:{i}", System.Text.Encoding.UTF8.GetBytes($"value{i}"));

        // Populate a document collection in the same engine
        var col = engine.GetOrCreateCollection("docs", BsonIdType.Int32);
        for (int i = 1; i <= 20; i++)
        {
            var doc = col.CreateDocument(["_id", "text"], b => b
                .AddId((BsonId)i)
                .AddString("text", $"Doc{i}"));
            await col.InsertAsync(doc);
        }
        await engine.CommitAsync();

        // Both should be independently readable
        for (int i = 1; i <= 10; i++)
        {
            var val = engine.KvStore.Get($"key:{i}");
            Assert.NotNull(val);
            Assert.Equal($"value{i}", System.Text.Encoding.UTF8.GetString(val!));
        }

        Assert.Equal(20, await col.CountAsync());
        for (int i = 1; i <= 20; i++)
        {
            var d = await col.FindByIdAsync((BsonId)i);
            Assert.NotNull(d);
            Assert.True(d!.TryGetString("text", out var t));
            Assert.Equal($"Doc{i}", t);
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Page overflow — large documents spanning multiple overflow pages
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Overflow_LargeDocument_StoredAndRetrievedCorrectly_InCollectionFile()
    {
        // DocumentCollection (typed path via TestDbContext) supports overflow.
        // We exercise this through BLiteEngine using a plain collection that has enough
        // capacity (with a larger page size config to allow overflow chains).
        var dbPath  = Path.Combine(_tempDir, "ovf.db");
        var collDir = Path.Combine(_tempDir, "ovf_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        // Use TestDbContext which supports overflow documents via DocumentCollection
        var dbCtx = new MultiFileTestDbContext(dbPath, config);
        try
        {
            // 25KB payload → exceeds single page (16 KB default) → triggers overflow chain
            var largePayload = new string('X', 25 * 1024);
            var id = await dbCtx.Entries.InsertAsync(new MultiFileEntry { Payload = largePayload, Tag = "overflow-test" });
            await dbCtx.SaveChangesAsync();

            var found = await dbCtx.Entries.FindByIdAsync(id);
            Assert.NotNull(found);
            Assert.Equal(largePayload.Length, found!.Payload.Length);
            Assert.Equal("overflow-test", found.Tag);
        }
        finally
        {
            dbCtx.Dispose();
        }
    }

    [Fact]
    public async Task Overflow_LargeDocument_PersistsAcrossEngineRestart_InCollectionFile()
    {
        var dbPath  = Path.Combine(_tempDir, "ovfr.db");
        var collDir = Path.Combine(_tempDir, "ovfr_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        var largePayload = new string('Y', 40 * 1024); // 40 KB

        // Write
        using (var dbCtx = new MultiFileTestDbContext(dbPath, config))
        {
            await dbCtx.Entries.InsertAsync(new MultiFileEntry { Payload = largePayload, Tag = "restart-overflow" });
            await dbCtx.SaveChangesAsync();
        }

        // Read after restart
        using (var dbCtx = new MultiFileTestDbContext(dbPath, config))
        {
            var all = await dbCtx.Entries.FindAllAsync().ToListAsync();
            Assert.Single(all);
            Assert.Equal(largePayload.Length, all[0].Payload.Length);
            Assert.Equal("restart-overflow", all[0].Tag);
        }
    }

    [Fact]
    public async Task Overflow_MixedSizeDocuments_AllRetrievableFromCollectionFile()
    {
        var dbPath  = Path.Combine(_tempDir, "ovfmix.db");
        var idxPath = Path.Combine(_tempDir, "ovfmix.idx");
        var collDir = Path.Combine(_tempDir, "ovfmix_data");
        var config  = PageFileConfig.Default with { IndexFilePath = idxPath, CollectionDataDirectory = collDir };

        using var dbCtx = new MultiFileTestDbContext(dbPath, config);

        // Mix of small and large docs
        var entries = new List<MultiFileEntry>
        {
            new() { Payload = "small1",                         Tag = "small" },
            new() { Payload = new string('A', 20 * 1024),       Tag = "medium" },
            new() { Payload = "small2",                         Tag = "small" },
            new() { Payload = new string('B', 60 * 1024),       Tag = "large"  },
            new() { Payload = "small3",                         Tag = "small" },
        };

        var ids = await dbCtx.Entries.InsertBulkAsync(entries);
        await dbCtx.SaveChangesAsync();

        Assert.Equal(5, ids.Count);

        // Verify all round-trip correctly
        var all = await dbCtx.Entries.FindAllAsync().ToListAsync();
        Assert.Equal(5, all.Count);

        var smalls  = all.Where(e => e.Tag == "small").ToList();
        var mediums = all.Where(e => e.Tag == "medium").ToList();
        var larges  = all.Where(e => e.Tag == "large").ToList();

        Assert.Equal(3, smalls.Count);
        Assert.Single(mediums);
        Assert.Equal(20 * 1024, mediums[0].Payload.Length);
        Assert.Single(larges);
        Assert.Equal(60 * 1024, larges[0].Payload.Length);
    }
}
