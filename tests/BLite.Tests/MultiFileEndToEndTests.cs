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
    public void CustomWalPath_DocumentsPersistedAfterRestart()
    {
        var dbPath  = Path.Combine(_tempDir, "wal2.db");
        var walPath = Path.Combine(_tempDir, "wal", "wal2.wal");
        var config  = PageFileConfig.Default with { WalPath = walPath };

        // Write
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("users");
            col.Insert(MakeDoc(col, 1, "Alice", 30));
            col.Insert(MakeDoc(col, 2, "Bob", 25));
            engine.Commit();
        }

        Assert.True(File.Exists(walPath), "WAL should be at custom path");

        // Read-back after restart
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col  = engine.GetOrCreateCollection("users");
            var docs = col.FindAll().ToList();
            Assert.Equal(2, docs.Count);
        }
    }

    [Fact]
    public void CustomWalPath_WalRecovery_RestoresUncommittedData()
    {
        var dbPath  = Path.Combine(_tempDir, "walrec.db");
        var walPath = Path.Combine(_tempDir, "wal", "walrec.wal");
        var config  = PageFileConfig.Default with { WalPath = walPath };

        BsonId id1, id2;

        // Write + commit but do NOT checkpoint (WAL still contains data)
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("items");
            id1 = col.Insert(MakeDoc(col, 1, "Item1", 10));
            id2 = col.Insert(MakeDoc(col, 2, "Item2", 20));
            engine.Commit();
            // Deliberate: no Checkpoint here — WAL holds the data
        }

        // Restart — engine must replay the WAL to recover
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col  = engine.GetOrCreateCollection("items");
            var all  = col.FindAll().ToList();
            Assert.Equal(2, all.Count);

            var found1 = col.FindById(id1);
            var found2 = col.FindById(id2);
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
    public void SeparateIndexFile_BTreeIndex_QueryReturnsCorrectResults()
    {
        var dbPath  = Path.Combine(_tempDir, "idx.db");
        var idxPath = Path.Combine(_tempDir, "idx.idx");
        var config  = PageFileConfig.Default with { IndexFilePath = idxPath };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("people", BsonIdType.Int32);

        for (int i = 1; i <= 20; i++)
            col.Insert(MakeDoc(col, i, $"User{i}", 20 + i));

        engine.Commit();

        col.CreateIndex("age", "idx_age");

        Assert.True(File.Exists(idxPath), "Index file should be created");
        Assert.True(new FileInfo(idxPath).Length > 0, "Index file should not be empty");

        // Query age between 25 and 30 (inclusive) — pages are in the .idx file
        var results = col.QueryIndex("idx_age", 25, 30).ToList();
        Assert.Equal(6, results.Count);
        foreach (var r in results)
        {
            Assert.True(r.TryGetInt32("age", out var age));
            Assert.InRange(age, 25, 30);
        }
    }

    [Fact]
    public void SeparateIndexFile_IndexQueryWorksAfterRestart()
    {
        var dbPath  = Path.Combine(_tempDir, "idxr.db");
        var idxPath = Path.Combine(_tempDir, "idxr.idx");
        var config  = PageFileConfig.Default with { IndexFilePath = idxPath };

        // First lifetime: populate + index
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("products", BsonIdType.Int32);
            for (int i = 1; i <= 10; i++)
                col.Insert(MakeDoc(col, i, $"Prod{i}", 100 + i));
            engine.Commit();
            col.CreateIndex("age", "idx_price");
            engine.Commit(); // commit B-tree entries written during index creation
        }

        // Second lifetime: index still usable
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col     = engine.GetOrCreateCollection("products", BsonIdType.Int32);
            var results = col.QueryIndex("idx_price", 105, 107).ToList();
            Assert.Equal(3, results.Count);
            foreach (var r in results)
            {
                Assert.True(r.TryGetInt32("age", out var v));
                Assert.InRange(v, 105, 107);
            }
        }
    }

    [Fact]
    public void SeparateIndexFile_MultipleCollectionsBothUseIndexFile()
    {
        var dbPath  = Path.Combine(_tempDir, "idxmc.db");
        var idxPath = Path.Combine(_tempDir, "idxmc.idx");
        var config  = PageFileConfig.Default with { IndexFilePath = idxPath };

        using var engine = new BLiteEngine(dbPath, config);
        var colA = engine.GetOrCreateCollection("colA", BsonIdType.Int32);
        var colB = engine.GetOrCreateCollection("colB", BsonIdType.Int32);

        for (int i = 1; i <= 5; i++)
        {
            colA.Insert(MakeDoc(colA, i, $"A{i}", 10 + i));
            colB.Insert(MakeDoc(colB, i, $"B{i}", 50 + i));
        }
        engine.Commit();

        colA.CreateIndex("age", "idx_age_a");
        colB.CreateIndex("age", "idx_age_b");

        // Both indexes should be readable and routed to the index file
        var ra = colA.QueryIndex("idx_age_a", 12, 14).ToList();
        var rb = colB.QueryIndex("idx_age_b", 52, 54).ToList();

        Assert.Equal(3, ra.Count);
        Assert.Equal(3, rb.Count);
    }

    // ─────────────────────────────────────────────────────────────────
    // Phase 4 — per-collection files: CRUD
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public void PerCollectionFiles_InsertAndFindById()
    {
        var dbPath  = Path.Combine(_tempDir, "coll.db");
        var collDir = Path.Combine(_tempDir, "coll_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("users", BsonIdType.Int32);

        var id = col.Insert(MakeDoc(col, 42, "Alice", 30));
        engine.Commit();

        var found = col.FindById(id);
        Assert.NotNull(found);
        Assert.True(found!.TryGetString("name", out var name));
        Assert.Equal("Alice", name);

        // Dedicated file must exist
        var collFile = Path.Combine(collDir, "users.db");
        Assert.True(File.Exists(collFile), "Collection file should exist");
    }

    [Fact]
    public void PerCollectionFiles_Update_ModifiesDocument()
    {
        var dbPath  = Path.Combine(_tempDir, "collupd.db");
        var collDir = Path.Combine(_tempDir, "collupd_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("users", BsonIdType.Int32);

        col.Insert(MakeDoc(col, 1, "Alice", 30));
        engine.Commit();

        var updated = col.CreateDocument(["_id", "name", "age"], b => b
            .AddId((BsonId)1)
            .AddString("name", "Alicia")
            .AddInt32("age", 31));
        Assert.True(col.Update((BsonId)1, updated));
        engine.Commit();

        var found = col.FindById((BsonId)1);
        Assert.NotNull(found);
        Assert.True(found!.TryGetString("name", out var n));
        Assert.Equal("Alicia", n);
    }

    [Fact]
    public void PerCollectionFiles_Delete_RemovesDocument()
    {
        var dbPath  = Path.Combine(_tempDir, "colldel.db");
        var collDir = Path.Combine(_tempDir, "colldel_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("orders", BsonIdType.Int32);

        col.Insert(MakeDoc(col, 1, "Order1", 100));
        col.Insert(MakeDoc(col, 2, "Order2", 200));
        engine.Commit();

        Assert.True(col.Delete((BsonId)1));
        engine.Commit();

        Assert.Null(col.FindById((BsonId)1));
        Assert.NotNull(col.FindById((BsonId)2));
        Assert.Equal(1, col.Count());
    }

    // ─────────────────────────────────────────────────────────────────
    // Phase 4 — per-collection files: scan
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public void PerCollectionFiles_FindAll_ReturnsAllDocuments()
    {
        var dbPath  = Path.Combine(_tempDir, "scan.db");
        var collDir = Path.Combine(_tempDir, "scan_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("docs", BsonIdType.Int32);

        // Insert enough documents to span multiple pages
        const int docCount = 200;
        for (int i = 1; i <= docCount; i++)
            col.Insert(MakeDoc(col, i, $"Doc{i}", i));
        engine.Commit();

        var all = col.FindAll().ToList();
        Assert.Equal(docCount, all.Count);
    }

    [Fact]
    public void PerCollectionFiles_Scan_FindsAllMatchingDocuments()
    {
        var dbPath  = Path.Combine(_tempDir, "scanp.db");
        var collDir = Path.Combine(_tempDir, "scanp_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("items", BsonIdType.Int32);

        const int total = 100;
        for (int i = 1; i <= total; i++)
            col.Insert(MakeDoc(col, i, $"Item{i}", i % 10));  // age cycles 1-9, 0
        engine.Commit();

        // Scan for age == 5 via Find(predicate)
        var matched = col.Find(doc =>
        {
            doc.TryGetInt32("age", out var age);
            return age == 5;
        }).ToList();

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
    public void PerCollectionFiles_MultipleCollections_DataIsolated()
    {
        var dbPath  = Path.Combine(_tempDir, "iso.db");
        var collDir = Path.Combine(_tempDir, "iso_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var users  = engine.GetOrCreateCollection("users",  BsonIdType.Int32);
        var orders = engine.GetOrCreateCollection("orders", BsonIdType.Int32);

        for (int i = 1; i <= 5; i++)
            users.Insert(MakeDoc(users, i, $"User{i}", 20 + i));
        for (int i = 1; i <= 3; i++)
            orders.Insert(MakeDoc(orders, i, $"Order{i}", 100 * i));
        engine.Commit();

        Assert.Equal(5, users.Count());
        Assert.Equal(3, orders.Count());

        // Separate physical files must exist
        Assert.True(File.Exists(Path.Combine(collDir, "users.db")));
        Assert.True(File.Exists(Path.Combine(collDir, "orders.db")));
    }

    [Fact]
    public void PerCollectionFiles_DataPersistsAfterEngineRestart()
    {
        var dbPath  = Path.Combine(_tempDir, "persist.db");
        var collDir = Path.Combine(_tempDir, "persist_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        // First lifetime
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("customers", BsonIdType.Int32);
            for (int i = 1; i <= 10; i++)
                col.Insert(MakeDoc(col, i, $"Customer{i}", 30 + i));
            engine.Commit();
        }

        // Second lifetime — data must still be there
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("customers", BsonIdType.Int32);
            var all = col.FindAll().ToList();
            Assert.Equal(10, all.Count);

            var found = col.FindById((BsonId)7);
            Assert.NotNull(found);
            Assert.True(found!.TryGetString("name", out var n));
            Assert.Equal("Customer7", n);
        }
    }

    [Fact]
    public void PerCollectionFiles_DropCollection_CollectionNoLongerQueryable()
    {
        var dbPath  = Path.Combine(_tempDir, "drop.db");
        var collDir = Path.Combine(_tempDir, "drop_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("temp", BsonIdType.Int32);
        col.Insert(MakeDoc(col, 1, "X", 1));
        engine.Commit();

        Assert.True(File.Exists(Path.Combine(collDir, "temp.db")),
            "Collection file must exist before drop");

        // Drop removes in-memory state and metadata
        Assert.True(engine.DropCollection("temp"));

        // Re-opening gives an empty collection
        var reopened = engine.GetOrCreateCollection("temp", BsonIdType.Int32);
        Assert.Equal(0, reopened.Count());
    }

    // ─────────────────────────────────────────────────────────────────
    // Phase 4 — per-collection files: WAL crash-recovery
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public void PerCollectionFiles_WalRecovery_RestoresDocumentsAfterRestart()
    {
        var dbPath  = Path.Combine(_tempDir, "walcoll.db");
        var collDir = Path.Combine(_tempDir, "walcoll_data");
        var config  = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        // Write but no explicit checkpoint — WAL holds the data
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("invoices", BsonIdType.Int32);
            for (int i = 1; i <= 5; i++)
                col.Insert(MakeDoc(col, i, $"Inv{i}", i * 10));
            engine.Commit();
            // No Checkpoint — WAL still contains these writes
        }

        // Restart triggers WAL recovery; collection pages must be replayed to the right file
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("invoices", BsonIdType.Int32);
            var all = col.FindAll().ToList();
            Assert.Equal(5, all.Count);

            for (int i = 1; i <= 5; i++)
            {
                var doc = col.FindById((BsonId)i);
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
    public void AllPhasesCombined_FullCrudPlusIndex_MultipleCollections()
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
                users.Insert(MakeDoc(users, i, $"User{i}", 18 + i));
            for (int i = 1; i <= 10; i++)
                orders.Insert(MakeDoc(orders, i, $"Order{i}", 100 + i));
            engine.Commit();

            // Index on users.age — pages go to the index file
            users.CreateIndex("age", "idx_users_age");
            engine.Commit(); // commit B-tree entries written during index creation
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
            Assert.Equal(20, users.Count());
            Assert.Equal(10, orders.Count());

            // Index query on separate-index-file index
            var over30 = users.QueryIndex("idx_users_age", 30, 40).ToList();
            Assert.True(over30.Count > 0);
            foreach (var d in over30)
            {
                Assert.True(d.TryGetInt32("age", out var age));
                Assert.InRange(age, 30, 40);
            }

            // CRUD on collection file
            Assert.True(users.Delete((BsonId)5));
            engine.Commit();
            Assert.Null(users.FindById((BsonId)5));
            Assert.Equal(19, users.Count());
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // PageFileConfig.Server() factory — end-to-end
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public void ServerConfig_FullCrudPlusIndexPersistsAcrossRestarts()
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
                col.Insert(MakeDoc(col, i, $"Item{i}", 10 * i));
            engine.Commit();

            col.CreateIndex("age", "idx_price");
            engine.Commit(); // commit B-tree entries written during index creation
        }

        // ── Second lifetime — data and indexes survive restart ────────
        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col  = engine.GetOrCreateCollection("catalog", BsonIdType.Int32);
            var all  = col.FindAll().ToList();
            Assert.Equal(15, all.Count);

            var cheap = col.QueryIndex("idx_price", 10, 50).ToList();
            Assert.Equal(5, cheap.Count); // items 1-5 (age = 10-50)
        }
    }
}
