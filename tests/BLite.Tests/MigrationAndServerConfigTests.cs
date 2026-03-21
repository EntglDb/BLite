using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Tests for:
///   - PageFileConfig.Server() accepting an optional base config (page size)
///   - BLiteMigration.ToMultiFile() — single-file to multi-file migration
///   - BLiteMigration.ToSingleFile() — multi-file to single-file migration
/// </summary>
public class MigrationAndServerConfigTests : IDisposable
{
    private readonly string _tempDir;

    public MigrationAndServerConfigTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"blite_mig_{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* best-effort */ }
    }

    // ─────────────────────────────────────────────────────────────────
    // PageFileConfig.Server() — base config override
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public void ServerConfig_DefaultBaseConfig_Uses16KPageSize()
    {
        var dbPath = Path.Combine(_tempDir, "srv_default.db");
        var config  = PageFileConfig.Server(dbPath);

        Assert.Equal(PageFileConfig.Default.PageSize,          config.PageSize);
        Assert.Equal(PageFileConfig.Default.GrowthBlockSize,   config.GrowthBlockSize);
        Assert.Equal(PageFileConfig.Default.Access,            config.Access);
        Assert.NotNull(config.WalPath);
        Assert.NotNull(config.IndexFilePath);
        Assert.NotNull(config.CollectionDataDirectory);
    }

    [Fact]
    public void ServerConfig_SmallBaseConfig_Uses8KPageSize()
    {
        var dbPath = Path.Combine(_tempDir, "srv_small.db");
        var config  = PageFileConfig.Server(dbPath, PageFileConfig.Small);

        Assert.Equal(PageFileConfig.Small.PageSize,          config.PageSize);
        Assert.Equal(PageFileConfig.Small.GrowthBlockSize,   config.GrowthBlockSize);
        Assert.NotNull(config.WalPath);
        Assert.NotNull(config.IndexFilePath);
        Assert.NotNull(config.CollectionDataDirectory);
    }

    [Fact]
    public void ServerConfig_LargeBaseConfig_Uses32KPageSize()
    {
        var dbPath = Path.Combine(_tempDir, "srv_large.db");
        var config  = PageFileConfig.Server(dbPath, PageFileConfig.Large);

        Assert.Equal(PageFileConfig.Large.PageSize,          config.PageSize);
        Assert.Equal(PageFileConfig.Large.GrowthBlockSize,   config.GrowthBlockSize);
        Assert.NotNull(config.WalPath);
        Assert.NotNull(config.IndexFilePath);
        Assert.NotNull(config.CollectionDataDirectory);
    }

    [Fact]
    public void ServerConfig_SmallBaseConfig_DatabaseFunctional()
    {
        var dbPath = Path.Combine(_tempDir, "srv_small_functional.db");
        var config  = PageFileConfig.Server(dbPath, PageFileConfig.Small);

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("items", BsonIdType.Int32);

        for (int i = 1; i <= 20; i++)
        {
            var doc = col.CreateDocument(["_id", "label"], b => b
                .AddId((BsonId)i)
                .AddString("label", $"item{i}"));
            col.Insert(doc);
        }
        engine.Commit();
        col.CreateIndex("label", "idx_label");
        engine.Commit();

        Assert.Equal(20, col.Count());
        var found = col.QueryIndex("idx_label", "item5", "item5").ToList();
        Assert.Single(found);
    }

    [Fact]
    public void ServerConfig_LargeBaseConfig_DatabaseFunctional()
    {
        var dbPath = Path.Combine(_tempDir, "srv_large_functional.db");
        var config  = PageFileConfig.Server(dbPath, PageFileConfig.Large);

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("records", BsonIdType.Int32);

        for (int i = 1; i <= 50; i++)
        {
            var doc = col.CreateDocument(["_id", "value"], b => b
                .AddId((BsonId)i)
                .AddInt32("value", i * 3));
            col.Insert(doc);
        }
        engine.Commit();

        Assert.Equal(50, col.Count());
        var d = col.FindById((BsonId)25);
        Assert.NotNull(d);
        Assert.True(d!.TryGetInt32("value", out var v));
        Assert.Equal(75, v);
    }

    [Fact]
    public void ServerConfig_MultipleDbsInSameDir_HaveDistinctPaths()
    {
        var dir = Path.Combine(_tempDir, "multi");
        Directory.CreateDirectory(dir);

        var config1 = PageFileConfig.Server(Path.Combine(dir, "db1.db"));
        var config2 = PageFileConfig.Server(Path.Combine(dir, "db2.db"));

        // WAL, index, and collection dir should all be different between the two databases
        Assert.NotEqual(config1.WalPath, config2.WalPath);
        Assert.NotEqual(config1.IndexFilePath, config2.IndexFilePath);
        Assert.NotEqual(config1.CollectionDataDirectory, config2.CollectionDataDirectory);
    }

    // ─────────────────────────────────────────────────────────────────
    // BLiteMigration.ToMultiFile
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public void ToMultiFile_SimpleCollections_DataMigratedCorrectly()
    {
        var dbPath  = Path.Combine(_tempDir, "src_single.db");
        var idxPath = Path.Combine(_tempDir, "src_single.idx");
        var collDir = Path.Combine(_tempDir, "src_single_data");

        // Create single-file source database
        using (var engine = new BLiteEngine(dbPath))
        {
            var col = engine.GetOrCreateCollection("users", BsonIdType.Int32);
            for (int i = 1; i <= 50; i++)
            {
                var doc = col.CreateDocument(["_id", "name", "score"], b => b
                    .AddId((BsonId)i)
                    .AddString("name", $"User{i}")
                    .AddInt32("score", i * 10));
                col.Insert(doc);
            }
            engine.Commit();
        }

        // Migrate to multi-file
        var targetConfig = PageFileConfig.Default with
        {
            IndexFilePath = idxPath,
            CollectionDataDirectory = collDir
        };
        BLiteMigration.ToMultiFile(dbPath, targetConfig);

        // Verify data is readable from the multi-file engine at the same path
        using (var engine = new BLiteEngine(dbPath, targetConfig))
        {
            var col = engine.GetOrCreateCollection("users", BsonIdType.Int32);
            Assert.Equal(50, col.Count());
            var d = col.FindById((BsonId)25);
            Assert.NotNull(d);
            Assert.True(d!.TryGetString("name", out var name));
            Assert.Equal("User25", name);
            Assert.True(d.TryGetInt32("score", out var score));
            Assert.Equal(250, score);
        }
    }

    [Fact]
    public void ToMultiFile_WithSecondaryIndex_IndexRebuiltInIdxFile()
    {
        var dbPath  = Path.Combine(_tempDir, "src_idx.db");
        var idxPath = Path.Combine(_tempDir, "src_idx.idx");
        var collDir = Path.Combine(_tempDir, "src_idx_data");

        // Create source with index
        using (var engine = new BLiteEngine(dbPath))
        {
            var col = engine.GetOrCreateCollection("products", BsonIdType.Int32);
            for (int i = 1; i <= 30; i++)
            {
                var doc = col.CreateDocument(["_id", "price"], b => b
                    .AddId((BsonId)i)
                    .AddInt32("price", i * 5));
                col.Insert(doc);
            }
            engine.Commit();
            col.CreateIndex("price", "idx_price");
            engine.Commit();
        }

        var targetConfig = PageFileConfig.Default with
        {
            IndexFilePath = idxPath,
            CollectionDataDirectory = collDir
        };
        BLiteMigration.ToMultiFile(dbPath, targetConfig);

        // Index should be usable after migration (index pages in .idx file)
        using (var engine = new BLiteEngine(dbPath, targetConfig))
        {
            var col = engine.GetOrCreateCollection("products", BsonIdType.Int32);
            var results = col.QueryIndex("idx_price", 25, 50).ToList();
            // prices 25, 30, 35, 40, 45, 50 → items 5,6,7,8,9,10 → 6 results
            Assert.Equal(6, results.Count);
            foreach (var r in results)
            {
                Assert.True(r.TryGetInt32("price", out var p));
                Assert.InRange(p, 25, 50);
            }
        }
    }

    [Fact]
    public void ToMultiFile_MultipleCollections_AllDataMigrated()
    {
        var dbPath  = Path.Combine(_tempDir, "src_multi.db");
        var collDir = Path.Combine(_tempDir, "src_multi_data");

        using (var engine = new BLiteEngine(dbPath))
        {
            var a = engine.GetOrCreateCollection("alpha", BsonIdType.Int32);
            var b = engine.GetOrCreateCollection("beta",  BsonIdType.Int32);

            for (int i = 1; i <= 20; i++)
            {
                a.Insert(a.CreateDocument(["_id", "v"], b2 => b2.AddId((BsonId)i).AddInt32("v", i)));
                b.Insert(b.CreateDocument(["_id", "v"], b2 => b2.AddId((BsonId)i).AddInt32("v", i * 2)));
            }
            engine.Commit();
        }

        var targetConfig = PageFileConfig.Default with { CollectionDataDirectory = collDir };
        BLiteMigration.ToMultiFile(dbPath, targetConfig);

        using (var engine = new BLiteEngine(dbPath, targetConfig))
        {
            var a = engine.GetOrCreateCollection("alpha", BsonIdType.Int32);
            var b = engine.GetOrCreateCollection("beta",  BsonIdType.Int32);
            Assert.Equal(20, a.Count());
            Assert.Equal(20, b.Count());

            var da = a.FindById((BsonId)10)!;
            Assert.True(da.TryGetInt32("v", out var va));
            Assert.Equal(10, va);

            var db2 = b.FindById((BsonId)10)!;
            Assert.True(db2.TryGetInt32("v", out var vb));
            Assert.Equal(20, vb);
        }
    }

    [Fact]
    public void ToMultiFile_WithKvStore_KvEntriesMigrated()
    {
        var dbPath  = Path.Combine(_tempDir, "src_kv.db");
        var walPath = Path.Combine(_tempDir, "wal", "src_kv.wal");
        var collDir = Path.Combine(_tempDir, "src_kv_data");

        // Populate single-file DB with KV entries
        using (var engine = new BLiteEngine(dbPath))
        {
            engine.KvStore.Set("config:theme", "dark"u8.ToArray());
            engine.KvStore.Set("config:lang",  "en"u8.ToArray());
            engine.KvStore.Set("session:1",    "abc"u8.ToArray());
        }

        var targetConfig = PageFileConfig.Default with
        {
            WalPath = walPath,
            CollectionDataDirectory = collDir
        };
        BLiteMigration.ToMultiFile(dbPath, targetConfig);

        using (var engine = new BLiteEngine(dbPath, targetConfig))
        {
            Assert.Equal("dark"u8.ToArray(), engine.KvStore.Get("config:theme"));
            Assert.Equal("en"u8.ToArray(),   engine.KvStore.Get("config:lang"));
            Assert.Equal("abc"u8.ToArray(),  engine.KvStore.Get("session:1"));
        }
    }

    [Fact]
    public void ToMultiFile_SourceFileNotFound_Throws()
    {
        var config = PageFileConfig.Default with
        {
            CollectionDataDirectory = Path.Combine(_tempDir, "nowhere")
        };
        Assert.Throws<FileNotFoundException>(() =>
            BLiteMigration.ToMultiFile(Path.Combine(_tempDir, "notexist.db"), config));
    }

    [Fact]
    public void ToMultiFile_NoMultiFilePaths_Throws()
    {
        var dbPath = Path.Combine(_tempDir, "src_nop.db");
        File.WriteAllBytes(dbPath, Array.Empty<byte>());

        Assert.Throws<InvalidOperationException>(() =>
            BLiteMigration.ToMultiFile(dbPath, PageFileConfig.Default));
    }

    // ─────────────────────────────────────────────────────────────────
    // BLiteMigration.ToSingleFile
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public void ToSingleFile_SimpleCollection_DataMigratedCorrectly()
    {
        var dbPath  = Path.Combine(_tempDir, "mf_src.db");
        var idxPath = Path.Combine(_tempDir, "mf_src.idx");
        var collDir = Path.Combine(_tempDir, "mf_src_data");
        var multiConfig = PageFileConfig.Default with
        {
            IndexFilePath = idxPath,
            CollectionDataDirectory = collDir
        };

        // Create multi-file source
        using (var engine = new BLiteEngine(dbPath, multiConfig))
        {
            var col = engine.GetOrCreateCollection("orders", BsonIdType.Int32);
            for (int i = 1; i <= 40; i++)
            {
                var doc = col.CreateDocument(["_id", "total"], b => b
                    .AddId((BsonId)i)
                    .AddInt32("total", i * 100));
                col.Insert(doc);
            }
            engine.Commit();
        }

        // Migrate to single-file at a new path
        var singlePath = Path.Combine(_tempDir, "mf_single.db");
        BLiteMigration.ToSingleFile(dbPath, multiConfig, singlePath);

        using (var engine = new BLiteEngine(singlePath))
        {
            var col = engine.GetOrCreateCollection("orders", BsonIdType.Int32);
            Assert.Equal(40, col.Count());
            var d = col.FindById((BsonId)20)!;
            Assert.True(d.TryGetInt32("total", out var t));
            Assert.Equal(2000, t);
        }
    }

    [Fact]
    public void ToSingleFile_InPlace_ReplacesSourceFile()
    {
        var dbPath  = Path.Combine(_tempDir, "mf_inplace.db");
        var idxPath = Path.Combine(_tempDir, "mf_inplace.idx");
        var collDir = Path.Combine(_tempDir, "mf_inplace_data");
        var multiConfig = PageFileConfig.Default with
        {
            IndexFilePath = idxPath,
            CollectionDataDirectory = collDir
        };

        // Create multi-file source
        using (var engine = new BLiteEngine(dbPath, multiConfig))
        {
            var col = engine.GetOrCreateCollection("items", BsonIdType.Int32);
            for (int i = 1; i <= 25; i++)
            {
                var doc = col.CreateDocument(["_id", "name"], b => b
                    .AddId((BsonId)i)
                    .AddString("name", $"item{i}"));
                col.Insert(doc);
            }
            engine.Commit();
        }

        // In-place migration: targetPath == sourcePath
        BLiteMigration.ToSingleFile(dbPath, multiConfig, dbPath);

        // After in-place migration the multi-file components should be gone
        Assert.False(File.Exists(idxPath),   "Index file should be deleted after in-place migration");
        Assert.False(Directory.Exists(collDir), "Collection dir should be deleted after in-place migration");

        // And the main .db should now be a valid single-file database
        using (var engine = new BLiteEngine(dbPath))
        {
            var col = engine.GetOrCreateCollection("items", BsonIdType.Int32);
            Assert.Equal(25, col.Count());
        }
    }

    [Fact]
    public void ToSingleFile_WithSecondaryIndex_IndexRebuiltInSingleFile()
    {
        var dbPath  = Path.Combine(_tempDir, "mf_idx.db");
        var idxPath = Path.Combine(_tempDir, "mf_idx.idx");
        var collDir = Path.Combine(_tempDir, "mf_idx_data");
        var multiConfig = PageFileConfig.Default with
        {
            IndexFilePath = idxPath,
            CollectionDataDirectory = collDir
        };

        using (var engine = new BLiteEngine(dbPath, multiConfig))
        {
            var col = engine.GetOrCreateCollection("events", BsonIdType.Int32);
            for (int i = 1; i <= 30; i++)
            {
                var doc = col.CreateDocument(["_id", "priority"], b => b
                    .AddId((BsonId)i)
                    .AddInt32("priority", i % 5));
                col.Insert(doc);
            }
            engine.Commit();
            col.CreateIndex("priority", "idx_priority");
            engine.Commit();
        }

        var singlePath = Path.Combine(_tempDir, "mf_single_idx.db");
        BLiteMigration.ToSingleFile(dbPath, multiConfig, singlePath);

        using (var engine = new BLiteEngine(singlePath))
        {
            var col = engine.GetOrCreateCollection("events", BsonIdType.Int32);
            // priority == 0 → items 5,10,15,20,25,30 → 6 results
            var zeros = col.QueryIndex("idx_priority", 0, 0).ToList();
            Assert.Equal(6, zeros.Count);
        }
    }

    [Fact]
    public void RoundTrip_SingleToMultiToSingle_DataPreserved()
    {
        var dbPath       = Path.Combine(_tempDir, "rt.db");
        var idxPath      = Path.Combine(_tempDir, "rt.idx");
        var collDir      = Path.Combine(_tempDir, "rt_data");
        var singlePath2  = Path.Combine(_tempDir, "rt_final.db");

        var multiConfig = PageFileConfig.Default with
        {
            IndexFilePath = idxPath,
            CollectionDataDirectory = collDir
        };

        // Step 1: create single-file source
        using (var engine = new BLiteEngine(dbPath))
        {
            var col = engine.GetOrCreateCollection("things", BsonIdType.Int32);
            for (int i = 1; i <= 100; i++)
            {
                var doc = col.CreateDocument(["_id", "tag"], b => b
                    .AddId((BsonId)i)
                    .AddString("tag", i % 2 == 0 ? "even" : "odd"));
                col.Insert(doc);
            }
            engine.Commit();
            col.CreateIndex("tag", "idx_tag");
            engine.Commit();
        }

        // Step 2: single → multi
        BLiteMigration.ToMultiFile(dbPath, multiConfig);

        // Step 3: multi → single (new path)
        BLiteMigration.ToSingleFile(dbPath, multiConfig, singlePath2);

        // Step 4: verify final single-file DB
        using (var engine = new BLiteEngine(singlePath2))
        {
            var col = engine.GetOrCreateCollection("things", BsonIdType.Int32);
            Assert.Equal(100, col.Count());

            var evens = col.QueryIndex("idx_tag", "even", "even").ToList();
            var odds  = col.QueryIndex("idx_tag", "odd",  "odd").ToList();
            Assert.Equal(50, evens.Count);
            Assert.Equal(50, odds.Count);
        }
    }

    [Fact]
    public void ToMultiFile_UsingServerFactory_WithSmallBaseConfig()
    {
        var dbPath = Path.Combine(_tempDir, "server_small.db");

        // Create single-file database with Small page size
        using (var engine = new BLiteEngine(dbPath, PageFileConfig.Small))
        {
            var col = engine.GetOrCreateCollection("data", BsonIdType.Int32);
            for (int i = 1; i <= 10; i++)
            {
                col.Insert(col.CreateDocument(["_id", "v"], b => b
                    .AddId((BsonId)i).AddInt32("v", i)));
            }
            engine.Commit();
        }

        // Use Server() with Small base config to get multi-file paths while keeping 8KB pages
        var targetConfig = PageFileConfig.Server(dbPath, PageFileConfig.Small);
        BLiteMigration.ToMultiFile(dbPath, targetConfig);

        // Verify: Small page size preserved + data intact + multi-file layout in use
        using (var engine = new BLiteEngine(dbPath, targetConfig))
        {
            var col = engine.GetOrCreateCollection("data", BsonIdType.Int32);
            Assert.Equal(10, col.Count());
        }

        // The main .db should have been written with Small page size
        var detected = PageFileConfig.DetectFromFile(dbPath);
        Assert.NotNull(detected);
        Assert.Equal(PageFileConfig.Small.PageSize, detected!.Value.PageSize);
    }
}
