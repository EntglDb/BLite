using BLite.Core;
using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace BLite.Tests;

/// <summary>
/// Tests for multi-file storage engine features:
/// Phase 2 — configurable WAL path,
/// Phase 3 — separate index file (.idx),
/// Phase 4 — per-collection files.
/// </summary>
public class MultiFileStorageTests : IDisposable
{
    private readonly string _tempDir;

    public MultiFileStorageTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"blite_multifile_{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* best-effort */ }
    }

    // ─────────────────────────────────────────────────────────────────
    // Phase 2 — Configurable WAL path
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public void StorageEngine_WithCustomWalPath_CreatesWalAtSpecifiedLocation()
    {
        var dbPath = Path.Combine(_tempDir, "test.db");
        var customWalPath = Path.Combine(_tempDir, "custom_wal", "test.wal");
        Directory.CreateDirectory(Path.GetDirectoryName(customWalPath)!);

        var config = PageFileConfig.Default with { WalPath = customWalPath };

        using (var engine = new StorageEngine(dbPath, config))
        {
            var txn = engine.BeginTransaction();
            var pageId = engine.AllocatePage();
            var data = new byte[engine.PageSize];
            engine.WritePage(pageId, txn.TransactionId, data);
            engine.CommitTransaction(txn);
        }

        Assert.True(File.Exists(customWalPath), "WAL file should exist at the custom path");
        Assert.False(File.Exists(Path.ChangeExtension(dbPath, ".wal")),
            "Default WAL path should NOT be created when a custom WAL path is configured");
    }

    [Fact]
    public void StorageEngine_WithDefaultConfig_WalIsAdjacentToDbFile()
    {
        var dbPath = Path.Combine(_tempDir, "default.db");

        using (var engine = new StorageEngine(dbPath, PageFileConfig.Default))
        {
            var txn = engine.BeginTransaction();
            var pageId = engine.AllocatePage();
            var data = new byte[engine.PageSize];
            engine.WritePage(pageId, txn.TransactionId, data);
            engine.CommitTransaction(txn);
        }

        var expectedWalPath = Path.ChangeExtension(dbPath, ".wal");
        Assert.True(File.Exists(expectedWalPath), "Default WAL should be adjacent to the .db file");
    }

    // ─────────────────────────────────────────────────────────────────
    // Phase 3 — Separate index file
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public void IndexPages_WithSeparateIndexFile_AreRoutedToIndexFile()
    {
        var dbPath = Path.Combine(_tempDir, "data.db");
        var idxPath = Path.Combine(_tempDir, "data.idx");

        var config = PageFileConfig.Default with { IndexFilePath = idxPath };

        using (var engine = new StorageEngine(dbPath, config))
        {
            // Allocate an index page
            var indexPageId = engine.AllocateIndexPage();
            Assert.NotEqual(0u, indexPageId);

            // Write something on that page (non-transactional for simplicity)
            var data = new byte[engine.PageSize];
            data[4] = (byte)PageType.Index; // mark page type
            engine.WritePageImmediate(indexPageId, data);
            engine.FlushPageFile();

            // Verify the index file was created and has content
            Assert.True(File.Exists(idxPath), "Index file should be created");
            Assert.True(new FileInfo(idxPath).Length > 0, "Index file should not be empty");

            // Verify we can read the page back through the engine
            var readBuffer = new byte[engine.PageSize];
            engine.ReadPage(indexPageId, null, readBuffer);
            Assert.Equal((byte)PageType.Index, readBuffer[4]);
        }
    }

    [Fact]
    public void DataPages_WithSeparateIndexFile_RemainInMainFile()
    {
        var dbPath = Path.Combine(_tempDir, "data.db");
        var idxPath = Path.Combine(_tempDir, "data.idx");

        var config = PageFileConfig.Default with { IndexFilePath = idxPath };

        long mainFileSize, idxFileSize;

        using (var engine = new StorageEngine(dbPath, config))
        {
            // Allocate data page (should go to main file)
            var dataPageId = engine.AllocatePage();
            var data = new byte[engine.PageSize];
            data[4] = (byte)PageType.Data;
            engine.WritePageImmediate(dataPageId, data);

            // Allocate index page (should go to idx file)
            var idxPageId = engine.AllocateIndexPage();
            var idxData = new byte[engine.PageSize];
            idxData[4] = (byte)PageType.Index;
            engine.WritePageImmediate(idxPageId, idxData);

            engine.FlushPageFile();

            mainFileSize = new FileInfo(dbPath).Length;
            idxFileSize = new FileInfo(idxPath).Length;
        }

        // Both files should be non-empty
        Assert.True(mainFileSize > 0, "Main data file should have content");
        Assert.True(idxFileSize > 0, "Index file should have content");
    }

    [Fact]
    public void StorageEngine_WithoutIndexFilePath_AllPagesGoToMainFile()
    {
        var dbPath = Path.Combine(_tempDir, "data.db");
        var idxPath = Path.Combine(_tempDir, "data.idx");

        // No IndexFilePath configured
        using (var engine = new StorageEngine(dbPath, PageFileConfig.Default))
        {
            var idxPageId = engine.AllocateIndexPage();
            var data = new byte[engine.PageSize];
            engine.WritePageImmediate(idxPageId, data);
            engine.FlushPageFile();
        }

        Assert.False(File.Exists(idxPath), "Index file should NOT be created when IndexFilePath is not configured");
    }

    // ─────────────────────────────────────────────────────────────────
    // Phase 4 — Per-collection files
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public void DropCollection_WithPerCollectionFiles_ReclaimsFileSystemSpace()
    {
        var dbPath = Path.Combine(_tempDir, "server.db");
        var collDir = Path.Combine(_tempDir, "collections");
        var config = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using (var engine = new StorageEngine(dbPath, config))
        {
            // Allocate several pages for "orders" collection
            for (int i = 0; i < 5; i++)
            {
                var pageId = engine.AllocateCollectionPage("orders");
                var data = new byte[engine.PageSize];
                engine.WritePageImmediate(pageId, data);
            }
            engine.FlushPageFile();

            var ordersFilePath = Path.Combine(collDir, "orders.db");
            Assert.True(File.Exists(ordersFilePath), "orders.db should be created");
            Assert.True(new FileInfo(ordersFilePath).Length > 0, "orders.db should not be empty");

            // Drop the collection file
            engine.DropCollectionFile("orders");

            // File should be deleted
            Assert.False(File.Exists(ordersFilePath), "orders.db should be deleted after DropCollectionFile");
        }
    }

    [Fact]
    public void AllocateCollectionPage_WithPerCollectionFiles_RoutesCorrectly()
    {
        var dbPath = Path.Combine(_tempDir, "server.db");
        var collDir = Path.Combine(_tempDir, "collections");
        var config = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using (var engine = new StorageEngine(dbPath, config))
        {
            var pageId = engine.AllocateCollectionPage("products");
            var data = new byte[engine.PageSize];
            data[4] = (byte)PageType.Data;
            engine.WritePageImmediate(pageId, data);
            engine.FlushPageFile();

            // Read it back through the engine — should find it
            var readBuffer = new byte[engine.PageSize];
            engine.ReadPage(pageId, null, readBuffer);
            Assert.Equal((byte)PageType.Data, readBuffer[4]);
        }
    }

    [Fact]
    public void AllocateCollectionPage_WithoutCollectionDataDirectory_FallsBackToMainFile()
    {
        var dbPath = Path.Combine(_tempDir, "embedded.db");
        // No CollectionDataDirectory — embedded mode
        using (var engine = new StorageEngine(dbPath, PageFileConfig.Default))
        {
            var pageId = engine.AllocateCollectionPage("orders");
            var data = new byte[engine.PageSize];
            data[4] = (byte)PageType.Data;
            engine.WritePageImmediate(pageId, data);
            engine.FlushPageFile();

            // Main file should contain the page
            Assert.True(new FileInfo(dbPath).Length > 0);

            // Read back
            var readBuffer = new byte[engine.PageSize];
            engine.ReadPage(pageId, null, readBuffer);
            Assert.Equal((byte)PageType.Data, readBuffer[4]);
        }
    }

    [Fact]
    public void PerCollectionFiles_MultipleCollections_GetSeparateFiles()
    {
        var dbPath = Path.Combine(_tempDir, "server.db");
        var collDir = Path.Combine(_tempDir, "collections");
        var config = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using (var engine = new StorageEngine(dbPath, config))
        {
            engine.AllocateCollectionPage("orders");
            engine.AllocateCollectionPage("products");
            engine.AllocateCollectionPage("customers");
            engine.FlushPageFile();
        }

        Assert.True(File.Exists(Path.Combine(collDir, "orders.db")));
        Assert.True(File.Exists(Path.Combine(collDir, "products.db")));
        Assert.True(File.Exists(Path.Combine(collDir, "customers.db")));
    }

    // ─────────────────────────────────────────────────────────────────
    // PageFileConfig.Server() factory
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public void PageFileConfig_Server_ReturnsCorrectPaths()
    {
        var dataDir = Path.Combine(_tempDir, "serverdata");
        var cfg = PageFileConfig.Server(dataDir);

        Assert.Equal(16384, cfg.PageSize);
        Assert.Equal(4 * 1024 * 1024, cfg.GrowthBlockSize);
        Assert.NotNull(cfg.WalPath);
        Assert.NotNull(cfg.IndexFilePath);
        Assert.NotNull(cfg.CollectionDataDirectory);

        Assert.Contains("wal", cfg.WalPath);
        Assert.Contains(".idx", cfg.IndexFilePath);
        Assert.Contains("collections", cfg.CollectionDataDirectory);
    }
}
