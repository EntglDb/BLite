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
        Directory.CreateDirectory(dataDir);
        var dbPath = Path.Combine(dataDir, "mydb.db");
        var cfg = PageFileConfig.Server(dbPath);

        Assert.Equal(PageFileConfig.Default.PageSize,        cfg.PageSize);
        Assert.Equal(PageFileConfig.Default.GrowthBlockSize, cfg.GrowthBlockSize);
        Assert.NotNull(cfg.WalPath);
        Assert.NotNull(cfg.IndexFilePath);
        Assert.NotNull(cfg.CollectionDataDirectory);

        Assert.Contains("wal", cfg.WalPath);
        Assert.Contains("mydb.wal", cfg.WalPath);
        Assert.Contains("mydb.idx", cfg.IndexFilePath);
        Assert.Contains("collections", cfg.CollectionDataDirectory);
        Assert.Contains("mydb", cfg.CollectionDataDirectory);
    }

    // ─────────────────────────────────────────────────────────────────
    // Async paths — CommitTransactionAsync, ReadPageAsync, CheckpointAsync
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task CommitTransactionAsync_WithCustomWalPath_WritesWalToCorrectLocation()
    {
        var dbPath = Path.Combine(_tempDir, "async_wal.db");
        var customWalPath = Path.Combine(_tempDir, "async_wal_dir", "test.wal");
        Directory.CreateDirectory(Path.GetDirectoryName(customWalPath)!);

        var config = PageFileConfig.Default with { WalPath = customWalPath };

        using var engine = new StorageEngine(dbPath, config);

        var txn = engine.BeginTransaction();
        var pageId = engine.AllocatePage();
        var data = new byte[engine.PageSize];
        engine.WritePage(pageId, txn.TransactionId, data);
        await engine.CommitTransactionAsync(txn);

        Assert.True(File.Exists(customWalPath), "WAL should exist at the custom path after async commit");
        Assert.False(File.Exists(Path.ChangeExtension(dbPath, ".wal")),
            "Default WAL path should NOT exist when custom WalPath is configured");
    }

    [Fact]
    public async Task ReadPageAsync_WithSeparateIndexFile_ReadsFromCorrectFile()
    {
        var dbPath = Path.Combine(_tempDir, "async_idx.db");
        var idxPath = Path.Combine(_tempDir, "async_idx.idx");
        var config = PageFileConfig.Default with { IndexFilePath = idxPath };

        using var engine = new StorageEngine(dbPath, config);

        // Write an index page via a transaction, commit async
        var indexPageId = engine.AllocateIndexPage();
        var writeData = new byte[engine.PageSize];
        writeData[4] = (byte)PageType.Index;

        var txn = engine.BeginTransaction();
        engine.WritePage(indexPageId, txn.TransactionId, writeData);
        await engine.CommitTransactionAsync(txn);

        // Force checkpoint so the page lands in the physical index file
        await engine.CheckpointAsync();

        // Read back asynchronously through the routing layer
        var readBuffer = new byte[engine.PageSize];
        await engine.ReadPageAsync(indexPageId, null, readBuffer.AsMemory());
        Assert.Equal((byte)PageType.Index, readBuffer[4]);
    }

    [Fact]
    public async Task ReadPageAsync_WithCollectionFile_ReadsFromCorrectFile()
    {
        var dbPath = Path.Combine(_tempDir, "async_coll.db");
        var collDir = Path.Combine(_tempDir, "async_collections");
        var config = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new StorageEngine(dbPath, config);

        var pageId = engine.AllocateCollectionPage("invoices");
        var writeData = new byte[engine.PageSize];
        writeData[4] = (byte)PageType.Data;
        writeData[5] = 0xAB; // sentinel value

        var txn = engine.BeginTransaction();
        engine.WritePage(pageId, txn.TransactionId, writeData);
        await engine.CommitTransactionAsync(txn);

        // Force checkpoint so the page lands in the physical collection file
        await engine.CheckpointAsync();

        // Read back asynchronously
        var readBuffer = new byte[engine.PageSize];
        await engine.ReadPageAsync(pageId, null, readBuffer.AsMemory());
        Assert.Equal((byte)PageType.Data, readBuffer[4]);
        Assert.Equal(0xAB, readBuffer[5]);
    }

    [Fact]
    public async Task CheckpointAsync_WithSeparateIndexFile_FlushesBothFiles()
    {
        var dbPath = Path.Combine(_tempDir, "async_ckpt.db");
        var idxPath = Path.Combine(_tempDir, "async_ckpt.idx");
        var config = PageFileConfig.Default with { IndexFilePath = idxPath };

        using var engine = new StorageEngine(dbPath, config);

        // Write one data page and one index page in the same transaction
        var txn = engine.BeginTransaction();

        var dataPageId = engine.AllocatePage();
        var dataBytes = new byte[engine.PageSize];
        dataBytes[4] = (byte)PageType.Data;
        engine.WritePage(dataPageId, txn.TransactionId, dataBytes);

        var idxPageId = engine.AllocateIndexPage();
        var idxBytes = new byte[engine.PageSize];
        idxBytes[4] = (byte)PageType.Index;
        engine.WritePage(idxPageId, txn.TransactionId, idxBytes);

        await engine.CommitTransactionAsync(txn);
        await engine.CheckpointAsync();

        // After checkpoint, both files should contain the flushed pages
        Assert.True(new FileInfo(dbPath).Length > 0, "Main file should have content after checkpoint");
        Assert.True(new FileInfo(idxPath).Length > 0, "Index file should have content after checkpoint");

        // Async reads after checkpoint must still route correctly
        var readData = new byte[engine.PageSize];
        var readIdx = new byte[engine.PageSize];
        await engine.ReadPageAsync(dataPageId, null, readData.AsMemory());
        await engine.ReadPageAsync(idxPageId, null, readIdx.AsMemory());

        Assert.Equal((byte)PageType.Data, readData[4]);
        Assert.Equal((byte)PageType.Index, readIdx[4]);
    }

    [Fact]
    public async Task CheckpointAsync_WithCollectionFiles_FlushesBothMainAndCollectionFiles()
    {
        var dbPath = Path.Combine(_tempDir, "async_collckpt.db");
        var collDir = Path.Combine(_tempDir, "async_collckpt_dir");
        var config = PageFileConfig.Default with { CollectionDataDirectory = collDir };

        using var engine = new StorageEngine(dbPath, config);

        var txn = engine.BeginTransaction();
        var pageId = engine.AllocateCollectionPage("shipments");
        var data = new byte[engine.PageSize];
        data[4] = (byte)PageType.Data;
        engine.WritePage(pageId, txn.TransactionId, data);
        await engine.CommitTransactionAsync(txn);

        await engine.CheckpointAsync();

        var shipPath = Path.Combine(collDir, "shipments.db");
        Assert.True(File.Exists(shipPath), "shipments.db should exist after checkpoint");
        Assert.True(new FileInfo(shipPath).Length > 0, "shipments.db should contain flushed pages");
    }

    // ─────────────────────────────────────────────────────────────────
    // pageId collision prevention
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public void MultiFileRouting_PageIdsDoNotCollideAcrossFiles()
    {
        var dbPath = Path.Combine(_tempDir, "collision.db");
        var idxPath = Path.Combine(_tempDir, "collision.idx");
        var collDir = Path.Combine(_tempDir, "collision_coll");
        var config = PageFileConfig.Default with
        {
            IndexFilePath = idxPath,
            CollectionDataDirectory = collDir
        };

        using var engine = new StorageEngine(dbPath, config);

        var dataPageId   = engine.AllocatePage();
        var indexPageId  = engine.AllocateIndexPage();
        var collPageId   = engine.AllocateCollectionPage("orders");

        // All three IDs must be distinct
        Assert.True(dataPageId  != indexPageId,  "data and index pageIds must not collide");
        Assert.True(dataPageId  != collPageId,   "data and collection pageIds must not collide");
        Assert.True(indexPageId != collPageId,   "index and collection pageIds must not collide");

        // Write distinct sentinel bytes to each page
        var dataBuf = new byte[engine.PageSize];
        dataBuf[8] = 0xAA;
        engine.WritePageImmediate(dataPageId, dataBuf);

        var idxBuf = new byte[engine.PageSize];
        idxBuf[8] = 0xBB;
        engine.WritePageImmediate(indexPageId, idxBuf);

        var collBuf = new byte[engine.PageSize];
        collBuf[8] = 0xCC;
        engine.WritePageImmediate(collPageId, collBuf);

        engine.FlushPageFile();

        // Read back — each must return its own sentinel
        var r1 = new byte[engine.PageSize];
        engine.ReadPage(dataPageId, null, r1);
        Assert.Equal(0xAA, r1[8]);

        var r2 = new byte[engine.PageSize];
        engine.ReadPage(indexPageId, null, r2);
        Assert.Equal(0xBB, r2[8]);

        var r3 = new byte[engine.PageSize];
        engine.ReadPage(collPageId, null, r3);
        Assert.Equal(0xCC, r3[8]);
    }

    // ─────────────────────────────────────────────────────────────────
    // Routing persistence — pages readable after engine restart
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public void MultiFileRouting_IndexAndCollectionPagesReadableAfterRestart()
    {
        var dbPath  = Path.Combine(_tempDir, "restart.db");
        var idxPath = Path.Combine(_tempDir, "restart.idx");
        var collDir = Path.Combine(_tempDir, "restart_coll");
        var config  = PageFileConfig.Default with
        {
            IndexFilePath = idxPath,
            CollectionDataDirectory = collDir
        };

        uint indexPageId, collPageId;

        // First engine lifetime — allocate + write
        using (var engine = new StorageEngine(dbPath, config))
        {
            indexPageId = engine.AllocateIndexPage();
            var idxData = new byte[engine.PageSize];
            idxData[4] = (byte)PageType.Index;
            idxData[8] = 0xBB;
            engine.WritePageImmediate(indexPageId, idxData);

            collPageId = engine.AllocateCollectionPage("users");
            var collData = new byte[engine.PageSize];
            collData[4] = (byte)PageType.Data;
            collData[8] = 0xCC;
            engine.WritePageImmediate(collPageId, collData);

            engine.FlushPageFile();
        }

        // Second engine lifetime — routing must work without in-memory maps
        using (var engine = new StorageEngine(dbPath, config))
        {
            var idxRead = new byte[engine.PageSize];
            engine.ReadPage(indexPageId, null, idxRead);
            Assert.Equal((byte)PageType.Index, idxRead[4]);
            Assert.Equal(0xBB, idxRead[8]);

            var collRead = new byte[engine.PageSize];
            engine.ReadPage(collPageId, null, collRead);
            Assert.Equal((byte)PageType.Data, collRead[4]);
            Assert.Equal(0xCC, collRead[8]);
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Concurrent read / write safety (locking correctness)
    // These tests exercise the ReaderWriterLockSlim paths added to
    // PageFile to prevent ObjectDisposedException when a concurrent
    // WritePage triggers a file-resize while ReadPage is active.
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentReadWrite_MainFile_NoCrash()
    {
        // Allocate enough pages to force at least one file growth event, then
        // hammer concurrent reads and writes to verify the shared read-lock path
        // and the exclusive write-lock resize path do not race on _mappedFile.
        var dbPath = Path.Combine(_tempDir, "concur_main.db");
        using var engine = new StorageEngine(dbPath, PageFileConfig.Small);

        const int pageCount  = 40;
        const int readerCount = 20;

        var pageIds = new uint[pageCount];
        for (int i = 0; i < pageCount; i++)
            pageIds[i] = engine.AllocatePage();

        // Seed distinct sentinel bytes
        for (int i = 0; i < pageCount; i++)
        {
            var data = new byte[engine.PageSize];
            data[0] = (byte)(i + 1);
            engine.WritePageImmediate(pageIds[i], data);
        }
        engine.FlushPageFile();

        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        // Readers and writers run concurrently
        var tasks = new List<Task>();

        // Concurrent readers
        for (int r = 0; r < readerCount; r++)
        {
            var idx = r % pageCount;
            tasks.Add(Task.Run(() =>
            {
                try
                {
                    var buf = new byte[engine.PageSize];
                    engine.ReadPage(pageIds[idx], null, buf);
                    // Sentinel must equal the page index + 1
                    Assert.Equal((byte)(idx + 1), buf[0]);
                }
                catch (Exception ex) { errors.Add(ex); }
            }));
        }

        // Concurrent transactional writers (may trigger file growth on first write
        // if the engine was freshly opened with Small page config)
        for (int w = 0; w < 10; w++)
        {
            var idx = w % pageCount;
            tasks.Add(Task.Run(() =>
            {
                try
                {
                    var txn = engine.BeginTransaction();
                    var data = new byte[engine.PageSize];
                    data[0] = (byte)(idx + 1);
                    engine.WritePage(pageIds[idx], txn.TransactionId, data);
                    engine.CommitTransaction(txn);
                }
                catch (Exception ex) { errors.Add(ex); }
            }));
        }

        await Task.WhenAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public async Task ConcurrentReadWrite_IndexFile_NoCrash()
    {
        var dbPath  = Path.Combine(_tempDir, "concur_idx.db");
        var idxPath = Path.Combine(_tempDir, "concur_idx.idx");
        var config  = PageFileConfig.Small with { IndexFilePath = idxPath };

        using var engine = new StorageEngine(dbPath, config);

        const int pageCount = 20;
        var pageIds = new uint[pageCount];
        for (int i = 0; i < pageCount; i++)
            pageIds[i] = engine.AllocateIndexPage();

        for (int i = 0; i < pageCount; i++)
        {
            var data = new byte[engine.PageSize];
            data[0] = (byte)(i + 1);
            engine.WritePageImmediate(pageIds[i], data);
        }
        engine.FlushPageFile();

        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();
        var tasks  = new List<Task>();

        for (int r = 0; r < 20; r++)
        {
            var idx = r % pageCount;
            tasks.Add(Task.Run(() =>
            {
                try
                {
                    var buf = new byte[engine.PageSize];
                    engine.ReadPage(pageIds[idx], null, buf);
                    Assert.Equal((byte)(idx + 1), buf[0]);
                }
                catch (Exception ex) { errors.Add(ex); }
            }));
        }

        for (int w = 0; w < 10; w++)
        {
            var idx = w % pageCount;
            tasks.Add(Task.Run(() =>
            {
                try
                {
                    var txn = engine.BeginTransaction();
                    var data = new byte[engine.PageSize];
                    data[0] = (byte)(idx + 1);
                    engine.WritePage(pageIds[idx], txn.TransactionId, data);
                    engine.CommitTransaction(txn);
                }
                catch (Exception ex) { errors.Add(ex); }
            }));
        }

        await Task.WhenAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public async Task ConcurrentReadWrite_CollectionFile_NoCrash()
    {
        var dbPath  = Path.Combine(_tempDir, "concur_coll.db");
        var collDir = Path.Combine(_tempDir, "concur_coll_dir");
        var config  = PageFileConfig.Small with { CollectionDataDirectory = collDir };

        using var engine = new StorageEngine(dbPath, config);

        const int pageCount = 20;
        var pageIds = new uint[pageCount];
        for (int i = 0; i < pageCount; i++)
            pageIds[i] = engine.AllocateCollectionPage("items");

        for (int i = 0; i < pageCount; i++)
        {
            var data = new byte[engine.PageSize];
            data[0] = (byte)(i + 1);
            engine.WritePageImmediate(pageIds[i], data);
        }
        engine.FlushPageFile();

        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();
        var tasks  = new List<Task>();

        for (int r = 0; r < 20; r++)
        {
            var idx = r % pageCount;
            tasks.Add(Task.Run(() =>
            {
                try
                {
                    var buf = new byte[engine.PageSize];
                    engine.ReadPage(pageIds[idx], null, buf);
                    Assert.Equal((byte)(idx + 1), buf[0]);
                }
                catch (Exception ex) { errors.Add(ex); }
            }));
        }

        for (int w = 0; w < 10; w++)
        {
            var idx = w % pageCount;
            tasks.Add(Task.Run(() =>
            {
                try
                {
                    var txn = engine.BeginTransaction();
                    var data = new byte[engine.PageSize];
                    data[0] = (byte)(idx + 1);
                    engine.WritePage(pageIds[idx], txn.TransactionId, data);
                    engine.CommitTransaction(txn);
                }
                catch (Exception ex) { errors.Add(ex); }
            }));
        }

        await Task.WhenAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public async Task ConcurrentAllocateAndRead_NoCrash()
    {
        // Exercises the race where AllocatePage() holds the write lock and grows
        // the file while concurrent ReadPage() calls hold a read lock.
        var dbPath = Path.Combine(_tempDir, "concur_alloc.db");
        using var engine = new StorageEngine(dbPath, PageFileConfig.Small);

        // Pre-allocate a stable page to read from during the race
        var stablePage = engine.AllocatePage();
        var seedData   = new byte[engine.PageSize];
        seedData[0] = 0x42;
        engine.WritePageImmediate(stablePage, seedData);
        engine.FlushPageFile();

        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();
        var tasks  = new List<Task>();

        // Readers continuously read the stable page
        for (int r = 0; r < 20; r++)
        {
            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int iter = 0; iter < 50; iter++)
                    {
                        var buf = new byte[engine.PageSize];
                        engine.ReadPage(stablePage, null, buf);
                        // Verify that the stable page content remains correct
                        Assert.Equal(0x42, buf[0]);
                    }
                }
                catch (Exception ex) { errors.Add(ex); }
            }));
        }

        // Writers allocate new pages, potentially growing the file
        for (int w = 0; w < 5; w++)
        {
            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int iter = 0; iter < 10; iter++)
                    {
                        var newPage = engine.AllocatePage();
                        var data = new byte[engine.PageSize];
                        engine.WritePageImmediate(newPage, data);
                    }
                }
                catch (Exception ex) { errors.Add(ex); }
            }));
        }

        await Task.WhenAll(tasks);
        Assert.Empty(errors);
    }
}
