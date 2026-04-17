using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace BLite.Tests;

/// <summary>
/// Tests for the in-memory storage backend (<see cref="MemoryPageStorage"/>,
/// <see cref="MemoryWriteAheadLog"/>) and the <see cref="BLiteEngine.CreateInMemory"/> factory.
/// These tests exercise the full database stack without touching the file system,
/// which is the foundation for WASM support.
/// </summary>
public class InMemoryStorageTests
{
    // ─── MemoryPageStorage unit tests ────────────────────────────────────────

    [Fact]
    public void MemoryPageStorage_Open_InitializesPages()
    {
        using var storage = new MemoryPageStorage(16384);
        storage.Open();

        Assert.Equal(16384, storage.PageSize);
        Assert.Equal(2u, storage.NextPageId); // pages 0 and 1 are pre-allocated

        // Page 0 must contain a valid PageHeader
        var buf = new byte[16384];
        storage.ReadPage(0, buf);
        var header = PageHeader.ReadFrom(buf);
        Assert.Equal(PageType.Header, header.PageType);
        Assert.Equal(PageHeader.CurrentFormatVersion, header.FormatVersion);
    }

    [Fact]
    public void MemoryPageStorage_AllocatePage_ReturnsSequentialIds()
    {
        using var storage = new MemoryPageStorage(16384);
        storage.Open(); // _nextPageId = 2

        var id1 = storage.AllocatePage();
        var id2 = storage.AllocatePage();
        var id3 = storage.AllocatePage();

        Assert.Equal(2u, id1);
        Assert.Equal(3u, id2);
        Assert.Equal(4u, id3);
    }

    [Fact]
    public void MemoryPageStorage_FreePage_ReusesFreePages()
    {
        using var storage = new MemoryPageStorage(16384);
        storage.Open();

        var id1 = storage.AllocatePage(); // 2
        var id2 = storage.AllocatePage(); // 3
        storage.FreePage(id1);

        var reused = storage.AllocatePage();
        Assert.Equal(id1, reused);

        // After reuse, next allocation should be fresh
        var next = storage.AllocatePage();
        Assert.Equal(id2 + 1, next); // id2=3, so next=4
    }

    [Fact]
    public void MemoryPageStorage_WriteThenRead_RoundTrips()
    {
        using var storage = new MemoryPageStorage(16384);
        storage.Open();

        var pageId = storage.AllocatePage();
        var written = new byte[16384];
        written[0] = 0xAB;
        written[100] = 0xCD;
        written[16383] = 0xEF;

        storage.WritePage(pageId, written);

        var read = new byte[16384];
        storage.ReadPage(pageId, read);

        Assert.Equal(written[0], read[0]);
        Assert.Equal(written[100], read[100]);
        Assert.Equal(written[16383], read[16383]);
    }

    [Fact]
    public void MemoryPageStorage_ReadUnallocatedPage_ReturnsZeroes()
    {
        using var storage = new MemoryPageStorage(16384);
        storage.Open();

        var buf = new byte[16384];
        storage.ReadPage(999, buf); // Page 999 was never written
        Assert.All(buf, b => Assert.Equal(0, b));
    }

    [Fact]
    public void MemoryPageStorage_ReadPageHeader_ReturnsPrefix()
    {
        using var storage = new MemoryPageStorage(16384);
        storage.Open();

        // Page 0 header starts at offset 0 — read just the 32-byte PageHeader
        var headerBuf = new byte[32];
        storage.ReadPageHeader(0, headerBuf);
        var header = PageHeader.ReadFrom(headerBuf);
        Assert.Equal(PageType.Header, header.PageType);
    }

    [Fact]
    public async Task MemoryPageStorage_ReadPageAsync_RoundTrips()
    {
        using var storage = new MemoryPageStorage(16384);
        storage.Open();

        var pageId = storage.AllocatePage();
        var written = new byte[16384];
        written[42] = 99;
        storage.WritePage(pageId, written);

        var read = new byte[16384];
        await storage.ReadPageAsync(pageId, read.AsMemory());
        Assert.Equal(99, read[42]);
    }

    [Fact]
    public void MemoryPageStorage_FlushAndFlushAsync_AreNoOps()
    {
        using var storage = new MemoryPageStorage(16384);
        storage.Open();
        storage.Flush(); // must not throw
        storage.FlushAsync().GetAwaiter().GetResult(); // must not throw
    }

    [Fact]
    public void MemoryPageStorage_BackupAsync_Throws()
    {
        using var storage = new MemoryPageStorage(16384);
        storage.Open();
        Assert.Throws<NotSupportedException>(() =>
            storage.BackupAsync("/tmp/backup.db").GetAwaiter().GetResult());
    }

    [Fact]
    public void MemoryPageStorage_FreePage0_Throws()
    {
        using var storage = new MemoryPageStorage(16384);
        storage.Open();
        Assert.Throws<InvalidOperationException>(() => storage.FreePage(0));
    }

    [Fact]
    public void MemoryPageStorage_AfterDispose_Throws()
    {
        var storage = new MemoryPageStorage(16384);
        storage.Open();
        storage.Dispose();

        var buf = new byte[16384];
        Assert.Throws<ObjectDisposedException>(() => storage.ReadPage(0, buf));
    }

    // ─── MemoryWriteAheadLog unit tests ──────────────────────────────────────

    [Fact]
    public async Task MemoryWriteAheadLog_WriteAndReadAll_RoundTrips()
    {
        using var wal = new MemoryWriteAheadLog();

        await wal.WriteBeginRecordAsync(1);
        await wal.WriteDataRecordAsync(1, 5, new byte[] { 1, 2, 3 }.AsMemory());
        await wal.WriteCommitRecordAsync(1);

        var records = wal.ReadAll();
        Assert.Equal(3, records.Count);
        Assert.Equal(WalRecordType.Begin, records[0].Type);
        Assert.Equal(WalRecordType.Write, records[1].Type);
        Assert.Equal(5u, records[1].PageId);
        Assert.Equal(WalRecordType.Commit, records[2].Type);
    }

    [Fact]
    public async Task MemoryWriteAheadLog_TruncateAsync_ClearsRecords()
    {
        using var wal = new MemoryWriteAheadLog();
        await wal.WriteBeginRecordAsync(1);
        await wal.WriteCommitRecordAsync(1);

        Assert.True(wal.GetCurrentSize() > 0);

        await wal.TruncateAsync();

        Assert.Equal(0, wal.GetCurrentSize());
        Assert.Empty(wal.ReadAll());
    }

    [Fact]
    public async Task MemoryWriteAheadLog_GetCurrentSize_TracksWrites()
    {
        using var wal = new MemoryWriteAheadLog();

        Assert.Equal(0, wal.GetCurrentSize());
        await wal.WriteBeginRecordAsync(1);
        Assert.True(wal.GetCurrentSize() > 0);
    }

    // ─── BLiteEngine.CreateInMemory integration tests ────────────────────────

    [Fact]
    public async Task CreateInMemory_InsertAndFind_Works()
    {
        using var engine = BLiteEngine.CreateInMemory();
        var col = engine.GetOrCreateCollection("users");

        var doc = col.CreateDocument(["_id", "name", "age"], b => b
            .AddString("name", "Alice")
            .AddInt32("age", 30));
        var id = await col.InsertAsync(doc);
        await engine.CommitAsync();

        var found = await col.FindByIdAsync(id);
        Assert.NotNull(found);
        Assert.True(found.TryGetInt32("age", out var age));
        Assert.Equal(30, age);
    }

    [Fact]
    public async Task CreateInMemory_MultipleCollections_Work()
    {
        using var engine = BLiteEngine.CreateInMemory();
        var users = engine.GetOrCreateCollection("users");
        var orders = engine.GetOrCreateCollection("orders");

        await users.InsertAsync(users.CreateDocument(["_id", "name"], b => b.AddString("name", "Bob")));
        await orders.InsertAsync(orders.CreateDocument(["_id", "item"], b => b.AddString("item", "Widget")));
        await engine.CommitAsync();

        Assert.Equal(1, await users.CountAsync());
        Assert.Equal(1, await orders.CountAsync());
    }

    [Fact]
    public async Task CreateInMemory_ExplicitTransaction_CommitMakesDataVisible()
    {
        using var engine = BLiteEngine.CreateInMemory();
        var col = engine.GetOrCreateCollection("items");

        var txn = await engine.BeginTransactionAsync();
        var doc = col.CreateDocument(["_id", "key"], b => b.AddString("key", "value"));
        await col.InsertAsync(doc, txn);
        await txn.CommitAsync();

        Assert.Equal(1, await col.CountAsync());
    }

    [Fact]
    public async Task CreateInMemory_ExplicitTransaction_RollbackDiscardsData()
    {
        using var engine = BLiteEngine.CreateInMemory();
        var col = engine.GetOrCreateCollection("items");

        var txn = await engine.BeginTransactionAsync();
        var doc = col.CreateDocument(["_id", "key"], b => b.AddString("key", "value"));
        await col.InsertAsync(doc, txn);
        await txn.RollbackAsync();

        Assert.Equal(0, await col.CountAsync());
    }

    [Fact]
    public async Task CreateInMemory_CustomPageSize_Works()
    {
        using var engine = BLiteEngine.CreateInMemory(pageSize: 8192);
        var col = engine.GetOrCreateCollection("data");

        var doc = col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 42));
        await col.InsertAsync(doc);
        await engine.CommitAsync();

        Assert.Equal(1, await col.CountAsync());
    }

    [Fact]
    public async Task CreateInMemory_DataNotPersisted_AfterDispose()
    {
        // Create, insert, dispose, then create again — data must be gone.
        var engine1 = BLiteEngine.CreateInMemory();
        var col1 = engine1.GetOrCreateCollection("data");
        var doc = col1.CreateDocument(["_id", "id"], b => b.AddInt32("id", 1));
        await col1.InsertAsync(doc);
        await engine1.CommitAsync();
        Assert.Equal(1, await col1.CountAsync());
        engine1.Dispose();

        // New engine — fresh memory — data is gone.
        using var engine2 = BLiteEngine.CreateInMemory();
        var col2 = engine2.GetOrCreateCollection("data");
        Assert.Equal(0, await col2.CountAsync());
    }

    // ─── BLiteEngine.CreateFromStorage integration tests ─────────────────────

    [Fact]
    public async Task CreateFromStorage_InsertAndFind_Works()
    {
        var pageStorage = new MemoryPageStorage(16384);
        pageStorage.Open();
        var wal = new MemoryWriteAheadLog();
        var storageEngine = new BLite.Core.Storage.StorageEngine(pageStorage, wal);

        using var engine = BLiteEngine.CreateFromStorage(storageEngine);
        var col = engine.GetOrCreateCollection("items");

        var doc = col.CreateDocument(["_id", "key"], b => b.AddString("key", "value"));
        await col.InsertAsync(doc);
        await engine.CommitAsync();

        var count = await col.CountAsync();
        Assert.Equal(1, count);
    }
}
