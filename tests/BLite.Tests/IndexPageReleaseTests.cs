using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;
using BLite.Core.Storage;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Regression tests for releasing the pages of indexes:
/// <list type="bullet">
///   <item>#238: freeing a page twice must be a no-op, never a free list that points at itself
///   and hands the same page to two owners.</item>
///   <item>#211: <see cref="DynamicCollection"/> frees the old tree on <c>DropIndex</c> and on
///   <c>VacuumAsync(RebuildIndexes)</c>, for B-Tree, vector and spatial indexes, and a failed
///   rebuild keeps the old index and frees the copy.</item>
///   <item>#209: repeatedly creating and dropping an index does not grow the file without bound.</item>
/// </list>
/// </summary>
public class IndexPageReleaseTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"idxrelease_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var f in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
            try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
    }

    // ======================================================================
    //  #238: double free
    // ======================================================================

    /// <summary>
    /// A page without a WAL image goes straight to the file's free list. Freeing it twice must
    /// not push it twice: the next two allocations would then both return it.
    /// </summary>
    [Fact]
    public void FreePage_Twice_HandsThePageOutOnce()
    {
        using var storage = new StorageEngine(_dbPath, PageFileConfig.Default);
        var page = storage.AllocatePage();
        var other = storage.AllocatePage();

        storage.FreePage(page);
        storage.FreePage(page);

        var first = storage.AllocatePage();
        var second = storage.AllocatePage();
        Assert.Equal(page, first);
        Assert.NotEqual(first, second);
        Assert.NotEqual(other, second);
    }

    /// <summary>
    /// A page queued for the next commit (<see cref="StorageEngine.FreePageOnCommit"/>) and also
    /// freed directly, as when an index is dropped while a transaction has merged its nodes, is
    /// handed out once after the commit.
    /// </summary>
    [Fact]
    public async Task FreePage_AndFreePageOnCommit_HandTheSamePageOutOnce()
    {
        using var storage = new StorageEngine(_dbPath, PageFileConfig.Default);
        var page = storage.AllocatePage();
        var written = storage.AllocatePage();

        using (var txn = storage.BeginTransaction())
        {
            storage.WritePage(written, txn.TransactionId, new byte[storage.PageSize]);
            storage.FreePageOnCommit(page, txn.TransactionId);
            storage.FreePage(page);
            await txn.CommitAsync();
        }

        var first = storage.AllocatePage();
        var second = storage.AllocatePage();
        Assert.Equal(page, first);
        Assert.NotEqual(first, second);
    }

    /// <summary>
    /// Dropping an index while an open transaction has merged nodes of it: the drop frees the
    /// committed tree at once, and the commit frees the merged-away nodes again (#233). The free
    /// list must still hand out every page once.
    /// <para>
    /// A merged-away node the transaction itself wrote is held in the WAL-pending set, which
    /// dedupes. The file-level double free needs a node it never wrote: keys inserted in
    /// ascending order leave every leaf but the last at the minimum fill, so deleting the lowest
    /// key underflows the leftmost leaf, which cannot borrow and merges into its untouched right
    /// sibling.
    /// </para>
    /// </summary>
    [Fact]
    public async Task DropIndex_WhileTransactionMergedItsNodes_FreesEachPageOnce()
    {
        using var db = new TestDbContext(_dbPath);
        var people = Enumerable.Range(0, 3000).Select(i => new PersonV2 { Name = $"p{i}", Age = i }).ToList();
        await db.PeopleV2.InsertBulkAsync(people);
        await db.CheckpointAsync(); // the tree's pages have no WAL image: frees reach the file at once
        var treePages = db.PeopleV2.IndexManager.GetIndex("idx_Age")!.CollectAllPages(0);
        Assert.True(treePages.Count > 10, "the index must span several pages so deletes merge nodes");

        using (var txn = db.BeginTransaction())
        {
            foreach (var p in people.Take(5))
                Assert.True(await db.PeopleV2.DeleteAsync(p.Id, txn));

            Assert.True(await db.PeopleV2.DropIndexAsync("idx_Age"));
            await txn.CommitAsync();
        }

        AssertFreeListHandsEachPageOutOnce(db.Storage);
        Assert.Equal(people.Count - 5, db.PeopleV2.AsQueryable().Count());
    }

    // ======================================================================
    //  #211: DynamicCollection releases old index trees
    // ======================================================================

    [Theory]
    [InlineData(IndexType.BTree)]
    [InlineData(IndexType.Vector)]
    [InlineData(IndexType.Spatial)]
    public async Task DynamicCollection_DropIndex_FreesTheTree(IndexType type)
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("docs");
        await InsertIndexableDocsAsync(col, 3000);
        var name = await CreateDynamicIndexAsync(col, type);
        await engine.CheckpointAsync();
        var oldPages = DynamicIndexPages(engine.Storage, "docs", name);
        Assert.True(oldPages.Count > 1, "the index must span more than its root");

        Assert.True(col.DropIndex(name));

        Assert.DoesNotContain(engine.Storage.GetCollectionMetadata("docs")!.Indexes, i => i.Name == name);
        await AssertPagesFreedAsync(engine, oldPages);
    }

    [Theory]
    [InlineData(IndexType.BTree)]
    [InlineData(IndexType.Vector)]
    [InlineData(IndexType.Spatial)]
    public async Task DynamicCollection_VacuumRebuildIndexes_FreesTheOldTree(IndexType type)
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("docs");
        await InsertIndexableDocsAsync(col, 3000);
        var name = await CreateDynamicIndexAsync(col, type);
        await engine.CheckpointAsync();
        var rootBefore = DynamicIndexRoot(engine.Storage, "docs", name);
        var oldPages = DynamicIndexPages(engine.Storage, "docs", name);
        Assert.True(oldPages.Count > 1, "the index must span more than its root");

        await col.VacuumAsync(new VacuumOptions { RebuildIndexes = true, TruncateFile = false });

        Assert.NotEqual(rootBefore, DynamicIndexRoot(engine.Storage, "docs", name));
        await AssertDynamicIndexAnswersAsync(col, type, name);
        await AssertPagesFreedAsync(engine, oldPages);
    }

    /// <summary>
    /// A rebuild that fails on a document that cannot be read keeps the old index, in memory and
    /// on disk, reports the failure and frees the half-built copy, including the pages its splits
    /// added outside the transaction.
    /// </summary>
    [Fact]
    public async Task DynamicCollection_VacuumRebuildIndexes_UnreadableDocument_FailsKeepsIndexAndFreesCopy()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("docs");
        await InsertIndexableDocsAsync(col, 3000);
        var name = await CreateDynamicIndexAsync(col, IndexType.BTree);
        await engine.CheckpointAsync();
        var rootBefore = DynamicIndexRoot(engine.Storage, "docs", name);
        CorruptLastDocument(col, engine.Storage);

        await Assert.ThrowsAnyAsync<Exception>(() => col.VacuumAsync(new VacuumOptions { RebuildIndexes = true, TruncateFile = false }));

        Assert.Equal(rootBefore, DynamicIndexRoot(engine.Storage, "docs", name));
        Assert.Equal(3000 / 90 + 1, (await col.QueryIndexAsync(name, 7, 7).ToListAsync()).Count);
        await AssertCopyWasFreedAsync(engine);
    }

    // ======================================================================
    //  #209: create/drop cycles
    // ======================================================================

    /// <summary>
    /// Each dropped tree's pages have a WAL image, so they wait for a WAL truncation before they
    /// can be reused, and a tight loop never fills the WAL enough to checkpoint on its own: every
    /// cycle appended a fresh tree. The pending frees must trigger the checkpoint themselves.
    /// </summary>
    [Fact]
    public async Task CreateDropIndexCycles_Typed_DoNotGrowTheFile()
    {
        using var db = new TestDbContext(_dbPath);
        await db.PeopleV2.InsertBulkAsync(Enumerable.Range(0, 1000).Select(i => new PersonV2 { Name = $"p{i}", Age = i % 90 }));

        async Task CycleAsync()
        {
            await db.PeopleV2.CreateIndexAsync(p => p.Name, "idx_tmp");
            await db.PeopleV2.DropIndexAsync("idx_tmp");
        }

        await AssertCyclesAreBoundedAsync(db.Storage, () => db.CheckpointAsync(), CycleAsync);
    }

    [Fact]
    public async Task CreateDropIndexCycles_Dynamic_DoNotGrowTheFile()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("docs");
        await InsertIndexableDocsAsync(col, 1000);

        async Task CycleAsync()
        {
            await col.CreateIndexAsync("value", "idx_tmp");
            Assert.True(col.DropIndex("idx_tmp"));
        }

        await AssertCyclesAreBoundedAsync(engine.Storage, () => engine.CheckpointAsync(), CycleAsync);
    }

    [Fact]
    public async Task FailedUniqueCreateIndexCycles_Dynamic_DoNotGrowTheFile()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("docs");
        // 1000 distinct codes, then a duplicate of the first: the fill writes a tree of several
        // leaves before it fails (#271), so the split pages must be freed too (#211).
        await col.InsertBulkAsync(Enumerable.Range(0, 1001).Select(i => col.CreateDocument(
            ["code", "payload"], b => b.AddInt32("code", i % 1000).AddString("payload", $"data-{i:D6}"))));

        async Task CycleAsync()
        {
            await Assert.ThrowsAnyAsync<InvalidOperationException>(() => col.CreateIndexAsync("code", "idx_code", unique: true));
            Assert.Empty(col.ListIndexes());
        }

        await AssertCyclesAreBoundedAsync(engine.Storage, () => engine.CheckpointAsync(), CycleAsync);
    }

    /// <summary>
    /// Runs a priming block of cycles that frees more pages than the pending-free threshold (4 MB:
    /// 256 pages of 16 KB; a tree of 1000 keys is a handful of pages), releases them all, then a
    /// measured block: it must allocate (almost) no new pages, because the dropped trees are
    /// reused. The checkpoint the queued frees request runs in the background; the loop waits
    /// for it, and for any other checkpoint, after each cycle instead of racing it, so the
    /// outcome does not depend on the machine's speed. Without the request the measured block
    /// appends one tree per cycle (hundreds of pages beyond the primed free list).
    /// </summary>
    private static async Task AssertCyclesAreBoundedAsync(StorageEngine storage, Func<Task> checkpoint, Func<Task> cycle)
    {
        const int priming = 100, measured = 200;

        async Task CyclesAsync(int n)
        {
            for (int i = 0; i < n; i++)
            {
                await cycle();
                await storage.PendingFreeCheckpointCompletion;
                await WaitForRunningCheckpointAsync(storage);
            }
        }

        await CyclesAsync(priming);
        await ReleasePendingFreesAsync(storage, checkpoint);
        var highWaterBefore = MainFile(storage).NextPageId;

        await CyclesAsync(measured);
        await ReleasePendingFreesAsync(storage, checkpoint);
        var highWaterAfter = MainFile(storage).NextPageId;

        var grown = (long)highWaterAfter - highWaterBefore;
        Assert.True(grown <= 16, $"{measured} create/drop cycles grew the file by {grown} pages ({grown * storage.PageSize / 1024} KB)");
    }

    /// <summary>
    /// Checkpoints until no freed page is still waiting for a WAL truncation. An explicit
    /// checkpoint returns without truncating while a background one is running.
    /// </summary>
    private static async Task ReleasePendingFreesAsync(StorageEngine storage, Func<Task> checkpoint)
    {
        for (int attempt = 0; attempt < 50; attempt++)
        {
            await checkpoint();
            if (PendingFreePages(storage).Count == 0) return;
            await Task.Delay(20);
        }
        Assert.Fail($"{PendingFreePages(storage).Count} freed pages are still waiting for a WAL truncation");
    }

    /// <summary>Waits until no checkpoint (requested by a commit or by the queued frees) is running.</summary>
    private static async Task WaitForRunningCheckpointAsync(StorageEngine storage)
    {
        var field = typeof(StorageEngine).GetField("_checkpointRunning", Flags)!;
        for (int attempt = 0; attempt < 5000 && (int)field.GetValue(storage)! != 0; attempt++)
            await Task.Delay(1);
        Assert.Equal(0, (int)field.GetValue(storage)!);
    }

    /// <summary>
    /// The pending-free checkpoint runs in the background. Disposing the engine while it runs
    /// used to close the files under it: the engine's own closing checkpoint returned at once
    /// because one was running, and the queued pages were leaked for good. Dispose now waits for
    /// it, however long it takes (the next test); here the task must have completed when Dispose
    /// returns, whatever the machine's speed, and the reopened file must reuse the pages.
    /// </summary>
    [Fact]
    public async Task Dispose_DuringPendingFreeCheckpoint_DoesNotLeakTheQueuedPages()
    {
        uint highWater;
        int count;
        Task checkpoint;
        using (var storage = new StorageEngine(_dbPath, PageFileConfig.Default))
        {
            count = (int)(4L * 1024 * 1024 / storage.PageSize) + 16; // past the 4 MB threshold
            var pages = new List<uint>(count);
            using (var txn = storage.BeginTransaction())
            {
                for (int i = 0; i < count; i++)
                {
                    var page = storage.AllocatePage();
                    storage.WritePage(page, txn.TransactionId, new byte[storage.PageSize]);
                    pages.Add(page);
                }
                await txn.CommitAsync();
            }
            highWater = MainFile(storage).NextPageId;

            foreach (var page in pages)
                storage.FreePage(page); // queued (WAL images); crossing the threshold requests a checkpoint
            checkpoint = storage.PendingFreeCheckpointCompletion;
            Assert.NotSame(Task.CompletedTask, checkpoint); // one was requested
        }
        Assert.True(checkpoint.IsCompleted, "Dispose returned while the pending-free checkpoint was still running");

        using (var storage = new StorageEngine(_dbPath, PageFileConfig.Default))
        {
            for (int i = 0; i < count; i++)
                Assert.True(storage.AllocatePage() < highWater, $"page {i} was appended: the queued frees were leaked");
        }
    }

    /// <summary>
    /// Dispose waits for the pending-free checkpoint until it completes, not for the lock timeout:
    /// a checkpoint slower than that (as on a loaded CI runner) was cut short, and the pages it had
    /// not released yet were leaked. A task the test completes stands in for the checkpoint, so the
    /// outcome does not depend on how long a real one takes on this machine: Dispose must still be
    /// waiting well past the lock timeout, and return once the task completes.
    /// </summary>
    [Fact]
    public async Task Dispose_WaitsForThePendingFreeCheckpoint_LongerThanTheLockTimeout()
    {
        var storage = new StorageEngine(_dbPath, PageFileConfig.Default);
        var timeout = storage.LockTimeout.WriteTimeoutMs;
        Assert.InRange(timeout, 1, 1000);
        var checkpoint = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        typeof(StorageEngine).GetField("_pendingFreeCheckpointTask", Flags)!.SetValue(storage, checkpoint.Task);

        var dispose = Task.Run(storage.Dispose);
        await Task.WhenAny(dispose, Task.Delay(3 * timeout));
        Assert.False(dispose.IsCompleted, $"Dispose returned within {3 * timeout} ms while the pending-free checkpoint was still running");

        checkpoint.SetResult(true);
        await dispose.WaitAsync(TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// DropIndex frees the tree, so it must not run while a writer holds the collection lock and
    /// may be mid-split in that index.
    /// </summary>
    [Fact]
    public async Task DynamicCollection_DropIndex_WaitsForTheCollectionLock()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("docs");
        await InsertIndexableDocsAsync(col, 100);
        await col.CreateIndexAsync("value", "idx_value");

        var collectionLock = (SemaphoreSlim)typeof(DynamicCollection).GetField("_collectionLock", Flags)!.GetValue(col)!;
        await collectionLock.WaitAsync();
        Task<bool> drop;
        try
        {
            drop = Task.Run(() => col.DropIndex("idx_value"));
            await Task.Delay(100);
            Assert.False(drop.IsCompleted);
            Assert.Contains("idx_value", col.ListIndexes());
        }
        finally
        {
            collectionLock.Release();
        }

        Assert.True(await drop);
        Assert.DoesNotContain("idx_value", col.ListIndexes());
    }

    /// <summary>
    /// Inserts from several tasks while another task drops one of two indexes: every document is
    /// stored and found through the remaining index, no page is handed out twice, and no page of
    /// the remaining index was freed.
    /// </summary>
    [Fact]
    public async Task DynamicCollection_DropIndex_WhileInserting_KeepsCollectionAndOtherIndexConsistent()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("docs");
        await InsertIndexableDocsAsync(col, 500);
        await col.CreateIndexAsync("value", "idx_value");
        await col.CreateIndexAsync("payload", "idx_payload");
        await engine.CheckpointAsync();

        const int writers = 8, perWriter = 150;
        var inserts = Enumerable.Range(0, writers).Select(w => Task.Run(async () =>
        {
            for (int i = 0; i < perWriter; i++)
                await col.InsertAsync(col.CreateDocument(["value", "payload"],
                    b => b.AddInt32("value", 1000 + w * perWriter + i).AddString("payload", $"w{w}-{i:D4}")));
        })).ToList();
        var drop = Task.Run(async () => { await Task.Delay(20); return col.DropIndex("idx_value"); });
        await Task.WhenAll(inserts);
        Assert.True(await drop);

        const int total = 500 + writers * perWriter;
        Assert.Equal(total, (await col.FindAllAsync().ToListAsync()).Count);
        Assert.Equal(total, (await col.QueryIndexAsync("idx_payload", null, null).ToListAsync()).Count);
        Assert.Equal(["idx_payload"], col.ListIndexes());

        await engine.CheckpointAsync();
        var livePages = DynamicIndexPages(engine.Storage, "docs", "idx_payload").ToHashSet();
        var reused = DrainFreeList(engine.Storage);
        Assert.Empty(livePages.Intersect(reused));
    }

    // ======================================================================
    //  Helpers
    // ======================================================================

    private static readonly System.Reflection.BindingFlags Flags =
        System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance;

    private static IPageStorage MainFile(StorageEngine storage)
        => (IPageStorage)typeof(StorageEngine).GetField("_pageFile", Flags)!.GetValue(storage)!;

    private static HashSet<uint> PendingFreePages(StorageEngine storage)
        => ((HashSet<uint>)typeof(StorageEngine).GetField("_pendingFreePages", Flags)!.GetValue(storage)!).ToHashSet();

    /// <summary>
    /// Allocates from the free list until it is exhausted and returns what it handed out. Fails
    /// if any page comes out twice (a free list that points at itself never ends).
    /// </summary>
    private static HashSet<uint> DrainFreeList(StorageEngine storage)
    {
        var highWater = MainFile(storage).NextPageId;
        var reused = new HashSet<uint>();
        uint allocated;
        while ((allocated = storage.AllocatePage()) < highWater)
        {
            Assert.True(reused.Add(allocated), $"page {allocated} was handed out twice");
            Assert.True(reused.Count <= highWater, "the free list is longer than the file");
        }
        return reused;
    }

    private static void AssertFreeListHandsEachPageOutOnce(StorageEngine storage) => DrainFreeList(storage);

    /// <summary>
    /// Every page in <paramref name="pages"/> is either back on the free list (handed out next,
    /// below the high-water mark) or still queued for the next WAL truncation.
    /// </summary>
    private static async Task AssertPagesFreedAsync(BLiteEngine engine, IReadOnlyCollection<uint> pages)
    {
        await engine.CheckpointAsync();
        var storage = engine.Storage;
        var queued = PendingFreePages(storage);
        var reused = DrainFreeList(storage);
        var leaked = pages.Where(p => !reused.Contains(p) && !queued.Contains(p)).ToList();
        Assert.True(leaked.Count == 0, $"{leaked.Count} of {pages.Count} pages were not freed: {string.Join(", ", leaked.Take(10))}");
    }

    /// <summary>
    /// The copy of a 3000-entry index spans dozens of pages. Each is either back on the free list
    /// or still queued for the next WAL truncation.
    /// </summary>
    private static async Task AssertCopyWasFreedAsync(BLiteEngine engine)
    {
        await engine.CheckpointAsync();
        var storage = engine.Storage;
        int queued = PendingFreePages(storage).Count;
        int reused = DrainFreeList(storage).Count;
        Assert.True(reused + queued > 20, $"only {reused} pages were returned to the free list and {queued} queued");
    }

    private static async Task InsertIndexableDocsAsync(DynamicCollection col, int count)
    {
        var docs = Enumerable.Range(0, count).Select(i => col.CreateDocument(
            ["value", "embedding", "location", "payload"],
            b => b.AddInt32("value", i % 90)
                  .AddFloatArray("embedding", [i % 17, i % 23, i % 29])
                  .AddCoordinates("location", (40.0 + (i % 100) * 0.01, 9.0 + (i / 100) * 0.01))
                  .AddString("payload", $"data-{i:D6}")));
        await col.InsertBulkAsync(docs);
    }

    private static async Task<string> CreateDynamicIndexAsync(DynamicCollection col, IndexType type)
    {
        switch (type)
        {
            case IndexType.BTree: await col.CreateIndexAsync("value", "idx_value"); return "idx_value";
            case IndexType.Vector: await col.CreateVectorIndexAsync("embedding", 3, VectorMetric.L2, "idx_embedding"); return "idx_embedding";
            case IndexType.Spatial: await col.CreateSpatialIndexAsync("location", "idx_location"); return "idx_location";
            default: throw new ArgumentOutOfRangeException(nameof(type));
        }
    }

    private static async Task AssertDynamicIndexAnswersAsync(DynamicCollection col, IndexType type, string name)
    {
        switch (type)
        {
            case IndexType.BTree:
                Assert.Equal(3000 / 90 + 1, (await col.QueryIndexAsync(name, 7, 7).ToListAsync()).Count);
                break;
            case IndexType.Vector:
                Assert.Single(await col.VectorSearchAsync(name, [0f, 0f, 0f], k: 1).ToListAsync());
                break;
            case IndexType.Spatial:
                Assert.Equal(100, (await col.WithinAsync(name, (40.0, 9.0), (40.095, 9.095)).ToListAsync()).Count);
                break;
        }
    }

    private static IndexMetadata DynamicIndexMetadata(StorageEngine storage, string collection, string name)
        => storage.GetCollectionMetadata(collection)!.Indexes.Single(i => i.Name == name);

    private static uint DynamicIndexRoot(StorageEngine storage, string collection, string name)
        => DynamicIndexMetadata(storage, collection, name).RootPageId;

    /// <summary>Walks the persisted tree of a dynamic collection's index from its stored root.</summary>
    private static IReadOnlyList<uint> DynamicIndexPages(StorageEngine storage, string collection, string name)
    {
        var meta = DynamicIndexMetadata(storage, collection, name);
        var field = meta.PropertyPaths[0];
        return meta.Type switch
        {
            IndexType.BTree => new BTreeIndex(storage, IndexOptions.CreateBTree(field), meta.RootPageId).CollectAllPages(),
            IndexType.Vector => new VectorSearchIndex(storage, IndexOptions.CreateVector(meta.Dimensions, meta.Metric, 16, 200, field), meta.RootPageId).CollectAllPages(),
            IndexType.Spatial => new RTreeIndex(storage, IndexOptions.CreateSpatial(field), meta.RootPageId).CollectAllPages(),
            _ => throw new ArgumentOutOfRangeException(nameof(name), $"unsupported index type {meta.Type}"),
        };
    }

    /// <summary>Corrupts the last document in primary-key order so a rebuild fails after most inserts.</summary>
    private static void CorruptLastDocument(DynamicCollection col, StorageEngine storage)
    {
        var primary = (BTreeIndex)typeof(DynamicCollection).GetField("_primaryIndex", Flags)!.GetValue(col)!;
        var location = primary.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Backward).First().Location;
        var page = new byte[storage.PageSize];
        storage.ReadPage(location.PageId, null, page);
        var slot = SlotEntry.ReadFrom(page.AsSpan(SlottedPageHeader.Size + location.SlotIndex * SlotEntry.Size));
        page.AsSpan(slot.Offset + 4, 12).Fill(0xFF); // invalid element types
        storage.WritePageImmediate(location.PageId, page);
    }
}
