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
