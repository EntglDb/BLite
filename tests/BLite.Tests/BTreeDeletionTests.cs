using BLite.Core.Indexing;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Tests for BTreeIndex deletion correctness, including B-Tree rebalancing operations:
///   - BorrowFromSibling (rotate-left / rotate-right)
///   - MergeWithSibling
///   - Root collapse (tree height reduction after merge)
///
/// Key constants: MaxEntriesPerNode = N, minEntries = N/2.
///
/// Tree state after inserting 1..N+1 in ascending order:
///   left  = [1..N/2]       (N/2 entries)
///   right = [N/2+1..N+1]   (N/2+1 entries)
///
/// Tree state after inserting N+1..1 in descending order:
///   left  = [1..N/2+1]     (N/2+1 entries)
///   right = [N/2+2..N+1]   (N/2 entries)
/// </summary>
public class BTreeDeletionTests : IDisposable
{
    private static readonly int N = BTreeIndex.MaxEntriesPerNode;
    private static readonly int Half = N / 2;
    private readonly string _dbPath;
    private readonly StorageEngine _storage;

    public BTreeDeletionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_btree_del_{Guid.NewGuid()}.db");
        _storage = new StorageEngine(_dbPath, PageFileConfig.Default);
    }

    public void Dispose()
    {
        _storage.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    private (BTreeIndex index, ulong txnId) CreateIndexWithItems(IEnumerable<int> values)
    {
        var opts = IndexOptions.CreateBTree("field");
        var index = new BTreeIndex(_storage, opts);
        var txnId = _storage.BeginTransaction().TransactionId;
        foreach (var v in values)
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();
        return (index, txnId);
    }

    private static List<int> AllKeys(BTreeIndex index, ulong txnId) =>
        index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txnId)
             .Select(e => e.Key.As<int>())
             .ToList();

    // ── Basic delete ────────────────────────────────────────────────────────

    [Fact]
    public void Delete_Basic_KeyNotFoundAfterDeletion()
    {
        var (index, txnId) = CreateIndexWithItems([10, 20, 30]);

        var deleted = index.Delete(IndexKey.Create(20), new DocumentLocation(20, 0), txnId);

        Assert.True(deleted);
        Assert.False(index.TryFind(IndexKey.Create(20), out _, txnId));
    }

    [Fact]
    public void Delete_ReturnsTrue_ForExistingKey()
    {
        var (index, txnId) = CreateIndexWithItems([5, 15, 25]);

        Assert.True(index.Delete(IndexKey.Create(5), new DocumentLocation(5, 0), txnId));
        Assert.True(index.Delete(IndexKey.Create(25), new DocumentLocation(25, 0), txnId));
    }

    [Fact]
    public void Delete_ReturnsFalse_ForNonExistentKey()
    {
        var (index, txnId) = CreateIndexWithItems([10, 20, 30]);

        Assert.False(index.Delete(IndexKey.Create(99), new DocumentLocation(99, 0), txnId));
    }

    [Fact]
    public void Delete_RemainingKeys_StillFindable()
    {
        var (index, txnId) = CreateIndexWithItems([1, 2, 3, 4, 5]);

        index.Delete(IndexKey.Create(3), new DocumentLocation(3, 0), txnId);

        Assert.True(index.TryFind(IndexKey.Create(1), out _, txnId));
        Assert.True(index.TryFind(IndexKey.Create(2), out _, txnId));
        Assert.True(index.TryFind(IndexKey.Create(4), out _, txnId));
        Assert.True(index.TryFind(IndexKey.Create(5), out _, txnId));
    }

    // ── Borrow from right sibling (rotate-left) ─────────────────────────────
    //
    // Setup: insert 1..N+1 ascending → left=[1..N/2] (N/2), right=[N/2+1..N+1] (N/2+1)
    // Delete key 1 from left → left underflows → right has N/2+1 > N/2 → borrow
    // Result: left=[2..N/2+1] (N/2), right=[N/2+2..N+1] (N/2)

    [Fact]
    public void Delete_BorrowFromRightSibling_RangeReturnsSortedRemainder()
    {
        var (index, txnId) = CreateIndexWithItems(Enumerable.Range(1, N + 1));

        // Delete key 1 (from the left leaf, which is at exactly minimum capacity).
        // Right sibling has N/2+1 entries (> min) → triggers borrow-from-right rotation.
        var deleted = index.Delete(IndexKey.Create(1), new DocumentLocation(1, 0), txnId);
        Assert.True(deleted);

        var keys = AllKeys(index, txnId);
        Assert.Equal(N, keys.Count);
        Assert.Equal(Enumerable.Range(2, N).ToList(), keys);
    }

    [Fact]
    public void Delete_BorrowFromRightSibling_AllKeysStillFindable()
    {
        var (index, txnId) = CreateIndexWithItems(Enumerable.Range(1, N + 1));

        index.Delete(IndexKey.Create(1), new DocumentLocation(1, 0), txnId);

        // Every key in 2..N+1 must be individually findable after rotation
        for (int v = 2; v <= N + 1; v++)
        {
            var found = index.TryFind(IndexKey.Create(v), out var loc, txnId);
            Assert.True(found, $"Key {v} not found after borrow-from-right rotation");
            Assert.Equal((uint)v, loc.PageId);
        }
    }

    // ── Borrow from left sibling (rotate-right) ──────────────────────────────
    //
    // Setup: insert N+1..1 descending → left=[1..N/2+1] (N/2+1), right=[N/2+2..N+1] (N/2)
    // Delete key N+1 from right → right underflows → no right sibling → borrow from left (N/2+1 > N/2)
    // Result: left=[1..N/2] (N/2), right=[N/2+1..N] (N/2)

    [Fact]
    public void Delete_BorrowFromLeftSibling_RangeReturnsSortedRemainder()
    {
        // Insert in descending order so left leaf ends up with N/2+1 entries
        var (index, txnId) = CreateIndexWithItems(Enumerable.Range(1, N + 1).Reverse());

        // Delete key N+1 (from the right leaf, which is at minimum capacity).
        // Right leaf has no right sibling; left sibling has N/2+1 entries (> min) → borrow-from-left.
        var deleted = index.Delete(IndexKey.Create(N + 1), new DocumentLocation((uint)(N + 1), 0), txnId);
        Assert.True(deleted);

        var keys = AllKeys(index, txnId);
        Assert.Equal(N, keys.Count);
        Assert.Equal(Enumerable.Range(1, N).ToList(), keys);
    }

    [Fact]
    public void Delete_BorrowFromLeftSibling_AllKeysStillFindable()
    {
        var (index, txnId) = CreateIndexWithItems(Enumerable.Range(1, N + 1).Reverse());

        index.Delete(IndexKey.Create(N + 1), new DocumentLocation((uint)(N + 1), 0), txnId);

        for (int v = 1; v <= N; v++)
        {
            var found = index.TryFind(IndexKey.Create(v), out var loc, txnId);
            Assert.True(found, $"Key {v} not found after borrow-from-left rotation");
            Assert.Equal((uint)v, loc.PageId);
        }
    }

    // ── Merge with sibling ───────────────────────────────────────────────────
    //
    // Setup: insert 1..N+1 → left=[1..N/2] (N/2), right=[N/2+1..N+1] (N/2+1)
    // Delete 1 → borrow from right → left=[2..N/2+1] (N/2), right=[N/2+2..N+1] (N/2)  [both at min]
    // Delete 2 → left underflows → right has N/2 = min (NOT > min) → no borrow → MERGE
    // After merge: single leaf=[3..N+1] (N-1 entries), internal root collapses

    [Fact]
    public void Delete_MergeLeaves_RangeReturnsSortedRemainder()
    {
        var (index, txnId) = CreateIndexWithItems(Enumerable.Range(1, N + 1));

        // First delete: triggers borrow-from-right (right had N/2+1)
        index.Delete(IndexKey.Create(1), new DocumentLocation(1, 0), txnId);
        // Second delete: triggers merge (both leaves now at min)
        index.Delete(IndexKey.Create(2), new DocumentLocation(2, 0), txnId);

        var keys = AllKeys(index, txnId);
        Assert.Equal(N - 1, keys.Count);
        Assert.Equal(Enumerable.Range(3, N - 1).ToList(), keys);
    }

    [Fact]
    public void Delete_MergeLeaves_AllKeysStillFindable()
    {
        var (index, txnId) = CreateIndexWithItems(Enumerable.Range(1, N + 1));

        index.Delete(IndexKey.Create(1), new DocumentLocation(1, 0), txnId);
        index.Delete(IndexKey.Create(2), new DocumentLocation(2, 0), txnId);

        for (int v = 3; v <= N + 1; v++)
        {
            var found = index.TryFind(IndexKey.Create(v), out _, txnId);
            Assert.True(found, $"Key {v} not found after merge");
        }
    }

    // ── Root collapse ────────────────────────────────────────────────────────
    //
    // After the merge above the tree should collapse from 3 pages (root internal + 2 leaves)
    // to 1 page (single leaf that becomes the new root).

    [Fact]
    public void Delete_MergeLeaves_CollapseRoot_SinglePageRemains()
    {
        var rootChanges = new List<uint>();
        var opts = IndexOptions.CreateBTree("field");
        var index = new BTreeIndex(_storage, opts, onRootChanged: newRoot => rootChanges.Add(newRoot));

        var txnId = _storage.BeginTransaction().TransactionId;
        int count = BTreeIndex.MaxEntriesPerNode + 1;
        foreach (var v in Enumerable.Range(1, count))
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        // Borrow first (brings both leaves to min)
        index.Delete(IndexKey.Create(1), new DocumentLocation(1, 0), txnId);
        // Merge + root collapse
        index.Delete(IndexKey.Create(2), new DocumentLocation(2, 0), txnId);

        // After root collapse, onRootChanged must have fired at least once during deletes
        Assert.NotEmpty(rootChanges);

        // The tree should now occupy exactly 1 page (the merged leaf = new root)
        var pages = index.CollectAllPages();
        Assert.Single(pages);

        // Sanity: full scan still works
        var keys = AllKeys(index, txnId);
        Assert.Equal(count - 2, keys.Count);
    }

    // ── Delete all entries ───────────────────────────────────────────────────

    [Fact]
    public void Delete_AllEntries_RangeReturnsEmpty()
    {
        var items = new[] { 10, 20, 30, 40, 50 };
        var (index, txnId) = CreateIndexWithItems(items);

        foreach (var v in items)
            index.Delete(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);

        var keys = AllKeys(index, txnId);
        Assert.Empty(keys);
    }

    [Fact]
    public void Delete_AllEntries_TryFindReturnsFalse()
    {
        var items = new[] { 1, 2, 3 };
        var (index, txnId) = CreateIndexWithItems(items);

        foreach (var v in items)
            index.Delete(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);

        Assert.False(index.TryFind(IndexKey.Create(1), out _, txnId));
        Assert.False(index.TryFind(IndexKey.Create(2), out _, txnId));
        Assert.False(index.TryFind(IndexKey.Create(3), out _, txnId));
    }

    [Fact]
    public void Delete_AllEntriesAcrossManyLeaves_RangeReturnsEmpty()
    {
        // 300 items spans 6 leaf pages → many merges required to drain the tree.
        // Values deliberately exceed 255 to verify correct big-endian key encoding.
        var values = Enumerable.Range(1, 300).ToList();
        var (index, txnId) = CreateIndexWithItems(values);

        foreach (var v in values)
            index.Delete(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);

        Assert.Empty(AllKeys(index, txnId));
    }

    // ── Many random deletes — remaining keys stay in sorted order ────────────

    [Fact]
    public void Delete_HalfOfManyEntries_RemainingInSortedOrder()
    {
        // Values exceed 255 to verify correct ordering across byte boundaries.
        const int max = 300;
        var values = Enumerable.Range(1, max)
            .OrderBy(x => (x * 37) % max)
            .ToList();

        var (index, txnId) = CreateIndexWithItems(values);

        // Delete all even numbers
        foreach (var v in values.Where(v => v % 2 == 0))
            index.Delete(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);

        var keys = AllKeys(index, txnId);
        var expected = Enumerable.Range(1, max).Where(v => v % 2 != 0).ToList();

        Assert.Equal(expected.Count, keys.Count);
        for (int i = 0; i < keys.Count - 1; i++)
            Assert.True(keys[i] < keys[i + 1], $"Out of order at index {i}: {keys[i]} >= {keys[i + 1]}");
        Assert.Equal(expected, keys);
    }

    [Fact]
    public void Delete_RandomDeletions_AllRemainingKeysFindable()
    {
        var (index, txnId) = CreateIndexWithItems(Enumerable.Range(1, 200));

        var toDelete = new HashSet<int>(Enumerable.Range(1, 200).Where(v => v % 3 == 0));
        foreach (var v in toDelete)
            index.Delete(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);

        for (int v = 1; v <= 200; v++)
        {
            bool found = index.TryFind(IndexKey.Create(v), out _, txnId);
            if (toDelete.Contains(v))
                Assert.False(found, $"Key {v} should have been deleted");
            else
                Assert.True(found, $"Key {v} should still be present");
        }
    }

    // ── Insert after delete ──────────────────────────────────────────────────

    [Fact]
    public void Insert_AfterDelete_KeyFindable()
    {
        var (index, txnId) = CreateIndexWithItems([10, 20, 30]);

        index.Delete(IndexKey.Create(20), new DocumentLocation(20, 0), txnId);
        index.Insert(IndexKey.Create(25), new DocumentLocation(25, 0), txnId);

        Assert.False(index.TryFind(IndexKey.Create(20), out _, txnId));
        Assert.True(index.TryFind(IndexKey.Create(25), out var loc, txnId));
        Assert.Equal(25u, loc.PageId);
    }

    [Fact]
    public void Insert_AfterMerge_TreeRemainsConsistent()
    {
        var (index, txnId) = CreateIndexWithItems(Enumerable.Range(1, N + 1));

        // Borrow then merge
        index.Delete(IndexKey.Create(1), new DocumentLocation(1, 0), txnId);
        index.Delete(IndexKey.Create(2), new DocumentLocation(2, 0), txnId);

        // Now insert new keys into the collapsed tree
        for (int v = 200; v <= 210; v++)
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);

        for (int v = 200; v <= 210; v++)
        {
            var found = index.TryFind(IndexKey.Create(v), out var loc, txnId);
            Assert.True(found, $"Key {v} not found after insert into merged tree");
            Assert.Equal((uint)v, loc.PageId);
        }
    }
}
