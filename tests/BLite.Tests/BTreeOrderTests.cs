using BLite.Core.Indexing;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Tests that verify BTreeIndex maintains correct sorted order when entries are inserted
/// in non-sequential / non-lexicographic order.
///
/// Background: InsertIntoLeaf previously appended entries at the end of the page (unsorted),
/// which caused Range scans to return incorrect results because the scan breaks early
/// when it encounters a key greater than maxKey — even if smaller keys still exist later.
/// </summary>
public class BTreeOrderTests : IDisposable
{
    private readonly string _dbPath;
    private readonly StorageEngine _storage;

    public BTreeOrderTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_btree_order_{Guid.NewGuid()}.db");
        _storage = new StorageEngine(_dbPath, PageFileConfig.Default);
    }

    public void Dispose()
    {
        _storage.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    private (BTreeIndex index, ulong txnId) CreateIndex()
    {
        var opts = IndexOptions.CreateBTree("field");
        var index = new BTreeIndex(_storage, opts);
        var txnId = _storage.BeginTransaction().TransactionId;
        return (index, txnId);
    }

    // ── Integer keys ────────────────────────────────────────────────────────

    [Fact]
    public void Range_Int_InsertedOutOfOrder_ReturnsSortedRange()
    {
        var (index, txnId) = CreateIndex();
        // Insert in descending order
        foreach (var v in new[] { 50, 10, 40, 20, 30 })
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var keys = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txnId)
                        .Select(e => e.Key.As<int>())
                        .ToList();

        Assert.Equal(new[] { 10, 20, 30, 40, 50 }, keys);
    }

    [Fact]
    public void Range_Int_SubRange_InsertedOutOfOrder_ReturnsCorrectSlice()
    {
        var (index, txnId) = CreateIndex();
        // Insert 1–10 in shuffled order
        foreach (var v in new[] { 7, 3, 9, 1, 5, 2, 8, 4, 6, 10 })
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var keys = index.Range(IndexKey.Create(3), IndexKey.Create(7), IndexDirection.Forward, txnId)
                        .Select(e => e.Key.As<int>())
                        .ToList();

        Assert.Equal(new[] { 3, 4, 5, 6, 7 }, keys);
    }

    [Fact]
    public void Equal_Int_InsertedOutOfOrder_FindsKey()
    {
        var (index, txnId) = CreateIndex();
        foreach (var v in new[] { 30, 10, 20 })
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var results = index.Equal(IndexKey.Create(10), txnId).ToList();
        Assert.Single(results);
        Assert.Equal(10u, results[0].Location.PageId);
    }

    [Fact]
    public void Equal_Int_SmallestKey_InsertedLast_IsFound()
    {
        var (index, txnId) = CreateIndex();
        // Insert smallest key last → previously would be at wrong position in leaf
        foreach (var v in new[] { 50, 40, 30, 20, 10 })
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var results = index.Equal(IndexKey.Create(10), txnId).ToList();
        Assert.Single(results);
    }

    // ── String keys ─────────────────────────────────────────────────────────

    [Fact]
    public void Range_String_SeattlePortland_ReturnsPortlandFirst()
    {
        // This is the exact bug scenario that was fixed
        var (index, txnId) = CreateIndex();
        // Alternating: Seattle, Portland, Seattle, Portland, Seattle
        var words = new[] { "Seattle", "Portland", "Seattle", "Portland", "Seattle" };
        for (uint i = 0; i < words.Length; i++)
            index.Insert(IndexKey.Create(words[i]), new DocumentLocation(i + 1, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        // Portland < Seattle lexicographically — previously returned 0 results
        var portland = index.Range(IndexKey.Create("Portland"), IndexKey.Create("Portland"),
                                   IndexDirection.Forward, txnId).ToList();
        Assert.Equal(2, portland.Count);

        var seattle = index.Range(IndexKey.Create("Seattle"), IndexKey.Create("Seattle"),
                                  IndexDirection.Forward, txnId).ToList();
        Assert.Equal(3, seattle.Count);
    }

    [Fact]
    public void Range_String_FullScan_ReturnsSortedOrder()
    {
        var (index, txnId) = CreateIndex();
        var words = new[] { "Mango", "Apple", "Cherry", "Banana", "Date" };
        for (uint i = 0; i < (uint)words.Length; i++)
            index.Insert(IndexKey.Create(words[i]), new DocumentLocation(i + 1, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var keys = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txnId)
                        .Select(e => e.Key.As<string>())
                        .ToList();

        Assert.Equal(new[] { "Apple", "Banana", "Cherry", "Date", "Mango" }, keys);
    }

    [Fact]
    public void Range_String_BackwardScan_ReturnsSortedDescending()
    {
        var (index, txnId) = CreateIndex();
        var words = new[] { "Mango", "Apple", "Cherry" };
        for (uint i = 0; i < (uint)words.Length; i++)
            index.Insert(IndexKey.Create(words[i]), new DocumentLocation(i + 1, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var keys = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Backward, txnId)
                        .Select(e => e.Key.As<string>())
                        .ToList();

        Assert.Equal(new[] { "Mango", "Cherry", "Apple" }, keys);
    }

    // ── Duplicate keys (non-unique index) ───────────────────────────────────

    [Fact]
    public void Range_DuplicateKeys_AllDocumentsReturned()
    {
        var (index, txnId) = CreateIndex();
        // Same key value, different document locations
        for (uint slot = 0; slot < 5; slot++)
            index.Insert(IndexKey.Create("duplicate"), new DocumentLocation(slot + 1, (ushort)slot), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var results = index.Range(IndexKey.Create("duplicate"), IndexKey.Create("duplicate"),
                                  IndexDirection.Forward, txnId).ToList();
        Assert.Equal(5, results.Count);
    }

    [Fact]
    public void Range_DuplicateIntKeys_MixedWithUniques_ReturnsAll()
    {
        var (index, txnId) = CreateIndex();
        // Insert: 1, 2, 2, 2, 3 (out-of-order to stress test)
        var items = new (int key, uint page)[] { (2, 10), (3, 30), (2, 20), (1, 1), (2, 25) };
        foreach (var (key, page) in items)
            index.Insert(IndexKey.Create(key), new DocumentLocation(page, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var all = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txnId)
                       .Select(e => e.Key.As<int>())
                       .ToList();

        // All 5 entries, sorted (with duplicates in order)
        Assert.Equal(5, all.Count);
        Assert.Equal(new[] { 1, 2, 2, 2, 3 }, all);
    }

    // ── Split behaviour with unsorted input ─────────────────────────────────

    [Fact]
    public void Range_ManyEntriesOutOfOrder_ForcingSplits_ReturnsSortedAndComplete()
    {
        var (index, txnId) = CreateIndex();

        // Insert 200 items in random-ish order (interleaved high/low).
        var values = Enumerable.Range(1, 200)
            .OrderBy(x => (x * 37) % 200) // deterministic shuffle
            .ToList();

        foreach (var v in values)
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var keys = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txnId)
                        .Select(e => e.Key.As<int>())
                        .ToList();

        Assert.Equal(200, keys.Count);
        // Verify fully sorted
        for (int i = 0; i < keys.Count - 1; i++)
            Assert.True(keys[i] <= keys[i + 1], $"Out of order at index {i}: {keys[i]} > {keys[i + 1]}");
        // Verify all values present
        Assert.Equal(Enumerable.Range(1, 200), keys);
    }

    [Fact]
    public void TryFind_AfterSplit_CanFindAllInsertedKeys()
    {
        var (index, txnId) = CreateIndex();

        // Values 1-150.
        var values = Enumerable.Range(1, 150)
            .OrderByDescending(x => x) // descending → forces splits with reversed input
            .ToList();

        foreach (var v in values)
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        // Every key must be findable
        foreach (var v in values)
        {
            var found = index.TryFind(IndexKey.Create(v), out var loc, txnId);
            Assert.True(found, $"Key {v} not found");
            Assert.Equal((uint)v, loc.PageId);
        }
    }

    // ── Values > 255 (regression for little-endian bug) ─────────────────────

    [Fact]
    public void Range_IntKeysAbove255_ReturnsSortedOrder()
    {
        // This is the exact regression test for the little-endian bug:
        // previously 256 < 1 because byte[0] of 256 (0x00) < byte[0] of 1 (0x01).
        var (index, txnId) = CreateIndex();
        var values = new[] { 1, 255, 256, 257, 1000, 65535, 65536, 100_000 };
        foreach (var v in values)
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var keys = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txnId)
                        .Select(e => e.Key.As<int>())
                        .ToList();

        Assert.Equal(values.OrderBy(x => x).ToList(), keys);
    }

    [Fact]
    public void Range_LargeIntRange_ForcingSplits_ReturnsSortedAndComplete()
    {
        // Insert 400 items spanning well beyond 255 in shuffled order.
        var (index, txnId) = CreateIndex();
        var values = Enumerable.Range(100, 400) // 100..499
            .OrderBy(x => (x * 37) % 400)
            .ToList();

        foreach (var v in values)
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var keys = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txnId)
                        .Select(e => e.Key.As<int>())
                        .ToList();

        Assert.Equal(400, keys.Count);
        for (int i = 0; i < keys.Count - 1; i++)
            Assert.True(keys[i] < keys[i + 1], $"Out of order at index {i}: {keys[i]} > {keys[i + 1]}");
        Assert.Equal(Enumerable.Range(100, 400).ToList(), keys);
    }

    [Fact]
    public void Range_NegativeInts_ReturnsSortedOrder()
    {
        var (index, txnId) = CreateIndex();
        var values = new[] { -1000, -255, -1, 0, 1, 255, 1000 };
        foreach (var v in values)
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)(v + 1001), 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var keys = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txnId)
                        .Select(e => e.Key.As<int>())
                        .ToList();

        Assert.Equal(values.OrderBy(x => x).ToList(), keys);
    }
}
