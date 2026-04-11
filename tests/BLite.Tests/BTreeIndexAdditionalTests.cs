using BLite.Core.Indexing;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Additional tests for <see cref="BTreeIndex"/> targeting mutation survivors not yet
/// covered by the existing BTree tests: unique-constraint edge cases,
/// LessThanOrEqual, Between exclusive bounds, and onRootChanged callback.
/// </summary>
public class BTreeIndexAdditionalTests : IDisposable
{
    private readonly string _dbPath;
    private readonly StorageEngine _storage;

    public BTreeIndexAdditionalTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_btree_add_{Guid.NewGuid():N}.db");
        _storage = new StorageEngine(_dbPath, PageFileConfig.Default);
    }

    public void Dispose()
    {
        _storage.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    private (BTreeIndex index, ulong txnId) CreateBTreeIndex(Action<uint>? onRootChanged = null)
    {
        var opts = IndexOptions.CreateBTree("field");
        var index = new BTreeIndex(_storage, opts, onRootChanged: onRootChanged);
        var txnId = _storage.BeginTransaction().TransactionId;
        return (index, txnId);
    }

    private (BTreeIndex index, ulong txnId) CreateUniqueIndex()
    {
        var opts = IndexOptions.CreateUnique("field");
        var index = new BTreeIndex(_storage, opts);
        var txnId = _storage.BeginTransaction().TransactionId;
        return (index, txnId);
    }

    // ─── Unique constraint ────────────────────────────────────────────────────

    [Fact]
    public void Unique_SameKeySameLocation_DoesNotThrow()
    {
        var (index, txnId) = CreateUniqueIndex();
        var key = IndexKey.Create(42);
        var loc = new DocumentLocation(1, 0);

        index.Insert(key, loc, txnId);

        // Second insert with identical key + location is a no-op — must not throw
        var ex = Record.Exception(() => index.Insert(key, loc, txnId));
        Assert.Null(ex);

        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();
    }

    [Fact]
    public void Unique_SameKeyDifferentLocation_ThrowsInvalidOperationException()
    {
        var (index, txnId) = CreateUniqueIndex();
        var key = IndexKey.Create(42);

        index.Insert(key, new DocumentLocation(1, 0), txnId);

        Assert.Throws<InvalidOperationException>(() =>
            index.Insert(key, new DocumentLocation(2, 0), txnId));
    }

    [Fact]
    public void Unique_DifferentKeys_BothInsertSuccessfully()
    {
        var (index, txnId) = CreateUniqueIndex();

        index.Insert(IndexKey.Create(10), new DocumentLocation(1, 0), txnId);
        index.Insert(IndexKey.Create(20), new DocumentLocation(2, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        index.TryFind(IndexKey.Create(10), out var loc1, txnId);
        index.TryFind(IndexKey.Create(20), out var loc2, txnId);

        Assert.Equal(new DocumentLocation(1, 0), loc1);
        Assert.Equal(new DocumentLocation(2, 0), loc2);
    }

    // ─── LessThan orEqual ─────────────────────────────────────────────────────

    [Fact]
    public void LessThanOrEqual_IncludesExactThreshold()
    {
        var (index, txnId) = CreateBTreeIndex();
        foreach (var v in new[] { 10, 20, 30, 40, 50 })
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var key = IndexKey.Create(30);
        var result = index.LessThan(key, orEqual: true, txnId).ToList();

        Assert.Equal(3, result.Count);
        Assert.Equal(IndexKey.Create(30), result[0].Key);
        Assert.Equal(IndexKey.Create(20), result[1].Key);
        Assert.Equal(IndexKey.Create(10), result[2].Key);
    }

    [Fact]
    public void LessThanOrEqual_VsStrict_DiffersByOneEntry()
    {
        var (index, txnId) = CreateBTreeIndex();
        foreach (var v in new[] { 10, 20, 30 })
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var key = IndexKey.Create(20);
        var strict = index.LessThan(key, orEqual: false, txnId).ToList();
        var inclusive = index.LessThan(key, orEqual: true, txnId).ToList();

        Assert.Equal(1, strict.Count);   // only 10
        Assert.Equal(2, inclusive.Count); // 20 and 10
    }

    // ─── Between exclusive bounds ─────────────────────────────────────────────

    [Fact]
    public void Between_ExclusiveBothEnds_ExcludesBoundaryEntries()
    {
        var (index, txnId) = CreateBTreeIndex();
        foreach (var v in new[] { 10, 20, 30, 40, 50 })
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var result = index.Between(
            IndexKey.Create(20), IndexKey.Create(40),
            startInclusive: false, endInclusive: false,
            txnId).ToList();

        Assert.Single(result);
        Assert.Equal(IndexKey.Create(30), result[0].Key);
    }

    [Fact]
    public void Between_ExclusiveStart_InclusiveEnd_ExcludesOnlyLower()
    {
        var (index, txnId) = CreateBTreeIndex();
        foreach (var v in new[] { 10, 20, 30, 40, 50 })
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var result = index.Between(
            IndexKey.Create(20), IndexKey.Create(40),
            startInclusive: false, endInclusive: true,
            txnId).ToList();

        Assert.Equal(2, result.Count);
        Assert.Equal(IndexKey.Create(30), result[0].Key);
        Assert.Equal(IndexKey.Create(40), result[1].Key);
    }

    [Fact]
    public void Between_InclusiveStart_ExclusiveEnd_ExcludesOnlyUpper()
    {
        var (index, txnId) = CreateBTreeIndex();
        foreach (var v in new[] { 10, 20, 30, 40, 50 })
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var result = index.Between(
            IndexKey.Create(20), IndexKey.Create(40),
            startInclusive: true, endInclusive: false,
            txnId).ToList();

        Assert.Equal(2, result.Count);
        Assert.Equal(IndexKey.Create(20), result[0].Key);
        Assert.Equal(IndexKey.Create(30), result[1].Key);
    }

    // ─── onRootChanged callback ───────────────────────────────────────────────

    [Fact]
    public void OnRootChanged_Callback_IsInvokedWhenRootSplits()
    {
        uint? capturedNewRoot = null;
        var (index, txnId) = CreateBTreeIndex(onRootChanged: newRoot => capturedNewRoot = newRoot);

        // Insert enough entries to force a root split
        for (int i = 1; i <= BTreeIndex.MaxEntriesPerNode + 1; i++)
            index.Insert(IndexKey.Create(i), new DocumentLocation((uint)i, 0), txnId);

        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        // Callback must have been invoked at least once
        Assert.NotNull(capturedNewRoot);
        // New root must differ from initial allocation (page 0 is invalid for an index)
        Assert.True(capturedNewRoot.Value > 0);
    }

    [Fact]
    public void OnRootChanged_Callback_IsNotInvokedWithoutSplit()
    {
        var callbackInvoked = false;
        var (index, txnId) = CreateBTreeIndex(onRootChanged: _ => callbackInvoked = true);

        // Insert far fewer than MaxEntriesPerNode — no split should occur
        for (int i = 1; i <= 10; i++)
            index.Insert(IndexKey.Create(i), new DocumentLocation((uint)i, 0), txnId);

        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        Assert.False(callbackInvoked);
    }

    // ─── RootPageId property ──────────────────────────────────────────────────

    [Fact]
    public void RootPageId_NonZeroAfterConstruction()
    {
        var (index, txnId) = CreateBTreeIndex();
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        Assert.True(index.RootPageId > 0);
    }

    // ─── GreaterThan strict vs orEqual – complementary coverage ──────────────

    [Fact]
    public void GreaterThan_Strict_ExcludesExactThreshold()
    {
        var (index, txnId) = CreateBTreeIndex();
        foreach (var v in new[] { 10, 20, 30 })
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var result = index.GreaterThan(IndexKey.Create(20), orEqual: false, txnId).ToList();

        Assert.Single(result);
        Assert.Equal(IndexKey.Create(30), result[0].Key);
    }

    [Fact]
    public void GreaterThan_OrEqual_IncludesExactThreshold()
    {
        var (index, txnId) = CreateBTreeIndex();
        foreach (var v in new[] { 10, 20, 30 })
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransactionAsync(txnId).GetAwaiter().GetResult();

        var result = index.GreaterThan(IndexKey.Create(20), orEqual: true, txnId).ToList();

        Assert.Equal(2, result.Count);
        Assert.Equal(IndexKey.Create(20), result[0].Key);
        Assert.Equal(IndexKey.Create(30), result[1].Key);
    }

    [Fact]
    public void Unique_AllKeysFoundAfterSplit()
    {
        var (index, txnId) = CreateUniqueIndex();
        int N = BTreeIndex.MaxEntriesPerNode;

        // Insert N+10 sequential keys to force at least one split
        for (int i = 1; i <= N + 10; i++)
            index.Insert(IndexKey.Create(i), new DocumentLocation((uint)i, 0), txnId);

        // Every key must be found
        for (int i = 1; i <= N + 10; i++)
        {
            bool found = index.TryFind(IndexKey.Create(i), out var loc, txnId);
            Assert.True(found, $"Key {i} not found after split (N={N})");
            Assert.Equal(new DocumentLocation((uint)i, 0), loc);
        }
    }

    [Fact]
    public void Unique_DuplicateAfterSplit_Throws()
    {
        var (index, txnId) = CreateUniqueIndex();
        int N = BTreeIndex.MaxEntriesPerNode;

        // Insert N+10 sequential keys
        for (int i = 1; i <= N + 10; i++)
            index.Insert(IndexKey.Create(i), new DocumentLocation((uint)i, 0), txnId);

        // Re-inserting any key with a different location must throw
        for (int i = 1; i <= N + 10; i++)
        {
            Assert.Throws<InvalidOperationException>(() =>
                index.Insert(IndexKey.Create(i), new DocumentLocation(999, (ushort)i), txnId));
        }
    }

    /// <summary>
    /// Reproduces the collection-level scenario: each insert uses a SEPARATE
    /// auto-commit transaction (begin → insert → commit).  With N=64 this
    /// forces a split at insert #65 and verifies that all keys are still
    /// findable across transaction boundaries.
    /// </summary>
    [Fact]
    public async Task Unique_AllKeysFoundAfterSplit_SeparateTransactions()
    {
        var opts = IndexOptions.CreateUnique("pk");
        var index = new BTreeIndex(_storage, opts);
        int N = BTreeIndex.MaxEntriesPerNode;
        int total = N + 36; // 100 inserts with N=64

        for (int i = 1; i <= total; i++)
        {
            var txn = _storage.BeginTransaction();
            index.Insert(IndexKey.Create(i), new DocumentLocation((uint)i, 0), txn.TransactionId);
            await _storage.CommitTransactionAsync(txn);
        }

        // Every key must be found after all commits
        for (int i = 1; i <= total; i++)
        {
            bool found = index.TryFind(IndexKey.Create(i), out var loc);
            Assert.True(found, $"Key {i} not found after split with separate transactions (N={N})");
            Assert.Equal(new DocumentLocation((uint)i, 0), loc);
        }
    }

    /// <summary>
    /// Same as above but with TWO indexes (primary unique + secondary non-unique)
    /// to simulate what DocumentCollection does with a secondary index on Age.
    /// </summary>
    [Fact]
    public async Task TwoIndexes_AllKeysFoundAfterSplit_SeparateTransactions()
    {
        var primaryOpts = IndexOptions.CreateUnique("pk");
        var primary = new BTreeIndex(_storage, primaryOpts);

        var secondaryOpts = IndexOptions.CreateBTree("age");
        var secondary = new BTreeIndex(_storage, secondaryOpts);

        int N = BTreeIndex.MaxEntriesPerNode;
        int total = N + 36; // 100 inserts

        for (int i = 1; i <= total; i++)
        {
            var txn = _storage.BeginTransaction();
            var loc = new DocumentLocation((uint)i, 0);

            // Primary: unique key = i
            primary.Insert(IndexKey.Create(i), loc, txn.TransactionId);
            // Secondary: non-unique key = (i-1) — simulates Age=i-1
            secondary.Insert(IndexKey.Create(i - 1), loc, txn.TransactionId);

            await _storage.CommitTransactionAsync(txn);
        }

        // Verify all primary keys
        for (int i = 1; i <= total; i++)
        {
            bool found = primary.TryFind(IndexKey.Create(i), out var loc);
            Assert.True(found, $"Primary key {i} not found (N={N})");
            Assert.Equal(new DocumentLocation((uint)i, 0), loc);
        }
    }
}
