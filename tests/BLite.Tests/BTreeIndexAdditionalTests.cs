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

        _storage.CommitTransaction(txnId);
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
        _storage.CommitTransaction(txnId);

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
        _storage.CommitTransaction(txnId);

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
        _storage.CommitTransaction(txnId);

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
        _storage.CommitTransaction(txnId);

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
        _storage.CommitTransaction(txnId);

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
        _storage.CommitTransaction(txnId);

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

        // Insert enough entries to force a root split (MaxEntriesPerNode = 100)
        for (int i = 1; i <= 101; i++)
            index.Insert(IndexKey.Create(i), new DocumentLocation((uint)i, 0), txnId);

        _storage.CommitTransaction(txnId);

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

        _storage.CommitTransaction(txnId);

        Assert.False(callbackInvoked);
    }

    // ─── RootPageId property ──────────────────────────────────────────────────

    [Fact]
    public void RootPageId_NonZeroAfterConstruction()
    {
        var (index, txnId) = CreateBTreeIndex();
        _storage.CommitTransaction(txnId);

        Assert.True(index.RootPageId > 0);
    }

    // ─── GreaterThan strict vs orEqual – complementary coverage ──────────────

    [Fact]
    public void GreaterThan_Strict_ExcludesExactThreshold()
    {
        var (index, txnId) = CreateBTreeIndex();
        foreach (var v in new[] { 10, 20, 30 })
            index.Insert(IndexKey.Create(v), new DocumentLocation((uint)v, 0), txnId);
        _storage.CommitTransaction(txnId);

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
        _storage.CommitTransaction(txnId);

        var result = index.GreaterThan(IndexKey.Create(20), orEqual: true, txnId).ToList();

        Assert.Equal(2, result.Count);
        Assert.Equal(IndexKey.Create(20), result[0].Key);
        Assert.Equal(IndexKey.Create(30), result[1].Key);
    }
}
