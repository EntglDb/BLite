using BLite.Bson;
using BLite.Core.Indexing;
using BLite.Core.Storage;
using Xunit;

namespace BLite.Tests;

/// <summary>
/// Unit tests for BTreeIndex async API (Phase 2):
///   - StorageEngine.ReadPageAsync (low-level I/O layer)
///   - BTreeIndex.TryFindAsync
///   - BTreeIndex.RangeAsync (forward + backward)
///   - Query primitive async: Equal, GreaterThan, LessThan, Between, StartsWith, In, Like
///   - CancellationToken propagation
/// </summary>
public class BTreeIndexAsyncTests : IDisposable
{
    private readonly string _dbPath;
    private readonly StorageEngine _storage;
    private readonly BTreeIndex _index;

    // Keys seeded: 10, 20, 30, 40, 50
    private static readonly int[] Seeds = [10, 20, 30, 40, 50];

    public BTreeIndexAsyncTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_btree_async_{Guid.NewGuid()}.db");
        _storage = new StorageEngine(_dbPath, PageFileConfig.Default);

        var options = IndexOptions.CreateBTree("_id");
        _index = new BTreeIndex(_storage, options);

        // Seed via sync path (writes committed to WAL index)
        var txnId = _storage.BeginTransaction().TransactionId;
        foreach (var key in Seeds)
            _index.Insert(IndexKey.Create(key), new DocumentLocation((uint)key, 0), txnId);
        _storage.CommitTransaction(txnId);
    }

    public void Dispose()
    {
        _storage.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // StorageEngine.ReadPageAsync — low-level smoke test
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task StorageEngine_ReadPageAsync_ReturnsConsistentDataWithSyncRead()
    {
        var syncBuf = new byte[_storage.PageSize];
        var asyncBuf = new byte[_storage.PageSize];

        // Read Page 1 (collection metadata) both ways
        _storage.ReadPage(1, null, syncBuf);
        await _storage.ReadPageAsync(1, null, asyncBuf.AsMemory());

        Assert.Equal(syncBuf, asyncBuf);
    }

    [Fact]
    public async Task StorageEngine_ReadPageAsync_RespectsTransactionIsolation()
    {
        // Write a page inside an uncommitted transaction
        var txn = _storage.BeginTransaction();
        var pageId = _storage.AllocatePage();
        var written = new byte[_storage.PageSize];
        written[0] = 0xAB;
        _storage.WritePage(pageId, txn.TransactionId, written);

        // Async read WITHIN same transaction should see uncommitted data
        var asyncBuf = new byte[_storage.PageSize];
        await _storage.ReadPageAsync(pageId, txn.TransactionId, asyncBuf.AsMemory());

        Assert.Equal(0xAB, asyncBuf[0]);

        // Async read WITHOUT transaction should NOT see uncommitted data
        var outsideBuf = new byte[_storage.PageSize];
        await _storage.ReadPageAsync(pageId, null, outsideBuf.AsMemory());

        Assert.NotEqual(0xAB, outsideBuf[0]);

        _storage.RollbackTransaction(txn.TransactionId);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // TryFindAsync
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TryFindAsync_ExistingKey_ReturnsFoundWithCorrectLocation()
    {
        var (found, location) = await _index.TryFindAsync(IndexKey.Create(30));

        Assert.True(found);
        Assert.Equal(30u, location.PageId);
    }

    [Fact]
    public async Task TryFindAsync_MissingKey_ReturnsNotFound()
    {
        var (found, _) = await _index.TryFindAsync(IndexKey.Create(99));

        Assert.False(found);
    }

    [Theory]
    [InlineData(10)]
    [InlineData(30)]
    [InlineData(50)]
    public async Task TryFindAsync_AllSeededKeys_AreFound(int key)
    {
        var (found, location) = await _index.TryFindAsync(IndexKey.Create(key));

        Assert.True(found, $"Key {key} should be found");
        Assert.Equal((uint)key, location.PageId);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // RangeAsync — forward
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task RangeAsync_Forward_FullRange_ReturnsAllEntries()
    {
        var entries = await _index
            .RangeAsync(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward)
            .ToListAsync();

        Assert.Equal(Seeds.Length, entries.Count);
        var keys = entries.Select(e => e.Key.As<int>()).ToList();
        Assert.Equal(Seeds, keys);
    }

    [Fact]
    public async Task RangeAsync_Forward_SubRange_ReturnsCorrectSlice()
    {
        var entries = await _index
            .RangeAsync(IndexKey.Create(20), IndexKey.Create(40), IndexDirection.Forward)
            .ToListAsync();

        Assert.Equal(3, entries.Count);
        Assert.Equal([20, 30, 40], entries.Select(e => e.Key.As<int>()).ToArray());
    }

    [Fact]
    public async Task RangeAsync_Forward_EmptyRange_ReturnsEmpty()
    {
        var entries = await _index
            .RangeAsync(IndexKey.Create(60), IndexKey.Create(99), IndexDirection.Forward)
            .ToListAsync();

        Assert.Empty(entries);
    }

    [Fact]
    public async Task RangeAsync_Forward_SingleKey_ReturnsSingleEntry()
    {
        var entries = await _index
            .RangeAsync(IndexKey.Create(30), IndexKey.Create(30), IndexDirection.Forward)
            .ToListAsync();

        Assert.Single(entries);
        Assert.Equal(30, entries[0].Key.As<int>());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // RangeAsync — backward
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task RangeAsync_Backward_SubRange_ReturnsReverseOrder()
    {
        var entries = await _index
            .RangeAsync(IndexKey.Create(20), IndexKey.Create(40), IndexDirection.Backward)
            .ToListAsync();

        Assert.Equal(3, entries.Count);
        Assert.Equal([40, 30, 20], entries.Select(e => e.Key.As<int>()).ToArray());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EqualAsync
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task EqualAsync_ExistingKey_ReturnsSingleEntry()
    {
        var entries = await _index.EqualAsync(IndexKey.Create(20), 0).ToListAsync();

        Assert.Single(entries);
        Assert.Equal(20, entries[0].Key.As<int>());
    }

    [Fact]
    public async Task EqualAsync_MissingKey_ReturnsEmpty()
    {
        var entries = await _index.EqualAsync(IndexKey.Create(99), 0).ToListAsync();

        Assert.Empty(entries);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // GreaterThanAsync
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GreaterThanAsync_Strict_ExcludesThreshold()
    {
        var entries = await _index.GreaterThanAsync(IndexKey.Create(30), orEqual: false, 0).ToListAsync();

        Assert.Equal([40, 50], entries.Select(e => e.Key.As<int>()).ToArray());
    }

    [Fact]
    public async Task GreaterThanAsync_OrEqual_IncludesThreshold()
    {
        var entries = await _index.GreaterThanAsync(IndexKey.Create(30), orEqual: true, 0).ToListAsync();

        Assert.Equal([30, 40, 50], entries.Select(e => e.Key.As<int>()).ToArray());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // LessThanAsync
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task LessThanAsync_Strict_ExcludesThreshold()
    {
        var entries = await _index.LessThanAsync(IndexKey.Create(30), orEqual: false, 0).ToListAsync();

        // descending
        Assert.Equal([20, 10], entries.Select(e => e.Key.As<int>()).ToArray());
    }

    [Fact]
    public async Task LessThanAsync_OrEqual_IncludesThreshold()
    {
        var entries = await _index.LessThanAsync(IndexKey.Create(30), orEqual: true, 0).ToListAsync();

        Assert.Equal([30, 20, 10], entries.Select(e => e.Key.As<int>()).ToArray());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // BetweenAsync
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task BetweenAsync_BothInclusive_IncludesBounds()
    {
        var entries = await _index
            .BetweenAsync(IndexKey.Create(20), IndexKey.Create(40), startInclusive: true, endInclusive: true, 0)
            .ToListAsync();

        Assert.Equal([20, 30, 40], entries.Select(e => e.Key.As<int>()).ToArray());
    }

    [Fact]
    public async Task BetweenAsync_BothExclusive_ExcludesBounds()
    {
        var entries = await _index
            .BetweenAsync(IndexKey.Create(20), IndexKey.Create(40), startInclusive: false, endInclusive: false, 0)
            .ToListAsync();

        Assert.Equal([30], entries.Select(e => e.Key.As<int>()).ToArray());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // StartsWithAsync — uses a separate string-key index
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task StartsWithAsync_FiltersStringKeysCorrectly()
    {
        // Build a separate index with string keys
        var strIndex = new BTreeIndex(_storage, IndexOptions.CreateBTree("name"));
        var txnId = _storage.BeginTransaction().TransactionId;
        strIndex.Insert(IndexKey.Create("alice"),   new DocumentLocation(10, 0), txnId);
        strIndex.Insert(IndexKey.Create("alfred"),  new DocumentLocation(11, 0), txnId);
        strIndex.Insert(IndexKey.Create("bob"),     new DocumentLocation(12, 0), txnId);
        strIndex.Insert(IndexKey.Create("charlie"), new DocumentLocation(13, 0), txnId);
        _storage.CommitTransaction(txnId);

        var entries = await strIndex.StartsWithAsync("al", 0).ToListAsync();

        Assert.Equal(2, entries.Count);
        Assert.All(entries, e => Assert.StartsWith("al", e.Key.As<string>(), StringComparison.Ordinal));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // InAsync
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task InAsync_SubsetOfKeys_ReturnsOnlyMatchingEntries()
    {
        var keys = new[] { IndexKey.Create(10), IndexKey.Create(30), IndexKey.Create(50) };

        var entries = await _index.InAsync(keys, 0).ToListAsync();

        Assert.Equal(3, entries.Count);
        Assert.Equal([10, 30, 50], entries.Select(e => e.Key.As<int>()).ToArray());
    }

    [Fact]
    public async Task InAsync_WithMissingKeys_SkipsMissing()
    {
        var keys = new[] { IndexKey.Create(10), IndexKey.Create(99), IndexKey.Create(50) };

        var entries = await _index.InAsync(keys, 0).ToListAsync();

        Assert.Equal(2, entries.Count);
        Assert.Equal([10, 50], entries.Select(e => e.Key.As<int>()).ToArray());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CancellationToken
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TryFindAsync_CancelledToken_ThrowsOperationCanceledException()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await _index.TryFindAsync(IndexKey.Create(30), ct: cts.Token);
        });
    }

    [Fact]
    public async Task RangeAsync_CancelledToken_ThrowsOperationCanceledException()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in _index.RangeAsync(
                IndexKey.MinKey, IndexKey.MaxKey, ct: cts.Token))
            {
                // Should not reach here
            }
        });
    }
}

/// <summary>
/// Async enumerable helpers for tests.
/// </summary>
file static class AsyncEnumerableExtensions
{
    public static async Task<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> source, CancellationToken ct = default)
    {
        var list = new List<T>();
        await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
            list.Add(item);
        return list;
    }
}
