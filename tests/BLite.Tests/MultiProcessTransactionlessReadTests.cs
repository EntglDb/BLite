using BLite.Core.Storage;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Two <see cref="StorageEngine"/> or context instances on the same files stand in for two
/// processes. A read made with no transaction (<c>FindAllAsync</c>, <c>CountAsync</c>, an index
/// query with no transaction, <c>ReadPage(pageId, null, ...)</c>) must see what the other process
/// has committed since, like a read inside a transaction does (#241).
/// </summary>
public sealed class MultiProcessTransactionlessReadTests : IDisposable
{
    private const uint P = 5, Q = 6;
    private readonly string _tempDir;
    private readonly string _dbPath;

    public MultiProcessTransactionlessReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"blite_mp_txless_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "shared.db");
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* best-effort */ }
    }

    private static readonly PageFileConfig MultiProcess = PageFileConfig.Default with { AllowMultiProcessAccess = true };

    private static async Task CommitAsync(StorageEngine engine, uint pageId, byte value)
    {
        var data = new byte[engine.PageSize];
        data[0] = value;
        using var tx = engine.BeginTransaction();
        engine.WritePage(pageId, tx.TransactionId, data);
        await tx.CommitAsync();
    }

    private static byte ReadWithoutTransaction(StorageEngine engine, uint pageId)
    {
        var buf = new byte[engine.PageSize];
        engine.ReadPage(pageId, null, buf);
        return buf[0];
    }

    private static byte ReadHeaderWithoutTransaction(StorageEngine engine, uint pageId)
    {
        var buf = new byte[16];
        engine.ReadPageHeader(pageId, null, buf);
        return buf[0];
    }

    private static async Task<byte> ReadAsyncWithoutTransaction(StorageEngine engine, uint pageId)
    {
        var buf = new byte[engine.PageSize];
        await engine.ReadPageAsync(pageId, null, buf);
        return buf[0];
    }

    [Fact]
    public async Task PageCachedFromSharedWal_IsRefreshedWhenTheOtherEngineRewritesIt()
    {
        using var a = new StorageEngine(_dbPath, MultiProcess);
        using var b = new StorageEngine(_dbPath, MultiProcess);

        await CommitAsync(a, P, 0x01);
        Assert.Equal(0x01, ReadWithoutTransaction(b, P)); // first read: replayed from the shared WAL into B's index

        await CommitAsync(a, P, 0x02);
        Assert.Equal(0x02, ReadWithoutTransaction(b, P));
        Assert.Equal(0x02, ReadHeaderWithoutTransaction(b, P));
        Assert.Equal(0x02, await ReadAsyncWithoutTransaction(b, P));
    }

    [Fact]
    public async Task WhileACommitHoldsTheLock_TheNewerImageIsReadThroughTheSharedPageIndex()
    {
        using var a = new StorageEngine(_dbPath, MultiProcess);
        using var b = new StorageEngine(_dbPath, MultiProcess);

        await CommitAsync(b, P, 0x01);
        await CommitAsync(b, Q, 0x01);
        await CommitAsync(a, P, 0x02);

        // A commit of B is in flight: the replay cannot run, so the read decides page by page.
        using (b.HoldCommitLockForTests())
        {
            Assert.Equal(0x02, ReadWithoutTransaction(b, P));
            Assert.Equal(0x02, ReadHeaderWithoutTransaction(b, P));
            Assert.Equal(0x02, await ReadAsyncWithoutTransaction(b, P));
            Assert.Equal(0x01, ReadWithoutTransaction(b, Q)); // untouched by A: B's own image is current
        }
    }

    [Fact]
    public async Task WhileACommitHoldsTheLock_APageCheckpointedOutOfTheSharedIndex_IsReadFromThePageFile()
    {
        using var a = new StorageEngine(_dbPath, MultiProcess);
        using var b = new StorageEngine(_dbPath, MultiProcess);
        using var c = new StorageEngine(_dbPath, MultiProcess); // opened now: opening on a non-empty WAL would truncate it

        await CommitAsync(a, P, 0x01);
        Assert.Equal(0x01, ReadWithoutTransaction(b, P));
        await CommitAsync(a, P, 0x02);

        // A reader slot of a third process pins the WAL end here, so A's checkpoint below writes P
        // to the page file but keeps Q, committed after this point, in the WAL: a partial checkpoint
        // that drops P from the shared page index without truncating the WAL. (B itself must not
        // begin a transaction here: that would replay P into its index.)
        using var pin = c.BeginTransaction();
        await CommitAsync(a, Q, 0x01);
        await a.CheckpointAsync();

        using (b.HoldCommitLockForTests())
        {
            Assert.Equal(0x02, ReadWithoutTransaction(b, P));
            Assert.Equal(0x01, ReadWithoutTransaction(b, Q));
        }
    }

    [Fact]
    public async Task PageCommittedByThisEngine_IsRefreshedWhenTheOtherEngineRewritesIt()
    {
        using var a = new StorageEngine(_dbPath, MultiProcess);
        using var b = new StorageEngine(_dbPath, MultiProcess);

        await CommitAsync(b, P, 0x01);
        await CommitAsync(a, P, 0x02);

        Assert.Equal(0x02, ReadWithoutTransaction(b, P));
        Assert.Equal(0x02, ReadHeaderWithoutTransaction(b, P));
        Assert.Equal(0x02, await ReadAsyncWithoutTransaction(b, P));
    }

    [Fact]
    public async Task PageRewrittenAfterTheOtherEngineCheckpointed_IsRefreshed()
    {
        using var a = new StorageEngine(_dbPath, MultiProcess);
        using var b = new StorageEngine(_dbPath, MultiProcess);

        await CommitAsync(a, P, 0x01);
        Assert.Equal(0x01, ReadWithoutTransaction(b, P));

        // A rewrites the page and checkpoints it, which here also truncates the WAL: B's cached
        // image belongs to the old WAL generation and the page file holds the new one.
        await CommitAsync(a, P, 0x02);
        await a.CheckpointAsync();

        Assert.Equal(0x02, ReadWithoutTransaction(b, P));
    }

    [Fact]
    public async Task CountFindAllAndIndexQuery_WithoutTransaction_SeeTheOtherContextsInserts()
    {
        using var a = new LcBaseContext(_dbPath, MultiProcess);
        using var b = new LcBaseContext(_dbPath, MultiProcess);

        for (int i = 0; i < 20; i++)
            await a.Docs.InsertAsync(new LifecycleDoc { Name = $"a{i}", Email = $"a{i}@x", Age = i % 10, Embedding = [i, 1, 2] });
        await b.Docs.InsertAsync(new LifecycleDoc { Name = "b0", Email = "b0@x", Age = 1, Embedding = [1, 1, 2] });

        // B has read every page once: data pages and index pages are now in its WAL index.
        Assert.Equal(21, await b.Docs.CountAsync());
        Assert.Single(await b.Docs.QueryIndexAsync("idx_email", "a7@x", "a7@x").ToListAsync());

        for (int i = 20; i < 25; i++)
            await a.Docs.InsertAsync(new LifecycleDoc { Name = $"a{i}", Email = $"a{i}@x", Age = i % 10, Embedding = [i, 1, 2] });

        Assert.Equal(26, await b.Docs.CountAsync());
        Assert.Equal(26, (await b.Docs.FindAllAsync().ToListAsync()).Count);
        Assert.Equal("a23", Assert.Single(await b.Docs.QueryIndexAsync("idx_email", "a23@x", "a23@x").ToListAsync()).Name);
    }
}
