using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace BLite.Tests;

/// <summary>
/// Two-phase commit in single-process mode: the records <see cref="StorageEngine.PrepareTransactionAsync(ulong)"/>
/// writes are appended under the commit lock and stay in the WAL until the transaction commits or
/// rolls back. <see cref="StorageEngine.MarkTransactionCommittedAsync"/> appends only the commit
/// record, so a truncation in between left a commit that recovery could not apply (#188).
/// Disposing the engine does not checkpoint, so reopening it replays the WAL as after a crash.
/// </summary>
public class PreparedTransactionWalTests : IDisposable
{
    private readonly string _dir;
    private readonly string _dbPath;

    public PreparedTransactionWalTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"blite_prepared_wal_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
        _dbPath = Path.Combine(_dir, "prepared.db");
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private long WalLength => new FileInfo(Path.ChangeExtension(_dbPath, ".wal")).Length;

    [Fact]
    public async Task PreparedRecords_SurviveACheckpoint_SoTheMarkedCommitIsRecovered()
    {
        uint other, prepared;
        using (var engine = new StorageEngine(_dbPath, PageFileConfig.Default))
        {
            other = engine.AllocatePage();
            prepared = engine.AllocatePage();
            await WriteAndCommitAsync(engine, other, 0x11); // gives the checkpoint a page to write

            using (var tx = engine.BeginTransaction())
            {
                engine.WritePage(prepared, tx.TransactionId, Filled(engine.PageSize, 0x22));
                Assert.True(await engine.PrepareTransactionAsync(tx.TransactionId));

                await engine.CheckpointAsync();
                Assert.NotEqual(0, WalLength); // the prepared records are still there

                await engine.MarkTransactionCommittedAsync(tx.TransactionId);
            }
            AssertPage(engine, prepared, 0x22);
        }

        using var reopened = new StorageEngine(_dbPath, PageFileConfig.Default);
        AssertPage(reopened, other, 0x11);
        AssertPage(reopened, prepared, 0x22);
    }

    [Fact]
    public async Task PreparedTransaction_HoldsBackTruncation_UntilItCommits()
    {
        uint other, prepared;
        using (var engine = new StorageEngine(_dbPath, PageFileConfig.Default))
        {
            other = engine.AllocatePage();
            prepared = engine.AllocatePage();
            await WriteAndCommitAsync(engine, other, 0x11);

            using (var tx = engine.BeginTransaction())
            {
                engine.WritePage(prepared, tx.TransactionId, Filled(engine.PageSize, 0x22));
                Assert.True(await tx.PrepareAsync());

                await engine.CheckpointAsync();
                Assert.NotEqual(0, WalLength);

                await tx.CommitAsync();
            }

            // Committed: the next checkpoint writes its pages and truncates the WAL again.
            await engine.CheckpointAsync();
            Assert.Equal(0, WalLength);
        }

        using var reopened = new StorageEngine(_dbPath, PageFileConfig.Default);
        AssertPage(reopened, other, 0x11);
        AssertPage(reopened, prepared, 0x22);
    }

    [Fact]
    public async Task PreparedTransaction_HoldsBackTruncation_UntilItRollsBack()
    {
        uint other, prepared;
        using (var engine = new StorageEngine(_dbPath, PageFileConfig.Default))
        {
            other = engine.AllocatePage();
            prepared = engine.AllocatePage();
            await WriteAndCommitAsync(engine, other, 0x11);

            using (var tx = engine.BeginTransaction())
            {
                engine.WritePage(prepared, tx.TransactionId, Filled(engine.PageSize, 0x22));
                Assert.True(await tx.PrepareAsync());

                await engine.CheckpointAsync();
                Assert.NotEqual(0, WalLength);

                await tx.RollbackAsync();
            }

            // Rolled back: it no longer holds back the truncation, and recovery never applies it.
            await WriteAndCommitAsync(engine, other, 0x33);
            await engine.CheckpointAsync();
            Assert.Equal(0, WalLength);
        }

        using var reopened = new StorageEngine(_dbPath, PageFileConfig.Default);
        AssertPage(reopened, other, 0x33);
        AssertNotPage(reopened, prepared, 0x22);
    }

    [Fact]
    public async Task Prepare_WaitsForTheCommitLock_AndWritesNothingWithoutIt()
    {
        // A prepare appends under the commit lock, as a commit batch or a truncation does, so its
        // records never interleave with theirs. Unable to take the lock, it fails without writing.
        uint prepared;
        using (var engine = new StorageEngine(_dbPath, PageFileConfig.Default))
        {
            prepared = engine.AllocatePage();
            using (var tx = engine.BeginTransaction())
            {
                engine.WritePage(prepared, tx.TransactionId, Filled(engine.PageSize, 0x22));

                long walBefore = WalLength;
                using (engine.HoldCommitLockForTests())
                {
                    Assert.False(await engine.PrepareTransactionAsync(tx.TransactionId));
                    Assert.False(await engine.PrepareTransactionAsync(tx.TransactionId, CancellationToken.None));
                    Assert.Equal(walBefore, WalLength);
                }

                Assert.True(await engine.PrepareTransactionAsync(tx.TransactionId));
                Assert.NotEqual(walBefore, WalLength);
                await engine.MarkTransactionCommittedAsync(tx.TransactionId);
            }
        }

        using var reopened = new StorageEngine(_dbPath, PageFileConfig.Default);
        AssertPage(reopened, prepared, 0x22);
    }

    [Fact]
    public async Task Recovery_KeepsThePreparedRecords()
    {
        uint other, prepared;
        using (var engine = new StorageEngine(_dbPath, PageFileConfig.Default))
        {
            other = engine.AllocatePage();
            prepared = engine.AllocatePage();
            await WriteAndCommitAsync(engine, other, 0x11);

            using (var tx = engine.BeginTransaction())
            {
                engine.WritePage(prepared, tx.TransactionId, Filled(engine.PageSize, 0x22));
                Assert.True(await engine.PrepareTransactionAsync(tx.TransactionId));

                await engine.RecoverAsync();
                Assert.NotEqual(0, WalLength);

                await engine.MarkTransactionCommittedAsync(tx.TransactionId);
            }
        }

        using var reopened = new StorageEngine(_dbPath, PageFileConfig.Default);
        AssertPage(reopened, other, 0x11);
        AssertPage(reopened, prepared, 0x22);
    }

    private static void AssertPage(StorageEngine storage, uint page, byte fill)
    {
        var read = new byte[storage.PageSize];
        storage.ReadPage(page, null, read);
        Assert.Equal(Filled(storage.PageSize, fill), read);
    }

    private static void AssertNotPage(StorageEngine storage, uint page, byte fill)
    {
        var read = new byte[storage.PageSize];
        storage.ReadPage(page, null, read);
        Assert.NotEqual(Filled(storage.PageSize, fill), read);
    }

    private static async Task WriteAndCommitAsync(StorageEngine storage, uint page, byte fill)
    {
        using var txn = storage.BeginTransaction();
        storage.WritePage(page, txn.TransactionId, Filled(storage.PageSize, fill));
        await txn.CommitAsync();
    }

    private static byte[] Filled(int size, byte value)
    {
        var buffer = new byte[size];
        buffer.AsSpan(32).Fill(value); // keep a zeroed page header
        return buffer;
    }
}
