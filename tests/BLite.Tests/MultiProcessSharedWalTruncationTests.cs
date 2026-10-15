using BLite.Core.Encryption;
using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace BLite.Tests;

/// <summary>
/// Two <see cref="StorageEngine"/> instances on the same files stand in for two processes
/// sharing one WAL (see <see cref="MultiProcessWalSharedMemoryTests"/>).
/// <list type="bullet">
///   <item>An encrypted WAL has one header, and so one key, per WAL generation: a process
///         never rewrites another's header, and it loads the new one after a truncation (#203).</item>
///   <item>Recovery on open truncates the shared WAL under the SHM writer lock and publishes it
///         like a checkpoint: the end offset is reset and the generation moves on (#200), which
///         the catch-up relies on to reject a WAL read while it was truncated (#199).</item>
/// </list>
/// </summary>
public sealed class MultiProcessSharedWalTruncationTests : IDisposable
{
    private const uint P = 5, Q = 6, R = 7;
    private readonly string _tempDir;
    private readonly string _dbPath;

    public MultiProcessSharedWalTruncationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"blite_mpwal_trunc_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "shared.db");
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* best-effort */ }
    }

    private string WalPath => Path.ChangeExtension(_dbPath, ".wal");

    private static PageFileConfig Config(bool encrypted) => PageFileConfig.Default with
    {
        AllowMultiProcessAccess = true,
        CryptoProvider = encrypted ? new AesGcmCryptoProvider(new CryptoOptions("mp_wal_trunc_passphrase", iterations: 1)) : null,
    };

    private static async Task CommitAsync(StorageEngine engine, uint pageId, byte value)
    {
        var data = new byte[engine.PageSize];
        data[0] = value;
        using var tx = engine.BeginTransaction();
        engine.WritePage(pageId, tx.TransactionId, data);
        await tx.CommitAsync();
    }

    private static byte Read(StorageEngine engine, uint pageId)
    {
        using var tx = engine.BeginTransaction();
        var buf = new byte[engine.PageSize];
        engine.ReadPage(pageId, tx.TransactionId, buf);
        return buf[0];
    }

    private long WalLength() => new FileInfo(WalPath).Length;

    /// <summary>Pages p, q and r hold 0x01 on disk and the WAL is empty.</summary>
    private async Task InitialiseAsync(bool encrypted)
    {
        using var setup = new StorageEngine(_dbPath, Config(encrypted));
        foreach (var page in new[] { P, Q, R })
            await CommitAsync(setup, page, 0x01);
        await setup.CheckpointAsync();
        Assert.Equal(0, WalLength());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task BothOpenWithEmptyWal_EachSeesTheOthersCommits_AndNoneIsLost(bool encrypted)
    {
        await InitialiseAsync(encrypted);

        using (var a = new StorageEngine(_dbPath, Config(encrypted)))
        using (var b = new StorageEngine(_dbPath, Config(encrypted)))
        {
            await CommitAsync(a, P, 0xAA);
            var afterA = WalLength();
            await CommitAsync(b, Q, 0xBB);
            Assert.True(WalLength() > afterA, "B's commit must be appended after A's, not over it.");

            Assert.Equal(0xBB, Read(a, Q));
            Assert.Equal(0xAA, Read(b, P));
        }

        using var reopened = new StorageEngine(_dbPath, Config(encrypted));
        Assert.Equal(0xAA, Read(reopened, P));
        Assert.Equal(0xBB, Read(reopened, Q));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AfterAnotherProcessTruncatesTheWal_BothUseTheNewWal(bool encrypted)
    {
        await InitialiseAsync(encrypted);

        using (var a = new StorageEngine(_dbPath, Config(encrypted)))
        using (var b = new StorageEngine(_dbPath, Config(encrypted)))
        {
            await CommitAsync(a, P, 0xAA);
            Assert.Equal(0xAA, Read(b, P));

            // A checkpoints and truncates the WAL; B still holds what it replayed from it.
            await a.CheckpointAsync();
            Assert.Equal(0, WalLength());

            await CommitAsync(b, Q, 0xBB);
            await CommitAsync(a, R, 0xCC);

            Assert.Equal(0xCC, Read(b, R));
            Assert.Equal(0xBB, Read(a, Q));
            Assert.Equal(0xAA, Read(b, P));
        }

        using var reopened = new StorageEngine(_dbPath, Config(encrypted));
        Assert.Equal(0xAA, Read(reopened, P));
        Assert.Equal(0xBB, Read(reopened, Q));
        Assert.Equal(0xCC, Read(reopened, R));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RecoveryOnOpen_PublishesTheTruncation_AndLaterCommitsAreVisible(bool encrypted)
    {
        await InitialiseAsync(encrypted);

        using var observer = WalSharedMemory.Open(WalPath + "-shm", PageFileConfig.Default.PageSize);

        using (var a = new StorageEngine(_dbPath, Config(encrypted)))
        {
            await CommitAsync(a, P, 0xAA);
            Assert.True(WalLength() > 0);
            Assert.True(observer.ReadWalEndOffset() > 0);
            long generation = observer.ReadWalGeneration();

            // B opens while the WAL holds A's commit: recovery writes it to the page file and
            // truncates the WAL. The others must learn about it from the SHM (#200).
            using (var b = new StorageEngine(_dbPath, Config(encrypted)))
            {
                Assert.Equal(0, WalLength());
                Assert.Equal(0, observer.ReadWalEndOffset());
                Assert.True(observer.ReadWalGeneration() > generation, "Recovery's truncation must start a new WAL generation.");

                // A's next commit lands at the start of the new WAL; B must see it, although
                // its offset is below the end offset the SHM held before the truncation.
                await CommitAsync(a, Q, 0xBB);
                Assert.Equal(0xBB, Read(b, Q));
                Assert.Equal(0xAA, Read(b, P));

                await CommitAsync(b, R, 0xCC);
                Assert.Equal(0xCC, Read(a, R));
                Assert.Equal(0xAA, Read(a, P));
            }
        }

        using var reopened = new StorageEngine(_dbPath, Config(encrypted));
        Assert.Equal(0xAA, Read(reopened, P));
        Assert.Equal(0xBB, Read(reopened, Q));
        Assert.Equal(0xCC, Read(reopened, R));
    }

    [Fact]
    public async Task EncryptedWal_RepeatedTruncationsByEitherProcess_LoseNoCommit()
    {
        await InitialiseAsync(encrypted: true);

        var expected = new Dictionary<uint, byte>();
        using (var a = new StorageEngine(_dbPath, Config(encrypted: true)))
        using (var b = new StorageEngine(_dbPath, Config(encrypted: true)))
        {
            for (int round = 0; round < 6; round++)
            {
                var (writer, other) = round % 2 == 0 ? (a, b) : (b, a);
                uint page = (uint)(10 + round);
                byte value = (byte)(0x40 + round);
                await CommitAsync(writer, page, value);
                await CommitAsync(other, page + 100, value);
                expected[page] = value;
                expected[page + 100] = value;

                Assert.Equal(value, Read(other, page));
                Assert.Equal(value, Read(writer, page + 100));

                if (round % 3 == 2) await other.CheckpointAsync();
                else await writer.CheckpointAsync();
            }
        }

        using var reopened = new StorageEngine(_dbPath, Config(encrypted: true));
        foreach (var (page, value) in expected)
            Assert.Equal(value, Read(reopened, page));
    }
}
