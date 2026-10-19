using System.Diagnostics;
using BLite.Core;
using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace BLite.Tests;

/// <summary>
/// Multi-process mode: once another process has checkpointed and truncated the shared WAL, the
/// pages this engine replayed from it are older than the page file and must never be read or
/// checkpointed again (#197). Two engines on the same files stand in for two processes.
/// </summary>
public class MultiProcessWalTruncationTests : IDisposable
{
    private readonly string _dir;
    private readonly string _dbPath;
    private static readonly PageFileConfig Config = PageFileConfig.Default with { AllowMultiProcessAccess = true };

    public MultiProcessWalTruncationTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"blite_mp_truncation_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
        _dbPath = Path.Combine(_dir, "shared.db");
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private long WalLength => new FileInfo(Path.ChangeExtension(_dbPath, ".wal")).Length;

    private string ShmPath => Path.ChangeExtension(_dbPath, ".wal") + "-shm";

    [Fact]
    public async Task ReplayedPage_AfterAnotherProcessTruncated_IsNotWrittenBackOnCheckpoint()
    {
        // The sequence from #197.
        using var a = new StorageEngine(_dbPath, Config);
        using var b = new StorageEngine(_dbPath, Config);
        var page = a.AllocatePage();

        await WriteAndCommitAsync(a, page, 0x11);
        using (b.BeginTransaction())
            AssertPage(b, page, 0x11); // B replays 0x11

        await WriteAndCommitAsync(a, page, 0x22);
        await a.CheckpointAsync();
        Assert.Equal(0, WalLength);    // A truncated the WAL

        using (b.BeginTransaction())
            AssertPage(b, page, 0x22);
        await b.CheckpointAsync();

        AssertPage(b, page, 0x22);
        AssertFreshEngineReads(page, 0x22);
    }

    [Fact]
    public async Task ReplayedPage_AfterAnotherProcessTruncated_IsNotWrittenBack_EvenWithoutANewTransaction()
    {
        using var a = new StorageEngine(_dbPath, Config);
        using var b = new StorageEngine(_dbPath, Config);
        var page = a.AllocatePage();

        await WriteAndCommitAsync(a, page, 0x11);
        using (b.BeginTransaction())
            AssertPage(b, page, 0x11);

        await WriteAndCommitAsync(a, page, 0x22);
        await a.CheckpointAsync();
        Assert.Equal(0, WalLength);

        // B checkpoints straight away, without a transaction that would catch up first.
        await b.CheckpointAsync();

        AssertFreshEngineReads(page, 0x22);
        AssertPage(b, page, 0x22);
    }

    [Fact]
    public async Task WalRegrownPastTheReplayPosition_IsReplayedFromItsStart()
    {
        using var a = new StorageEngine(_dbPath, Config);
        using var b = new StorageEngine(_dbPath, Config);
        var first = a.AllocatePage();
        var second = a.AllocatePage();
        var third = a.AllocatePage();

        await WriteAndCommitAsync(a, first, 0x11);
        using (b.BeginTransaction())
            AssertPage(b, first, 0x11); // B's replay position: the end of one transaction
        long bPosition = WalLength;

        await a.CheckpointAsync();
        Assert.Equal(0, WalLength);

        // A appends more than B had replayed, so the new WAL's end passes B's old position.
        await WriteAndCommitAsync(a, second, 0x22);
        await WriteAndCommitAsync(a, third, 0x33);
        Assert.True(WalLength > bPosition);

        // B catches up without reading the pages (a read would fetch them from the WAL on its
        // own). Replaying from B's old position would skip the new WAL's first record; B would
        // then believe it holds everything, checkpoint, and truncate that record away.
        using (b.BeginTransaction()) { }
        await b.CheckpointAsync();
        AssertFreshEngineReads(first, 0x11);
        AssertFreshEngineReads(second, 0x22);
        AssertFreshEngineReads(third, 0x33);
    }

    [Fact]
    public async Task OwnCommit_AfterAnotherProcessTruncated_IsKept()
    {
        using var a = new StorageEngine(_dbPath, Config);
        using var b = new StorageEngine(_dbPath, Config);
        var shared = a.AllocatePage();
        var own = a.AllocatePage();

        await WriteAndCommitAsync(a, shared, 0x11);
        using (b.BeginTransaction())
            AssertPage(b, shared, 0x11);

        await WriteAndCommitAsync(a, shared, 0x22);
        await a.CheckpointAsync();
        Assert.Equal(0, WalLength);

        // B commits without beginning in between; its commit lands in the new WAL.
        using (var tx = b.BeginTransaction())
        {
            b.WritePage(own, tx.TransactionId, Filled(b.PageSize, 0xBB));
            await tx.CommitAsync();
        }
        AssertPage(b, own, 0xBB);
        AssertPage(b, shared, 0x22);

        await b.CheckpointAsync();
        AssertFreshEngineReads(shared, 0x22);
        AssertFreshEngineReads(own, 0xBB);
    }

    [Fact]
    public async Task TruncationByABackup_IsSeenByTheOtherProcess()
    {
        using var a = new StorageEngine(_dbPath, Config);
        using var b = new StorageEngine(_dbPath, Config);
        var page = a.AllocatePage();

        await WriteAndCommitAsync(a, page, 0x11);
        using (b.BeginTransaction())
            AssertPage(b, page, 0x11);
        await WriteAndCommitAsync(a, page, 0x22);

        // A backup checkpoints and truncates the WAL, like a checkpoint does.
        await a.BackupAsync(Path.Combine(_dir, "backup.db"));
        Assert.Equal(0, WalLength);

        await b.CheckpointAsync();
        AssertFreshEngineReads(page, 0x22);
        AssertPage(b, page, 0x22);
    }

    [Fact]
    public async Task RollbackAfterAnotherProcessTruncated_LeavesNoGapThatHidesLaterCommits()
    {
        using var a = new StorageEngine(_dbPath, Config);
        using var b = new StorageEngine(_dbPath, Config);
        var p1 = a.AllocatePage();
        var p2 = a.AllocatePage();
        var p3 = a.AllocatePage();

        // A writes two transactions, so its own write position is past one transaction's length.
        await WriteAndCommitAsync(a, p1, 0x11);
        await WriteAndCommitAsync(a, p1, 0x12);
        using (b.BeginTransaction()) { }
        await b.CheckpointAsync();
        Assert.Equal(0, WalLength);
        await WriteAndCommitAsync(b, p2, 0x22);

        // A rolls back. An abort record written at A's stale position would leave a gap of zeros
        // after B's record, and B's replay would stop there and miss A's next commit.
        using (a.BeginTransaction()) { }
        await WriteAndCommitAsync(a, p3, 0x33);

        await b.CheckpointAsync();
        AssertFreshEngineReads(p1, 0x12);
        AssertFreshEngineReads(p2, 0x22);
        AssertFreshEngineReads(p3, 0x33);
    }

    [Fact]
    public async Task RandomInterleavings_KeepEveryCommit()
    {
        // Seeded, so a failure reproduces. Commits, empty and rolled-back transactions, reads and
        // checkpoints (each of which may truncate the WAL) from either engine.
        for (int seed = 0; seed < 25; seed++)
        {
            var dbPath = Path.Combine(_dir, $"stress_{seed}.db");
            var rnd = new Random(seed);
            var expected = new Dictionary<uint, byte>();
            using (var a = new StorageEngine(dbPath, Config))
            using (var b = new StorageEngine(dbPath, Config))
            {
                var engines = new[] { a, b };
                var pages = Enumerable.Range(0, 8).Select(_ => a.AllocatePage()).ToArray();
                for (int step = 0; step < 300; step++)
                {
                    var engine = engines[rnd.Next(2)];
                    var page = pages[rnd.Next(pages.Length)];
                    switch (rnd.Next(6))
                    {
                        case 0:
                        case 1:
                            var fill = (byte)rnd.Next(1, 255);
                            await WriteAndCommitAsync(engine, page, fill);
                            expected[page] = fill;
                            break;
                        case 2:
                            using (engine.BeginTransaction()) { }
                            break;
                        case 3:
                            using (var tx = engine.BeginTransaction())
                                engine.WritePage(page, tx.TransactionId, Filled(engine.PageSize, 0xEE)); // rolled back
                            break;
                        case 4:
                            await engine.CheckpointAsync();
                            break;
                        case 5:
                            using (engine.BeginTransaction())
                                if (expected.TryGetValue(page, out var current))
                                    AssertPage(engine, page, current);
                            break;
                    }
                }
                await a.CheckpointAsync();
                await b.CheckpointAsync();
                await a.CheckpointAsync();
            }

            using var c = new StorageEngine(dbPath, Config);
            using (c.BeginTransaction())
                foreach (var (page, fill) in expected)
                    AssertPage(c, page, fill);
        }
    }

    [Fact]
    public async Task TornBatchOfACrashedWriter_IsCutOff_SoTheNextCommitIsReplayed()
    {
        using var a = new StorageEngine(_dbPath, Config);
        using var b = new StorageEngine(_dbPath, Config);
        var p1 = a.AllocatePage();
        var p2 = a.AllocatePage();
        var p3 = a.AllocatePage();

        await WriteAndCommitAsync(a, p1, 0x11);
        using (b.BeginTransaction()) { }

        // A writer dies part-way through a batch: its bytes are in the file, but it never
        // published a new WAL end.
        long published = WalLength;
        AppendToWal(TornWriteRecord());
        Assert.True(WalLength > published);

        // The next commit appends at the published end, so no reader meets the torn bytes.
        await WriteAndCommitAsync(a, p2, 0x22);

        // B replays A's commit, checkpoints it and can truncate the WAL. Had the commit been
        // appended after the torn bytes, B's replay would stop there on every attempt, and B could
        // never truncate the WAL again.
        using (b.BeginTransaction()) { }
        await b.CheckpointAsync();
        Assert.Equal(0, WalLength);

        await WriteAndCommitAsync(b, p3, 0x33);
        using (a.BeginTransaction()) { }
        await a.CheckpointAsync();

        AssertFreshEngineReads(p1, 0x11);
        AssertFreshEngineReads(p2, 0x22);
        AssertFreshEngineReads(p3, 0x33);
    }

    [Fact]
    public async Task UnreadableRecordInsideTheWal_StopsTheReplay_WithoutLettingTheReaderTruncate()
    {
        using var a = new StorageEngine(_dbPath, Config);
        using var b = new StorageEngine(_dbPath, Config);
        var p1 = a.AllocatePage();
        var p2 = a.AllocatePage();
        var p3 = a.AllocatePage();
        var p4 = a.AllocatePage();

        await WriteAndCommitAsync(b, p3, 0x33); // B's own page; B's bookmark: right after it
        long firstRecordOfA = WalLength;
        await WriteAndCommitAsync(a, p1, 0x11);
        long secondRecord = WalLength;
        await WriteAndCommitAsync(a, p2, 0x22);

        // The first of A's records becomes unreadable for B.
        CorruptWalByte(firstRecordOfA);

        // B cannot check a commit against records it cannot replay, so it rejects it and writes
        // nothing (#304).
        long walBeforeCheckpoint = WalLength;
        await Assert.ThrowsAsync<BLiteTransactionConflictException>(() => WriteAndCommitAsync(b, p4, 0x44));
        Assert.Equal(walBeforeCheckpoint, WalLength);

        // B's replay stops at it, so it must not count A's records as replayed: checkpointing its
        // own page must leave the WAL, and with it A's records, in place.
        await b.CheckpointAsync();
        Assert.Equal(walBeforeCheckpoint, WalLength);
        Assert.True(walBeforeCheckpoint > secondRecord);

        // A still holds its pages and checkpoints them, together with B's.
        await a.CheckpointAsync();
        AssertFreshEngineReads(p1, 0x11);
        AssertFreshEngineReads(p2, 0x22);
        AssertFreshEngineReads(p3, 0x33);
    }

    [Fact]
    public async Task CutOff_CannotDeleteCommitsOfTheDirectOrTwoPhasePaths()
    {
        // The repro from the review of #202: a commit through the direct path never publishes
        // the WAL end, so the next group commit's cut-off would delete it. Those paths are
        // rejected in multi-process mode, so every acknowledged commit lies within the published
        // end and survives the cut-off.
        uint p, q;
        using (var engine = new StorageEngine(_dbPath, Config))
        {
            p = engine.AllocatePage();
            q = engine.AllocatePage();
            using (var tx = engine.BeginTransaction())
            {
                engine.WritePage(p, tx.TransactionId, Filled(engine.PageSize, 0x44));
                await Assert.ThrowsAsync<NotSupportedException>(() => engine.CommitTransactionAsync(tx.TransactionId));
                await Assert.ThrowsAsync<NotSupportedException>(() => engine.PrepareTransactionAsync(tx.TransactionId));
                await Assert.ThrowsAsync<NotSupportedException>(() => engine.MarkTransactionCommittedAsync(tx.TransactionId));
                AssertNothingBeyondThePublishedEnd();
                await tx.CommitAsync();
            }

            await WriteAndCommitAsync(engine, q, 0x55);
            AssertNothingBeyondThePublishedEnd();
        }

        using var reopened = new StorageEngine(_dbPath, Config);
        using (reopened.BeginTransaction())
        {
            AssertPage(reopened, p, 0x44);
            AssertPage(reopened, q, 0x55);
        }
    }

    [Fact]
    public async Task UnreadableRecordInsideThePublishedRange_IsReportedAsAStall()
    {
        using var a = new StorageEngine(_dbPath, Config);
        using var b = new StorageEngine(_dbPath, Config);
        var metrics = b.EnsureMetrics();
        var p1 = a.AllocatePage();
        var p2 = a.AllocatePage();
        var p3 = a.AllocatePage();

        long firstRecordOfA;
        var warnings = new WarningCollector(Path.ChangeExtension(_dbPath, ".wal"));
        Trace.Listeners.Add(warnings);
        try
        {
            await WriteAndCommitAsync(b, p3, 0x33); // something for B to checkpoint; B's bookmark: right after it
            firstRecordOfA = WalLength;
            await WriteAndCommitAsync(a, p1, 0x11);
            CorruptWalByte(firstRecordOfA); // A's record: unreadable for B, whose bookmark lies before it
            await WriteAndCommitAsync(a, p2, 0x22);

            for (int i = 0; i < 2 * StorageEngine.CatchUpStallThreshold; i++)
                using (b.BeginTransaction()) { }
            await b.CheckpointAsync();
        }
        finally
        {
            Trace.Listeners.Remove(warnings);
        }

        // Reported, but not on every attempt: the first, then every power of two in a row.
        var messages = warnings.Messages;
        Assert.NotEmpty(messages);
        Assert.InRange(messages.Count, 2, 7);
        Assert.All(messages, m => Assert.Contains($"the record at offset {firstRecordOfA} is unreadable", m));
        Assert.Contains(messages, m => m.Contains("stalled"));
        Assert.DoesNotContain("stalled", messages[0]);

        // The checkpoint wrote nothing, and says so.
        Assert.NotEqual(0, WalLength);
        await WaitUntilAsync(() => metrics.GetSnapshot().CheckpointsFailedTotal >= 1);
        Assert.Equal(0, metrics.GetSnapshot().CheckpointsTotal);
    }

    private void AssertNothingBeyondThePublishedEnd()
    {
        using var shm = WalSharedMemory.Open(ShmPath, Config.PageSize);
        Assert.Equal(WalLength, shm.ReadWalEndOffset());
    }

    /// <summary>Collects the trace warnings that name one WAL file.</summary>
    private sealed class WarningCollector(string walPath) : TraceListener
    {
        private readonly List<string> _messages = new();

        public List<string> Messages { get { lock (_messages) return _messages.ToList(); } }

        public override void Write(string? message) { }

        public override void WriteLine(string? message)
        {
            if (message != null && message.Contains(walPath))
                lock (_messages) _messages.Add(message);
        }
    }

    /// <summary>The start of a Write record whose page data never made it to disk.</summary>
    private static byte[] TornWriteRecord()
    {
        var record = new byte[1 + 16 + 100];
        record[0] = 2; // WalRecordType.Write
        BitConverter.GetBytes(999_999UL).CopyTo(record, 1);   // transaction id
        BitConverter.GetBytes(12345u).CopyTo(record, 9);      // page id
        BitConverter.GetBytes(16384).CopyTo(record, 13);      // data length; only 100 bytes follow
        return record;
    }

    private void AppendToWal(byte[] bytes)
    {
        using var stream = new FileStream(Path.ChangeExtension(_dbPath, ".wal"), FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
        stream.Seek(0, SeekOrigin.End);
        stream.Write(bytes);
    }

    private void CorruptWalByte(long offset)
    {
        using var stream = new FileStream(Path.ChangeExtension(_dbPath, ".wal"), FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
        stream.Position = offset;
        stream.WriteByte(0xFF); // not a record type
    }

    [Fact]
    public async Task DirectCommitPath_IsRejected_SoAnotherProcessesTruncationCannotLoseIt()
    {
        // The sequence from the review of #201. The direct path neither catches up with the
        // shared WAL nor publishes its end: A would commit, checkpoint and truncate without
        // seeing B's record, and B would then discard it along with the rest of the old WAL.
        using var a = new StorageEngine(_dbPath, Config);
        using var b = new StorageEngine(_dbPath, Config);
        var p = a.AllocatePage();
        var q = a.AllocatePage();

        await WriteAndCommitAsync(a, p, 0x11);
        using (b.BeginTransaction())
            AssertPage(b, p, 0x11);

        long walBefore = WalLength;
        using (var tx = b.BeginTransaction())
        {
            b.WritePage(p, tx.TransactionId, Filled(b.PageSize, 0x44));
            await Assert.ThrowsAsync<NotSupportedException>(() => b.CommitTransactionAsync(tx.TransactionId));
            Assert.Equal(walBefore, WalLength); // nothing appended outside group commit

            // The transaction is untouched and commits through group commit.
            await tx.CommitAsync();
        }

        await WriteAndCommitAsync(a, q, 0x55);
        await a.CheckpointAsync();
        Assert.Equal(0, WalLength);

        using (b.BeginTransaction())
            AssertPage(b, p, 0x44);
        AssertFreshEngineReads(p, 0x44);
        AssertFreshEngineReads(q, 0x55);
    }

    [Fact]
    public async Task TwoPhaseCommitPaths_AreRejected()
    {
        using var engine = new StorageEngine(_dbPath, Config);
        var page = engine.AllocatePage();
        long walBefore = WalLength;

        using (var tx = engine.BeginTransaction())
        {
            engine.WritePage(page, tx.TransactionId, Filled(engine.PageSize, 0x66));
            await Assert.ThrowsAsync<NotSupportedException>(() => engine.PrepareTransactionAsync(tx.TransactionId));
            await Assert.ThrowsAsync<NotSupportedException>(() => engine.PrepareTransactionAsync(tx.TransactionId, CancellationToken.None));
            await Assert.ThrowsAsync<NotSupportedException>(() => engine.MarkTransactionCommittedAsync(tx.TransactionId));
            await Assert.ThrowsAsync<NotSupportedException>(() => tx.PrepareAsync());
        }

        Assert.Equal(walBefore, WalLength);
    }

    [Fact]
    public async Task DirectAndTwoPhaseCommitPaths_StillWork_InSingleProcessMode()
    {
        using var engine = new StorageEngine(_dbPath, PageFileConfig.Default);
        var direct = engine.AllocatePage();
        var prepared = engine.AllocatePage();

        using (var tx = engine.BeginTransaction())
        {
            engine.WritePage(direct, tx.TransactionId, Filled(engine.PageSize, 0x77));
            await engine.CommitTransactionAsync(tx.TransactionId);
        }
        using (var tx = engine.BeginTransaction())
        {
            engine.WritePage(prepared, tx.TransactionId, Filled(engine.PageSize, 0x78));
            Assert.True(await engine.PrepareTransactionAsync(tx.TransactionId));
            await engine.MarkTransactionCommittedAsync(tx.TransactionId);
        }

        AssertPage(engine, direct, 0x77);
        AssertPage(engine, prepared, 0x78);
    }

    [Fact]
    public async Task CheckpointThatCannotTakeTheWriterLock_IsReportedAsFailed()
    {
        using var engine = new StorageEngine(_dbPath, Config);
        var metrics = engine.EnsureMetrics();
        var page = engine.AllocatePage();
        await WriteAndCommitAsync(engine, page, 0x11);

        // Another holder of the cross-process writer lock (an instance in the same process
        // excludes it like another process would).
        using (var other = WalSharedMemory.Open(ShmPath, Config.PageSize))
        {
            Assert.True(other.TryAcquireWriterLock(1_000));
            await engine.CheckpointAsync();
            other.ReleaseWriterLock();
        }
        Assert.NotEqual(0, WalLength); // nothing was written or truncated

        await WaitUntilAsync(() => metrics.GetSnapshot().CheckpointsFailedTotal == 1);
        Assert.Equal(0, metrics.GetSnapshot().CheckpointsTotal);

        await engine.CheckpointAsync();
        Assert.Equal(0, WalLength);
        await WaitUntilAsync(() => metrics.GetSnapshot().CheckpointsTotal == 1);
        Assert.Equal(1, metrics.GetSnapshot().CheckpointsFailedTotal);
    }

    [Fact]
    public async Task ShmOpenedWithAnotherPageSize_WhileInUse_ThrowsAndKeepsTheHeader()
    {
        using var a = new StorageEngine(_dbPath, Config);
        var page = a.AllocatePage();
        await WriteAndCommitAsync(a, page, 0x11);
        await a.CheckpointAsync();                // generation 1
        await WriteAndCommitAsync(a, page, 0x22); // the WAL has records again

        Assert.Throws<InvalidOperationException>(() => WalSharedMemory.Open(ShmPath, Config.PageSize * 2));

        using (var shm = WalSharedMemory.Open(ShmPath, Config.PageSize))
        {
            Assert.Equal(1, shm.ReadWalGeneration());
            Assert.Equal(WalLength, shm.ReadWalEndOffset());
        }
        using (a.BeginTransaction())
            AssertPage(a, page, 0x22);
        AssertFreshEngineReads(page, 0x22);
    }

    [Fact]
    public async Task ShmOpenedWithAnotherPageSize_WhenIdle_IsReinitialisedWithoutGoingBack()
    {
        // E.g. a .wal-shm left behind by a database recreated with another page size.
        ulong nextTransactionId;
        using (var a = new StorageEngine(_dbPath, Config))
        {
            var page = a.AllocatePage();
            await WriteAndCommitAsync(a, page, 0x11);
            await a.CheckpointAsync(); // generation 1, WAL end 0
            using var shm = WalSharedMemory.Open(ShmPath, Config.PageSize);
            Assert.Equal(1, shm.ReadWalGeneration());
            nextTransactionId = shm.ReadNextTransactionId();
        }

        using var reopened = WalSharedMemory.Open(ShmPath, Config.PageSize * 2);
        // A generation never seen before: a process still attached to the old one sees a change.
        Assert.Equal(2, reopened.ReadWalGeneration());
        Assert.Equal(nextTransactionId, reopened.ReadNextTransactionId());
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "Condition not met within 10 s.");
            await Task.Delay(20);
        }
    }

    private void AssertFreshEngineReads(uint page, byte fill)
    {
        using var c = new StorageEngine(_dbPath, Config);
        using (c.BeginTransaction())
            AssertPage(c, page, fill);
    }

    private static void AssertPage(StorageEngine storage, uint page, byte fill)
    {
        var read = new byte[storage.PageSize];
        storage.ReadPage(page, null, read);
        Assert.Equal(Filled(storage.PageSize, fill)[32], read[32]);
        Assert.Equal(Filled(storage.PageSize, fill), read);
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
