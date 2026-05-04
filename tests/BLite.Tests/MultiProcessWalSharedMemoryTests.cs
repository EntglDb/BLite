using BLite.Core.Encryption;
using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace BLite.Tests;

/// <summary>
/// Integration tests for the multi-process WAL coordination layer
/// (<c>roadmap/v5/MULTI_PROCESS_WAL.md</c>).
/// <para>
/// Covers the opt-in surface, the <see cref="WalSharedMemory"/> primitives in isolation,
/// and end-to-end behaviour of two <see cref="StorageEngine"/> instances opened on the
/// same physical files (which is the in-process equivalent of two OS processes — the
/// kernel-level <c>FileShare.ReadWrite</c>, named-Mutex / OFD-lock and SHM coordination
/// paths are all exercised the same way).
/// </para>
/// </summary>
public sealed class MultiProcessWalSharedMemoryTests : IDisposable
{
    private readonly string _tempDir;

    // Cache the current-process PID once so tests don't repeatedly allocate Process
    // instances (each Process.GetCurrentProcess() returns a new disposable wrapper).
    private static readonly int CurrentPid = System.Diagnostics.Process.GetCurrentProcess().Id;

    public MultiProcessWalSharedMemoryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"blite_mpwal_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* best-effort */ }
    }

    private string ShmPath() => Path.Combine(_tempDir, "test.wal-shm");

    // ── WalSharedMemory unit-level behaviour ─────────────────────────────────

    [Fact]
    public void Open_CreatesFileWithValidMagicAndVersion()
    {
        using var shm = WalSharedMemory.Open(ShmPath(), pageSize: 4096);
        Assert.True(File.Exists(ShmPath()));

        // Re-read raw header bytes to confirm magic / version / page size are persisted.
        byte[] header = File.ReadAllBytes(ShmPath());
        Assert.True(header.Length >= 64);
        int magic = BitConverter.ToInt32(header, 0);
        int version = BitConverter.ToInt32(header, 4);
        int pageSize = BitConverter.ToInt32(header, 8);

        Assert.Equal(0x48534C42, magic); // 'B','L','S','H'
        Assert.Equal(1, version);
        Assert.Equal(4096, pageSize);
    }

    [Fact]
    public void Open_CorruptedMagic_RecreatesHeaderInPlace()
    {
        // Create, then corrupt the magic.
        using (var shm = WalSharedMemory.Open(ShmPath(), 4096)) { }
        using (var fs = new FileStream(ShmPath(), FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
        {
            fs.Position = 0;
            fs.Write(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }, 0, 4);
        }

        // Re-opening must NOT throw; it detects bad magic and re-initialises.
        using var shm2 = WalSharedMemory.Open(ShmPath(), 4096);
        byte[] header = File.ReadAllBytes(ShmPath());
        Assert.Equal(0x48534C42, BitConverter.ToInt32(header, 0));
    }

    [Fact]
    public void AllocateTransactionId_MonotonicAndUniqueUnderConcurrency()
    {
        using var shm = WalSharedMemory.Open(ShmPath(), 4096);

        const int total = 4_000;
        const int workers = 8;
        var ids = new System.Collections.Concurrent.ConcurrentBag<ulong>();
        Parallel.For(0, workers, _ =>
        {
            for (int i = 0; i < total / workers; i++)
                ids.Add(shm.AllocateTransactionId());
        });

        var distinct = new HashSet<ulong>(ids);
        Assert.Equal(total, distinct.Count);
        Assert.DoesNotContain(0UL, distinct);
        Assert.Equal((ulong)total, distinct.Max());
    }

    [Fact]
    public void TwoSharedMemoryInstances_OnSameFile_ShareTransactionIdCounter()
    {
        using var a = WalSharedMemory.Open(ShmPath(), 4096);
        using var b = WalSharedMemory.Open(ShmPath(), 4096);

        var ids = new HashSet<ulong>();
        for (int i = 0; i < 50; i++)
        {
            Assert.True(ids.Add(a.AllocateTransactionId()));
            Assert.True(ids.Add(b.AllocateTransactionId()));
        }
        Assert.Equal(100, ids.Count);

        // Both views observe the same end state.
        Assert.Equal(a.ReadNextTransactionId(), b.ReadNextTransactionId());
    }

    [Fact]
    public void WriterLock_IsMutualExclusion_AcrossInstances()
    {
        using var a = WalSharedMemory.Open(ShmPath(), 4096);
        using var b = WalSharedMemory.Open(ShmPath(), 4096);

        Assert.True(a.TryAcquireWriterLock(timeoutMs: 1_000));
        try
        {
            // Second acquirer must time out.
            var sw = System.Diagnostics.Stopwatch.StartNew();
            Assert.False(b.TryAcquireWriterLock(timeoutMs: 200));
            sw.Stop();
            Assert.InRange(sw.ElapsedMilliseconds, 150, 2_000);
        }
        finally
        {
            a.ReleaseWriterLock();
        }

        // After release, the second instance can take it.
        Assert.True(b.TryAcquireWriterLock(timeoutMs: 1_000));
        b.ReleaseWriterLock();
    }

    [Fact]
    public void WriterLock_RecordsOwnerPid_AndClearsOnRelease()
    {
        using var shm = WalSharedMemory.Open(ShmPath(), 4096);
        Assert.Equal(0, shm.ReadWriterOwnerPid());

        Assert.True(shm.TryAcquireWriterLock(1_000));
        Assert.Equal(CurrentPid, shm.ReadWriterOwnerPid());

        shm.ReleaseWriterLock();
        Assert.Equal(0, shm.ReadWriterOwnerPid());
    }

    [Fact]
    public void ForceClearStaleWriter_ClearsDeadPid_KeepsLivePid()
    {
        // Inject a definitely-dead PID directly into the SHM file (bypassing our API)
        // and verify ForceClearStaleWriter detects and resets it.
        using (var shm = WalSharedMemory.Open(ShmPath(), 4096)) { /* create file */ }

        using (var fs = new FileStream(ShmPath(), FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
        {
            fs.Position = 40; // OffsetWriterOwnerPid
            fs.Write(BitConverter.GetBytes(int.MaxValue - 1), 0, 4);
        }

        using var shm2 = WalSharedMemory.Open(ShmPath(), 4096);
        Assert.True(shm2.ForceClearStaleWriter());
        Assert.Equal(0, shm2.ReadWriterOwnerPid());

        // Second call returns false (nothing left to clear).
        Assert.False(shm2.ForceClearStaleWriter());

        // And the live PID of the current process is treated as still-alive.
        Assert.True(shm2.TryAcquireWriterLock(1_000));
        try
        {
            Assert.False(shm2.ForceClearStaleWriter()); // current process is alive
            Assert.Equal(CurrentPid, shm2.ReadWriterOwnerPid());
        }
        finally
        {
            shm2.ReleaseWriterLock();
        }
    }

    [Fact]
    public void ReaderSlots_AcquireReleaseAndMinReadOffsetTrackCorrectly()
    {
        using var shm = WalSharedMemory.Open(ShmPath(), 4096, maxReaders: 4);

        // No readers → MaxValue.
        Assert.Equal(long.MaxValue, shm.GetMinReaderOffset());

        Assert.True(shm.TryAcquireReaderSlot(out int s1, maxReadOffset: 1000));
        Assert.True(shm.TryAcquireReaderSlot(out int s2, maxReadOffset: 500));
        Assert.True(shm.TryAcquireReaderSlot(out int s3, maxReadOffset: 2000));
        Assert.NotEqual(s1, s2);
        Assert.NotEqual(s2, s3);
        Assert.NotEqual(s1, s3);

        Assert.Equal(500, shm.GetMinReaderOffset());

        shm.UpdateReaderSlot(s2, maxReadOffset: 1500);
        Assert.Equal(1000, shm.GetMinReaderOffset());

        shm.ReleaseReaderSlot(s1);
        Assert.Equal(1500, shm.GetMinReaderOffset());

        shm.ReleaseReaderSlot(s2);
        shm.ReleaseReaderSlot(s3);
        Assert.Equal(long.MaxValue, shm.GetMinReaderOffset());
    }

    [Fact]
    public void ReaderSlots_AllFull_ReturnsFalse()
    {
        using var shm = WalSharedMemory.Open(ShmPath(), 4096, maxReaders: 2);
        Assert.True(shm.TryAcquireReaderSlot(out _, 1));
        Assert.True(shm.TryAcquireReaderSlot(out _, 2));
        Assert.False(shm.TryAcquireReaderSlot(out var s3, 3));
        Assert.Equal(-1, s3);
    }

    [Fact]
    public void WalEndOffset_AdvancesMonotonically()
    {
        using var shm = WalSharedMemory.Open(ShmPath(), 4096);
        Assert.Equal(0, shm.ReadWalEndOffset());

        shm.AdvanceWalEndOffset(100);
        Assert.Equal(100, shm.ReadWalEndOffset());

        // Must NOT regress.
        shm.AdvanceWalEndOffset(50);
        Assert.Equal(100, shm.ReadWalEndOffset());

        shm.AdvanceWalEndOffset(500);
        Assert.Equal(500, shm.ReadWalEndOffset());
    }

    // ── End-to-end StorageEngine integration ─────────────────────────────────

    [Fact]
    public void StorageEngine_WithMultiProcessAccess_CreatesShmSidecar()
    {
        var dbPath = Path.Combine(_tempDir, "engine.db");
        var cfg = PageFileConfig.Default with { AllowMultiProcessAccess = true };

        using var engine = new StorageEngine(dbPath, cfg);

        // The .wal-shm file must exist and the engine must hold a non-null SHM reference.
        Assert.NotNull(engine.SharedMemory);
        Assert.True(File.Exists(engine.SharedMemory!.Path));
        Assert.EndsWith(".wal-shm", engine.SharedMemory.Path);
    }

    [Fact]
    public void StorageEngine_WithoutFlag_DoesNotCreateShmSidecar()
    {
        var dbPath = Path.Combine(_tempDir, "engine.db");
        using var engine = new StorageEngine(dbPath, PageFileConfig.Default);

        Assert.Null(engine.SharedMemory);
        Assert.False(File.Exists(dbPath + ".wal-shm"));
        Assert.False(File.Exists(Path.ChangeExtension(dbPath, ".wal") + "-shm"));
    }

    [Fact]
    public void StorageEngine_TwoInstances_CanOpenSameFiles_WhenFlagIsSet()
    {
        var dbPath = Path.Combine(_tempDir, "shared.db");
        var cfg = PageFileConfig.Default with { AllowMultiProcessAccess = true };

        using var engineA = new StorageEngine(dbPath, cfg);
        // Without FileShare.ReadWrite this would throw IOException — that is the headline
        // regression this PR prevents.
        using var engineB = new StorageEngine(dbPath, cfg);

        Assert.NotNull(engineA.SharedMemory);
        Assert.NotNull(engineB.SharedMemory);

        // Both engines must observe the same SHM file.
        Assert.Equal(engineA.SharedMemory!.Path, engineB.SharedMemory!.Path);
    }

    [Fact]
    public void StorageEngine_SecondOpen_FailsWithoutFlag()
    {
        var dbPath = Path.Combine(_tempDir, "single.db");
        using var engineA = new StorageEngine(dbPath, PageFileConfig.Default);

        // Second open in single-process mode must fail (FileShare.None preserved).
        Assert.ThrowsAny<IOException>(() =>
        {
            using var engineB = new StorageEngine(dbPath, PageFileConfig.Default);
        });
    }

    [Fact]
    public void StorageEngine_TransactionIds_AreSharedAcrossInstances_InMultiProcessMode()
    {
        var dbPath = Path.Combine(_tempDir, "tids.db");
        var cfg = PageFileConfig.Default with { AllowMultiProcessAccess = true };

        using var a = new StorageEngine(dbPath, cfg);
        using var b = new StorageEngine(dbPath, cfg);

        // Allocate from both in interleaved fashion. Every id must be unique because
        // both engines share the SHM-backed counter. Dispose each transaction
        // immediately so it doesn't linger in _activeTransactions and mask any
        // lifecycle bugs.
        var ids = new HashSet<ulong>();
        for (int i = 0; i < 20; i++)
        {
            using var ta = a.BeginTransaction();
            using var tb = b.BeginTransaction();
            Assert.True(ids.Add(ta.TransactionId), $"duplicate tid {ta.TransactionId}");
            Assert.True(ids.Add(tb.TransactionId), $"duplicate tid {tb.TransactionId}");
        }
        Assert.Equal(40, ids.Count);
    }

    // ── Phase 4: SHM WAL page→offset hash table ──────────────────────────────

    [Fact]
    public void ShmWalIndex_UpdateAndLookup_RoundTrips()
    {
        using var shm = WalSharedMemory.Open(ShmPath(), 4096);

        var entries = new List<(uint pageId, long walOffset)>
        {
            (1u, 17L),
            (42u, 200L),
            (999u, 8192L),
        };
        shm.TryAcquireWriterLock(1000);
        shm.UpdatePageOffsets(entries);
        shm.ReleaseWriterLock();

        Assert.Equal(17L,   shm.LookupPageOffset(1u));
        Assert.Equal(200L,  shm.LookupPageOffset(42u));
        Assert.Equal(8192L, shm.LookupPageOffset(999u));
        Assert.Equal(-1L,   shm.LookupPageOffset(0u));   // reserved
        Assert.Equal(-1L,   shm.LookupPageOffset(100u)); // not inserted
    }

    [Fact]
    public void ShmWalIndex_UpdateExistingPage_OverwritesOffset()
    {
        using var shm = WalSharedMemory.Open(ShmPath(), 4096);

        shm.TryAcquireWriterLock(1000);
        shm.UpdatePageOffsets(new List<(uint, long)> { (5u, 100L) });
        shm.UpdatePageOffsets(new List<(uint, long)> { (5u, 500L) }); // update
        shm.ReleaseWriterLock();

        Assert.Equal(500L, shm.LookupPageOffset(5u));
    }

    [Fact]
    public void ShmWalIndex_RebuildIndex_ClearsAndRepopulates()
    {
        using var shm = WalSharedMemory.Open(ShmPath(), 4096);

        shm.TryAcquireWriterLock(1000);
        shm.UpdatePageOffsets(new List<(uint, long)>
        {
            (10u, 100L), (20u, 200L), (30u, 300L)
        });
        // Rebuild keeping only page 20 as survivor
        shm.RebuildIndex(new List<(uint, long)> { (20u, 200L) });
        shm.ReleaseWriterLock();

        Assert.Equal(-1L,  shm.LookupPageOffset(10u)); // removed
        Assert.Equal(200L, shm.LookupPageOffset(20u)); // survivor
        Assert.Equal(-1L,  shm.LookupPageOffset(30u)); // removed
    }

    [Fact]
    public void ShmWalIndex_RebuildEmpty_ClearsAll()
    {
        using var shm = WalSharedMemory.Open(ShmPath(), 4096);

        shm.TryAcquireWriterLock(1000);
        shm.UpdatePageOffsets(new List<(uint, long)> { (7u, 42L) });
        shm.RebuildIndex(System.Array.Empty<(uint, long)>());
        shm.ReleaseWriterLock();

        Assert.Equal(-1L, shm.LookupPageOffset(7u));
    }

    [Fact]
    public void WriteWalEndOffset_CanDecrease_ToZero()
    {
        using var shm = WalSharedMemory.Open(ShmPath(), 4096);

        shm.AdvanceWalEndOffset(12345L);
        Assert.Equal(12345L, shm.ReadWalEndOffset());

        shm.WriteWalEndOffset(0L);
        Assert.Equal(0L, shm.ReadWalEndOffset());
    }

    // ── Phase 4: cross-engine read via SHM index ─────────────────────────────

    [Fact]
    public async Task CrossEngineRead_CommittedByA_VisibleToB_AfterPhase4Replay()
    {
        // Simulates two "processes" (two StorageEngine instances on the same files).
        // Engine A commits a page. Engine B's local _walIndex is empty (it never saw
        // that commit) — but by the time BeginTransaction is called on B (Phase 7),
        // or when B reads a page (Phase 4), it should be able to fetch the data.

        var dbPath = Path.Combine(_tempDir, "phase4.db");
        var cfg = PageFileConfig.Default with { AllowMultiProcessAccess = true };

        using var engineA = new StorageEngine(dbPath, cfg);

        // Write page 2 via engine A (use correct page size).
        var pageData = new byte[engineA.PageSize];
        pageData[0] = 0xAB;
        ulong txAId;
        using (var txA = engineA.BeginTransaction())
        {
            engineA.WritePage(2, txA.TransactionId, pageData);
            txAId = txA.TransactionId;
            await txA.CommitAsync();
        }

        // Engine B opens after A has already committed — its _walIndex starts empty.
        using var engineB = new StorageEngine(dbPath, cfg);

        // Phase 7: BeginTransaction on B triggers incremental WAL replay since
        // _shm.ReadWalEndOffset() > 0 and _lastKnownWalEndOffset == 0.
        using var txB = engineB.BeginTransaction();

        // Read page 2 — should be visible via _walIndex (populated by replay) or
        // via Phase 4 SHM lookup → ReadPageAt fallback.
        var buf = new byte[engineB.PageSize];
        engineB.ReadPage(2, txAId, buf.AsSpan());

        Assert.Equal(0xAB, buf[0]);
    }

    // ── Phase 5: reader slot lifecycle ──────────────────────────────────────

    [Fact]
    public void BeginTransaction_WithShm_AcquiresReaderSlot()
    {
        var dbPath = Path.Combine(_tempDir, "rslot.db");
        var cfg = PageFileConfig.Default with { AllowMultiProcessAccess = true };
        using var engine = new StorageEngine(dbPath, cfg);

        var minBefore = engine.SharedMemory!.GetMinReaderOffset();
        using var txn = engine.BeginTransaction();

        // A reader slot should have been acquired (slotIndex >= 0).
        Assert.True(txn.ShmReaderSlotIndex >= 0,
            "BeginTransaction did not acquire a reader slot in multi-process mode.");
    }

    [Fact]
    public void Transaction_Dispose_ReleasesReaderSlot()
    {
        var dbPath = Path.Combine(_tempDir, "rslot2.db");
        var cfg = PageFileConfig.Default with { AllowMultiProcessAccess = true };
        using var engine = new StorageEngine(dbPath, cfg);

        var shm = engine.SharedMemory!;
        int slotIndex;

        using (var txn = engine.BeginTransaction())
        {
            slotIndex = txn.ShmReaderSlotIndex;
            Assert.True(slotIndex >= 0);
        }
        // After dispose the slot should be cleared — GetMinReaderOffset returns MaxValue
        // when no readers are active.
        Assert.Equal(long.MaxValue, shm.GetMinReaderOffset());
    }

    [Fact]
    public void MultipleTransactions_EachGetOwnSlot_AllReleasedAfterDispose()
    {
        var dbPath = Path.Combine(_tempDir, "rslot3.db");
        var cfg = PageFileConfig.Default with { AllowMultiProcessAccess = true };
        using var engine = new StorageEngine(dbPath, cfg);

        var shm = engine.SharedMemory!;
        var slots = new HashSet<int>();

        // Open 4 concurrent transactions — each should get a distinct slot.
        var txns = new List<BLite.Core.Transactions.Transaction>();
        for (int i = 0; i < 4; i++)
        {
            var t = engine.BeginTransaction();
            slots.Add(t.ShmReaderSlotIndex);
            txns.Add(t);
        }

        // All slots should be distinct (each transaction tracks a different offset).
        Assert.Equal(4, slots.Count);

        foreach (var t in txns) t.Dispose();

        // After all transactions, no active reader slots remain.
        Assert.Equal(long.MaxValue, shm.GetMinReaderOffset());
    }

    // ── Phase 6: checkpoint bounded by GetMinReaderOffset ────────────────────

    [Fact]
    public async Task CheckpointAsync_WithNoActiveReaders_CheckpointsProceedsNormally()
    {
        var dbPath = Path.Combine(_tempDir, "ckpt.db");
        var cfg = PageFileConfig.Default with { AllowMultiProcessAccess = true };
        using var engine = new StorageEngine(dbPath, cfg);

        // Write a page, commit, and immediately dispose the transaction so the
        // reader slot is released before checkpoint runs.
        var pageData = new byte[engine.PageSize];
        pageData[0] = 0x55;
        using (var txn = engine.BeginTransaction())
        {
            engine.WritePage(3, txn.TransactionId, pageData);
            await txn.CommitAsync();
        } // reader slot released here

        // No active readers → GetMinReaderOffset() == long.MaxValue →
        // safeOffset = walSize → checkpoint should flush everything.
        await engine.CheckpointAsync();

        // WAL should now be empty (truncated).
        Assert.Equal(0, engine.GetWalSize());
    }

    [Fact]
    public async Task CheckpointAsync_WithParallelFlush_DoesNotCorruptData()
    {
        // Tests that the parallel Task.WhenAll flush path works without data corruption.
        var dbPath = Path.Combine(_tempDir, "ckptpar.db");
        var cfg = PageFileConfig.Default with { AllowMultiProcessAccess = true };
        using var engine = new StorageEngine(dbPath, cfg);
        int pageSize = engine.PageSize;

        // Write multiple pages (to exercise the flush path), dispose each transaction
        // so reader slots don't block checkpoint.
        const int pageCount = 10;
        for (uint p = 2; p < 2 + pageCount; p++)
        {
            var d = new byte[pageSize];
            d[0] = (byte)p;
            using var t = engine.BeginTransaction();
            engine.WritePage(p, t.TransactionId, d);
            await t.CommitAsync();
        }

        await engine.CheckpointAsync();
        Assert.Equal(0, engine.GetWalSize());

        // Re-open and verify data integrity.
        using var engine2 = new StorageEngine(dbPath, cfg);
        for (uint p = 2; p < 2 + pageCount; p++)
        {
            var buf = new byte[engine2.PageSize];
            await engine2.ReadPageAsync(p, 0, buf.AsMemory());
            Assert.Equal((byte)p, buf[0]);
        }
    }

    // ── Phase 7: incremental WAL replay on BeginTransaction ──────────────────

    [Fact]
    public async Task BeginTransaction_Phase7_ReplaysCrossProcessCommits()
    {
        // Engine A commits a page AFTER engine B has already been opened.
        // When engine B calls BeginTransaction, it should trigger Phase-7 replay
        // and populate its _walIndex from the WAL records committed by A.

        var dbPath = Path.Combine(_tempDir, "phase7.db");
        var cfg = PageFileConfig.Default with { AllowMultiProcessAccess = true };

        using var engineA = new StorageEngine(dbPath, cfg);
        using var engineB = new StorageEngine(dbPath, cfg);

        // A commits a page.
        var pageData = new byte[engineA.PageSize];
        pageData[0] = 0xCC;
        ulong txAId;
        using (var txA = engineA.BeginTransaction())
        {
            engineA.WritePage(5, txA.TransactionId, pageData);
            txAId = txA.TransactionId;
            await txA.CommitAsync();
        }

        // B starts a new transaction — Phase 7 replay should pick up A's commit.
        using var txB = engineB.BeginTransaction();

        // Reading the page in B's context should now return A's data.
        var buf = new byte[engineB.PageSize];
        engineB.ReadPage(5, txAId, buf.AsSpan());
        Assert.Equal(0xCC, buf[0]);
    }

    [Fact]
    public async Task Phase7_ReplayDoesNotDuplicate_LocalCommits()
    {
        // When engine B commits a page itself, the Phase-7 replay on a subsequent
        // BeginTransaction must not overwrite B's own (newer) committed version.

        var dbPath = Path.Combine(_tempDir, "phase7b.db");
        var cfg = PageFileConfig.Default with { AllowMultiProcessAccess = true };

        using var engineA = new StorageEngine(dbPath, cfg);
        using var engineB = new StorageEngine(dbPath, cfg);

        // Both engines write to page 6, B's version is newer.
        var dataA = new byte[engineA.PageSize]; dataA[0] = 0xAA;
        using (var txA = engineA.BeginTransaction())
        {
            engineA.WritePage(6, txA.TransactionId, dataA);
            await txA.CommitAsync();
        }

        var dataB = new byte[engineB.PageSize]; dataB[0] = 0xBB;
        ulong txBId;
        using (var txB = engineB.BeginTransaction())
        {
            engineB.WritePage(6, txB.TransactionId, dataB);
            txBId = txB.TransactionId;
            await txB.CommitAsync();
        }

        // Next BeginTransaction on B — replay runs, but B's own version should win.
        using var txB2 = engineB.BeginTransaction();
        var buf = new byte[engineB.PageSize];
        engineB.ReadPage(6, txBId, buf.AsSpan());

        // 0xBB (B's version) should be visible, not 0xAA (A's older version).
        Assert.Equal(0xBB, buf[0]);
    }

    // ── Encrypted WAL multi-process ──────────────────────────────────────────

    [Fact]
    public async Task EncryptedWal_CrossEngine_CommitVisibleViaPhase7Replay()
    {
        // Validates that Phase 7 incremental WAL replay works correctly when both
        // engines are configured with an encrypted WAL. Engine A commits a page;
        // Engine B must be able to read it after BeginTransaction triggers replay.
        // This exercises the encrypted branch of ReadCommittedPagesSince.
        //
        // Engine B is intentionally opened AFTER Engine A has committed so that the
        // WAL file already contains the 64-byte BLCE crypto header — this ensures
        // Engine B's WriteAheadLog initializes _cryptoInitialized=true on open.

        var opts   = new CryptoOptions("mp_wal_test_passphrase", iterations: 1);
        var cfgA   = PageFileConfig.Default with
        {
            AllowMultiProcessAccess = true,
            CryptoProvider = new AesGcmCryptoProvider(opts),
        };
        var cfgB   = PageFileConfig.Default with
        {
            AllowMultiProcessAccess = true,
            CryptoProvider = new AesGcmCryptoProvider(opts),
        };

        var dbPath = Path.Combine(_tempDir, "enc_phase7.db");

        using var engineA = new StorageEngine(dbPath, cfgA);

        // Engine A commits a page first so the WAL crypto header exists on disk.
        var pageData = new byte[engineA.PageSize];
        pageData[0] = 0xEE;
        ulong txAId;
        using (var txA = engineA.BeginTransaction())
        {
            engineA.WritePage(7, txA.TransactionId, pageData);
            txAId = txA.TransactionId;
            await txA.CommitAsync();
        }

        // Engine B opens AFTER the WAL crypto header has been written by A, so its
        // WriteAheadLog instance can derive the decryption key from the file header.
        using var engineB = new StorageEngine(dbPath, cfgB);

        // Engine B begins a new transaction — Phase 7 replay decrypts and ingests A's commit.
        using var txB = engineB.BeginTransaction();

        var buf = new byte[engineB.PageSize];
        engineB.ReadPage(7, txAId, buf.AsSpan());
        Assert.Equal(0xEE, buf[0]);
    }
}
