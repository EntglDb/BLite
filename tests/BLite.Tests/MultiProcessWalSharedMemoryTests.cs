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
        // both engines share the SHM-backed counter.
        var ids = new HashSet<ulong>();
        for (int i = 0; i < 20; i++)
        {
            var ta = a.BeginTransaction();
            var tb = b.BeginTransaction();
            Assert.True(ids.Add(ta.TransactionId), $"duplicate tid {ta.TransactionId}");
            Assert.True(ids.Add(tb.TransactionId), $"duplicate tid {tb.TransactionId}");
        }
        Assert.Equal(40, ids.Count);
    }
}
