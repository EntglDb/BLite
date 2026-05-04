using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace BLite.Tests;

/// <summary>
/// Pins the public configuration surface for the multi-process WAL feature
/// (<c>roadmap/v5/MULTI_PROCESS_WAL.md</c>): the <see cref="PageFileConfig.AllowMultiProcessAccess"/>
/// flag itself, default values across the canonical configs, and that the flag is
/// forwarded into the <see cref="WriteAheadLog"/> owned by the <see cref="StorageEngine"/>.
/// <para>
/// Behavioural side effects of the flag (the <c>.wal-shm</c> sidecar, the
/// <see cref="FileShare.ReadWrite"/> relaxation, two engines on the same files) are
/// covered by <see cref="MultiProcessWalSharedMemoryTests"/>; this file only validates
/// the configuration plumbing.
/// </para>
/// </summary>
public class MultiProcessAccessConfigTests
{
    [Fact]
    public void PageFileConfig_AllowMultiProcessAccess_DefaultsToFalse()
    {
        Assert.False(default(PageFileConfig).AllowMultiProcessAccess);
        Assert.False(PageFileConfig.Default.AllowMultiProcessAccess);
        Assert.False(PageFileConfig.Small.AllowMultiProcessAccess);
        Assert.False(PageFileConfig.Large.AllowMultiProcessAccess);
    }

    [Fact]
    public void PageFileConfig_AllowMultiProcessAccess_CanBeSetViaWithExpression()
    {
        var cfg = PageFileConfig.Default with { AllowMultiProcessAccess = true };

        Assert.True(cfg.AllowMultiProcessAccess);
        // Other fields must remain untouched.
        Assert.Equal(PageFileConfig.Default.PageSize, cfg.PageSize);
        Assert.Equal(PageFileConfig.Default.GrowthBlockSize, cfg.GrowthBlockSize);
    }

    [Fact]
    public void WriteAheadLog_StoresAllowMultiProcessAccessFlag()
    {
        var walPath = Path.Combine(Path.GetTempPath(), $"test_mpwal_{Guid.NewGuid()}.wal");
        try
        {
            using var wal = new WriteAheadLog(walPath, crypto: null, writeTimeoutMs: 1_000, allowMultiProcessAccess: true);
            Assert.True(wal.AllowMultiProcessAccess);
        }
        finally
        {
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public void WriteAheadLog_DefaultsAllowMultiProcessAccessToFalse()
    {
        var walPath = Path.Combine(Path.GetTempPath(), $"test_mpwal_{Guid.NewGuid()}.wal");
        try
        {
            using var wal = new WriteAheadLog(walPath, crypto: null);
            Assert.False(wal.AllowMultiProcessAccess);
        }
        finally
        {
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    /// <summary>
    /// Behavioural assertion of the forwarding path that the previous review
    /// flagged as missing: a <see cref="StorageEngine"/> built with the flag set
    /// must produce a WAL that observably opens its file with <see cref="FileShare.ReadWrite"/>,
    /// which we verify here by opening the same WAL path a second time
    /// concurrently — an operation that fails with <see cref="IOException"/>
    /// in single-process mode but succeeds in multi-process mode.
    /// </summary>
    [Fact]
    public void StorageEngine_ForwardsAllowMultiProcessAccess_ToOwnedWal()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"blite_mpcfg_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        try
        {
            var dbPath = Path.Combine(dir, "fwd.db");
            var cfg = PageFileConfig.Default with { AllowMultiProcessAccess = true };

            using var engine = new StorageEngine(dbPath, cfg);

            // The owned SHM sidecar is the externally observable evidence that the
            // flag reached every layer (PageFile + WriteAheadLog + StorageEngine).
            Assert.NotNull(engine.SharedMemory);

            // And the WAL file must be openable by a second handle (FileShare.ReadWrite).
            var walPath = Path.ChangeExtension(dbPath, ".wal");
            Assert.True(File.Exists(walPath));
            using var probe = new FileStream(walPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            Assert.True(probe.CanRead);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort */ }
        }
    }
}
