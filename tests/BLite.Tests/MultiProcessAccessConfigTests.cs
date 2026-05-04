using System.Reflection;
using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace BLite.Tests;

/// <summary>
/// Phase 0 of the multi-process WAL plan (roadmap/v5/MULTI_PROCESS_WAL.md):
/// the opt-in <see cref="PageFileConfig.AllowMultiProcessAccess"/> flag is wired through
/// to <see cref="WriteAheadLog"/> but has no observable runtime effect yet.
/// These tests pin the public configuration surface so subsequent phases can rely on it.
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

            var field = typeof(WriteAheadLog).GetField("_allowMultiProcessAccess",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);
            Assert.True((bool)field!.GetValue(wal)!);
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

            var field = typeof(WriteAheadLog).GetField("_allowMultiProcessAccess",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);
            Assert.False((bool)field!.GetValue(wal)!);
        }
        finally
        {
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }
}
