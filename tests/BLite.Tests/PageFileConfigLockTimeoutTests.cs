using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Regression test for #322: a <see cref="PageFileConfig"/> built with an object initializer
/// that does not set <see cref="PageFileConfig.LockTimeout"/> used to open with
/// <c>default(LockTimeout)</c>, which has the same values as <see cref="LockTimeout.Immediate"/>,
/// so every contended lock failed at once.
/// </summary>
public class PageFileConfigLockTimeoutTests
{
    [Fact]
    public void StorageEngine_PartialConfigWithoutLockTimeout_GetsDefault_ExplicitImmediateIsKept()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"blite_locktimeout_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        try
        {
            using (var engine = new StorageEngine(Path.Combine(dir, "partial.db"), new PageFileConfig { AllowMultiProcessAccess = false }))
                Assert.Equal(LockTimeout.Default, engine.LockTimeout);

            using (var engine = new StorageEngine(Path.Combine(dir, "immediate.db"), new PageFileConfig { LockTimeout = LockTimeout.Immediate }))
                Assert.Equal(LockTimeout.Immediate, engine.LockTimeout);

            using (var engine = new StorageEngine(Path.Combine(dir, "from.db"), new PageFileConfig { LockTimeout = LockTimeout.From(0, 0, 0) }))
                Assert.Equal(LockTimeout.Immediate, engine.LockTimeout);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort */ }
        }
    }
}
