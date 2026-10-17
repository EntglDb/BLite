using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Settings for tests whose subject is correctness under concurrency, not lock timeouts.
/// <para>
/// <see cref="LockTimeout.Default"/> gives a writer 500 ms to take the file-wide write lock or the
/// commit lock. On a loaded CI runner (xunit runs test classes in parallel on a few cores, and
/// other classes block threads) a writer regularly waits longer than that for a commit that is
/// itself waiting for the disk, and the test fails with a <see cref="TimeoutException"/> that
/// says nothing about what it checks. These tests open their database with a timeout no loaded
/// runner reaches, so a timeout can only mean a real deadlock.
/// </para>
/// </summary>
internal static class LoadTolerant
{
    /// <summary>Lock timeouts a loaded runner does not reach; only a deadlock does.</summary>
    public static readonly LockTimeout LockTimeout = LockTimeout.From(readTimeoutMs: 30_000, writeTimeoutMs: 30_000);

    /// <summary><see cref="PageFileConfig.Default"/> with <see cref="LockTimeout"/>.</summary>
    public static PageFileConfig SingleFile => PageFileConfig.Default with { LockTimeout = LockTimeout };

    /// <summary><see cref="PageFileConfig.Server(string, PageFileConfig?)"/> with <see cref="LockTimeout"/>.</summary>
    public static PageFileConfig Server(string databasePath) => PageFileConfig.Server(databasePath, SingleFile);

    /// <summary>
    /// Polls <paramref name="condition"/> until it holds, instead of sleeping a fixed time and
    /// hoping the background work is done by then. Fails with <paramref name="what"/> after
    /// <paramref name="timeoutMs"/>.
    /// </summary>
    public static async Task WaitUntilAsync(Func<bool> condition, string what, int timeoutMs = 10_000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (!condition())
        {
            if (Environment.TickCount64 >= deadline)
                Assert.Fail($"Timed out after {timeoutMs} ms waiting until {what}");
            await Task.Delay(5);
        }
    }
}
