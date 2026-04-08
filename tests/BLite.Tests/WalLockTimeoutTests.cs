using System.Reflection;
using BLite.Core.Transactions;
using Xunit.Abstractions;

namespace BLite.Tests;

public class WalLockTimeoutTests : IDisposable
{
    private readonly string _walPath;
    private readonly ITestOutputHelper _output;

    /// <summary>
    /// Small WAL write timeout used in all tests so they complete quickly instead of waiting
    /// the production default of 5 s per operation.
    /// </summary>
    private const int TestWriteTimeoutMs = 100;

    public WalLockTimeoutTests(ITestOutputHelper output)
    {
        _output = output;
        _walPath = Path.Combine(Path.GetTempPath(), $"test_wal_timeout_{Guid.NewGuid()}.wal");
    }

    /// <summary>
    /// Grabs the private _lock SemaphoreSlim from a WriteAheadLog instance via reflection.
    /// </summary>
    private static SemaphoreSlim GetInternalLock(WriteAheadLog wal)
    {
        var field = typeof(WriteAheadLog)
            .GetField("_lock", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException("Could not find _lock field");
        return (SemaphoreSlim)field.GetValue(wal)!;
    }

    [Fact]
    public async Task WriteBeginRecord_ThrowsTimeoutException_WhenLockIsHeld()
    {
        using var wal = new WriteAheadLog(_walPath, writeTimeoutMs: TestWriteTimeoutMs);
        var semaphore = GetInternalLock(wal);

        // Simulate a stuck writer holding the lock
        await semaphore.WaitAsync();
        try
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var ex = await Assert.ThrowsAsync<TimeoutException>(
                () => wal.WriteBeginRecordAsync(1).AsTask());
            sw.Stop();

            _output.WriteLine($"TimeoutException after {sw.ElapsedMilliseconds} ms: {ex.Message}");
            Assert.True(sw.ElapsedMilliseconds >= TestWriteTimeoutMs - 20,
                $"Should wait ~{TestWriteTimeoutMs} ms, was {sw.ElapsedMilliseconds} ms");
            Assert.True(sw.ElapsedMilliseconds < TestWriteTimeoutMs * 5,
                $"Should not wait much longer than {TestWriteTimeoutMs} ms, was {sw.ElapsedMilliseconds} ms");
        }
        finally
        {
            semaphore.Release();
        }
    }

    [Fact]
    public async Task WriteCommitRecord_ThrowsTimeoutException_WhenLockIsHeld()
    {
        using var wal = new WriteAheadLog(_walPath, writeTimeoutMs: TestWriteTimeoutMs);
        var semaphore = GetInternalLock(wal);

        await semaphore.WaitAsync();
        try
        {
            var ex = await Assert.ThrowsAsync<TimeoutException>(
                () => wal.WriteCommitRecordAsync(1).AsTask());

            _output.WriteLine($"TimeoutException: {ex.Message}");
        }
        finally
        {
            semaphore.Release();
        }
    }

    [Fact]
    public async Task WriteDataRecord_ThrowsTimeoutException_WhenLockIsHeld()
    {
        using var wal = new WriteAheadLog(_walPath, writeTimeoutMs: TestWriteTimeoutMs);
        var semaphore = GetInternalLock(wal);

        await semaphore.WaitAsync();
        try
        {
            var data = new byte[64];
            var ex = await Assert.ThrowsAsync<TimeoutException>(
                () => wal.WriteDataRecordAsync(1, 42, data).AsTask());

            _output.WriteLine($"TimeoutException: {ex.Message}");
        }
        finally
        {
            semaphore.Release();
        }
    }

    [Fact]
    public async Task FlushAsync_ThrowsTimeoutException_WhenLockIsHeld()
    {
        using var wal = new WriteAheadLog(_walPath, writeTimeoutMs: TestWriteTimeoutMs);
        var semaphore = GetInternalLock(wal);

        await semaphore.WaitAsync();
        try
        {
            var ex = await Assert.ThrowsAsync<TimeoutException>(
                () => wal.FlushAsync());

            _output.WriteLine($"TimeoutException: {ex.Message}");
        }
        finally
        {
            semaphore.Release();
        }
    }

    [Fact]
    public void GetCurrentSize_ThrowsTimeoutException_WhenLockIsHeld()
    {
        using var wal = new WriteAheadLog(_walPath, writeTimeoutMs: TestWriteTimeoutMs);
        var semaphore = GetInternalLock(wal);

        semaphore.Wait();
        try
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var ex = Assert.Throws<TimeoutException>(() => wal.GetCurrentSize());
            sw.Stop();

            _output.WriteLine($"TimeoutException after {sw.ElapsedMilliseconds} ms: {ex.Message}");
            Assert.True(sw.ElapsedMilliseconds >= TestWriteTimeoutMs - 20,
                $"Should wait ~{TestWriteTimeoutMs} ms, was {sw.ElapsedMilliseconds} ms");
        }
        finally
        {
            semaphore.Release();
        }
    }

    [Fact]
    public void ReadAll_ThrowsTimeoutException_WhenLockIsHeld()
    {
        using var wal = new WriteAheadLog(_walPath, writeTimeoutMs: TestWriteTimeoutMs);
        var semaphore = GetInternalLock(wal);

        semaphore.Wait();
        try
        {
            var ex = Assert.Throws<TimeoutException>(() => wal.ReadAll());
            _output.WriteLine($"TimeoutException: {ex.Message}");
        }
        finally
        {
            semaphore.Release();
        }
    }

    [Fact]
    public async Task CancellationToken_AbortsBeforeTimeout()
    {
        using var wal = new WriteAheadLog(_walPath, writeTimeoutMs: TestWriteTimeoutMs);
        var semaphore = GetInternalLock(wal);

        await semaphore.WaitAsync();
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(10));

            var sw = System.Diagnostics.Stopwatch.StartNew();
            // Should throw OperationCanceledException well before the timeout
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => wal.WriteBeginRecordAsync(1, cts.Token).AsTask());
            sw.Stop();

            _output.WriteLine($"Cancelled after {sw.ElapsedMilliseconds} ms");
            Assert.True(sw.ElapsedMilliseconds < TestWriteTimeoutMs,
                $"Cancellation should fire before the timeout, was {sw.ElapsedMilliseconds} ms");
        }
        finally
        {
            semaphore.Release();
        }
    }

    public void Dispose()
    {
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch { }
    }
}
