using System;

namespace BLite.Core.Storage;

/// <summary>
/// Configures lock acquisition timeouts for database operations.
/// <para>
/// When a lock cannot be acquired within the specified timeout, a
/// <see cref="TimeoutException"/> is thrown — analogous to SQLite's
/// <c>SQLITE_BUSY</c> result code.
/// </para>
/// <para>
/// Use <see cref="Immediate"/> for fail-fast behaviour (SQLite's default when
/// <c>busy_timeout</c> is 0) or specify custom values to give concurrent callers
/// time to release contended locks.
/// </para>
/// <para>
/// <c>default(LockTimeout)</c>, or <c>new LockTimeout()</c> with no property set, means
/// "not specified": <see cref="PageFileConfig.Normalize"/> replaces it with
/// <see cref="Default"/>. To fail fast, set <see cref="Immediate"/> explicitly.
/// </para>
/// </summary>
public readonly struct LockTimeout : IEquatable<LockTimeout>
{
    /// <summary>
    /// Timeout for acquiring read locks (milliseconds).
    /// <list type="bullet">
    ///   <item><c>0</c> — immediate failure if the lock is not available (SQLITE_BUSY).</item>
    ///   <item><c>-1</c> — wait indefinitely.</item>
    ///   <item><c>&gt;0</c> — wait up to this many milliseconds.</item>
    /// </list>
    /// </summary>
    public int ReadTimeoutMs { get => _readTimeoutMs; init { _readTimeoutMs = value; _isSpecified = true; } }

    /// <summary>
    /// Timeout for acquiring write locks (milliseconds).
    /// <list type="bullet">
    ///   <item><c>0</c> — immediate failure if the lock is not available (SQLITE_BUSY).</item>
    ///   <item><c>-1</c> — wait indefinitely.</item>
    ///   <item><c>&gt;0</c> — wait up to this many milliseconds.</item>
    /// </list>
    /// </summary>
    public int WriteTimeoutMs { get => _writeTimeoutMs; init { _writeTimeoutMs = value; _isSpecified = true; } }

    /// <summary>
    /// Maximum number of threads that can simultaneously enter the commit path.
    /// <para>
    /// Acts as an admission gate: when more than <see cref="MaxConcurrentWriters"/>
    /// threads attempt to commit at the same time, the excess threads wait on the
    /// gate semaphore (cheap, low-contention) instead of piling up on the internal
    /// WAL/commit locks where deep queues cause latency spikes and timeouts.
    /// </para>
    /// <list type="bullet">
    ///   <item><c>0</c> — no admission control (unlimited).</item>
    ///   <item><c>&gt;0</c> — at most this many concurrent commit operations.</item>
    /// </list>
    /// </summary>
    public int MaxConcurrentWriters { get => _maxConcurrentWriters; init { _maxConcurrentWriters = value; _isSpecified = true; } }

    private readonly int _readTimeoutMs;
    private readonly int _writeTimeoutMs;
    private readonly int _maxConcurrentWriters;
    private readonly bool _isSpecified;

    /// <summary>
    /// <c>false</c> for <c>default(LockTimeout)</c>: no property was set, so the value is
    /// indistinguishable from <see cref="Immediate"/> but was not chosen. Not part of equality.
    /// </summary>
    internal bool IsSpecified => _isSpecified;

    /// <summary>
    /// Immediate failure on contention — equivalent to SQLite's default
    /// (<c>busy_timeout = 0</c>, returns <c>SQLITE_BUSY</c>).
    /// Suitable when the caller implements its own retry/back-off logic.
    /// </summary>
    public static LockTimeout Immediate => new() { ReadTimeoutMs = 0, WriteTimeoutMs = 0, MaxConcurrentWriters = 0 };

    /// <summary>
    /// Balanced default: 500 ms write timeout, 500 ms read timeout,
    /// 96 max concurrent writers.
    /// <para>
    /// 500 ms is long enough for commits under normal load (observed p99 well under 500 ms)
    /// but short enough to fail-fast when the engine is saturated rather than leaving
    /// threads blocked for seconds, starving the thread-pool.
    /// </para>
    /// </summary>
    public static LockTimeout Default => new() { ReadTimeoutMs = 500, WriteTimeoutMs = 500, MaxConcurrentWriters = 96 };

    /// <summary>
    /// Creates a <see cref="LockTimeout"/> from explicit millisecond values.
    /// </summary>
    public static LockTimeout From(int readTimeoutMs, int writeTimeoutMs, int maxConcurrentWriters = 96) =>
        new() { ReadTimeoutMs = readTimeoutMs, WriteTimeoutMs = writeTimeoutMs, MaxConcurrentWriters = maxConcurrentWriters };

    public bool Equals(LockTimeout other) =>
        ReadTimeoutMs == other.ReadTimeoutMs && WriteTimeoutMs == other.WriteTimeoutMs && MaxConcurrentWriters == other.MaxConcurrentWriters;

    public override bool Equals(object? obj) => obj is LockTimeout other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(ReadTimeoutMs, WriteTimeoutMs, MaxConcurrentWriters);
    public static bool operator ==(LockTimeout left, LockTimeout right) => left.Equals(right);
    public static bool operator !=(LockTimeout left, LockTimeout right) => !left.Equals(right);

    public override string ToString() => $"LockTimeout(Read={ReadTimeoutMs}ms, Write={WriteTimeoutMs}ms, MaxWriters={MaxConcurrentWriters})";
}
