using System.Collections.Concurrent;
using System.Runtime.InteropServices;

namespace BLite.Core.Transactions;

/// <summary>
/// Native interop for byte-range file locks used as the cross-process WAL writer lock
/// on all platforms:
/// <list type="bullet">
/// <item><description><b>Windows:</b> <c>LockFileEx</c> / <c>UnlockFileEx</c> — file-handle locks,
/// thread-agnostic (can be released from any thread), auto-released by the OS when the
/// owning process exits or its file handle is closed. A named <c>Mutex</c> is intentionally
/// <em>not</em> used because Windows <c>Mutex</c> is thread-owned (only the acquiring thread
/// may call <c>ReleaseMutex</c>) which is incompatible with <c>async/await</c> continuations
/// that may resume on a different thread pool thread.</description></item>
/// <item><description><b>Linux:</b> <c>fcntl(F_OFD_SETLK)</c> — open-file-description locks,
/// owned by the file description rather than the process/thread, auto-released on close.</description></item>
/// <item><description><b>macOS/iOS:</b> <c>fcntl(F_SETLK)</c> — traditional POSIX advisory
/// locks (per-process at the kernel level). An in-process companion <c>SemaphoreSlim</c> keyed
/// by the SHM path provides intra-process mutual exclusion because <c>F_SETLK</c> does not
/// distinguish between handles within the same process.</description></item>
/// </list>
/// <para>
/// All platforms also use the in-process <c>SemaphoreSlim</c> companion lock so that two
/// <see cref="WalSharedMemory"/> instances opened on the same file within a single process
/// properly exclude each other.
/// </para>
/// <para>
/// Implemented with <see cref="DllImportAttribute"/> rather than the source-generated
/// <c>LibraryImport</c> so this file compiles for both <c>net10.0</c> and
/// <c>netstandard2.1</c> targets.
/// </para>
/// </summary>
internal static class WalShmFcntl
{
    // ── fcntl command numbers ────────────────────────────────────────────────
    // F_OFD_SETLK is a Linux-specific extension that creates "open file description"
    // locks — owned by the open file rather than the process — which is the modern
    // equivalent of SQLite's WAL coordination on Unix. macOS/iOS do not implement
    // F_OFD_*; we fall back to traditional POSIX advisory locks (F_SETLK) which are
    // *per-process*, so two engines in the same process would not see each other's
    // lock at the kernel level. We compensate for that with an in-process,
    // per-SHM-path lock (s_localLocksByPath) acquired before the OS call — see
    // TryAcquireWriteLock for details.
    private const int F_OFD_SETLK_LINUX  = 37;

    private const int F_SETLK_MACOS      = 8;

    // ── errno values (cross-platform) ────────────────────────────────────────
    // Linux & macOS agree on the values for the codes we care about: EAGAIN/EWOULDBLOCK
    // and EACCES are the only "non-fatal, retry" returns from F_SETLK. Anything else
    // (EBADF=9, EINVAL=22, EFAULT=14, EDEADLK=35/11, EOVERFLOW=…) is a real bug we
    // surface as IOException rather than silently retrying until the timeout fires.
    private const int EACCES_VAL    = 13;
    private const int EAGAIN_LINUX  = 11;   // EAGAIN == EWOULDBLOCK on Linux
    private const int EAGAIN_MACOS  = 35;   // EAGAIN == EWOULDBLOCK on macOS/Darwin

    // ── Lock types (l_type field of struct flock) ────────────────────────────
    private const short F_WRLCK = 1;
    private const short F_UNLCK = 2;

    private const short SEEK_SET = 0;

    // ── Windows LockFileEx flags ─────────────────────────────────────────────
    private const uint LOCKFILE_EXCLUSIVE_LOCK   = 0x00000002u;
    private const uint LOCKFILE_FAIL_IMMEDIATELY = 0x00000001u;

    // We lock a single byte at a fixed offset that does not overlap any real SHM data.
    // POSIX/Linux allow locking bytes past EOF; LockFileEx on Windows also permits this.
    private const long WriterLockByteOffset = 1L << 30; // 1 GiB

    // In-process, per-SHM-path coordination. On macOS/iOS the kernel-level F_SETLK is
    // per-process, so two WalSharedMemory instances in the *same* process backed by
    // the same SHM file would not exclude each other via fcntl alone. On Windows,
    // LockFileEx is similarly per-handle but within-process locking behaviour can vary
    // by Windows version. We always acquire this in-process lock first on all platforms
    // for uniform semantics and guaranteed intra-process exclusion.
    private static readonly ConcurrentDictionary<string, SemaphoreSlim> s_localLocksByPath
        = new(StringComparer.Ordinal);

    private static SemaphoreSlim GetLocalLock(FileStream shmFile)
    {
        // Use the absolute, normalised path as the key. Two FileStream instances open
        // on the same file — even via different relative paths — must hash to the
        // same SemaphoreSlim.
        var key = System.IO.Path.GetFullPath(shmFile.Name);
        return s_localLocksByPath.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
    }

    // ── Unix structs / imports ────────────────────────────────────────────────

    // struct flock layout differs between Linux glibc and macOS libc — both are covered below.

    [StructLayout(LayoutKind.Sequential)]
    private struct flock_linux
    {
        public short l_type;
        public short l_whence;
        public long  l_start;
        public long  l_len;
        public int   l_pid;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct flock_macos
    {
        public long  l_start;
        public long  l_len;
        public int   l_pid;
        public short l_type;
        public short l_whence;
    }

    [DllImport("libc", EntryPoint = "fcntl", SetLastError = true)]
    private static extern int fcntl_linux(int fd, int cmd, ref flock_linux arg);

    [DllImport("libc", EntryPoint = "fcntl", SetLastError = true)]
    private static extern int fcntl_macos(int fd, int cmd, ref flock_macos arg);

    [DllImport("libc", EntryPoint = "kill", SetLastError = true)]
    public static extern int Kill(int pid, int sig);

    // ── Windows LockFileEx structs / imports ─────────────────────────────────

    // OVERLAPPED structure used by LockFileEx/UnlockFileEx. We only use the offset
    // fields (OffsetLow/OffsetHigh) to specify which byte to lock; hEvent is null so
    // LockFileEx blocks (or returns immediately with LOCKFILE_FAIL_IMMEDIATELY).
    [StructLayout(LayoutKind.Sequential)]
    private struct OVERLAPPED
    {
        public UIntPtr Internal;
        public UIntPtr InternalHigh;
        public uint    OffsetLow;
        public uint    OffsetHigh;
        public IntPtr  hEvent;
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool LockFileEx(
        IntPtr hFile,
        uint   dwFlags,
        uint   dwReserved,
        uint   nNumberOfBytesToLockLow,
        uint   nNumberOfBytesToLockHigh,
        ref    OVERLAPPED lpOverlapped);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool UnlockFileEx(
        IntPtr hFile,
        uint   dwReserved,
        uint   nNumberOfBytesToLockLow,
        uint   nNumberOfBytesToLockHigh,
        ref    OVERLAPPED lpOverlapped);

    // ── Public API ───────────────────────────────────────────────────────────

    /// <summary>
    /// Acquires an exclusive byte-range lock on the SHM backing file with exponential
    /// back-off until the lock is acquired or <paramref name="timeoutMs"/> elapses.
    /// Returns <c>true</c> on success, <c>false</c> on timeout.
    /// <para>
    /// On Windows: uses <c>LockFileEx</c> (thread-agnostic, auto-released on process
    /// death). On Linux: <c>fcntl(F_OFD_SETLK)</c>. On macOS: <c>fcntl(F_SETLK)</c>.
    /// All platforms additionally hold an in-process <c>SemaphoreSlim</c> to handle
    /// same-process multi-instance exclusion.
    /// </para>
    /// <para>
    /// Throws <see cref="IOException"/> if the underlying syscall returns an unexpected
    /// error code — that indicates a programming bug (bad fd/handle) rather than lock
    /// contention, and silently retrying would hide it as a bogus timeout.
    /// </para>
    /// </summary>
    public static bool TryAcquireWriteLock(FileStream shmFile, int timeoutMs)
    {
        // Take the in-process lock first so two engines in the same process can't both
        // succeed at the OS-level lock. Mandatory pair with the release in ReleaseWriteLock.
        var localLock = GetLocalLock(shmFile);
        if (!localLock.Wait(timeoutMs <= 0 ? 0 : timeoutMs))
            return false;

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            var deadline = timeoutMs <= 0
                ? DateTime.UtcNow
                : DateTime.UtcNow.AddMilliseconds(timeoutMs);
            int sleepMs = 1;
            while (true)
            {
                if (TryLockFileEx(shmFile)) return true;
                if (DateTime.UtcNow >= deadline)
                {
                    localLock.Release();
                    return false;
                }
                Thread.Sleep(sleepMs);
                if (sleepMs < 16) sleepMs *= 2;
            }
        }
        else
        {
            // On Unix, SafeFileHandle wraps a raw int file descriptor — ToInt32() is the
            // correct extraction. (It throws OverflowException on 64-bit values, which
            // can never happen for real fds.)
            int fd = shmFile.SafeFileHandle.DangerousGetHandle().ToInt32();

            var deadline = timeoutMs <= 0
                ? DateTime.UtcNow                                     // single-shot try
                : DateTime.UtcNow.AddMilliseconds(timeoutMs);

            int sleepMs = 1;
            while (true)
            {
                int errno;
                if (TrySetLock(fd, F_WRLCK, out errno)) return true;
                if (!IsLockContentionErrno(errno))
                {
                    // Real failure (EBADF, EINVAL, etc.) — release our in-process lock and
                    // surface the error rather than silently spinning until the timeout.
                    localLock.Release();
                    throw new IOException(
                        $"fcntl(F_SETLK, F_WRLCK) failed with errno={errno} on '{shmFile.Name}'.");
                }
                if (DateTime.UtcNow >= deadline)
                {
                    localLock.Release();
                    return false;
                }
                Thread.Sleep(sleepMs);
                if (sleepMs < 16) sleepMs *= 2; // exponential back-off, capped at 16 ms
            }
        }
    }

    /// <summary>Releases the writer lock previously acquired via <see cref="TryAcquireWriteLock"/>.</summary>
    public static void ReleaseWriteLock(FileStream shmFile)
    {
        try
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                // Unlock failures are best-effort; ignore them to avoid masking primary errors.
                TryUnlockFileEx(shmFile);
            }
            else
            {
                int fd = shmFile.SafeFileHandle.DangerousGetHandle().ToInt32();
                // Unlock failures here are best-effort; surfacing them would mask the
                // primary error path (e.g. dispose during shutdown).
                TrySetLock(fd, F_UNLCK, out _);
            }
        }
        finally
        {
            // Always release the in-process companion lock, even if the OS-level
            // unlock above threw.
            GetLocalLock(shmFile).Release();
        }
    }

    // ── Windows helpers ──────────────────────────────────────────────────────

    private static bool TryLockFileEx(FileStream shmFile)
    {
        var ov = new OVERLAPPED
        {
            OffsetLow  = (uint)(WriterLockByteOffset & 0xFFFFFFFF),
            OffsetHigh = (uint)(WriterLockByteOffset >> 32),
        };
        // LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY → non-blocking try for one byte.
        return LockFileEx(
            shmFile.SafeFileHandle.DangerousGetHandle(),
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            dwReserved: 0,
            nNumberOfBytesToLockLow: 1,
            nNumberOfBytesToLockHigh: 0,
            ref ov);
    }

    private static void TryUnlockFileEx(FileStream shmFile)
    {
        var ov = new OVERLAPPED
        {
            OffsetLow  = (uint)(WriterLockByteOffset & 0xFFFFFFFF),
            OffsetHigh = (uint)(WriterLockByteOffset >> 32),
        };
        UnlockFileEx(
            shmFile.SafeFileHandle.DangerousGetHandle(),
            dwReserved: 0,
            nNumberOfBytesToLockLow: 1,
            nNumberOfBytesToLockHigh: 0,
            ref ov);
    }

    // ── Unix helpers ─────────────────────────────────────────────────────────

    private static bool IsLockContentionErrno(int errno)
    {
        return errno == EACCES_VAL
            || errno == EAGAIN_LINUX
            || errno == EAGAIN_MACOS;
    }

    private static bool TrySetLock(int fd, short lockType, out int errno)
    {
        // Use RuntimeInformation rather than OperatingSystem.* so this file compiles
        // for both net10.0 and netstandard2.1 target frameworks.
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            var fl = new flock_macos
            {
                l_type = lockType,
                l_whence = SEEK_SET,
                l_start = WriterLockByteOffset,
                l_len = 1,
                l_pid = 0,
            };
            int rc = fcntl_macos(fd, F_SETLK_MACOS, ref fl);
            errno = rc == 0 ? 0 : Marshal.GetLastWin32Error();
            return rc == 0;
        }
        else
        {
            // Linux + Android — F_OFD_SETLK
            var fl = new flock_linux
            {
                l_type = lockType,
                l_whence = SEEK_SET,
                l_start = WriterLockByteOffset,
                l_len = 1,
                l_pid = 0,
            };
            int rc = fcntl_linux(fd, F_OFD_SETLK_LINUX, ref fl);
            errno = rc == 0 ? 0 : Marshal.GetLastWin32Error();
            return rc == 0;
        }
    }
}
