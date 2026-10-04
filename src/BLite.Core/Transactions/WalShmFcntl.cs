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
/// <item><description><b>macOS/iOS:</b> <c>flock(LOCK_EX | LOCK_NB)</c> on a <em>dedicated
/// lock file</em> (<c>&lt;shm&gt;.lock</c>) opened with a raw <c>open()</c> descriptor for
/// the duration of the lock. The lock lives on its own open file description, so it is
/// released by closing that descriptor (or by process exit) and excludes other processes and
/// other instances in the same process. It must not be taken on the <see cref="FileStream"/>
/// of the SHM file itself: .NET's Unix <see cref="FileStream"/> already holds a shared
/// <c>flock(LOCK_SH)</c> on every file it opens with <see cref="FileShare.ReadWrite"/>, so
/// two processes could never upgrade to <c>LOCK_EX</c> (both writers time out) and an
/// exclusive lock would make other <c>FileStream</c> opens of the file fail. Other options
/// are ruled out too: <see cref="FileStream.Lock(long, long)"/> throws
/// <see cref="PlatformNotSupportedException"/> on macOS, and a direct <c>fcntl</c> P/Invoke
/// is unreliable there (variadic ABI on arm64, different <c>struct flock</c> layout).</description></item>
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
    // ── fcntl command numbers (Linux) ────────────────────────────────────────
    // F_OFD_SETLK is a Linux-specific extension that creates "open file description"
    // locks — owned by the open file rather than the process — which is the modern
    // equivalent of SQLite's WAL coordination on Unix.
    private const int F_OFD_SETLK_LINUX  = 37;

    // ── flock() operation flags (macOS/iOS) ──────────────────────────────────
    // See remarks on the class for why macOS flocks a dedicated lock file.
    private const int LOCK_EX_MACOS = 0x02;
    private const int LOCK_NB_MACOS = 0x04;
    private const int LOCK_UN_MACOS = 0x08;
    private const int O_RDONLY_MACOS = 0x0000; // flock() needs no write access
    private const int O_CLOEXEC_MACOS = 0x01000000;

    // Lock descriptor held while the macOS writer lock is taken, keyed by SHM path.
    // At most one entry per path: the in-process semaphore serialises holders.
    private static readonly ConcurrentDictionary<string, int> s_macLockFds
        = new(StringComparer.Ordinal);

    // ── errno values (cross-platform) ────────────────────────────────────────
    // Linux & macOS agree on the values for the codes we care about: EAGAIN/EWOULDBLOCK
    // and EACCES are the only "non-fatal, retry" returns from a contended lock attempt.
    // Anything else (EBADF=9, EINVAL=22, EFAULT=14, EDEADLK=35/11, EOVERFLOW=…) is a
    // real bug we surface as IOException rather than silently retrying until the
    // timeout fires.
    private const int EACCES_VAL    = 13;
    private const int EAGAIN_LINUX  = 11;   // EAGAIN == EWOULDBLOCK on Linux
    private const int EAGAIN_MACOS  = 35;   // EAGAIN == EWOULDBLOCK on macOS/Darwin

    // ── Lock type (l_type field of struct flock, Linux only) ─────────────────
    private const short F_WRLCK_LINUX = 1;
    private const short F_UNLCK_LINUX = 2;

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

    [StructLayout(LayoutKind.Sequential)]
    private struct flock_linux
    {
        public short l_type;
        public short l_whence;
        public long  l_start;
        public long  l_len;
        public int   l_pid;
    }

    [DllImport("libc", EntryPoint = "fcntl", SetLastError = true)]
    private static extern int fcntl_linux(int fd, int cmd, ref flock_linux arg);

    [DllImport("libc", EntryPoint = "flock", SetLastError = true)]
    private static extern int flock_macos(int fd, int operation);

    // open(path, flags) — the optional mode argument is only read with O_CREAT, which we
    // never pass (the lock file is created beforehand through FileStream), so declaring the
    // variadic function with two arguments is safe.
    [DllImport("libc", EntryPoint = "open", SetLastError = true)]
    private static extern int open_macos([MarshalAs(UnmanagedType.LPUTF8Str)] string path, int flags);

    [DllImport("libc", EntryPoint = "close", SetLastError = true)]
    private static extern int close_macos(int fd);

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
    /// death). On Linux: <c>fcntl(F_OFD_SETLK)</c>. On macOS: <c>flock</c> on a dedicated lock file.
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
                bool locked;
                try
                {
                    locked = TrySetLock(shmFile, fd, write: true, out errno);
                }
                catch
                {
                    // E.g. the lock file cannot be created: never leave the in-process lock held.
                    localLock.Release();
                    throw;
                }
                if (locked) return true;
                if (!IsLockContentionErrno(errno))
                {
                    // Real failure (EBADF, EINVAL, etc.) — release our in-process lock and
                    // surface the error rather than silently spinning until the timeout.
                    localLock.Release();
                    throw new IOException(
                        $"Acquiring the writer lock failed with errno={errno} on '{shmFile.Name}'.");
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
                TrySetLock(shmFile, fd, write: false, out _);
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

    private static bool TrySetLock(FileStream shmFile, int fd, bool write, out int errno)
    {
        // Use RuntimeInformation rather than OperatingSystem.* so this file compiles
        // for both net10.0 and netstandard2.1 target frameworks.
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            var key = System.IO.Path.GetFullPath(shmFile.Name);
            if (!write)
            {
                if (s_macLockFds.TryRemove(key, out var heldFd))
                {
                    // Closing the descriptor drops the flock; LOCK_UN first is belt and braces.
                    flock_macos(heldFd, LOCK_UN_MACOS);
                    close_macos(heldFd);
                }
                errno = 0;
                return true;
            }

            var lockPath = key + ".lock";
            if (!File.Exists(lockPath))
            {
                // Create through FileStream (raw open() would need the variadic mode argument).
                // Another process may be creating/locking the file at the same moment, in which
                // case .NET's own flock(LOCK_SH) fails with an IOException: treat it as
                // contention and retry.
                try
                {
                    using var created = new FileStream(lockPath, FileMode.OpenOrCreate,
                        FileAccess.ReadWrite, FileShare.ReadWrite);
                }
                catch (IOException)
                {
                    errno = EAGAIN_MACOS;
                    return false;
                }
            }

            int lfd = open_macos(lockPath, O_RDONLY_MACOS | O_CLOEXEC_MACOS);
            if (lfd < 0)
            {
                errno = Marshal.GetLastWin32Error();
                return false;
            }
            if (flock_macos(lfd, LOCK_EX_MACOS | LOCK_NB_MACOS) != 0)
            {
                errno = Marshal.GetLastWin32Error();
                close_macos(lfd);
                return false;
            }
            s_macLockFds[key] = lfd;
            errno = 0;
            return true;
        }
        else
        {
            // Linux + Android — F_OFD_SETLK
            var fl = new flock_linux
            {
                l_type = write ? F_WRLCK_LINUX : F_UNLCK_LINUX,
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
