using System.Collections.Concurrent;
using System.Runtime.InteropServices;

namespace BLite.Core.Transactions;

/// <summary>
/// Native interop for <c>fcntl(F_OFD_SETLK)</c> / traditional <c>fcntl(F_SETLK)</c>
/// byte-range locks on Unix-like platforms (Linux, macOS, Android, iOS), plus
/// <c>kill(pid, 0)</c> for PID liveness checks. These primitives are auto-released by
/// the kernel when the owning process dies, which is what makes the cross-process
/// writer lock crash-safe.
/// <para>
/// On Windows this class is unused — see <see cref="WalSharedMemory"/> which uses a
/// named <see cref="System.Threading.Mutex"/> in the <c>Local\</c> namespace instead.
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

    // We lock a single byte at a fixed offset that does not overlap any real SHM data.
    // POSIX/Linux allow locking bytes past EOF.
    private const long WriterLockByteOffset = 1L << 30; // 1 GiB

    // In-process, per-SHM-path coordination. On macOS/iOS the kernel-level F_SETLK is
    // per-process, so two WalSharedMemory instances in the *same* process backed by
    // the same SHM file would not exclude each other via fcntl alone. This dictionary
    // holds one SemaphoreSlim per absolute SHM path; every TryAcquireWriteLock /
    // ReleaseWriteLock pair acquires/releases this in addition to the OS call.
    // On Linux F_OFD_SETLK already provides correct intra-process exclusion, but the
    // extra in-process lock is cheap (~uncontended SemaphoreSlim) and harmless, so
    // we use it on every Unix-like platform for uniform semantics.
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

    /// <summary>
    /// Polls <c>fcntl(F_OFD_SETLK)</c> (Linux) / <c>fcntl(F_SETLK)</c> (macOS) with
    /// exponential back-off until the lock is acquired or <paramref name="timeoutMs"/>
    /// elapses. Returns <c>true</c> on success, <c>false</c> on timeout.
    /// <para>
    /// Throws <see cref="IOException"/> if <c>fcntl</c> returns an unexpected errno
    /// (i.e. anything other than <c>EACCES</c> / <c>EAGAIN</c>) — those indicate a
    /// programming bug (bad fd, bad struct) rather than lock contention, and silently
    /// retrying would just hide them as bogus timeouts.
    /// </para>
    /// </summary>
    public static bool TryAcquireWriteLock(FileStream shmFile, int timeoutMs)
    {
        // Take the in-process lock first so two engines in the same process can't both
        // succeed at the per-process F_SETLK on macOS. Mandatory pair with the release
        // in ReleaseWriteLock.
        var localLock = GetLocalLock(shmFile);
        if (!localLock.Wait(timeoutMs <= 0 ? 0 : timeoutMs))
            return false;

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

    /// <summary>Releases the writer lock previously acquired via <see cref="TryAcquireWriteLock"/>.</summary>
    public static void ReleaseWriteLock(FileStream shmFile)
    {
        try
        {
            int fd = shmFile.SafeFileHandle.DangerousGetHandle().ToInt32();
            // Unlock failures here are best-effort; surfacing them would mask the
            // primary error path (e.g. dispose during shutdown).
            TrySetLock(fd, F_UNLCK, out _);
        }
        finally
        {
            // Always release the in-process companion lock, even if the OS-level
            // unlock above threw.
            GetLocalLock(shmFile).Release();
        }
    }

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
