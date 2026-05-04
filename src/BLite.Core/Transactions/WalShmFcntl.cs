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
    // F_OFD_*; we fall back to traditional POSIX advisory locks (F_SETLK) there,
    // which are per-process — adequate for cross-process serialisation but not safe
    // within the same process. BLite's in-process _commitLock prevents intra-process
    // contention, so this is acceptable.
    private const int F_OFD_SETLK_LINUX  = 37;

    private const int F_SETLK_MACOS      = 8;

    // ── Lock types (l_type field of struct flock) ────────────────────────────
    private const short F_WRLCK = 1;
    private const short F_UNLCK = 2;

    private const short SEEK_SET = 0;

    // We lock a single byte at a fixed offset that does not overlap any real SHM data.
    // POSIX/Linux allow locking bytes past EOF.
    private const long WriterLockByteOffset = 1L << 30; // 1 GiB

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
    /// </summary>
    public static bool TryAcquireWriteLock(FileStream shmFile, int timeoutMs)
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
            if (TrySetLock(fd, F_WRLCK)) return true;
            if (DateTime.UtcNow >= deadline) return false;
            Thread.Sleep(sleepMs);
            if (sleepMs < 16) sleepMs *= 2; // exponential back-off, capped at 16 ms
        }
    }

    /// <summary>Releases the writer lock previously acquired via <see cref="TryAcquireWriteLock"/>.</summary>
    public static void ReleaseWriteLock(FileStream shmFile)
    {
        int fd = shmFile.SafeFileHandle.DangerousGetHandle().ToInt32();
        TrySetLock(fd, F_UNLCK);
    }

    private static bool TrySetLock(int fd, short lockType)
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
            return rc == 0;
        }
    }
}
