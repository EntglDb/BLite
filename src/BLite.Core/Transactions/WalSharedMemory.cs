using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;

namespace BLite.Core.Transactions;

/// <summary>
/// Shared-memory sidecar (<c>.wal-shm</c>) that coordinates multi-process WAL access
/// on the same host (N readers + 1 writer).
/// <para>
/// Implements the design described in <c>roadmap/v5/MULTI_PROCESS_WAL.md</c>:
/// a memory-mapped file storing only coordination metadata (atomic counters, writer
/// owner PID, reader-slot array). It contains <b>no page data</b> — if the file is
/// deleted or its magic header is invalid, any process opening the database recreates
/// it from scratch (the SHM is always reconstructible from the WAL).
/// </para>
/// <para>
/// Cross-process locking:
/// <list type="bullet">
/// <item><description><b>Windows:</b> a named <see cref="Mutex"/> (in the <c>Local\</c> namespace,
/// keyed by a hash of the WAL path). The OS auto-releases the mutex if the owner process dies.</description></item>
/// <item><description><b>Linux/macOS/Android/iOS:</b> <c>fcntl(F_OFD_SETLK)</c> byte-range locks on
/// the SHM file. The kernel auto-releases the lock if the owner process dies.</description></item>
/// </list>
/// </para>
/// <para>Layout (see <c>WalSharedMemoryLayout</c> constants):</para>
/// <code>
/// 0    4   Magic              ("BLSH" = 0x48534C42 little-endian)
/// 4    4   Version            (current = 1)
/// 8    4   PageSize           (must match .db file)
/// 12   4   MaxReaders         (default 8, max 32)
/// 16   8   NextTransactionId  (Interlocked)
/// 24   8   WalEndOffset       (Interlocked)
/// 32   8   CheckpointedOffset (Interlocked)
/// 40   4   WriterOwnerPid     (0 = unlocked; set under writer lock)
/// 44   20  Reserved           (padded to 64-byte header)
/// 64   N×16 Reader-slot array  (N = MaxReaders; per slot: long PID + long MaxReadOffset)
/// </code>
/// </summary>
public sealed class WalSharedMemory : IDisposable
{
    /// <summary>Default number of reader slots reserved in the SHM.</summary>
    public const int DefaultMaxReaders = 8;

    /// <summary>Hard upper bound on the number of reader slots (per header field width).</summary>
    public const int MaxAllowedReaders = 32;

    private readonly string _shmPath;
    private readonly int _pageSize;
    private readonly int _maxReaders;
    private readonly int _shmFileSize;

    private FileStream? _backingFile;
    private MemoryMappedFile? _mmf;
    private MemoryMappedViewAccessor? _accessor;

    // Cached base pointer into the memory-mapped view. Acquired once after Open()
    // and released only on Dispose() — every atomic operation reads/writes through
    // this pointer without taking the Acquire/Release ref-count cost on every call.
    private unsafe byte* _basePtr;
    private bool _basePtrAcquired;

    // Cross-process writer lock primitives.
    private readonly Mutex? _writerMutex;       // Windows
    // Unix uses _backingFile's file descriptor with fcntl OFD locks (see WalShmFcntl).

    private bool _writerLockHeld;
    private bool _disposed;

    /// <summary>
    /// Returns the configured maximum number of reader slots in this SHM file.
    /// </summary>
    public int MaxReaders => _maxReaders;

    /// <summary>
    /// Returns the path to the underlying <c>.wal-shm</c> file.
    /// </summary>
    public string Path => _shmPath;

    // Cached current-process PID. Environment.ProcessId is .NET 5+ only; we use
    // Process.GetCurrentProcess().Id for netstandard2.1 compatibility and cache the
    // result since the value is constant for the lifetime of the process.
    private static int s_currentProcessIdInit;
    private static int s_currentProcessId;
    private static int CurrentProcessId
    {
        get
        {
            if (Volatile.Read(ref s_currentProcessIdInit) == 0)
            {
                int pid = System.Diagnostics.Process.GetCurrentProcess().Id;
                Interlocked.Exchange(ref s_currentProcessId, pid);
                Volatile.Write(ref s_currentProcessIdInit, 1);
            }
            return s_currentProcessId;
        }
    }

    private WalSharedMemory(string shmPath, int pageSize, int maxReaders)
    {
        _shmPath = shmPath;
        _pageSize = pageSize;
        _maxReaders = maxReaders;
        _shmFileSize = WalSharedMemoryLayout.HeaderSize + maxReaders * WalSharedMemoryLayout.ReaderSlotSize;

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            _writerMutex = CreateWriterMutexWindows(shmPath);
        }
    }

    private static Mutex CreateWriterMutexWindows(string shmPath)
    {
        // Local\ namespace mutex keyed by SHA-256 of the absolute SHM path so that two
        // databases at different paths never collide. "Local\" scopes the mutex to the
        // current Windows session, which is what we want for same-host coordination.
        var absolutePath = System.IO.Path.GetFullPath(shmPath);
        byte[] hashBytes;
        using (var sha = SHA256.Create())
        {
            hashBytes = sha.ComputeHash(Encoding.UTF8.GetBytes(absolutePath));
        }
        // Hex-encode the first 16 bytes (cross-target compatible, no Convert.ToHexString).
        var sb = new StringBuilder(32);
        for (int i = 0; i < 16; i++) sb.Append(hashBytes[i].ToString("x2"));
        var name = $"Local\\BLite_walshm_w_{sb}";
        return new Mutex(initiallyOwned: false, name: name, out _);
    }

    /// <summary>
    /// Opens or creates the SHM file at <paramref name="shmPath"/>.
    /// If the file is missing, empty, or has an invalid magic / version / page-size
    /// header it is (re)initialized — the SHM is always reconstructible from the
    /// underlying WAL, so this is safe.
    /// </summary>
    public static WalSharedMemory Open(string shmPath, int pageSize, int maxReaders = DefaultMaxReaders)
    {
        if (string.IsNullOrEmpty(shmPath)) throw new ArgumentNullException(nameof(shmPath));
        if (pageSize <= 0) throw new ArgumentOutOfRangeException(nameof(pageSize));
        if (maxReaders <= 0 || maxReaders > MaxAllowedReaders)
            throw new ArgumentOutOfRangeException(nameof(maxReaders), $"Must be 1..{MaxAllowedReaders}");

        var dir = System.IO.Path.GetDirectoryName(shmPath);
        if (!string.IsNullOrWhiteSpace(dir))
            Directory.CreateDirectory(dir);

        var shm = new WalSharedMemory(shmPath, pageSize, maxReaders);
        try
        {
            shm.OpenOrInitialize();
            return shm;
        }
        catch
        {
            shm.Dispose();
            throw;
        }
    }

    private void OpenOrInitialize()
    {
        // Open with FileShare.ReadWrite so multiple processes can map the same file.
        _backingFile = new FileStream(
            _shmPath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.ReadWrite,
            bufferSize: 4096,
            FileOptions.None);

        bool needInit = _backingFile.Length < WalSharedMemoryLayout.HeaderSize;

        // Grow the file to the required size before mapping.
        if (_backingFile.Length < _shmFileSize)
        {
            _backingFile.SetLength(_shmFileSize);
        }
        _backingFile.Flush();

        _mmf = MemoryMappedFile.CreateFromFile(
            _backingFile,
            mapName: null,                              // anonymous map name; the file handle is what's shared
            capacity: _shmFileSize,
            access: MemoryMappedFileAccess.ReadWrite,
            inheritability: HandleInheritability.None,
            leaveOpen: true);

        _accessor = _mmf.CreateViewAccessor(0, _shmFileSize, MemoryMappedFileAccess.ReadWrite);

        // Acquire the base pointer once for the lifetime of this WalSharedMemory instance.
        // Released in Dispose(). All ref-based atomics use this pointer directly.
        unsafe
        {
            byte* p = null;
            _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref p);
            _basePtr = p;
            _basePtrAcquired = true;
        }

        // Validate magic / version / page-size; if anything is wrong, re-init in place.
        // We use a CAS on the magic field so two processes racing to initialize a fresh
        // SHM file don't both write conflicting headers.
        if (needInit)
        {
            InitializeHeader();
        }
        else
        {
            int magic = _accessor.ReadInt32(WalSharedMemoryLayout.OffsetMagic);
            int version = _accessor.ReadInt32(WalSharedMemoryLayout.OffsetVersion);
            int filePageSize = _accessor.ReadInt32(WalSharedMemoryLayout.OffsetPageSize);
            int fileMaxReaders = _accessor.ReadInt32(WalSharedMemoryLayout.OffsetMaxReaders);

            bool valid = magic == WalSharedMemoryLayout.Magic
                         && version == WalSharedMemoryLayout.CurrentVersion
                         && filePageSize == _pageSize
                         && fileMaxReaders == _maxReaders;

            if (!valid)
            {
                // Treat as corruption: zero the header and re-initialize. Reader slots
                // (which carry only PID + offset) are also reset — any live readers will
                // re-register on their next BeginTransaction.
                ZeroFile();
                InitializeHeader();
            }
        }
    }

    private void ZeroFile()
    {
        var zeros = new byte[Math.Min(_shmFileSize, 4096)];
        for (int written = 0; written < _shmFileSize; written += zeros.Length)
        {
            int n = Math.Min(zeros.Length, _shmFileSize - written);
            _accessor!.WriteArray(written, zeros, 0, n);
        }
        _accessor!.Flush();
    }

    private void InitializeHeader()
    {
        _accessor!.Write(WalSharedMemoryLayout.OffsetMagic, WalSharedMemoryLayout.Magic);
        _accessor.Write(WalSharedMemoryLayout.OffsetVersion, WalSharedMemoryLayout.CurrentVersion);
        _accessor.Write(WalSharedMemoryLayout.OffsetPageSize, _pageSize);
        _accessor.Write(WalSharedMemoryLayout.OffsetMaxReaders, _maxReaders);
        _accessor.Write(WalSharedMemoryLayout.OffsetNextTransactionId, 0L);
        _accessor.Write(WalSharedMemoryLayout.OffsetWalEndOffset, 0L);
        _accessor.Write(WalSharedMemoryLayout.OffsetCheckpointedOffset, 0L);
        _accessor.Write(WalSharedMemoryLayout.OffsetWriterOwnerPid, 0);
        _accessor.Flush();
    }

    // ── Atomic counters ──────────────────────────────────────────────────────

    /// <summary>
    /// Atomically increments and returns the next transaction ID. Replaces the
    /// in-process <c>Interlocked.Increment(ref _nextTransactionId)</c> when
    /// multi-process access is enabled, so two processes never observe the same ID.
    /// </summary>
    public ulong AllocateTransactionId()
    {
        ThrowIfDisposed();
        return (ulong)Interlocked.Increment(ref RefLong(WalSharedMemoryLayout.OffsetNextTransactionId));
    }

    /// <summary>
    /// Returns the current value of <c>NextTransactionId</c> without modifying it.
    /// Used by single-process startup recovery to seed its in-process counter.
    /// </summary>
    public ulong ReadNextTransactionId()
    {
        ThrowIfDisposed();
        return (ulong)Volatile.Read(ref RefLong(WalSharedMemoryLayout.OffsetNextTransactionId));
    }

    /// <summary>
    /// Bumps <c>NextTransactionId</c> to <paramref name="value"/> if and only if it is
    /// strictly greater than the current value. Used during startup recovery so that
    /// transaction IDs replayed from the WAL never get reused.
    /// </summary>
    public void EnsureNextTransactionIdAtLeast(ulong value)
    {
        ThrowIfDisposed();
        ref long slot = ref RefLong(WalSharedMemoryLayout.OffsetNextTransactionId);
        while (true)
        {
            long current = Volatile.Read(ref slot);
            if ((ulong)current >= value) return;
            if (Interlocked.CompareExchange(ref slot, (long)value, current) == current) return;
        }
    }

    /// <summary>Reads the current end-of-WAL byte offset published by the most recent writer.</summary>
    public long ReadWalEndOffset()
    {
        ThrowIfDisposed();
        return Volatile.Read(ref RefLong(WalSharedMemoryLayout.OffsetWalEndOffset));
    }

    /// <summary>
    /// Atomically advances <c>WalEndOffset</c> to <paramref name="newOffset"/> if it is
    /// greater than the current value. Called by the group-commit writer after each batch flush.
    /// </summary>
    public void AdvanceWalEndOffset(long newOffset)
    {
        ThrowIfDisposed();
        ref long slot = ref RefLong(WalSharedMemoryLayout.OffsetWalEndOffset);
        while (true)
        {
            long current = Volatile.Read(ref slot);
            if (newOffset <= current) return;
            if (Interlocked.CompareExchange(ref slot, newOffset, current) == current) return;
        }
    }

    /// <summary>Reads the last byte offset that has been safely checkpointed to the page file.</summary>
    public long ReadCheckpointedOffset()
    {
        ThrowIfDisposed();
        return Volatile.Read(ref RefLong(WalSharedMemoryLayout.OffsetCheckpointedOffset));
    }

    /// <summary>Updates <c>CheckpointedOffset</c> after a successful checkpoint.</summary>
    public void WriteCheckpointedOffset(long offset)
    {
        ThrowIfDisposed();
        Volatile.Write(ref RefLong(WalSharedMemoryLayout.OffsetCheckpointedOffset), offset);
    }

    /// <summary>
    /// Returns the PID currently recorded as the writer owner, or 0 if no writer is registered.
    /// </summary>
    public int ReadWriterOwnerPid()
    {
        ThrowIfDisposed();
        return Volatile.Read(ref RefInt(WalSharedMemoryLayout.OffsetWriterOwnerPid));
    }

    // ── Cross-process writer lock ───────────────────────────────────────────

    /// <summary>
    /// Acquires the cross-process writer lock. Blocks up to <paramref name="timeoutMs"/> ms.
    /// On success, records the current process ID into <c>WriterOwnerPid</c> for stale-PID
    /// recovery. Idempotent within a single process while held (re-entrant calls return true).
    /// </summary>
    public bool TryAcquireWriterLock(int timeoutMs)
    {
        ThrowIfDisposed();
        if (_writerLockHeld) return true;

        bool acquired;
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            try
            {
                acquired = _writerMutex!.WaitOne(timeoutMs);
            }
            catch (AbandonedMutexException)
            {
                // Previous owner died without releasing. We now own the mutex.
                // Treat this as a successful acquisition; the SHM WriterOwnerPid recovery
                // path (Process.GetProcessById) will detect any inconsistent state.
                acquired = true;
            }
        }
        else
        {
            acquired = WalShmFcntl.TryAcquireWriteLock(_backingFile!, timeoutMs);
        }

        if (acquired)
        {
            _writerLockHeld = true;
            // Stamp our PID into the SHM as the current writer.
            Volatile.Write(ref RefInt(WalSharedMemoryLayout.OffsetWriterOwnerPid), CurrentProcessId);
        }
        return acquired;
    }

    /// <summary>Releases the cross-process writer lock previously acquired by this instance.</summary>
    public void ReleaseWriterLock()
    {
        if (!_writerLockHeld) return;

        // Clear PID first so a reader doing a stale-PID check after our release
        // doesn't see our (now-stale) PID.
        Volatile.Write(ref RefInt(WalSharedMemoryLayout.OffsetWriterOwnerPid), 0);

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            try { _writerMutex!.ReleaseMutex(); } catch (ApplicationException) { /* not held */ }
        }
        else
        {
            WalShmFcntl.ReleaseWriteLock(_backingFile!);
        }
        _writerLockHeld = false;
    }

    /// <summary>
    /// Detects whether a previous writer crashed without releasing the writer lock.
    /// If <c>WriterOwnerPid</c> is non-zero and that PID is not a live process,
    /// the field is force-cleared and <c>true</c> is returned so the caller can
    /// trigger crash recovery (replay committed records, truncate WAL).
    /// </summary>
    /// <remarks>
    /// PID reuse is a theoretical concern; on practical timescales (an OS-level
    /// recycle of the same numeric PID before the next open) this check is sufficient.
    /// The OS-level writer mutex / OFD lock additionally guarantees that a live writer
    /// is never preempted, regardless of whether their PID was matched.
    /// </remarks>
    public bool ForceClearStaleWriter()
    {
        ThrowIfDisposed();
        int pid = ReadWriterOwnerPid();
        if (pid == 0) return false;
        if (IsProcessAlive(pid)) return false;
        Volatile.Write(ref RefInt(WalSharedMemoryLayout.OffsetWriterOwnerPid), 0);
        return true;
    }

    // ── Reader slot registration (Phase 5) ──────────────────────────────────

    /// <summary>
    /// Acquires a reader slot, recording <see cref="CurrentProcessId"/> and the
    /// supplied <paramref name="maxReadOffset"/> (the WAL end offset visible to this reader).
    /// Stale slots (those whose recorded PID is no longer alive) are reclaimed during the scan.
    /// </summary>
    /// <returns>
    /// <c>true</c> and a non-negative <paramref name="slotIndex"/> when a slot was acquired;
    /// <c>false</c> when all <see cref="MaxReaders"/> slots are occupied by live processes
    /// (caller should fall back to the in-process read path).
    /// </returns>
    public bool TryAcquireReaderSlot(out int slotIndex, long maxReadOffset)
    {
        ThrowIfDisposed();
        int myPid = CurrentProcessId;
        for (int i = 0; i < _maxReaders; i++)
        {
            int slotOffset = WalSharedMemoryLayout.HeaderSize + i * WalSharedMemoryLayout.ReaderSlotSize;
            ref long pidSlot = ref RefLong(slotOffset);
            long currentPid = Volatile.Read(ref pidSlot);

            // Slot is free, OR slot belongs to a dead process — try to claim it via CAS.
            if (currentPid == 0 || (currentPid != myPid && !IsProcessAlive((int)currentPid)))
            {
                if (Interlocked.CompareExchange(ref pidSlot, myPid, currentPid) == currentPid)
                {
                    Volatile.Write(ref RefLong(slotOffset + 8), maxReadOffset);
                    slotIndex = i;
                    return true;
                }
            }
        }
        slotIndex = -1;
        return false;
    }

    /// <summary>Updates the <c>MaxReadOffset</c> recorded in a previously-acquired slot.</summary>
    public void UpdateReaderSlot(int slotIndex, long maxReadOffset)
    {
        ThrowIfDisposed();
        if (slotIndex < 0 || slotIndex >= _maxReaders) throw new ArgumentOutOfRangeException(nameof(slotIndex));
        int slotOffset = WalSharedMemoryLayout.HeaderSize + slotIndex * WalSharedMemoryLayout.ReaderSlotSize;
        Volatile.Write(ref RefLong(slotOffset + 8), maxReadOffset);
    }

    /// <summary>Releases a reader slot. Idempotent: a no-op if the slot is no longer ours.</summary>
    public void ReleaseReaderSlot(int slotIndex)
    {
        if (_disposed) return;
        if (slotIndex < 0 || slotIndex >= _maxReaders) return;
        int slotOffset = WalSharedMemoryLayout.HeaderSize + slotIndex * WalSharedMemoryLayout.ReaderSlotSize;
        ref long pidSlot = ref RefLong(slotOffset);
        long myPid = CurrentProcessId;
        // CAS so we don't accidentally clear a slot reclaimed by another process.
        Interlocked.CompareExchange(ref pidSlot, 0, myPid);
        Volatile.Write(ref RefLong(slotOffset + 8), 0);
    }

    /// <summary>
    /// Returns the minimum <c>MaxReadOffset</c> across all active (live PID) reader slots,
    /// or <see cref="long.MaxValue"/> when no readers are registered. Used by the checkpoint
    /// algorithm as the safe upper bound for WAL truncation. Stale slots are reclaimed in-place.
    /// </summary>
    public long GetMinReaderOffset()
    {
        ThrowIfDisposed();
        long min = long.MaxValue;
        for (int i = 0; i < _maxReaders; i++)
        {
            int slotOffset = WalSharedMemoryLayout.HeaderSize + i * WalSharedMemoryLayout.ReaderSlotSize;
            ref long pidSlot = ref RefLong(slotOffset);
            long pid = Volatile.Read(ref pidSlot);
            if (pid == 0) continue;
            if (!IsProcessAlive((int)pid))
            {
                // Reclaim stale slot.
                Interlocked.CompareExchange(ref pidSlot, 0, pid);
                Volatile.Write(ref RefLong(slotOffset + 8), 0);
                continue;
            }
            long offset = Volatile.Read(ref RefLong(slotOffset + 8));
            if (offset < min) min = offset;
        }
        return min;
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private unsafe ref long RefLong(int offset)
    {
        return ref Unsafe.AsRef<long>(_basePtr + offset);
    }

    private unsafe ref int RefInt(int offset)
    {
        return ref Unsafe.AsRef<int>(_basePtr + offset);
    }

    private static bool IsProcessAlive(int pid)
    {
        if (pid <= 0) return false;
        if (pid == CurrentProcessId) return true;

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            try
            {
                using var p = System.Diagnostics.Process.GetProcessById(pid);
                return !p.HasExited;
            }
            catch (ArgumentException) { return false; }            // PID not found
            catch (InvalidOperationException) { return false; }    // process exited
        }

        // Unix: kill(pid, 0) returns 0 if the process is alive, ESRCH (3) otherwise.
        // EPERM (1) means the process exists but we lack permission — still alive.
        int rc = WalShmFcntl.Kill(pid, 0);
        if (rc == 0) return true;
        int errno = Marshal.GetLastWin32Error();
        return errno != 3 /* ESRCH */;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(WalSharedMemory));
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // Best-effort lock release in case Dispose runs while still held.
        try { ReleaseWriterLock(); } catch { /* swallow */ }

        // Release the cached base pointer reservation before disposing the accessor.
        if (_basePtrAcquired)
        {
            try { _accessor?.SafeMemoryMappedViewHandle.ReleasePointer(); } catch { }
            _basePtrAcquired = false;
            unsafe { _basePtr = null; }
        }

        try { _accessor?.Dispose(); } catch { }
        try { _mmf?.Dispose(); } catch { }
        try { _backingFile?.Dispose(); } catch { }
        try { _writerMutex?.Dispose(); } catch { }
    }
}
