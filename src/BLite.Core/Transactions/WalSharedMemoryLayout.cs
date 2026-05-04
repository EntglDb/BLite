namespace BLite.Core.Transactions;

/// <summary>
/// Compile-time offsets and sizes for the <see cref="WalSharedMemory"/> binary layout.
/// All offsets are absolute byte positions in the <c>.wal-shm</c> file and are
/// stable for a given <see cref="CurrentVersion"/>. See <see cref="WalSharedMemory"/>
/// for the human-readable layout description.
/// </summary>
internal static class WalSharedMemoryLayout
{
    /// <summary>
    /// Magic value at offset 0. Reads as ASCII <c>"BLSH"</c> when interpreted as
    /// little-endian bytes, allowing trivial human inspection of the file with <c>xxd</c>.
    /// </summary>
    public const int Magic = 0x48534C42; // 'B' 'L' 'S' 'H' little-endian → 0x48534C42

    /// <summary>Layout version. Bumped whenever the on-disk header / slot layout changes.</summary>
    public const int CurrentVersion = 1;

    // Header field offsets (must match the layout documented on WalSharedMemory).
    public const int OffsetMagic              = 0;
    public const int OffsetVersion            = 4;
    public const int OffsetPageSize           = 8;
    public const int OffsetMaxReaders         = 12;
    public const int OffsetNextTransactionId  = 16;
    public const int OffsetWalEndOffset       = 24;
    public const int OffsetCheckpointedOffset = 32;
    public const int OffsetWriterOwnerPid     = 40;
    // bytes 44..63 reserved for future use (padding to 64-byte cache line)

    /// <summary>Total header size in bytes (also the offset of the first reader slot).</summary>
    public const int HeaderSize = 64;

    /// <summary>
    /// Size of a single reader slot in bytes:
    /// <c>[long ProcessId (8)][long MaxReadOffset (8)]</c>.
    /// </summary>
    public const int ReaderSlotSize = 16;

    // ── WAL index hash table (Phase 4) ────────────────────────────────────────
    //
    // A single open-addressing hash table mapping uint pageId → long walByteOffset.
    // Stored immediately after the reader slot array in the SHM file.
    //
    // Slot layout (16 bytes, 8-byte aligned):
    //   [long walOffset (8B)] — 0 = empty sentinel; valid WAL Write records are always at offset > 0
    //   [uint pageId    (4B)] — written AFTER walOffset to ensure happens-before ordering for readers
    //   [uint reserved  (4B)]
    //
    // Write protocol (writer holds exclusive _commitLock + SHM writer lock):
    //   1. Locate slot by linear probe from hash(pageId).
    //   2. For new entries: Volatile.Write(walOffset) then Volatile.Write(pageId).
    //   3. For updates (same pageId already present): Volatile.Write(walOffset) only.
    //
    // Read protocol (lock-free; concurrent with writer):
    //   1. Probe from hash(pageId).
    //   2. At each slot: Volatile.Read(walOffset); if 0 → stop (not found).
    //   3.                Volatile.Read(pageId); if matches → return walOffset.
    //   4. Otherwise probe next slot. Wrap-around terminates the search.
    //
    // A reader that races a write may see a stale offset or miss the entry entirely;
    // Phase 7 (incremental WAL replay on BeginTransaction) corrects staleness before
    // any read in a new transaction.

    /// <summary>Number of hash table slots. Must be a power of two.</summary>
    public const int WalIndexCapacity = 4096;

    /// <summary>Size of one hash table slot in bytes.</summary>
    public const int WalIndexSlotSize = 16;

    /// <summary>Total bytes occupied by the WAL index hash table.</summary>
    public const int WalIndexTableBytes = WalIndexCapacity * WalIndexSlotSize; // 65536 bytes

    /// <summary>
    /// Returns the byte offset within the SHM file where the WAL index hash table begins.
    /// This is immediately after the reader slot array and depends on <paramref name="maxReaders"/>.
    /// </summary>
    public static int GetWalIndexOffset(int maxReaders) =>
        HeaderSize + maxReaders * ReaderSlotSize;

    /// <summary>
    /// Returns the total SHM file size in bytes for the given <paramref name="maxReaders"/> count.
    /// </summary>
    public static int GetShmFileSize(int maxReaders) =>
        GetWalIndexOffset(maxReaders) + WalIndexTableBytes;
}
