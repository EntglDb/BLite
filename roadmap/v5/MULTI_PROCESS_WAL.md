# BLite — Development Plan: Multi-Process WAL Access

> Date: May 2, 2026
> References: [WriteAheadLog.cs](../../src/BLite.Core/Transactions/WriteAheadLog.cs), [StorageEngine.cs](../../src/BLite.Core/Storage/StorageEngine.cs), [StorageEngine.GroupCommit.cs](../../src/BLite.Core/Storage/StorageEngine.GroupCommit.cs), [StorageEngine.Recovery.cs](../../src/BLite.Core/Storage/StorageEngine.Recovery.cs), [PageFile.cs](../../src/BLite.Core/Storage/PageFile.cs), [WASM_SUPPORT.md](../../WASM_SUPPORT.md)

---

## Overview

BLite implements a WAL (Write-Ahead Logging) system for crash safety. Unlike SQLite, which uses a `-shm` (shared memory) sidecar file to coordinate concurrent readers and writers across OS processes, BLite's current WAL operates exclusively within a single process. All synchronization primitives are in-process (`SemaphoreSlim`, `ReaderWriterLockSlim`, Interlocked counters), and every database file is opened with `FileShare.None`, causing a second process to receive an `IOException` immediately on open.

This plan introduces a shared sidecar file (`.wal-shm`) to enable N-reader / 1-writer multi-process access to the same BLite database, following the same principles as SQLite's WAL-mode SHM but adapted to BLite's sequential record format.

The feature is **opt-in** (`EnableMultiProcessAccess = false` by default). All existing single-process behaviour is preserved unchanged.

---

## Gap Analysis

### 1. `FileShare.None` on Every File

| File | Class | Effect |
|------|-------|--------|
| `*.db` (page file) | `PageFile.cs` | Second process gets `IOException` on `FileStream` open |
| `*.wal` (WAL file) | `WriteAheadLog.cs` | Same |

Both constructors open their `FileStream` with `FileShare.None` (verified at `PageFile.cs` line ~313 and `WriteAheadLog.cs` line ~75).

### 2. In-Process-Only Synchronization

| Field | Type | Multi-process problem |
|-------|------|-----------------------|
| `_walIndex` | `ConcurrentDictionary<uint, byte[]>` | Committed pages written by process A are invisible to process B |
| `_nextTransactionId` | `long` (Interlocked) | Two processes independently generate identical transaction IDs → WAL corruption |
| `_commitLock` | `SemaphoreSlim(1,1)` | Does not protect against a second OS process writing concurrently |
| `_checkpointRunning` | `int` (CAS) | Two processes can run checkpoint simultaneously, double-writing and double-truncating the WAL |
| WAL `_lock` | `SemaphoreSlim(1,1)` | In-process only; no cross-process serialization of WAL record writes |

### 3. WAL Record Format vs. SQLite Frame Format

SQLite's WAL uses fixed-size 4 KB frames (header + page data), making offset-by-frame-number trivial. BLite uses variable-length sequential records:

| Record type | Size |
|-------------|------|
| `BEGIN` | 17 bytes (1 type + 8 txnId + 8 timestamp) |
| `WRITE` | 17 bytes + page data (variable) |
| `COMMIT` / `ABORT` | 17 bytes |
| Encrypted envelope | `[size(4LE)][nonce(12)][ciphertext(N)][GCM tag(16)]` |

The shared index must therefore map `pageId → byte offset` in the WAL file, not a frame number.

---

## Multi-File Mode Analysis

BLite supports a multi-file layout (`PageFileConfig.Server(...)`) where the main `.db`, the index `.idx`, and up to 64 per-collection `.db` files reside as separate `IPageStorage` instances. The question arises naturally: could multi-file mode allow one writer per physical file simultaneously (e.g., one process writes to collection A's file while another writes to collection B's file), and would that require a separate WAL per file?

### Current architecture (verified in source)

There is exactly **one `WriteAheadLog` instance** regardless of how many physical files exist. All physical files share it.

Page IDs are globally encoded with target-file bits:

| Bit range | Meaning |
|-----------|---------|
| `[31:30] = 00` | Main `.db` file |
| `[31:30] = 10` | Index `.idx` file |
| `[31:30] = 11` | Per-collection file (bits `[29:24]` = slot 0-63) |

The `_walIndex` in `StorageEngine` is a single `ConcurrentDictionary<uint, byte[]>` keyed on these **global** page IDs. A single `PendingCommit` therefore routinely contains pages destined for different physical files. For example, inserting a document into a collection writes both:
- A page in the collection file (document data)
- One or more pages in the index file (BTree node update for the collection's BTree index)

These two writes are part of the same transaction and reach the WAL as a single `BEGIN … WRITE … WRITE … COMMIT` sequence.

### Why per-file WALs would not help

Splitting into one WAL per physical file would require one of two things, both unacceptable:

1. **Restrict transactions to a single physical file** — an `Insert<T>` touches at minimum the collection file and the index file in the same commit. Making these two separate atomic units would break BLite's ACID guarantee: a crash between the two commits would leave index and data out of sync.

2. **Two-phase commit across WALs** — a prepare phase writes to all participating file WALs, a commit phase finalizes. This adds a round-trip to every write, multiplies fsync cost (one per file WAL instead of one shared), and makes crash recovery dramatically more complex (up to 66 WAL files = 64 collections + 1 index + 1 main, each requiring independent replay and coordination).

Additionally, the group commit's core optimization — **one `FlushAsync()` per batch regardless of how many pages or files** — depends on all writes going to the same sequential stream. Splitting the stream across files eliminates this amortization.

### What multi-file mode does gain in multi-process access

Even with a single shared WAL, multi-file mode provides two natural parallelism benefits that require no SHM changes:

1. **Parallel checkpoint I/O** — during checkpoint, pages for different physical files can be flushed concurrently (`Parallel.ForEach` over `_collectionFiles` + `_indexFile`). Each file has its own `FileStream` and `MemoryMappedFile` handle, so OS I/O can proceed in parallel across files. This is a pure optimization within Phase 6 of this plan.

2. **Parallel post-checkpoint reads** — after checkpoint, each process reads from its own MMF handles (one per physical file). Reads for collection A's file and collection B's file hit different OS page-cache regions and different physical sectors simultaneously. No coordination needed.

### Conclusion

The SHM design remains **single-WAL** and does not need per-file writer locks, per-file WAL index segments, or per-file reader slot arrays. The one cross-process writer lock serializes all WAL writes, exactly as the in-process `_commitLock` does today. Multi-file mode's only specific contribution to this plan is enabling parallel checkpoint I/O in Phase 6.

---

## Architecture

### Sidecar File: `.wal-shm`

A new binary file `<dbname>.wal-shm` is created alongside the existing `.db` and `.wal` files. It is opened as a `MemoryMappedFile` by every participating process and contains only coordination metadata — **no page data**. If the file is deleted or corrupted (invalid magic), any process that opens the database recreates it from scratch (it is always reconstructible from the WAL).

```
[Process A]                     [Process B]
StorageEngine                   StorageEngine
     │                               │
     ├── PageFile (.db)  ◄──────────►├── PageFile (.db)
     │    FileShare.ReadWrite         │    FileShare.ReadWrite
     │                               │
     ├── WriteAheadLog (.wal) ───────►├── WriteAheadLog (.wal)
     │    FileShare.ReadWrite         │    FileShare.ReadWrite
     │                               │
     └── WalSharedMemory (.wal-shm) ─┘
          MemoryMappedFile (shared)
          • NextTransactionId (atomic)
          • WalEndOffset (atomic)
          • CheckpointedOffset (atomic)
          • Writer lock slot (owner PID)
          • Reader slot array (up to MaxReaders × 16 bytes)
          • WAL index hash table (pageId → byte offset)
```

### SHM Binary Layout

```
Offset    Size    Field
0         4       Magic = 0x424C5348  ("BLSH")
4         4       Version = 1
8         4       PageSize (must match .db file)
12        4       MaxReaders (default 8, max 32)
16        8       NextTransactionId  [Interlocked via Unsafe.AsRef]
24        8       WalEndOffset       [last byte written to WAL, Interlocked]
32        8       CheckpointedOffset [last byte safely checkpointed, Interlocked]
40        4       WriterOwnerPid     [0 = unlocked]
44        20      Reserved           [pad to 64 bytes]
─────────────────────────────────────────────────────────
64        MaxReaders × 16    Reader Slot Array
                             Per slot: [long ProcessId][long MaxReadOffset]
─────────────────────────────────────────────────────────
64 + (MaxReaders × 16)      WAL Index — Hash Table A
64 + (MaxReaders × 16) + N  WAL Index — Hash Table B  (mirror for atomic swap)
```

The WAL index is a double-buffered open-addressing hash table mapping `uint pageId → long walByteOffset`. The double-buffer allows the writer to rebuild one half while readers continue using the other, then atomically flip a pointer in the SHM header.

---

## Platform Matrix

| Platform | Writer lock | Byte-range lock | Named MMF |
|----------|-------------|-----------------|-----------|
| **Windows** | `Mutex("Local\\BLite_w_{hash}")` — auto-released on process death by OS | `LockFile` / `UnlockFile` Win32 | `MemoryMappedFile.CreateOrOpen(name, size)` |
| **Linux** | `fcntl F_OFD_SETLK` on `.wal-shm` — auto-released on process death by kernel | Same `F_OFD_SETLK` | `MemoryMappedFile.CreateFromFile(path)` |
| **macOS** | `fcntl F_OFD_SETLK` — same as Linux | Same | `MemoryMappedFile.CreateFromFile(path)` |
| **Android** | `fcntl F_OFD_SETLK` (Linux kernel) — same as Linux | Same | `MemoryMappedFile.CreateFromFile(path)` in shared app internal storage |
| **iOS** | `fcntl F_OFD_SETLK` (BSD) — same as macOS | Same | `MemoryMappedFile.CreateFromFile(path)` in App Group container |
| **WASM / Browser** | **Not supported** — no filesystem, no `MemoryMappedFile` | — | — |

**Android note**: multi-process access requires that both processes belong to the same APK (e.g. main process + a declared background service process). The database path must be in the app's internal storage directory, which both processes can reach.

**iOS note**: multi-process access requires that all processes share the same App Group container path (`FileManager.containerURL(forSecurityApplicationGroupIdentifier:)`). Works for the main app + App Extensions.

### AOT Compatibility

| Component | Status |
|-----------|--------|
| `MemoryMappedFile` (file-backed) | ✅ AOT-safe on all supported platforms |
| `Mutex` with name (Windows) | ✅ AOT-safe |
| P/Invoke `fcntl` (Linux / macOS / Android / iOS) | ✅ AOT-safe — `[LibraryImport]` static binding |
| `Unsafe.AsRef<long>` on MMF pointer | ✅ AOT-safe |
| `Interlocked` on raw memory | ✅ AOT-safe via `Unsafe.AsRef<int>` |
| `Process.GetProcessById(pid)` (PID liveness check) | ⚠️ Wrap with `[SupportedOSPlatform]`; use `kill(pid, 0)` on Unix via P/Invoke |

---

## Implementation Phases

### Phase 0 — Configuration Flag *(no behaviour change)*

Add the opt-in surface. Nothing in the engine changes.

**Files to modify:**

| File | Change |
|------|--------|
| `src/BLite.Core/Engine/BLiteConfiguration.cs` | Add `bool EnableMultiProcessAccess { get; init; } = false` |
| `src/BLite.Core/Storage/PageFileConfig.cs` | Add `bool AllowMultiProcessAccess { get; init; } = false` |
| `src/BLite.Core/Transactions/WriteAheadLog.cs` | Accept `bool allowMultiProcessAccess` constructor parameter |
| `src/BLite.Core/Storage/StorageEngine.cs` | Read `EnableMultiProcessAccess`; pass flag to `PageFile` and `WriteAheadLog` constructors |

**Acceptance criteria**: all existing tests pass without modification.

---

### Phase 1 — FileShare and File Co-existence *(unsafe alone, gated on flag)*

Change `FileShare.None` to `FileShare.ReadWrite` in both `PageFile` and `WriteAheadLog` when the flag is active. Also change `MemoryMappedFile.CreateFromFile` to `MemoryMappedFile.CreateOrOpen` on Windows so the mapping can be shared between processes.

**Files to modify:**

| File | Change |
|------|--------|
| `src/BLite.Core/Storage/PageFile.cs` | `FileShare.None` → `FileShare.ReadWrite` (when `AllowMultiProcessAccess`) |
| `src/BLite.Core/Transactions/WriteAheadLog.cs` | Same |
| `src/BLite.Core/Storage/PageFile.cs` | `MemoryMappedFile.CreateFromFile(...)` → `CreateOrOpen(name, size)` on Windows when flag active |

**Acceptance criteria**: two processes can open the same database files without `IOException`. Data safety is NOT yet guaranteed at this phase.

---

### Phase 2 — SHM Infrastructure

Introduce the shared memory file and its access layer.

**New files:**

| File | Purpose |
|------|---------|
| `src/BLite.Core/Transactions/WalSharedMemory.cs` | Opens / creates `.wal-shm`; exposes atomic read/write methods for every SHM field |
| `src/BLite.Core/Transactions/WalSharedMemoryLayout.cs` | Compile-time `const int` offsets for every SHM field |
| `src/BLite.Core/Transactions/ReaderSlot.cs` | `readonly struct` with `ProcessId` and `MaxReadOffset`, marshalled to/from 16-byte SHM slots |
| `src/BLite.Core/Transactions/WalFrameIndex.cs` | Double-buffered open-addressing hash table (pageId → WAL offset), backed by SHM memory |

**`WalSharedMemory` public API:**

```csharp
public sealed class WalSharedMemory : IDisposable
{
    // Opens or creates the .wal-shm file; validates magic; initializes if new.
    public static WalSharedMemory Open(string walShmPath, int pageSize, int maxReaders);

    // Atomically increments NextTransactionId and returns the new value.
    public ulong AllocateTransactionId();

    // Atomically advances WalEndOffset (called by writer after each WRITE record).
    public void AdvanceWalEndOffset(long newOffset);

    // Returns current WalEndOffset (used by readers to know what is visible).
    public long ReadWalEndOffset();

    // Returns CheckpointedOffset.
    public long ReadCheckpointedOffset();

    // Updates CheckpointedOffset after a successful checkpoint.
    public void WriteCheckpointedOffset(long offset);

    // Cross-process writer lock (Windows: named Mutex; Unix: fcntl F_OFD_SETLK).
    public bool TryAcquireWriterLock(int timeoutMs);
    public void ReleaseWriterLock();

    // Cross-process checkpoint lock (separate byte range / named object).
    public bool TryAcquireCheckpointLock(int timeoutMs);
    public void ReleaseCheckpointLock();

    // Reader slot registration. Returns false if all slots are full.
    public bool TryAcquireReaderSlot(out int slotIndex, long maxReadOffset);
    public void UpdateReaderSlot(int slotIndex, long maxReadOffset);
    public void ReleaseReaderSlot(int slotIndex);

    // Returns the minimum MaxReadOffset across all active (live PID) reader slots.
    // Stale slots (dead PID) are reclaimed during this scan.
    public long GetMinReaderOffset();

    // WAL index: returns WAL byte offset for a committed pageId, or -1 if not in WAL.
    public long LookupPageOffset(uint pageId);

    // Updates the WAL index after a commit. Called by group commit writer under writer lock.
    public void UpdatePageOffsets(IEnumerable<(uint pageId, long walOffset)> entries);

    // Rebuilds the WAL index hash table from scratch (called after checkpoint / WAL truncation).
    public void RebuildIndex(IEnumerable<(uint pageId, long walOffset)> survivors);
}
```

**Files to modify:**

| File | Change |
|------|--------|
| `src/BLite.Core/Storage/StorageEngine.cs` | Add `WalSharedMemory? _shm` field; instantiate in constructor when flag is active |

**Acceptance criteria**: `WalSharedMemory.Open()` creates a valid `.wal-shm` file; two processes can call `AllocateTransactionId()` concurrently and receive distinct values; reader slot acquire/release works; stale PID cleanup works.

---

### Phase 3 — Cross-Process Writer Serialization

The in-process `_commitLock` (`SemaphoreSlim`) is **kept unchanged**. An OS-level writer lock is added as an inner guard, acquired *after* `_commitLock` and released *before* it. The mandatory lock order is:

```
1. await _commitLock.WaitAsync()          ← SemaphoreSlim, in-process, microseconds
2. _shm.TryAcquireWriterLock(timeout)     ← OS lock (Mutex / fcntl), cross-process
3. … write WAL records, FlushAsync() …
4. _shm.ReleaseWriterLock()
5. _commitLock.Release()
```

**Why individual writer threads are unaffected**: writer threads never hold `_commitLock` and never touch the OS lock. They post a `PendingCommit` to `_commitChannel` and await a `TaskCompletionSource<bool>`. The OS lock is acquired exactly once per group-commit batch (up to 64 transactions) by the single background group-commit writer task. In single-process mode (`EnableMultiProcessAccess = false`) steps 2 and 4 are entirely skipped — no performance regression.

**Why this lock order is required**: the checkpoint path also acquires `_commitLock` first, then `_shm.TryAcquireCheckpointLock()`. Keeping both paths in the same `_commitLock → OS lock` order prevents any possibility of cross-lock deadlock between the writer and the checkpoint.

Transaction ID allocation is redirected to `_shm.AllocateTransactionId()` when multi-process is active, replacing the local `Interlocked.Increment(ref _nextTransactionId)`.

**Files to modify:**

| File | Change |
|------|--------|
| `src/BLite.Core/Storage/StorageEngine.GroupCommit.cs` | After acquiring `_commitLock`: call `_shm.TryAcquireWriterLock(timeout)` before writing any WAL records; release it after `_wal.FlushAsync()` |
| `src/BLite.Core/Storage/StorageEngine.cs` | `Interlocked.Increment(ref _nextTransactionId)` → `_shm?.AllocateTransactionId() ?? Interlocked.Increment(...)` |
| `src/BLite.Core/Transactions/WriteAheadLog.cs` | `WriteDataRecordAsync` returns `long walByteOffset` of the record written (needed by Phase 4) |

**Acceptance criteria**: two processes running concurrent writers do not interleave WAL records; WAL file remains valid after concurrent writes from both processes.

---

### Phase 4 — Shared WAL Index and Multi-Process Read Path

The in-process `_walIndex` (`ConcurrentDictionary<uint, byte[]>`) is supplemented by the SHM-backed `WalFrameIndex`. When a read misses the local cache, the engine consults `_shm.LookupPageOffset(pageId)`, seeks to that offset in the WAL file, and reads the page directly from disk.

A local LRU page cache (`_localPageCache`, default 1 000 pages) sits in front of the SHM lookup to amortize repeated reads of hot pages.

**Read path (multi-process):**

```
ReadPage(pageId)
  ├─ 1. Check _localPageCache  → hit: return
  ├─ 2. Check _shm WAL index  → hit: seek + read WAL file, populate cache, return
  └─ 3. Read from PageFile (MMF) → populate cache, return
```

**Write path (multi-process, after group commit flush):**

```
foreach (pageId, walOffset) in batch:
    _shm.UpdatePageOffsets(pageId, walOffset)
    _shm.AdvanceWalEndOffset(walEndAfterBatch)
```

**Cache invalidation:** on every `BeginTransactionAsync`, compare `_shm.ReadWalEndOffset()` with the last known local offset. If the SHM offset has advanced (a different process committed), evict stale entries from `_localPageCache` and update the local snapshot of `WalEndOffset`.

**Files to modify:**

| File | Change |
|------|--------|
| `src/BLite.Core/Storage/StorageEngine.cs` | Add `_localPageCache` (LRU); update read path; add `_lastKnownWalEndOffset` field |
| `src/BLite.Core/Storage/StorageEngine.GroupCommit.cs` | After `_wal.FlushAsync()`: call `_shm.UpdatePageOffsets(...)` and `_shm.AdvanceWalEndOffset(...)` |

**Acceptance criteria**: process B can read a page written and committed by process A without restarting; no stale reads after process A advances `WalEndOffset`.

---

### Phase 5 — Reader Slot Registration

Register reader slots so that the checkpoint algorithm can determine the safe truncation boundary (the minimum offset any active reader might still need to read from the WAL).

**Read transaction lifecycle:**

```
BeginTransactionAsync (read):
    slotAcquired = _shm.TryAcquireReaderSlot(out slotIndex, currentWalEndOffset)

EndTransaction / Dispose:
    if (slotAcquired) _shm.ReleaseReaderSlot(slotIndex)
```

If `TryAcquireReaderSlot` returns false (all slots full), the read transaction falls back to single-process behaviour (no slot, no cross-process coordination). This is a degraded-but-safe path.

**Files to modify:**

| File | Change |
|------|--------|
| `src/BLite.Core/Storage/StorageEngine.cs` | Acquire/release reader slot around read transactions when `_shm != null` |

**Acceptance criteria**: `_shm.GetMinReaderOffset()` returns the correct minimum across all registered live readers; slots are reclaimed when a process dies.

---

### Phase 6 — Multi-Process Checkpoint

The checkpoint algorithm is extended to use `_shm.GetMinReaderOffset()` as the safe upper bound: it must not checkpoint WAL records beyond the minimum offset held by any active reader slot.

**Updated checkpoint algorithm (`StorageEngine.Recovery.cs`):**

```
CheckpointAsync():
  1. Acquire in-process _commitLock
  2. Acquire _shm.TryAcquireCheckpointLock(timeout)
  3. safeOffset = min(_wal.GetCurrentSize(), _shm.GetMinReaderOffset())
  4. Snapshot WAL index entries with walOffset <= safeOffset
  5. (Multi-file mode) Group snapshot entries by target physical file
  6. Write pages to their target PageFile:
       Single-file:  sequential write loop
       Multi-file:   Parallel.ForEach over (_pageFile, _indexFile, _collectionFiles[*])
                     — each physical file written and flushed concurrently
  7. _shm.WriteCheckpointedOffset(safeOffset)
  8. Remove checkpointed entries from SHM WAL index
  9. If safeOffset == _wal.GetCurrentSize() (full checkpoint):
       Truncate WAL file
       _shm.RebuildIndex([])   // empty index after full truncation
       _shm.AdvanceWalEndOffset(0)
  10. Release checkpoint lock
  11. Release _commitLock
```

The `Parallel.ForEach` in step 6 is the only multi-file-specific addition. It requires no SHM changes: each physical file has an independent `FileStream` and `MemoryMappedFile` handle, so OS I/O can proceed on all files concurrently. The checkpoint lock in SHM still covers the entire batch as a single atomic operation.

**Files to modify:**

| File | Change |
|------|--------|
| `src/BLite.Core/Storage/StorageEngine.Recovery.cs` | Insert `_shm` checkpoint lock and `GetMinReaderOffset()` safe boundary; add parallel flush loop for multi-file mode |

**Acceptance criteria**: a checkpoint does not truncate WAL records that a concurrently reading process still needs; after full checkpoint, SHM index is empty and `WalEndOffset = 0`; in multi-file mode, all collection files and the index file are flushed concurrently.

---

### Phase 7 — Crash Recovery and Stale State Cleanup

**Writer crash:** The writer lock slot in SHM stores the owner PID. On any process opening the database, if `WriterOwnerPid != 0` and that PID is not a running process, the lock is force-released and `RecoverAsync()` is invoked (replay committed records from WAL, flush, truncate). This is the same algorithm as today's single-process recovery, now triggered cross-process.

**Reader crash:** Already covered by the stale PID reclaim in `GetMinReaderOffset()` (Phase 5).

**SHM corruption / wrong magic:** Delete and recreate the `.wal-shm` file. Reconstruct the WAL index by replaying the WAL from offset 0. This is always safe because the SHM contains no source-of-truth data — it is a pure cache of information recoverable from the WAL and PageFile.

**SHM version mismatch:** If the `Version` field in the header does not match the current `WalSharedMemoryLayout.CurrentVersion`, treat as corruption: delete and recreate.

**Files to modify:**

| File | Change |
|------|--------|
| `src/BLite.Core/Storage/StorageEngine.cs` | On open with `EnableMultiProcessAccess`: check SHM writer lock PID; if stale, force-release and trigger `RecoverAsync()` |
| `src/BLite.Core/Transactions/WalSharedMemory.cs` | `Open()`: validate magic and version; if invalid, delete and recreate; rebuild WAL index from WAL replay |

**Acceptance criteria**: process B successfully opens a database whose previous writer process A crashed mid-write; the resulting state is consistent (no partially written transactions visible).

---

## New Files

| File | Purpose |
|------|---------|
| `src/BLite.Core/Transactions/WalSharedMemory.cs` | Core SHM coordination: MMF, writer lock, checkpoint lock, reader slots, WAL end offset |
| `src/BLite.Core/Transactions/WalSharedMemoryLayout.cs` | Compile-time offset constants for every SHM field |
| `src/BLite.Core/Transactions/ReaderSlot.cs` | `readonly struct ReaderSlot` — 16-byte slot layout |
| `src/BLite.Core/Transactions/WalFrameIndex.cs` | Double-buffered hash table backed by SHM memory (pageId → WAL byte offset) |

---

## Modified Files

| File | Change summary |
|------|----------------|
| `src/BLite.Core/Engine/BLiteConfiguration.cs` | Add `EnableMultiProcessAccess` flag |
| `src/BLite.Core/Storage/PageFileConfig.cs` | Add `AllowMultiProcessAccess` flag |
| `src/BLite.Core/Storage/PageFile.cs` | `FileShare.None` → `FileShare.ReadWrite` when flag active; `CreateOrOpen` on Windows |
| `src/BLite.Core/Transactions/WriteAheadLog.cs` | `FileShare.None` → `FileShare.ReadWrite` when flag active; `WriteDataRecordAsync` returns `long walByteOffset` |
| `src/BLite.Core/Storage/StorageEngine.cs` | Add `_shm`; redirect txnId allocation; add `_localPageCache`; stale writer detection on open |
| `src/BLite.Core/Storage/StorageEngine.GroupCommit.cs` | Wrap batch write in `_shm.TryAcquireWriterLock`; update SHM index and `WalEndOffset` after flush |
| `src/BLite.Core/Storage/StorageEngine.Recovery.cs` | Add `_shm` checkpoint lock and `GetMinReaderOffset()` safe boundary |

---

## Testing Plan

| Test | Type | Description |
|------|------|-------------|
| Two processes: A writes, B reads via SHM index | Integration | B reads a page committed by A without reopening the database |
| Two processes write concurrently: one serialized behind the other | Integration | Writer lock ensures no WAL interleaving; both transactions durable after completion |
| Checkpoint with active reader slot: WAL not truncated beyond min reader offset | Integration | `GetMinReaderOffset()` correctly limits checkpoint boundary |
| Writer crash mid-write: process B detects stale PID, recovers, reads consistent state | Integration | Only committed transactions are visible after crash recovery |
| Reader crash: slot reclaimed by subsequent `GetMinReaderOffset()` call | Integration | Checkpoint is not blocked indefinitely by a dead reader |
| SHM magic corrupted: delete + recreate, operations continue normally | Integration | SHM reconstruction from WAL replay produces correct index |
| SHM version mismatch on open: treated as corruption, recreated | Integration | Older SHM file from a previous BLite version does not block newer process |
| `EnableMultiProcessAccess = false` (default): all existing tests pass, `FileShare.None` preserved | Regression | No behaviour change in single-process mode |
| Android in-process multi-process (main + service): both read and write | Platform integration | `fcntl F_OFD_SETLK` on Linux kernel behaves as expected |
| iOS App Group multi-process (app + extension): both read and write | Platform integration | `fcntl F_OFD_SETLK` on BSD behaves as expected |

---

## Scope

**In scope:**
- N concurrent readers + 1 writer across OS processes on the same host
- Windows, Linux, macOS, Android, iOS
- Process crash detection and recovery
- Opt-in configuration (`EnableMultiProcessAccess`)
- AOT compatibility on all supported platforms

**Out of scope:**
- Multiple simultaneous writers (N-writer concurrency requires BTree/SlottedPage to be page-latch-safe first — see [blite-collection-lock.md](../../CONTRIBUTING.md))
- WASM / browser (no filesystem, no `MemoryMappedFile`)
- Network / distributed file systems (NFS, SMB) — file locking semantics are unreliable on remote filesystems
- SHM encryption (the SHM contains only byte offsets, never page data — no sensitive content)
