# WASM Support — Design & Implementation Roadmap

This document captures the analysis of the [WASM support request](https://github.com/EntglDb/BLite/issues) and breaks it into separate, actionable sub-issues. Each issue is self-contained and can be implemented and reviewed independently.

---

## Background

BLite's current storage stack (v4.x) relies on:

| Component | Implementation | WASM blocker? |
|---|---|---|
| Page storage | `PageFile` — memory-mapped file (`MemoryMappedFile`) | ✅ Yes — `MemoryMappedFile` is not available in browsers |
| Write-ahead log | `WriteAheadLog` — sequential `FileStream` | ✅ Yes — `FileStream` is not available in browsers |
| Directory helpers | `Directory.CreateDirectory`, `File.Exists`, ... | ✅ Yes — filesystem APIs are not available in browsers |

The maintainer noted (in the original thread) that the engine must be decoupled from its wrappers before WASM storage backends can be plugged in.

---

## What Has Been Implemented (v4.3 — this PR)

The foundational abstraction layer has been added:

### `IPageStorage` (new interface — `src/BLite.Core/Storage/IPageStorage.cs`)

```
IPageStorage
├── int PageSize
├── uint NextPageId
├── void Open()
├── void ReadPage(uint pageId, Span<byte> destination)
├── void ReadPageHeader(uint pageId, Span<byte> destination)
├── ValueTask ReadPageAsync(uint pageId, Memory<byte> destination, CancellationToken ct)
├── void WritePage(uint pageId, ReadOnlySpan<byte> source)
├── uint AllocatePage()
├── void FreePage(uint pageId)
├── void Flush()
├── Task FlushAsync(CancellationToken ct)
└── Task BackupAsync(string destinationPath, CancellationToken ct)
```

`PageFile` now implements `IPageStorage`. No existing behaviour has changed.

### `MemoryPageStorage` (new class — `src/BLite.Core/Storage/MemoryPageStorage.cs`)

A `ConcurrentDictionary<uint, byte[]>`-backed, fully in-memory implementation of `IPageStorage`:
- Zero file-system dependencies (WASM compatible today)
- Suitable for unit tests, ephemeral caches, and in-browser WASM apps

### `IWriteAheadLog` (new interface — `src/BLite.Core/Transactions/IWriteAheadLog.cs`)

```
IWriteAheadLog
├── ValueTask WriteBeginRecordAsync(ulong transactionId, CancellationToken ct)
├── ValueTask WriteCommitRecordAsync(ulong transactionId, CancellationToken ct)
├── ValueTask WriteAbortRecordAsync(ulong transactionId, CancellationToken ct)
├── ValueTask WriteDataRecordAsync(ulong transactionId, uint pageId, ReadOnlyMemory<byte> afterImage, CancellationToken ct)
├── Task FlushAsync(CancellationToken ct)
├── long GetCurrentSize()
├── Task TruncateAsync(CancellationToken ct)
└── List<WalRecord> ReadAll()
```

`WriteAheadLog` now implements `IWriteAheadLog`. No existing behaviour has changed.

### `MemoryWriteAheadLog` (new class — `src/BLite.Core/Transactions/MemoryWriteAheadLog.cs`)

An in-memory, `List<WalRecord>`-backed WAL implementation:
- All records stored in process memory — no file I/O
- `FlushAsync` is a no-op (records survive until `TruncateAsync` or disposal)
- Full `ReadAll()` support for recovery path compatibility

### `StorageEngine` pluggable constructor (updated — `src/BLite.Core/Storage/StorageEngine.cs`)

```csharp
// New constructor — accepts any IPageStorage + IWriteAheadLog:
public StorageEngine(IPageStorage pageStorage, IWriteAheadLog wal)
```

The existing `StorageEngine(string databasePath, PageFileConfig config)` is completely unchanged.

### `BLiteEngine.CreateInMemory()` (new factory — `src/BLite.Core/BLiteEngine.cs`)

```csharp
// Creates a fully in-memory BLiteEngine — no file system required:
var engine = BLiteEngine.CreateInMemory();
// Optional page size and KV options:
var engine = BLiteEngine.CreateInMemory(pageSize: 8192);
```

### `DocumentDbContext` pluggable constructor (updated — `src/BLite.Core/DocumentDbContext.cs`)

```csharp
// Subclasses can now use in-memory storage:
protected DocumentDbContext(StorageEngine storage, BLiteKvOptions? kvOptions = null)
```

---

## Implemented in `BLite.Wasm` Package

### `BLite.Wasm` project (`src/BLite.Wasm/BLite.Wasm.csproj`)

New NuGet package targeting `net10.0-browser` with OPFS and IndexedDB storage backends.

### `OpfsPageStorage` (Issue 1 — `src/BLite.Wasm/Storage/OpfsPageStorage.cs`)

OPFS (Origin Private File System) page storage using `FileSystemSyncAccessHandle` via JS interop:
- High-performance synchronous I/O from Worker threads
- Pages stored as sequential regions in a single OPFS file
- Supported in Chrome 102+, Firefox 111+, Safari 15.2+

### `IndexedDbPageStorage` (Issue 2 — `src/BLite.Wasm/Storage/IndexedDbPageStorage.cs`)

IndexedDB page storage using async IDB transactions via JS interop:
- Universal browser compatibility (all modern browsers)
- Each page stored as a keyed blob in an IndexedDB object store
- Data exchanged as base64 strings for `[JSImport]` async compatibility

### `OpfsWriteAheadLog` (Issue 3 — `src/BLite.Wasm/Transactions/OpfsWriteAheadLog.cs`)

OPFS-backed WAL for crash recovery in browser contexts:
- Appends records to a dedicated OPFS `.wal` file
- Binary format matches the file-based `WriteAheadLog`
- Full `ReadAll()` support for recovery path

### `IndexedDbWriteAheadLog` (Issue 3 — `src/BLite.Wasm/Transactions/IndexedDbWriteAheadLog.cs`)

IndexedDB-backed WAL for crash recovery:
- Records stored as serialised byte arrays in an IDB object store
- `TruncateAsync` clears all entries in a single IDB transaction

### `BLiteWasm` factory (Issue 4 — `src/BLite.Wasm/BLiteWasm.cs`)

```csharp
// Auto-selects OPFS when available, falls back to IndexedDB:
var engine = await BLiteWasm.CreateAsync("mydb");

// Or pick a specific backend:
var engine = await BLiteWasm.CreateAsync("mydb", WasmStorageBackend.IndexedDb);
```

### `AddBLiteWasm` Blazor extension (Issue 4 — `src/BLite.Wasm/BLiteWasmServiceExtensions.cs`)

```csharp
// In Program.cs of a Blazor WASM app:
builder.Services.AddBLiteWasm("mydb");
```

### `BLiteEngine.CreateFromStorage` (updated — `src/BLite.Core/BLiteEngine.cs`)

New public factory method for creating engines from custom storage backends:
```csharp
var engine = BLiteEngine.CreateFromStorage(storageEngine, kvOptions);
```

---

## Remaining Sub-Issues

The following issue should be tracked separately.

---

### Issue 5 — WASM Demo & Documentation

**Scope:** Provide an end-to-end example of BLite running in a Blazor WASM app:
- `samples/BLite.BlazorWasm/` — minimal Blazor WASM app storing and querying BSON documents
  entirely in the browser using the OPFS or IndexedDB backend.
- Update `README.md` with a WASM section.
- Update `BENCHMARKS.md` with WASM throughput numbers (OPFS vs IndexedDB vs in-memory).

---

## Recommended Sequencing

```
[Done]  Issue 0  Storage abstraction (IPageStorage, IWriteAheadLog, MemoryPageStorage, MemoryWriteAheadLog)
[Done]  Issue 3  Browser WAL implementations (OPFS / IndexedDB)
[Done]  Issue 1  OPFS page storage backend
[Done]  Issue 2  IndexedDB page storage backend (compatibility fallback)
[Done]  Issue 4  BLite.Wasm NuGet package + factory API
[ ]     Issue 5  Blazor WASM sample + docs
```

---

## Testing Strategy

Each backend should be verified by:
1. Running the existing `InMemoryStorageTests` suite against the new backend (swap `MemoryPageStorage`
   for the new implementation).
2. A Playwright / browser automation test that exercises `BLiteEngine.CreateInMemory()` inside a
   `dotnet-wasm` test harness.
3. Throughput benchmarks comparing OPFS, IndexedDB, and in-memory.

---

## MessagePack / MemoryPack Serialisation (Separate Track)

The original issue also raised the question of why BLite uses C-BSON instead of MessagePack
or MemoryPack. This is a separate concern from WASM storage and should be tracked as an
independent issue:

- **MessagePack engine** — Replace or augment the BSON serialisation layer with MessagePack-CSharp.
  Smaller on-disk document size for mixed-type data; excellent AOT compatibility.
- **MemoryPack engine** — Zero-copy, struct-layout serialisation for pure C# workloads.
  Potentially the fastest option for query-heavy, schema-stable data.

Both would implement a new `IDocumentSerializer` interface (to be designed) so that the
storage layer and serialisation layer remain independently swappable.
