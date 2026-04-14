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

## Remaining Sub-Issues

The following issues should be tracked separately and implemented in order.

---

### Issue 1 — OPFS Storage Backend for WASM (`BLite.Wasm.Opfs`)

**Scope:** Implement `OpfsPageStorage : IPageStorage` that stores pages in the browser's
[Origin Private File System (OPFS)](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system).

**Motivation:**
OPFS has the highest throughput of all browser persistence APIs (comparable to native file I/O
in benchmarks). It is supported in Chrome 102+, Firefox 111+, and Safari 15.2+ in dedicated
worker contexts.

**Implementation sketch:**
```csharp
// src/BLite.Wasm/Storage/OpfsPageStorage.cs
public sealed class OpfsPageStorage : IPageStorage
{
    // Uses JavaScript interop via [JSImport] / [DynamicDependency] to call
    // the OPFS SyncAccessHandle (synchronous, high-perf) in a Worker thread.
    // Pages are stored as sequential regions in a single OPFS file.
    // ReadPage / WritePage map directly to ReadSync / WriteSync on the handle.
}
```

**Project:** New `src/BLite.Wasm/BLite.Wasm.csproj`
- Target: `net8.0-browser` (or `net9.0-browser`)
- References `BLite.Core`
- Depends on `Microsoft.AspNetCore.Components.WebAssembly`

**References:**
- [wa-sqlite OPFS benchmark](https://github.com/rhashimoto/wa-sqlite/tree/master/src/examples#vfs-comparison)
- [OPFS SyncAccessHandle spec](https://fs.spec.whatwg.org/#api-filesystemsyncaccesshandle)

---

### Issue 2 — IndexedDB Storage Backend for WASM (`BLite.Wasm.IndexedDb`)

**Scope:** Implement `IndexedDbPageStorage : IPageStorage` backed by the browser's
[IndexedDB API](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API).

**Motivation:**
IndexedDB is universally supported (all modern browsers, including Safari 7+) and persists
across sessions. Throughput is lower than OPFS but it is the safest choice for maximum
compatibility, especially in main-thread Blazor WASM contexts where OPFS Workers are not
readily available.

**Implementation sketch:**
```csharp
// Pages stored as Uint8Array blobs keyed by (databaseName, pageId)
// in an IndexedDB object store.
// ReadPageAsync / WritePageAsync use [JSImport] to call the browser IDB API.
public sealed class IndexedDbPageStorage : IPageStorage
{
    // Read/write are async; the synchronous ReadPage/WritePage overloads
    // block using a TaskCompletionSource pattern (acceptable in WASM
    // where the main thread uses cooperative scheduling).
}
```

**References:**
- [MDN IndexedDB Guide](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API/Using_IndexedDB)

---

### Issue 3 — WASM-targeted WAL: `OpfsWriteAheadLog` / `IndexedDbWriteAheadLog`

**Scope:** WAL implementations that persist records to OPFS or IndexedDB, enabling crash
recovery in browser contexts.

**Motivation:**
`MemoryWriteAheadLog` (added in this PR) has no persistence — if the browser tab or Worker
crashes, un-checkpointed data is lost. A browser-persistent WAL closes that gap.

**Implementation sketch:**
- `OpfsWriteAheadLog : IWriteAheadLog` — appends records to an OPFS file using
  `FileSystemSyncAccessHandle.write()`.
- `IndexedDbWriteAheadLog : IWriteAheadLog` — stores WAL records as IndexedDB
  key/value entries; `TruncateAsync` deletes all entries in a single IDB transaction.

---

### Issue 4 — `BLite.Wasm` NuGet Package

**Scope:** Ship a purpose-built `BLite.Wasm` NuGet package targeting `net8.0-browser`
(or `net9.0-browser`) that bundles:
- `OpfsPageStorage` (primary recommendation)
- `IndexedDbPageStorage` (compatibility fallback)
- `OpfsWriteAheadLog` / `IndexedDbWriteAheadLog`
- Convenience factory methods:
  ```csharp
  // Auto-selects OPFS when available, falls back to IndexedDB
  var engine = await BLiteWasm.CreateAsync("mydb");
  ```
- A Blazor service extension:
  ```csharp
  // In Program.cs of a Blazor WASM app:
  builder.Services.AddBLiteWasm("mydb");
  ```

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
[ ]     Issue 3  Browser WAL implementations (OPFS / IndexedDB)
[ ]     Issue 1  OPFS page storage backend
[ ]     Issue 2  IndexedDB page storage backend (compatibility fallback)
[ ]     Issue 4  BLite.Wasm NuGet package + factory API
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
