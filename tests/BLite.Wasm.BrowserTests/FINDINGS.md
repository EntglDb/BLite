# BLite.Wasm in a real browser: findings

Run of `tests/BLite.Wasm.BrowserTests` against `upstream/main` at `0450b97`, headless
Chromium 153 (Playwright 1.63), .NET 10.0.13 browser-wasm runtime (one thread,
interpreted dynamic code). Every row below is reproduced by a named test in
[BrowserScenarioTests.cs](BrowserScenarioTests.cs); the raw outcomes are in the
JSON-lines report the fixture writes.

## Summary

Same outcome in all four publish variants:

| Variant | Publish | `BLite.Core` in the bundle | `dotnet.native.wasm` | IL warnings | Tests |
|---------|---------|---------------------------:|---------------------:|------------:|-------|
| `trimmed` | Release, Blazor default `TrimMode=partial` | 474 KB (from 1,024 KB) | 2.9 MB | 0 | 29 pass, 20 fail |
| `fulltrim` | `TrimMode=full` | 474 KB | 2.9 MB | 0 | 29 pass, 20 fail |
| `untrimmed` | `PublishTrimmed=false` | 1,024 KB | 3.0 MB | 0 | 29 pass, 20 fail |
| `aot` | `RunAOTCompilation=true` (wasm-tools) | 474 KB | 22.7 MB | 0 | 29 pass, 20 fail |

Per scenario ("crash" = the .NET runtime in the tab exited):

| Scenario | IndexedDB | OPFS | In-memory |
|----------|-----------|------|-----------|
| CRUD through the async API | pass | pass | pass |
| 50 concurrent `InsertAsync` | pass | pass | pass |
| BLQL async operators | pass | pass | pass |
| Explicit transactions | pass (4 modes) | – | pass (1 mode) |
| Typed context, no index, async LINQ | pass | pass | pass |
| Typed context with a declared index | **fail** (#2) | – | **fail** (#2) |
| Synchronous LINQ on a typed collection (5 operators) | – | – | **fail** (#3) |
| BLQL `ToList` / `Count`, `FirstOrDefault`, `Any` | – | – | **fail** / pass (#3) |
| Write, `CheckpointAsync`, reload | pass | pass | – |
| Write without checkpoint (engine open or disposed), reload | **fail** (#1) | **fail** (#1) | – |
| Two tabs writing one database | **fail**: lost update (#5) | **fail**: 2nd tab cannot open (#5) | – |
| 5 MB: fill, `CheckpointAsync`, reload | intermittent **data loss** (#1) | intermittent **data loss** (#1) | – |
| 20 MB: fill, `CheckpointAsync`, reload | pass, slow (#6) | pass, slow (#6) | – |
| 50 MB: fill, `CheckpointAsync`, reload | **fail**: out of memory (#6) | **fail**: out of memory (#6) | – |
| `IsAvailable()` as the first call | **crash** (#7) | **crash** (#7) | – |

The 29/20 counts come from the runs before the size test also checked the document
count after reopening; with that check, the 5 MB rows fail whenever the data is
lost (6 of 12 runs).

The failures are the same in every publish variant, so none of them comes from
trimming or AOT: BLite's trimming annotations hold up in the browser. The problems
are the single-threaded runtime and the storage design.

What works on all three backends (IndexedDB, OPFS, in-memory): the async document
API (insert, update, delete, find by id), 50 concurrent `InsertAsync` calls, the
async BLQL operators, explicit transactions (async commit, synchronous
`BeginTransaction`, rollback through `Dispose` of an uncommitted transaction,
synchronous `Rollback`), and a typed `DocumentDbContext` **without** a declared
index, including async LINQ and a write that survives a reload after
`CheckpointAsync`.

## Findings

Ranked by impact. "Observed" is what the tests show; "Cause" cites the code and
says where it is an inference rather than something a test isolated.

### 1. Committed writes are lost on reload, and `CheckpointAsync` does not reliably prevent it

Tests: `Committed_write_survives_reload` (IndexedDb/Opfs × commit-only/dispose),
`Typed_write_survives_reload` (checkpoint: False), `Large_database_open_time_and_memory` (5 MB).

Observed: a document inserted through the auto-commit API, with the engine then
left open or disposed, is gone after a page reload, on both persistent backends.
The IndexedDB diagnostics show the WAL holds the transaction's 4 records before and
after the reload; the reopen replays them (page count 3 → 5) and truncates the WAL
(4 → 0), yet the collection then holds 0 documents. A second reopen also finds 0,
so the loss is permanent: the WAL that held the data has been truncated. With
`CheckpointAsync` before the reload, the single document survives.

`CheckpointAsync` is not enough once the WAL has grown: in 6 of 12 runs that filled
5 MB (5,242 documents), called `CheckpointAsync` and reloaded, the reopened database
held **0** documents. In every one of those 6 runs `CheckpointAsync` had returned in
0 ms with 1,089–1,119 records still in the WAL; in the 6 runs where it took
95–135 ms, all documents were there. Why it returns at once is in #6.

Cause (inference, consistent with the page and WAL counts): the browser page
stores keep every page in a managed cache and their synchronous `Flush()` does
nothing ([IndexedDbPageStorage.cs:216](../../src/BLite.Wasm/Storage/IndexedDbPageStorage.cs),
[OpfsPageStorage.cs:217](../../src/BLite.Wasm/Storage/OpfsPageStorage.cs)). State the
engine writes outside the WAL (page allocation and the next page id, the header,
the key dictionary through `WritePageImmediate`,
[StorageEngine.Pages.cs:135](../../src/BLite.Core/Storage/StorageEngine.Pages.cs))
therefore only reaches IndexedDB/OPFS in a checkpoint's `FlushAsync`. After a reload
the replayed page images refer to pages and keys that were never persisted. In
addition, `StorageEngine(IPageStorage, IWriteAheadLog)` reads the dictionary and KV
roots in its constructor and documents that the caller must replay the WAL
*before* constructing it ([StorageEngine.cs:420](../../src/BLite.Core/Storage/StorageEngine.cs)),
while `BLiteWasm.CreateAsync` constructs first and calls `RecoverAsync` afterwards
([BLiteWasm.cs:125](../../src/BLite.Wasm/BLiteWasm.cs)). `Dispose` does not checkpoint
committed pages either.

### 2. A typed `DocumentDbContext` with any declared index cannot open

Tests: `Typed_context_with_async_linq` (InMemory, indexed: True),
`Typed_write_survives_reload` (IndexedDb, indexed: True).

Observed: constructing a context whose `OnModelCreating` declares `HasIndex` throws
`InvalidOperationException: Cannot build index 'idx_Value' of collection 'items'
from the documents already in it: Cannot wait on monitors on this runtime. Nothing
was saved, but every open fails the same way …`, on an empty, newly created
database. The same entity without the index works.

Cause: `ApplyIndexBuilder` fills the new index with
`Task.Run(() => FillUnregisteredIndexAsync(index, ct)).GetAwaiter().GetResult()`
([DocumentCollection.cs:1233](../../src/BLite.Core/Collections/DocumentCollection.cs)),
a blocking wait the single-threaded browser runtime rejects with
`PlatformNotSupportedException`. The same pattern is at
[DocumentCollection.cs:927](../../src/BLite.Core/Collections/DocumentCollection.cs).
The message also misleads: the collection holds no documents.

### 3. Synchronous LINQ and BLQL `ToList` throw

Tests: `Typed_sync_linq_operator` (tolist, count, first, any, foreach),
`Blql_sync_operator` (tolist).

Observed: every synchronous System.Linq terminal operator on
`collection.AsQueryable()` throws `PlatformNotSupportedException: Cannot wait on
monitors on this runtime`. BLQL `ToList()` throws the same; BLQL `Count()`,
`FirstOrDefault()` and `Any()` pass.

Cause: `BTreeQueryProvider.Execute` runs the async path through
`Task.Run(...).GetAwaiter().GetResult()` ([BTreeQueryProvider.cs:95](../../src/BLite.Core/Query/BTreeQueryProvider.cs)),
and so does `BlqlQuery.AsEnumerable` ([BlqlQuery.cs:146](../../src/BLite.Core/Query/Blql/BlqlQuery.cs)).
The three BLQL operators that pass call `XxxAsync().GetAwaiter().GetResult()`
without `Task.Run`; they only work because, with every page cached in memory, the
async path completes without ever yielding. A backend that really awaits I/O
inside a read would make them fail too.

### 4. Every `Dispose` freezes the tab for 5 seconds

Tests: every scenario that disposes an engine (for example `Crud`, `ReadMarkers`).

Observed: scenarios that dispose an engine take 5.0–5.2 s; the same work without
`Dispose` takes 40–100 ms. The browser's only thread is blocked for that time, so
the page does not respond.

Cause: `StorageEngine.Dispose` waits for the group-commit writer with
`_writerTask?.Wait(TimeSpan.FromSeconds(5))` ([StorageEngine.cs:462](../../src/BLite.Core/Storage/StorageEngine.cs)).
The writer can only finish on the thread that is waiting for it, so the wait always
runs to its timeout.

### 5. Two tabs on one database: silent lost update (IndexedDB), no second tab (OPFS)

Test: `Two_tabs_writing_the_same_database_keep_both_writes`.

Observed (IndexedDB): tabs A and B open the same database; A inserts `a` and
checkpoints, then B inserts `b` and checkpoints. Both calls succeed. A fresh third
tab sees only `b`: A's committed write is gone, with no error anywhere. B never
sees `a` either.

Observed (OPFS): the second tab cannot open the database at all:
`JSException: Failed to execute 'createSyncAccessHandle' on 'FileSystemFileHandle':
Access Handles cannot be created if there is another open Access Handle …`.

Cause: each tab keeps its own full page cache and writes it back on checkpoint;
nothing coordinates tabs (no `navigator.locks`, `BroadcastChannel` or
`onversionchange` handling in the JS modules).

### 6. A 50 MB database cannot be reopened; 20 MB costs half a gigabyte

Test: `Large_database_open_time_and_memory`.

| Database | `CheckpointAsync` took | Documents after reopen | Open time | Managed heap growth on open | WebAssembly memory after open (MiB) |
|----------|-----------------------|------------------------|----------:|----------------------------:|------------------------------:|
| 5 MB, 5,242 docs | 95–135 ms (6 of 12 runs) | all | 165–180 ms | +7.5 MB | 66–80 |
| 5 MB, 5,242 docs | 0 ms (6 of 12 runs) | **none** | 310–450 ms | +137–198 MB | 362–393 |
| 20 MB, 20,971 docs | 0 ms (12 of 12) | all | 1.5–2.5 s | +545–865 MB | 1,261–1,358 |
| 50 MB, 52,428 docs | 0 ms (12 of 12) | out of memory | – | – | 1,162–1,468 at failure |

Twelve runs per size: for each of the two backends, the four publish-variant runs
plus two extra runs of the size test. Documents carry a 1,000-byte string.

Observed: after filling, `CheckpointAsync` returns in 0 ms for 20 MB and 50 MB (and
in half of the 5 MB runs) and leaves thousands of records in the WAL (20 MB:
4,650–5,705; 50 MB: 12,448–16,111). Reopening reads that whole WAL: at 20 MB the
open takes about 2 s and half a gigabyte of managed heap, at 50 MB it fails with
`OutOfMemoryException` in `JsonSerializer.Deserialize<string[]>`.

Cause: past 16 MB of WAL ([StorageEngine.cs:124](../../src/BLite.Core/Storage/StorageEngine.cs))
a commit starts a background checkpoint (`_ = Task.Run(() => CheckpointAsync())`,
[StorageEngine.GroupCommit.cs:292](../../src/BLite.Core/Storage/StorageEngine.GroupCommit.cs)).
While it runs, an explicit `CheckpointAsync` returns at once
([StorageEngine.Recovery.cs:59](../../src/BLite.Core/Storage/StorageEngine.Recovery.cs)),
although `BLiteEngine.CheckpointAsync` documents that it guarantees the data file is
up to date. On reopen, `IndexedDbWriteAheadLog.OpenAsync` reads the entire WAL as a
single JSON string of base64 records ([IndexedDbWriteAheadLog.cs:56](../../src/BLite.Wasm/Transactions/IndexedDbWriteAheadLog.cs)),
and both page stores load every page into memory before the engine starts.

### 7. `IsAvailable()` kills the .NET runtime when it is the first call

Test: `IsAvailable_before_any_other_call`.

Observed: calling the public `IndexedDbPageStorage.IsAvailable()` or
`OpfsPageStorage.IsAvailable()` before `BLiteWasm.CreateAsync` or
`DetectBestBackendAsync` does not throw: the runtime asserts
(`ES6 module ./blite-indexeddb.mjs was not imported yet`) and exits, and every later
.NET call in the tab fails.

Cause: both methods call a `[JSImport]` function whose module only
`EnsureLoadedAsync` imports ([IndexedDbPageStorage.cs:60](../../src/BLite.Wasm/Storage/IndexedDbPageStorage.cs),
[OpfsPageStorage.cs:61](../../src/BLite.Wasm/Storage/OpfsPageStorage.cs)).

### 8. The `BLite.Wasm` NuGet package does not contain its JavaScript modules

Observed: the package built from `src/BLite.Wasm` contains only
`lib/net10.0/BLite.Wasm.dll`. The four modules the interop imports
(`blite-indexeddb.mjs`, `blite-opfs.mjs`, `blite-opfs-bridge.mjs`,
`blite-opfs-worker.mjs`) are missing, so a consumer of the package gets a runtime
failure until they copy the files from the repository (the sample does this by
hand, and WASM_SUPPORT.md describes copying them).

Cause: `BLite.Wasm.csproj` uses `Microsoft.NET.Sdk`, so its `wwwroot` is not packed
as static web assets; `Microsoft.NET.Sdk.Razor` would ship them under
`_content/BLite.Wasm/`.

### 9. No supported way to open a typed context on a browser backend

Observed: `BLiteWasm.CreateAsync` returns only a dynamic `BLiteEngine`. A typed
`DocumentDbContext` has to assemble the backend through the public storage types.
For OPFS that fails with `IndexedDB not open for '<name>-wal'`, because
`IndexedDbWriteAheadLog.OpenAsync` needs its database opened by the internal
`IndexedDbInterop.OpenAsync` first ([BLiteWasm.cs:92](../../src/BLite.Wasm/BLiteWasm.cs)).
The harness works around it by opening an `IndexedDbPageStorage` of the same name.

## Not covered

- Firefox and WebKit (`BLITE_WASM_BROWSER=firefox|webkit` runs the same suite;
  not run for this report).
- Multi-threaded WebAssembly (`WasmEnableThreads`), which needs cross-origin
  isolation headers.
- A browser crash or tab kill in the middle of a commit; the reload tests use an
  orderly reload.
