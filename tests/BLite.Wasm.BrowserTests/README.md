# BLite.Wasm browser tests

Runs BLite.Wasm in a real browser. Nothing else in `tests/` exercises the OPFS or
IndexedDB backends, the single-threaded browser runtime, or a Release (trimmed)
publish. It follows the conventions of the oracle suites (`docs/ORACLE_TESTS.md`, #250):
every row is checked against a reference rather than a hand-written value, a seed
proves the suite can fail, and a candidate registry carries what it found until the
issue is fixed.

```bash
dotnet test tests/BLite.Wasm.BrowserTests
```

Needs the .NET SDK of the repository and, for the `aot` variant, the `wasm-tools`
workload (`dotnet workload install wasm-tools`). The Playwright browser is downloaded
on first use (`playwright install chromium`, run by the fixture; about 150 MB). A run
takes about five minutes for one variant: the publish is a minute, each row that
disposes an engine costs five seconds (candidate BW4), and the size rows fill up to
50 MB.

The fixture:

1. publishes `tests/BLite.Wasm.BrowserHost` (a Blazor WebAssembly app that exposes
   each scenario as a `[JSInvokable]` method),
2. serves the publish folder from Kestrel on `http://localhost:<port>` (a secure
   context, which OPFS needs),
3. installs the Playwright browser on first use and opens a fresh browser context
   per test, so every test starts with empty OPFS and IndexedDB storage.

A scenario ends in one of four states, all appended to a JSON-lines report:

| Status  | Meaning |
|---------|---------|
| `pass`  | The scenario returned and its expectations held. |
| `fail`  | The scenario threw; the report has the exception type, message and stack. |
| `hang`  | No answer before the timeout: a blocking wait froze the browser's only thread. |
| `crash` | The .NET runtime in the tab exited (a failed runtime assertion). |

## The reference per scenario

Each test is a row, named by its method and label (`Crud_through_the_async_api Opfs`).
What a row is checked against:

| Rows | Reference |
|---|---|
| `Crud_through_the_async_api`, `Concurrent_async_inserts`, `Blql_async_operators`, `Explicit_transactions`, `Typed_context_with_async_linq` | **The in-memory backend.** `MemoryPageStorage` and `MemoryWriteAheadLog` run the same engine with no browser storage underneath, so the same scenario on IndexedDB and OPFS must end the same way as on `InMemory`, and the counts it reports must be what the scenario inserted. |
| `Typed_sync_linq_operator`, `Blql_sync_operator` | **The async operator of the same query** (`Typed_context_with_async_linq`, `Blql_async_operators`): same documents, same predicate, so the synchronous terminal must return what the asynchronous one returns. |
| `Committed_write_survives_reload`, `Typed_write_survives_reload` | **A reload as the crash, the committed state as the model.** A page reload discards the tab's .NET runtime and keeps the storage, which is the browser's process crash (orderly: no write is cut). The reference is what was acknowledged before it, the contract of `WalCrashTests`: a committed state, never older than the last acknowledged commit. The `checkpoint`, `dispose` and `commit-only` modes are the three ways a tab can leave a committed write behind. |
| `Two_tabs_writing_the_same_database_keep_both_writes` | **Two tabs as two processes.** Each tab is a separate runtime on the same storage, the browser's multi-process mode. The reference is the one the README gives `AllowMultiProcessAccess`: a third tab sees both acknowledged writes. |
| `Large_database_open_time_and_memory` | **The fill's own count.** After `CheckpointAsync` and a reload, the reopened database must hold every document the fill inserted; open time, managed heap and WebAssembly memory are recorded in the report, not asserted. |
| `IsAvailable_before_any_other_call`, `Probe_reports_runtime_and_backend_detection` | **The documented contract**: `IsAvailable()` is a feature probe that returns a bool. |
| `Dispose_returns_promptly` | **The work before it.** Opening an engine and inserting a document takes tens of milliseconds on the same thread; `Dispose` must not take seconds. |
| `Typed_context_opens_through_the_public_storage_types` | **The IndexedDB row.** A typed context over the public storage types (page storage, WAL, `StorageEngine`, `RecoverAsync`) opens on IndexedDB; the same assembly must open on OPFS. |
| `Package_ships_the_js_modules` | **`src/BLite.Wasm/wwwroot`.** The four modules the `[JSImport]` interop loads must be in the NuGet package. |

## Candidates and seeds

**Registry** ([candidates.md](candidates.md)). A finding that is known and not yet fixed is a
row of the registry table: the rows it covers, a substring of the failure, the issue, and
a summary. `BrowserScenarioTests.RowAsync` applies it:

- a covered row that fails with a matching message passes and prints
  `KNOWN <row> [known candidate: BW<n>, #<issue>]: <message>`; while the Issue cell is
  empty it keeps failing with the candidate's id, so nothing is quarantined before it is filed;
- a covered row of kind `always` that passes fails with
  `candidate fixed: remove the entry for '<row>'`, so the fix's PR deletes the rows and the
  registry cannot outlive the defect; kind `sometimes` marks a row that fails only in some
  runs, where a pass proves nothing;
- a stale row matches nothing and costs nothing, so a fix checks with `grep` that it removed
  what it names.

**Seeds** ([seeds/](seeds)). A test that passes with and without a defect is not a guard, so
the suite ships a patch against `src/` that it must catch:
`BrowserScenarioTests.opfs-flush-dropped.patch` makes `OpfsPageStorage.FlushAsync` clear its
dirty set without writing a page, and the OPFS rows that pass today because they checkpoint
before the reload then lose their data. The header of the patch lists the rows it fails.

```bash
git apply --ignore-whitespace tests/BLite.Wasm.BrowserTests/seeds/BrowserScenarioTests.opfs-flush-dropped.patch
dotnet test tests/BLite.Wasm.BrowserTests
git checkout -- src
```

`tools/oracle-seeds.cs` (#250) only runs test classes under `tests/BLite.Tests`; this seed is
proven by hand as above.

## Reading a run

Next to the test assembly (`bin/Debug/net10.0/TestResults/`):

- `wasm-browser-<browser>-<variant>.summary.txt`: the counts (`pass`, `known`, `fail`,
  `fixed`) and one line per row that is not a plain pass, with its candidate and issue.
- `wasm-browser-<browser>-<variant>.jsonl`: every scenario call (status, elapsed time,
  exception, the data the scenario reported) and every row's outcome.
- `publish-<variant>.log` one level up: the publish, including any trimming or AOT warning.

The xunit output of a row has the scenario's data (`[Opfs/checkpoint] ReadMarkers: ok
markers=m1 ...`), the IndexedDB store counts before and after a reload for the durability
rows, and the KNOWN line.

## Variables

| Variable | Default | Values |
|----------|---------|--------|
| `BLITE_WASM_VARIANT` | `trimmed` | `trimmed` (Release publish, the Blazor default `TrimMode=partial`), `fulltrim` (`TrimMode=full`), `untrimmed` (`PublishTrimmed=false`), `aot` (`RunAOTCompilation=true`, needs `dotnet workload install wasm-tools`) |
| `BLITE_WASM_BROWSER` | `chromium` | `chromium`, `firefox`, `webkit` |
| `BLITE_WASM_PUBLISH_DIR` | – | Serve an existing publish folder instead of publishing. |
| `BLITE_WASM_REPORT` | `bin/.../TestResults/wasm-browser-<browser>-<variant>.jsonl` | Report file. |

Each variant is published with its own `--artifacts-path`, because an incremental
publish would otherwise reuse the trimmed assemblies of the previous variant. The
publish log (including any trimming or AOT warnings) is written next to the test
assembly as `publish-<variant>.log`.

## What it does not see

- Firefox and WebKit (`BLITE_WASM_BROWSER=firefox|webkit` runs the same suite; not run for
  the results in [FINDINGS.md](FINDINGS.md)).
- Multi-threaded WebAssembly (`WasmEnableThreads`), which needs cross-origin isolation
  headers; the runtime under test has one thread.
- A browser crash or tab kill in the middle of a commit: the reload is orderly, so a write
  cut inside a record (the `TornWrite` mode of `WalCrashTests`) and a lost IndexedDB
  transaction (its `PowerLoss`) are not modelled.
- Real races between tabs: the two-tab row alternates the tabs, it does not interleave them.
- `BLiteWasmServiceExtensions.AddBLiteWasm` and the sample app; a consumer of the NuGet
  package rather than the project reference (except the package's contents).
- Performance beyond the open time and memory the size rows record.

## Where this belongs in `docs/ORACLE_TESTS.md`

The document lives on #250, so the section it should get once that merges is kept here.
It goes under "What it is", after "Native AOT smoke test", as a fifth kind:

> ### Browser harness (`tests/BLite.Wasm.BrowserTests/`)
>
> A fifth kind, in its own test project because it publishes a Blazor WebAssembly app and
> drives a browser: **the same engine on another backend, with a reload as the crash**.
> `tests/BLite.Wasm.BrowserHost` exposes each scenario as a `[JSInvokable]` method; the test
> project publishes it (Release trimmed by default, or `fulltrim`, `untrimmed`, `aot`), serves
> it from Kestrel and calls the scenarios through Playwright, one fresh browser context per
> row. Three references:
>
> - **the in-memory backend** (`MemoryPageStorage`, `MemoryWriteAheadLog`) for the async API,
>   transactions, LINQ and BLQL on IndexedDB and OPFS: the same scenario must end the same
>   way on all three backends, and the asynchronous operator is the reference for the
>   synchronous one;
> - **the committed state before a reload** for durability: a page reload discards the
>   runtime and keeps the storage, the browser's process crash, and what was acknowledged
>   before it must be there after it (the `WalCrashTests` contract); two tabs are two
>   processes, and a third tab must see both their acknowledged writes;
> - **the fill's own count** after a checkpoint and a reload for databases of 5, 20 and 50 MB,
>   with open time and memory recorded.
>
> Its registry is `tests/BLite.Wasm.BrowserTests/candidates.md`, read by the test class: a
> covered row that fails with the registered message is reported as `KNOWN ... [known
> candidate: BW<n>, #<issue>]`, a covered row that passes fails with `candidate fixed`. Its
> seed, `seeds/BrowserScenarioTests.opfs-flush-dropped.patch`, drops the OPFS page flush and
> is proven by hand (`tools/oracle-seeds.cs` runs only `tests/BLite.Tests`). Its first run, on
> upstream `0450b97`, found nine (BW1 to BW9), identical in all four publish variants with no
> trimming warning: committed writes lost on a reload, a typed context with an index that
> cannot open, synchronous LINQ and BLQL that throw, a five-second `Dispose`, lost updates
> between tabs, a 50 MB database that cannot be reopened, an `IsAvailable()` that kills the
> runtime, a package without its JS modules, and no typed-context factory.
>
> It does not see Firefox and WebKit (selectable, not run), multi-threaded WebAssembly, a
> crash in the middle of a commit, or real races between tabs. Details and the result per
> run are in `tests/BLite.Wasm.BrowserTests/README.md` and `FINDINGS.md`.

And one line under "How detection grows with the feature set":

> - **A new browser storage backend or a new WASM entry point** is one more backend in
>   `AllBackends` or one more `[JSInvokable]` scenario in `tests/BLite.Wasm.BrowserHost`:
>   every existing row then runs over it, in every publish variant.
