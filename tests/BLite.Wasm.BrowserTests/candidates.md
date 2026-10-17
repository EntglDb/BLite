Status: **filed** on 2026-10-17: BW1 as #289, BW2, BW3, BW4 and BW7 together as #290 (one cause,
a blocking wait on the single-threaded browser runtime), BW5 as #291, BW6 as #292, BW8 and BW9
together as #293 (the package and its API); the registry's Issue column links each row. Triage of what
`tests/BLite.Wasm.BrowserTests` reports on `test/wasm-browser-harness` (upstream `0450b97`,
v5.2.3), headless Chromium 153 (Playwright 1.63), .NET 10.0.13 `browser-wasm` runtime, on
2026-10-17. Nine candidates, the same in all four publish variants (`trimmed`, `fulltrim`,
`untrimmed`, `aot`) with 0 trimming warnings, so none of them comes from trimming or AOT. The
drafts, with the measurements and the mechanism of each, are in [FINDINGS.md](FINDINGS.md); the
numbering is the same.

Open and closed issues checked on 2026-10-17 (`gh issue list --repo EntglDb/BLite --state all
--search` for WASM, OPFS, IndexedDB, Blazor and browser): #57 (the WASM support request, closed
by #63, which added BLite.Wasm) and #110 (the multi-process WAL sidecar, closed). Neither covers
a candidate below. WASM_SUPPORT.md lists no remaining sub-issue. BW1 is the browser face of the
class behind #260 to #262: writes the engine makes outside the WAL, which on desktop are durable
at once and leak on a crash, and in the browser are not durable at all until a checkpoint.

**Registry.** `BrowserScenarioTests.RowAsync` reads the table below. A row is a test method and
its label (`*` matches anything). A row that fails with a message containing the Match text of
a candidate that covers it is reported as `KNOWN <row> [known candidate: BW<n>, #<issue>]` and
does not fail the run; while the Issue cell is empty it keeps failing, with the candidate's id in
the message. A row of kind `always` that passes fails with `candidate fixed: remove the entry`,
so the fix's PR deletes its rows; a row of kind `sometimes` fails only in some runs, and a pass
proves nothing. A stale row matches nothing and costs nothing, so check with `grep` that a fix
removed what it names.

| Id | Kind | Row | Match | Issue | Summary |
|---|---|---|---|---|---|
| BW1 | always | `Committed_write_survives_reload IndexedDb/commit-only` | `Expected marker m1 after reload, found ''` | #289 | a committed write is gone after a reload; the WAL replays and is truncated, the collection is empty |
| BW1 | always | `Committed_write_survives_reload IndexedDb/dispose` | `Expected marker m1 after reload, found ''` | #289 | the same with the engine disposed before the reload |
| BW1 | always | `Committed_write_survives_reload Opfs/commit-only` | `Expected marker m1 after reload, found ''` | #289 | the same on OPFS |
| BW1 | always | `Committed_write_survives_reload Opfs/dispose` | `Expected marker m1 after reload, found ''` | #289 | |
| BW1 | always | `Typed_write_survives_reload IndexedDb/checkpoint=False/indexed=False` | `Expected t1 after reload, found ''` | #289 | the same through a typed context and SaveChangesAsync |
| BW1 | always | `Typed_write_survives_reload Opfs/checkpoint=False/indexed=False` | `Expected t1 after reload, found ''` | #289 | |
| BW1 | sometimes | `Large_database_open_time_and_memory IndexedDb/5MB` | `but the reopened database holds 0` | #289 | CheckpointAsync returns in 0 ms while a background checkpoint runs; 5 MB then reopens empty in half the runs |
| BW1 | sometimes | `Large_database_open_time_and_memory Opfs/5MB` | `but the reopened database holds 0` | #289 | |
| BW2 | always | `Typed_context_with_async_linq InMemory/indexed=True` | `Cannot build index 'idx_Value'` | #290 | a typed context with a declared index cannot open: the index fill blocks on a thread-pool task |
| BW2 | always | `Typed_write_survives_reload IndexedDb/checkpoint=True/indexed=True` | `Cannot build index 'idx_Value'` | #290 | the same on IndexedDB |
| BW3 | always | `Typed_sync_linq_operator InMemory/*` | `Cannot wait on monitors on this runtime` | #290 | every synchronous LINQ terminal operator throws PlatformNotSupportedException |
| BW3 | always | `Blql_sync_operator InMemory/tolist` | `Cannot wait on monitors on this runtime` | #290 | BLQL ToList throws the same; Count, FirstOrDefault and Any pass only because nothing yields |
| BW4 | always | `Dispose_returns_promptly` | `Dispose blocked the browser thread for` | #290 | every Dispose waits 5 s for a writer task that can only run on the waiting thread |
| BW5 | always | `Two_tabs_writing_the_same_database_keep_both_writes IndexedDb` | `A fresh tab should see both writes, found 'b'` | #291 | two tabs on one database: the first tab's checkpointed write is lost without an error |
| BW5 | always | `Two_tabs_writing_the_same_database_keep_both_writes Opfs` | `Access Handles cannot be created if there is another open Access Handle` | #291 | on OPFS the second tab cannot open the database |
| BW6 | always | `Large_database_open_time_and_memory IndexedDb/50MB` | `System.OutOfMemoryException: Out of memory` | #292 | a 50 MB database cannot be reopened: the WAL is read as one JSON string of base64 records |
| BW6 | always | `Large_database_open_time_and_memory Opfs/50MB` | `System.OutOfMemoryException: Out of memory` | #292 | |
| BW7 | always | `IsAvailable_before_any_other_call IndexedDb` | `ES6 module ./blite-indexeddb.mjs was not imported yet` | #290 | IsAvailable() as the first call kills the .NET runtime: its module is not imported yet |
| BW7 | always | `IsAvailable_before_any_other_call Opfs` | `ES6 module ./blite-opfs-bridge.mjs was not imported yet` | #290 | |
| BW8 | always | `Package_ships_the_js_modules` | `ships none of the JS modules the interop imports` | #293 | the NuGet package contains only the assembly, none of the four .mjs modules |
| BW9 | always | `Typed_context_opens_through_the_public_storage_types Opfs` | `IndexedDB not open for` | #293 | no typed-context factory; on OPFS the public types alone cannot open the WAL |
