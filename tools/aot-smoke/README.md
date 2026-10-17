# Native AOT smoke test

A consumer of BLite published with Native AOT, checked against two oracles: the same consumer
under JIT (the dumps must be byte-identical), and the publish log (no BLite warning for the APIs
AOT_LIMITATIONS.md calls safe, a warning at the call site for the ones it annotates). Local only:
a publish needs clang and a linker, and is not part of CI. `docs/ORACLE_TESTS.md` places it among
the oracle tests.

```bash
tools/aot-smoke/run.sh                 # seed 1, 300 steps
tools/aot-smoke/run.sh --seed 7 --steps 500 --keep
tools/aot-smoke/run.sh --prove-seeds   # every seed under seeds/ must make the run fail
```

## Requirements

- The .NET SDK of the repository (net10.0) and the ILCompiler pack for the host RID: restored from
  NuGet (`Microsoft.DotNet.ILCompiler` and `runtime.<rid>.Microsoft.DotNet.ILCompiler`) or shipped
  with a source-built SDK. `run.sh` detects the RID from `dotnet --info`.
- **clang and the platform linker.** macOS: the Xcode command line tools (`xcode-select --install`);
  `run.sh` links with `/usr/bin/clang`, the xcrun shim, because a `clang` earlier on `PATH`
  (swiftly, Homebrew llvm) or Xcode's own binary called directly does not pass the SDK and fails
  with `ld: library 'dl' not found`. Linux: `clang` and the zlib development package. Windows: the
  MSVC build tools, and `run.sh` needs a bash (WSL or Git Bash); it has not been run there.
- With the Homebrew build of the SDK (`/opt/homebrew/Cellar/dotnet`), its NativeAOT runtime links
  against Homebrew's OpenSSL and Brotli without passing their library directories (`ld: library
  'ssl' not found`); `run.sh` sets `LIBRARY_PATH` to `$(brew --prefix)/opt/{openssl,brotli}/lib`
  for the publish. Nothing is installed.

A missing prerequisite prints a `MISSING` line and exits 2.

## What it runs

`Consumer/` (net10.0, `PublishAot`, `TrimMode=full`, trim, AOT and single-file analyzers on,
`TrimmerSingleWarn=false` so every warning is itemised, warnings not errors, invariant
globalization) references `src/BLite`, `BLite.Core`, `BLite.Bson`, `BLite.Caching` and the source
generator, and runs one section per feature against fresh databases under a temp directory:

| Section | APIs |
|---|---|
| typed collection: keys from the generated mapper | a source-generated context as a consumer writes it: insert, bulk insert, update, scan |
| typed collection: store sequence | the 300-step sequence of `StoreModelTests` (insert, update, delete, upsert, bulk insert and delete, FindById, Begin, Commit, Rollback, Checkpoint, Backup, reopen) against the store model, verified after every step through FindAllAsync, CountAsync, QueryIndexAsync on the model-declared indexes and ScanAsync with a static delegate; then every document, every index entry both ways, a paged index range, FindById over the id pool, checkpoint and a final backup |
| encryption at rest: store sequence | the same sequence (seed + 1, 150 steps) on an AES-GCM encrypted engine |
| encryption at rest: marker, reopen, wrong passphrase | the sequence with every transaction committed, a marker document, the files searched for it in plaintext, a backup, a reopen with the passphrase and one with a wrong one |
| dynamic collection: CRUD, index, CDC, BLQL | `BLiteEngine`, `DynamicCollection` insert, bulk insert, update, upsert, delete, a unique index violation, a rolled-back and a committed transaction, `Watch` and `SubscribeToChanges` (every event in order), QueryIndexAsync, ScanAsync, BLQL with a JSON filter, typed filters, sort, skip, take and projection, checkpoint, backup, reopen |
| key-value store | Set, Get, Delete, Exists, Refresh, Batch, PurgeExpired, ScanKeys, reopen |
| BLite.Caching | `BLiteDistributedCache` with byte arrays and with the typed helpers |

The generator of the store sequence is a verbatim copy of `StoreModel.Generate`
(`Consumer/StoreModelGenerator.cs`): the test project's runner verifies through LINQ, which is not
AOT-safe, so it cannot be linked. `run.sh` checks the copy against the source on every run.

Every section catches its own exception and writes it to the dump as a line starting with `!!`,
so a feature that fails under AOT does not hide the others; the exit code is then 1.

The typed sections other than the first register the mapper's BSON keys on the engine before the
context opens (`StorageEngine.RegisterKeys`), the workaround for candidate A1: without it every
typed write fails under AOT and nothing of the sequence would be compared.

`Consumer.Linq/` compiles the same sources plus `LinqCalls.cs`: one `AsQueryable().Where(...)`,
one `CreateIndexAsync`, one typed `Watch`. Each line carries a marker, `// aot-expect: IL2026
IL3050`, naming the warnings it must get. It keeps the default `TrimmerSingleWarn`, so a BLite
assembly with warnings outside its annotations would show up as an IL2104/IL3053 rollup.

## Verdicts

One line per check; `KNOWN` lines are registered candidates, which do not fail the run.

```
SAME      generator: Consumer/StoreModelGenerator.cs matches tests/BLite.Tests/Oracles/StoreModel.cs
SAME      dump: JIT and Native AOT dumps are byte-identical            (or: every difference is a registered candidate)
DIFFERENT dump section '<name>': ...                                   a new divergence between JIT and AOT
FAIL      both builds: <section>: <exception>                          a section that fails under JIT and AOT alike
KNOWN     Consumer publish: N BLite warnings, expected 0; all known candidates: A1 x2, ...
SAME      call site LinqCalls.cs:19 AsQueryable: IL2026 IL3050 present
SAME      Consumer.Linq: no IL2104/IL3053 rollup for a BLite assembly
KNOWN     Consumer.Linq additional: N BLite warnings, expected 0; ...  warnings inside BLite that only the annotated calls add
PASS      no new finding                                               exit 0; otherwise FAIL and exit 1
```

Warnings are ILC's (`Trim analysis warning`, `AOT analysis warning`), deduplicated and attributed to
the assembly of their source file: `src/<project>/...` is that project, the generated
`*.Mappers.g.cs` in the consumer's `obj/` is `BLite.SourceGenerators (generated into the consumer)`,
anything else in `tools/aot-smoke/` is the consumer. `run.sh` removes the consumers'
`obj/Release/net10.0/<rid>/native` before each publish: ILC prints warnings only when it runs, and
an incremental publish skips it.

`--keep` leaves the work directory (both dumps, both publish logs, the warning lists) in place.

## Result on upstream HEAD

`test/oracle-aot` (the tool as of `3dd43d0`, upstream `0450b97`, v5.2.3, plus the oracle-test
branches), osx-arm64, .NET SDK 10.0.401 (Homebrew), ILCompiler 10.0.12, Apple clang 21, 2026-10-17:

```
SAME    generator: Consumer/StoreModelGenerator.cs matches tests/BLite.Tests/Oracles/StoreModel.cs
KNOWN   dump section 'typed collection: keys from the generated mapper' differs [known candidate: A1]: !! typed collection: keys from the generated mapper: System.InvalidOperationException: BSON Ke...
SAME    dump: every difference between the JIT and the AOT dump is a registered candidate
KNOWN   both builds [known candidate: A2]: encryption at rest: store sequence: System.Security.Cryptography.AuthenticationTagMismatchException: The computed authentication tag did not match the inp...
KNOWN   both builds [known candidate: A5]: BLite.Caching: System.InvalidOperationException: Reflection-based serialization has been disabled for this application. Either use the source generator AP...
        Consumer publish warnings by assembly: BLite.Caching=4, BLite.Core=4, BLite.SourceGenerators (generated into the consumer)=2 (total 10)
KNOWN   Consumer publish: 10 BLite warnings, expected 0; all known candidates: A1 x2, A3 x2, A4 x2, A5 x4
SAME    call site LinqCalls.cs:19 AsQueryable: IL2026 IL3050 present
SAME    call site LinqCalls.cs:22 CreateIndexAsync: IL3050 present
SAME    call site LinqCalls.cs:25 Watch: IL2026 present
SAME    Consumer.Linq: no IL2104/IL3053 rollup for a BLite assembly
        Consumer.Linq additional warnings by assembly: BLite.Core=11 (total 11)
KNOWN   Consumer.Linq additional: 11 BLite warnings, expected 0; all known candidates: A6 x10, A7 x1
        timings: JIT build 4.6 s, JIT run 1.8 s, AOT publish 5.9 s, AOT run 0.9 s, Consumer.Linq publish 6.1 s
        exit codes: JIT 1, AOT 1 (1 = a section failed, see the dumps)
PASS    no new finding
```

The 300-step typed sequence, the encrypted marker section, the dynamic collection with its 66
`Watch` and 63 `SubscribeToChanges` events, BLQL and the key-value store are byte-identical under
JIT and AOT (871 dump lines). A run takes about 20 s with warm build caches, a publish about 6 s
(ILC included); `--prove-seeds`, which builds everything in a fresh worktree per seed, 37 s for
both. The AOT binary is 6.5 MB.

The seven candidates are in [`candidates.md`](candidates.md), filed as #273 (A1), #274 (A2),
#275 (A3, A4, A6, A7) and #276 (A5): A1, typed collections cannot
write under Native AOT (the generated mapper reports no BSON keys); A2, an encrypted database
cannot read a page that a never-committed transaction allocated (both builds; it also makes such a
database unopenable after a close with an open transaction); A3, every generated DbContext warns
in its generated code; A4, `ValueConverterRegistry.Empty`; A5, BLite.Caching's typed helpers; A6,
LINQ internals warning inside BLite.Core; A7, typed `Watch` annotated but undocumented. Six of
the seven come from `#pragma warning disable IL...` in BLite.Core, which keeps the library's own
analyzer quiet and ILC not.

Two observations that are not candidates: `BLiteEngine.GetCollection(name)` returns null for a
collection that exists in the file but was not opened in this process (`ListCollections` or
`GetOrCreateCollection` loads it); and `IBLiteKvStore.PurgeExpired()` counts every entry the page
compaction drops, deleted keys and superseded values included, not only expired ones (4 in the
Consumer's sequence, where nothing has expired).

## Seeds

`seeds/*.patch` are defects that the tool must catch, in the header format of
`tests/BLite.Tests/Oracles/Seeds`. `run.sh --prove-seeds [filter]` applies each in a scratch git
worktree of HEAD under the temp directory, runs `run.sh` there and prints `CAUGHT` (the run
failed), `MISSED` (it passed) or `ERROR` (the build or publish failed, which proves nothing);
exit 1 unless every seed is caught. `tools/oracle-seeds.cs` runs a unit-test class per seed and
cannot run these; `OracleSeedsTests` checks only `tests/BLite.Tests/Oracles/Seeds`.

Result on `7f13eb2`, osx-arm64, 2026-10-17:

```
CAUGHT  aot-smoke.create-index-unannotated.patch: FAIL    call site LinqCalls.cs:22 CreateIndexAsync: expected IL3050, absent;
        FAIL    Consumer.Linq additional: 12 BLite warnings, expected 0; 1 not registered; known: A6 x10, A7 x1;
        FAIL    2 check(s) failed
CAUGHT  aot-smoke.index-key-reflection.patch: FAIL    Consumer publish: 11 BLite warnings, expected 0; 1 not registered; known: A1 x2, A3 x2, A4 x2, A5 x4;
        FAIL    1 check(s) failed
```

- `aot-smoke.create-index-unannotated.patch` removes `[RequiresDynamicCode]` from
  `DocumentCollection.CreateIndexAsync` and its interface declaration and silences the analyzer
  around the inner call, so BLite still builds. The call-site check fails, and the warning appears
  inside BLite.Core instead.
- `aot-smoke.index-key-reflection.patch` sends `int` index keys through `MakeGenericMethod` in
  `CollectionSecondaryIndex.ConvertToIndexKey`, with the analyzer silenced. The BLite warning count
  fails; the dump stays the same, because `IndexKey.Create<int>` is compiled into the binary anyway.

## Candidates

`candidates.md` holds the triage and, at its top, the registry table `run.sh` reads: a row names a
candidate, a kind (`warning`, `linq-warning` for the warnings only the annotated calls add, or
`dump` for a failed section) and a substring of the warning or of the `!!` line. A match prints
`KNOWN ... [known candidate: A<n>]`. A section that a `dump` row matches is not compared beyond its
failure, so remove the candidate's rows in the PR that fixes it, and the section is compared again.

## How it grows

- **A new public API** is one more call in the Consumer, in the section of its feature or in a
  new one, with what it returns written to the dump. If AOT_LIMITATIONS.md calls it safe, or does
  not mention it, it belongs in `Consumer/`; if it is annotated, in `Consumer.Linq/LinqCalls.cs`.
- **A new annotation** is one more call in `LinqCalls.cs` with an `// aot-expect: IL....` marker
  naming the codes; `run.sh` reads the markers.
- **A new storage operation** in `StoreModel` reaches the Consumer when the generator is copied
  again: `run.sh` fails with `generator ... drifted` until it is.
- **A fix** removes its rows from the registry table.

## What it does not see

IL2CPP (Unity), Mono AOT and MAUI targets, `browser-wasm` (WASM_SUPPORT.md covers it separately;
the Consumer's sections could run there later over `MemoryPageStorage`), `netstandard2.1` builds,
other RIDs than the host's, and runtime behaviour that both builds share (A2 was found only
because the sequence ran on an encrypted engine, not because of AOT). A dump section that a
registered candidate breaks is not compared beyond the failure.
