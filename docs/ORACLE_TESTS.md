# Oracle tests

How BLite is checked against an exact reference over many generated cases at once, what proves
that those checks can fail, how a known divergence is tracked until it is fixed, and how the
coverage grows as the feature set grows. The code lives under `tests/BLite.Tests/Oracles/`.

## Why they exist

Most of the fixes that landed in October 2026 (#164 to #243) were index, LINQ and WAL correctness
bugs found one at a time through issues: a unique index not enforced on typed collections (#176),
string range bounds matching every longer value with the same prefix (#206), `Take`, `Skip` and
`OrderBy` applied in a different order than written (#230), an AND dropping one of its bounds on a
single index (#223), B-Tree pages freed before the transaction committed (#233). Each of these is a
case where the engine's answer differs from the answer of a simpler, exact reference: the same
query over the same documents with LINQ-to-Objects, or a dictionary that records what the collection
should contain.

An oracle test checks the engine against such a reference, not against a hand-written expected
value, and does so over many generated cases in one run. A whole class of divergence is then found by
one test, rather than one issue per instance. The first run of the differential LINQ test found 14
query shapes diverging from LINQ-to-Objects on upstream `0450b97`, in four families, from one test
file; `tests/BLite.Tests/Oracles/Seeds/candidate-issues.md` has the drafts.

## What it is

Three mechanisms, each in one file, plus two commit trailers.

### Oracle tests (`tests/BLite.Tests/Oracles/*Tests.cs`)

A test that compares BLite against an exact reference. Two kinds exist today, in three tests.

- **Differential**, `DifferentialQueryTests.cs`: a catalogue of query shapes runs over the same seeded
  documents on a collection with a B-Tree index on every queried property
  (`DiffIndexedDbContext`), on the same collection with no index (`DiffPlainDbContext`), each sync
  and async, and once with LINQ-to-Objects over the in-memory list, which is the oracle. Unordered
  queries must agree as id sets, ordered ones as key sequences, terminals as values or as the
  exception type. The entities and contexts are in `tests/BLite.Shared/DifferentialContexts.cs`.
- **Model-based**, `StoreModelTests.cs`: a seeded random sequence of Insert, Update, Delete, Upsert,
  InsertBulk, DeleteBulk, FindById, Begin, Commit, Rollback, Checkpoint, Backup and reopen runs
  against a typed collection and against an in-memory dictionary that keeps a committed and a
  pending view. After every step the collection's committed contents (full scan, count, point
  lookups, both secondary indexes) must equal the committed view, and reads through an open
  transaction must equal the pending view. A failing sequence is shrunk and printed as the shortest
  sequence that still fails. The entity is in `tests/BLite.Shared/StoreModelContexts.cs`.
- **Model-based with page accounting**, `BTreeModelTests.cs`: a seeded random sequence of inserts
  (batches, long runs of one duplicate key), deletes (single and consecutive runs), commits,
  rollbacks, checkpoints and reopens runs against a `BTreeIndex` over a `StorageEngine` on 4 KB pages
  and against a sorted dictionary keyed by `IndexKey` in the index's own order. After every step each
  query primitive (TryFind, CountRange, Range both ways, Equal, In, Between, GreaterThan, LessThan,
  StartsWith, Like, sync and async, cursor walks and windows) must answer what the model answers, and
  a cycle-safe walk of the tree must find it sound. After every commit, rollback, checkpoint and
  reopen, the reachable pages plus the free list plus the queued frees must be exactly the pages
  allocated since the index was created. Each key generator (int, long and double extremes, prefix
  strings, control bytes, keys up to the size limit, unique, multi-process) times each check is a
  row of its own, so one defect does not hide another; a failing row is shrunk for that check.

Other kinds fit BLite and are not written yet: round trip (a document through the BSON writer and
reader, a page through encryption and back), differential BLQL against LINQ on a `DynamicCollection`,
`DynamicCollection` against the typed collection for the same documents and queries, and fault
injection at the WAL boundary (a write cut at every offset, then reopen and compare with the model).

### Seeds (`tests/BLite.Tests/Oracles/Seeds/*.patch`)

A test that passes with and without a defect is not a guard. So every oracle test ships at least one
seed: a patch against `src/` only that breaks what the test checks, named
`<TestClass>.<defect>.patch`, with the defect and the failures it causes described above the diff.
The seed is made by reverting a known fix or by hand, and the test is shown to fail with it applied.

- `OracleSeedsTests.cs` holds in CI without a build: every `*Tests.cs` under `Oracles/` has a seed,
  every seed changes files under `src/` only, and every seed still applies to the current tree. A
  refactor that strands a seed fails CI instead of leaving the oracle unproven.
- `tools/oracle-seeds.cs` proves locally that each seed is caught: it applies the seed in a scratch
  git worktree of HEAD under the OS temp directory, builds `tests/BLite.Tests`, runs the test class
  and requires at least one failure that is not a labelled candidate. One `CAUGHT`, `MISSED` or
  `ERROR` line per seed; exit code 1 unless every seed is caught. It is not part of CI.

### Candidate registry (`tests/BLite.Tests/Oracles/Candidates.cs`)

A divergence that is found but not yet fixed is listed in the test's `Candidates` with a family
label and the issue that tracks it. The rule has three outcomes for a listed row:

- it still disagrees and its issue is **filed**: the row passes and writes
  `SKIPPED [known candidate: <family>, <issue>] <disagreement>` to the test output (xunit 2 has no
  dynamic skip, so this line is the trace a run shows);
- it still disagrees and its issue is **empty**: the row keeps failing with
  `[known candidate: <family>]`, so nothing is quarantined before it is filed;
- it **agrees**: the row fails with `candidate fixed: remove the entry for '<name>'`, so the fix's
  PR removes the entry and the registry cannot outlive the defect.

A `Fact` per oracle test fails on an entry that names no row, for example after a row is renamed. So
the suite stays green, the known divergences stay visible and linked to their issues, and a fix is
forced to clean up after itself.

### Commit trailers (`CONTRIBUTING.md`)

Fix commits carry `Found-By:` (`RV` review, `OT` oracle test, `IS` issue report, `US` user, `CA`
analyst) and `Defect-Class:` (`IDX`, `WAL`, `TXN`, `BSON`, `LINQ`, `MP`, `PLT`), so `git log` alone
answers which finder found what, and in which class.

## How to use it

**Run the oracle tests.**

```bash
dotnet test tests/BLite.Tests --filter FullyQualifiedName~BLite.Tests.Oracles
```

`BLITE_DIFF_SEED=<n>` changes the documents of the differential test; the seed is part of every
failure message so a run can be repeated.

**Read a failure.** A differential failure names the seed, the query (`query 'nick == null'`) and,
per line, the engine that disagreed: `indexed/sync`, `indexed/async`, `plain/sync`, `plain/async`.
If only the indexed lines appear, an index changes the answer; if the plain lines appear too, the
evaluator itself is wrong. A model-based failure names the seed, the step that failed and the
shortest failing sequence, one operation per line; replaying that sequence by hand is the repro.

**Triage, in this order.**

1. Is the oracle or the model wrong? LINQ-to-Objects is rarely wrong, but the comparison can be:
   check the `Compare` rule of the case (set, sequence or scalar) and, for the model, the contract
   it encodes (the `<remarks>` of `StoreModelTests`).
2. Do indexed and plain agree with each other but differ from the reference? Then the question is
   one of semantics, to settle against `README.md` before anything else: the documented behaviour
   may be the intended one (typed collections not indexing null keys is an example).
3. Only the indexed path diverges? That is a bug with no further question: the index serves a
   different answer than a scan of the same documents.

**Register a candidate and file its issue.** Add the row's name under a family label in the test's
`Candidates` with an empty issue; it keeps failing with the label. Write the draft (rows, repro,
expected, actual, hypothesis with code references), file it, put the number in the entry. From then
on the row is reported as skipped with the label and the issue.

**Remove the entry when the fix lands.** The row then fails with `candidate fixed: remove the entry
for '<name>'`; delete it in the same PR as the fix, and add the `Found-By: OT` trailer to the commit.

**Add a new oracle test.**

1. Pick the oracle kind (differential, model-based, round trip, fault injection) and write
   `tests/BLite.Tests/Oracles/<Name>Tests.cs` in namespace `BLite.Tests.Oracles`, with entities in
   `tests/BLite.Shared/`, seeded and deterministic, with the seed and the row's name in every failure.
2. Make the seed: `git revert --no-commit <fix>` restricted to `src/`, or a hand-made defect. Run
   the test, confirm it fails, and save `git diff src/` as
   `tests/BLite.Tests/Oracles/Seeds/<Name>Tests.<defect>.patch` with a header that describes the
   defect and the failures.
3. Run `dotnet run tools/oracle-seeds.cs <Name>` and commit the test and the patch together.

## How detection grows with the feature set

The oracle tests are catalogues, and a feature extends a catalogue rather than adding a test. Each
addition is checked against everything already in the catalogue, which is why one new case can
expose an old defect.

- **A new LINQ operator or terminal** is one more `QueryCase` in the `BuildCatalogue` method of
  `DifferentialQueryTests.cs` (a `W(...)` predicate, a sequence case or a scalar case). It runs at
  once against every existing document shape, on the indexed and the plain collection, sync and
  async, with no new fixture.
- **A new indexable type or index kind** (composite, nested, descending, full-text, spatial) is one
  more property on `DiffDoc` with a `.HasIndex(...)` in `DiffIndexedDbContext`, plus the predicates
  that use it. Every existing predicate shape then also runs over the new index.
- **A new index key type or key shape** is one more key generator (a `Scenario` in
  `BTreeModelTests.cs`: how to draw a value, and its special values). Every check, the page
  accounting and the key-order check against the type's natural comparison then run over it.
- **A new collection operation or storage feature** (a transaction mode, a maintenance command, a
  multi-process behaviour) is one more `OpKind` in `StoreModelTests.cs` with its effect on the model's
  committed and pending views. From then on it appears in every generated sequence, interleaved with
  every other operation, in single- and multi-process mode.
- **A new serialization or encryption option** is a new round-trip oracle test with its own seed,
  after which every later option is one more case in it.

What the tests do not see: targets other than `net10.0` (`netstandard2.1`, Unity, MAUI; the test
project runs on `net10.0` only), real multi-process races beyond the alternation the model drives,
performance, and anything whose correct answer has no exact reference, such as API design and
documentation drift. Those stay with review.
