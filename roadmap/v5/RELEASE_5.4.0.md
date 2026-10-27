# BLite 5.4.0 — Release Plan

> Date: October 27, 2026  
> Milestone: [v5.4.0](https://github.com/EntglDb/BLite/milestone/5)

---

## Theme

A correctness release. Concurrent transactions stop losing writes, sessions and lock defaults behave as documented, a commit can be made durable against power loss, and queries return the same answer with or without an index.

No breaking API. The new public surface is additive: `BLiteTransactionConflictException`, `BLiteSession.RollbackAsync(CancellationToken)`, and a WAL sync mode. Dynamic (schema-less) indexes change key format and are rebuilt automatically on open, as typed indexes were for #252 in 5.3.0.

---

## Decisions

| Topic | Decision |
|-------|----------|
| Write-write conflicts (#304, PR #310) | In 5.4.0. An explicit `CommitAsync` that loses a conflict throws `BLiteTransactionConflictException` and must be retried by the caller; auto-commit writes retry internally. Called out first in the CHANGELOG. |
| WAL durability (#260, PR #280) | Default commit latency unchanged; per-commit fsync is opt-in. Independently of the mode, the WAL is synced before a freed page reaches the page file and before the WAL is truncated. Each mode documents what it survives (process crash vs. power loss). `LockTimeout.Default` stays 500 ms. |
| Stalled PRs #280, #253, #302 | Finished by the maintainers on new branches built on the author's commits; the originals are closed as superseded. |
| Dynamic index keys | #254 and #269 together, one rebuild on open. #234 (non-unique dynamic keys) stays in v6. |
| BLQL range semantics (#267) | Same type class only, as MongoDB: a numeric bound matches numbers (Int32, Int64, Double, Decimal128 compare with each other), a string bound matches strings; null and other types never match a range. |
| BLite.Wasm (#289–#293) | Out of 5.x; all moved to v6. |

---

## Work, in order

### 1. Release pipeline

The last *Publish to NuGet* run on `main` failed on timing tests (`MetricsTests.WatchMetrics_EmitsSnapshots`, a PageFile write-lock timeout in `HnswGraphQualityTests`), and every red PR check seen during triage was a timeout of the same kind.

- [ ] Replace #302: one shared load-tolerant lock timeout for the test suite instead of per-test values; cover `WatchMetrics_EmitsSnapshots`, `VacuumTests` Spatial_FailedRebuild, `CrossCollectionWriteRaceTests`; the readers' "every reader did work" check must not pass on a single read after the writers finish.

### 2. Ready to merge (review points addressed 2026-10-19)

- [ ] #318 — GdprMode.Strict warning text (#299)
- [ ] #316 — `BLiteSession.Rollback()` awaits the rollback; `RollbackAsync(ct)` (#307)
- [ ] #319 — schema-less API reads typed overflow documents (#301)
- [ ] #326 — README: full-text search (docs only, no review yet)
- [ ] #327 — one index key for every NaN, with key migration (#257); supersedes #282, which is to be closed

### 3. Concurrency and durability

- [ ] #310 — write-write conflict detection (#304); review points addressed, CI red only on `WatchMetrics_EmitsSnapshots`
- [ ] #325 — collection-per-file mode: concurrent explicit transactions corrupt the free-space index (not covered by #310)
- [ ] #305 — engine and session hold separate `DynamicCollection` instances
- [ ] #322 — `PageFileConfig` object initializer gets `LockTimeout.Immediate`
- [ ] Replace #280 — WAL sync mode (#260), per the decision above

### 4. Query correctness

- [ ] Replace #253 — typed indexes: decline `== null`, `Contains(null)`, and orderings only on properties that can hold null (#247); a non-nullable string `OrderBy(...).Take(n)` stays index-served
- [ ] #254 + #269 — dynamic index key encoding (order-preserving doubles, numeric bounds across BSON types, Decimal128 and Boolean keyed), rebuilt on open
- [ ] #267 — BLQL range semantics, after #269
- [ ] #265 — BLQL compares ObjectIds as `ObjectId.CompareTo`
- [ ] #266 — `Query().Skip(n).Any()` ignores Skip (First/FirstOrDefault already fixed by #284)

### 5. Release

- [ ] CHANGELOG: conflict exception and retry first; dynamic index rebuild on first open; WAL sync modes.
- [ ] Bump to 5.4.0.

---

## Not in 5.4.0

- **v6 milestone**: BLite.Wasm #289, #290, #291, #292, #293; #320, #234, #212, #190, #124, #20.
- **Unscheduled, low severity** (no user-visible wrong result): #262, #258, #237 (space leaks), #255 (internal B-tree primitives, no query path), #275 (AOT warnings), #264 (BLQL dotted paths, a missing feature), #220, #188, #226 (multi-process follow-ups), #297 (LiteDB migration docs).
