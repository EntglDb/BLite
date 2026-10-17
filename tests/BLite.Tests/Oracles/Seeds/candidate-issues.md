# Candidate issues from the differential LINQ test

Status: **filed 2026-10-17 as #247 (drafts 1 and 4), #248 (draft 2) and #249 (draft 3).** Triage of the 14 rows that `DifferentialQueryTests` reports as
disagreeing with LINQ-to-Objects on upstream `0450b97` (v5.2.3). Each family has a standalone
repro below (three or four documents, the unindexed collection as control) and a hypothesis
checked against the code.

All four repros fail on `0450b97`; the controls on the plain collection pass except in family 2,
where the predicate itself is wrong and no index is involved. The repros were run as Facts over
`DiffIndexedDbContext` / `DiffPlainDbContext` from `tests/BLite.Shared/DifferentialContexts.cs`;
they are not part of the test suite, the differential rows listed under each family are.

Open issues checked on 2026-10-16 (`gh issue list --repo EntglDb/BLite --state open`): none of
#237, #234, #226, #220, #212, #210, #190, #188, #124, #20 covers any of the four families. The
closest closed ones are #150 (decimal index keys) and #171 (index on an existing collection stays
empty); neither is about null keys, `!=` or signed zero. #212 (keys not prefix-free) touches the same
`IndexKey` encoding as family 3 but a different defect.

Affected versions below are read from `git describe --contains` of the commit that introduced each
code path; the tests were only run on `0450b97`.

**Registry entries.** Each draft is one family of `s_candidates` in
`tests/BLite.Tests/Oracles/DifferentialQueryTests.cs`; its second argument is the issue and is empty
until filed. Once an issue exists, put its number there (for example `"#250"`) and the family's rows
are reported as skipped instead of failing:

| Draft | Registry family label | Rows |
|---|---|---|
| 1 | `null equality on an indexed property finds nothing` | 9 |
| 2 | `!= excludes null values` | 2 |
| 3 | `-0.0 is not found by an equality on an indexed double` | 2 |
| 4 | `indexed top-N window skips documents with a null key` | 1 |

Drafts 1 and 4 share a root cause; if they are filed as one issue, both entries get the same number.

---

## 1. `== null` on an indexed nullable property of a typed collection finds nothing

**Rows:** `nick == null`, `maybe == null`, `maybe == captured null int?`, `Count(nick == null)`,
`Count(maybe == null)`, `nick == null and flag`, `nick == null or nick == a`,
`maybe in list with null`, `Where(maybe == null).First()`.

**Repro:**

```csharp
// DiffDoc has string? Nick and int? MaybeCount, both with .HasIndex(...) in DiffIndexedDbContext.
await ctx.Docs.InsertBulkAsync([
    new DiffDoc { Name = "d1", Nick = null, MaybeCount = null },
    new DiffDoc { Name = "d2", Nick = "a",  MaybeCount = 5 },
    new DiffDoc { Name = "d3", Nick = "b",  MaybeCount = null }]);

ctx.Docs.AsQueryable().Where(x => x.Nick == null).ToList();        // expected [d1],     actual []
ctx.Docs.AsQueryable().Where(x => x.MaybeCount == null).ToList();  // expected [d1, d3], actual []
ctx.Docs.AsQueryable().Count(x => x.MaybeCount == null);           // expected 2,        actual 0
```

**Expected:** the documents whose property is null, as LINQ-to-Objects and as the same query on a
collection without the index return them.

**Actual:** an empty result (and 0 for `Count`, `InvalidOperationException` for `First`). Every shape
that routes the null comparison through the index is affected: AND/OR combinations, `Contains` on a
list that holds a null, and the aggregate push-downs.

**Hypothesis (confirmed):** the planner and the indexer disagree about whether typed collections
have a null bucket.

- `CollectionSecondaryIndex.Insert` skips a document whose key is null
  (`src/BLite.Core/Indexing/CollectionSecondaryIndex.cs:118`, "Skip null keys or failed
  traversal"); `Update` and `Delete` do the same. No typed index ever contains
  `IndexKey.NullSentinel`.
- `IndexOptimizer.OptimizeExpression` plans `x.Prop == null` on an indexed property as an exact
  point lookup with `MinValue = MaxValue = DBNull.Value` and `FilterCompleteness.Exact`
  (`src/BLite.Core/Query/IndexOptimizer.cs:293-304`), and `CollectionSecondaryIndex.ConvertToIndexKey`
  maps `DBNull` to `IndexKey.NullSentinel` (`CollectionSecondaryIndex.cs:776`); a null element of a
  `Contains` list is normalised to `DBNull` the same way (`IndexOptimizer.cs:894`). The lookup scans an
  empty bucket, and because the plan is marked exact no post-filter runs.
- `DynamicCollection` does write the bucket (`DynamicCollection.cs:2720`, `BsonType.Null =>
  IndexKey.NullSentinel`), so the same query on a schema-less collection works. The README states
  the difference explicitly in the unique-index section (README.md:880, added 2026-10-13 with #176:
  "typed collections do not index `null` keys"), and `UniqueIndexTests.Typed_Unique_Index_Ignores_Null_Values`
  pins it.

**Affected versions:** the null-skip at insert dates from `fd5f9e8` (2026-03-06); the planner has
answered `== null` from the null bucket since `c2ba69a` (#34 "Null-sentinel indexing", 2026-04-04,
first in v4.1.3). Reproduced on v5.2.3 / `0450b97`.

**Fix considerations (for the discussion, not a proposal):**

- Either typed indexes write `NullSentinel` for null keys (as `DynamicCollection` does), or the
  optimizer stops using the index for null equality on typed collections (fall back to the BSON
  predicate, which already handles `BsonType.Null`, see `CreateNullCheckPredicate`).
- Writing nulls has side effects to decide on: a typed unique index would then reject a second null
  (the behaviour #176 documented as the opposite), existing indexes would need a rebuild to pick up
  the null-keyed documents already stored, and `CollectionSecondaryIndex.Range(null, max)` uses
  `IndexKey.MinKey` as the open lower bound (`CollectionSecondaryIndex.cs:448`), which would then
  include the null bucket in `x.Prop < v` and `x.Prop <= v`. `DynamicCollection` avoids that with
  `NullSentinelNext` (`DynamicCollection.cs:1065`); the typed path would need the same.
- Family 4 has the same root cause and would need the same decision.

---

## 2. `x.Prop != constant` does not match documents whose property is null

**Rows:** `maybe != 5`, `nick != ab` (indexed and unindexed collections agree with each other).

**Repro:**

```csharp
await ctx.Docs.InsertBulkAsync([
    new DiffDoc { Name = "d1", Nick = null, MaybeCount = null },
    new DiffDoc { Name = "d2", Nick = "ab", MaybeCount = 5 },
    new DiffDoc { Name = "d3", Nick = "a",  MaybeCount = 7 }]);

ctx.Docs.AsQueryable().Where(x => x.MaybeCount != 5).ToList();  // expected [d1, d3], actual [d3]
ctx.Docs.AsQueryable().Where(x => x.Nick != "ab").ToList();     // expected [d1, d3], actual [d3]
```

**Expected:** in C#, `(int?)null != 5` and `(string?)null != "ab"` are true, so d1 is part of both
results. LINQ-to-Objects over the same list returns `[d1, d3]`.

**Actual:** `[d3]` on both the indexed and the plain collection.

**Hypothesis (confirmed, mechanism slightly different from the first guess):** the BSON predicate
has no branch for a null field, not a wrong `NotEqual` branch.

- `BsonExpressionEvaluator.TryCompile` compiles `x.Prop != constant` to
  `CreatePredicate(field, value, ExpressionType.NotEqual)` (`src/BLite.Core/Query/BsonExpressionEvaluator.cs:308`).
- `Compare` switches on the stored BSON type (Int32, String, Int64, Double, Boolean, DateTime,
  ObjectId, Decimal128) and ends in `return false;` (`BsonExpressionEvaluator.cs:1223`). A nullable
  property is written as `BsonType.Null` by the generated mapper (`writer.WriteNull`), which matches
  no branch, so every operator yields false, including `NotEqual`. A field that is absent from the
  document takes the same `return false` at the end of `CreatePredicate` (`:905`).
- The optimizer never plans `NotEqual` through an index (`IndexOptimizer.cs:342-347`), which is why
  the indexed and the plain collection agree: both evaluate this predicate.
- `CreateNullCheckPredicate` (`:375`) treats null and absent alike and is correct for `== null` /
  `!= null`; only the constant comparison is affected.

**Affected versions:** the predicate compiler has had this shape since the project was renamed
(`4236d9e`, 2026-02-11), so in practice all releases. Reproduced on v5.2.3 / `0450b97`.

**Fix considerations:** `Compare` (and the generated-filter equivalent, if there is one) should
return `op == ExpressionType.NotEqual` when the stored type is `BsonType.Null`, and the absent-field
fall-through in `CreatePredicate` should do the same. The ordering operators keep returning false,
which matches lifted comparisons in C# (`null < 5` is false).

---

## 3. `x.Ratio == 0.0` through an index on a double misses documents storing `-0.0`

**Rows:** `ratio == 0.0`, `Count(ratio == 0.0)`.

**Repro:**

```csharp
await ctx.Docs.InsertBulkAsync([
    new DiffDoc { Name = "d1", Ratio = -0.0 },
    new DiffDoc { Name = "d2", Ratio = 0.0 },
    new DiffDoc { Name = "d3", Ratio = 1.0 }]);

ctx.Docs.AsQueryable().Where(x => x.Ratio == 0.0).ToList();  // expected [d1, d2], actual [d2]
ctx.Docs.AsQueryable().Count(x => x.Ratio == 0.0);           // expected 2,        actual 1
```

**Expected:** IEEE 754 equality, which C#, LINQ-to-Objects and the BSON predicate on the plain
collection all use, says `-0.0 == 0.0`.

**Actual:** only the `+0.0` document on the indexed collection.

**Hypothesis (confirmed):** `IndexKey(double)` is an order-preserving bit encoding
(`src/BLite.Core/Indexing/IndexKey.cs:94-105`): negative values flip all bits, positive values flip
the sign bit. `-0.0` has the sign bit set, so it encodes to `0x7FFF…FFFF` while `+0.0` encodes to
`0x8000…0000`. The two are adjacent but distinct keys, and the equality plan is an exact point lookup
on the key of `+0.0`. The same encoding is used for the stored key and for the query bound, so the
mismatch is deterministic. By the same encoding `x.Ratio >= 0.0` would exclude `-0.0` and
`x.Ratio < 0.0` would include it, both unlike C#; not verified by a test.

**Affected versions:** since the sort-preserving double encoding in `f83e0d5` (2026-03-17, first in
v3.7.0). Reproduced on v5.2.3 / `0450b97`.

**Fix considerations:** normalise `-0.0` to `+0.0` in `IndexKey(double)` (`if (value == 0) value = 0.0;`
before taking the bits), which fixes stored keys and query bounds together; existing indexes holding
a `-0.0` key would need a rebuild. NaN is a separate question (C# says `NaN != NaN`, the encoding
gives it one or several keys) and is not covered by the differential catalogue.

---

## 4. `OrderBy(x => x.Nullable).Take(n)` served by an index drops the documents whose key is null

**Rows:** `orderby maybe take 20 (nulls)`.

**Repro:**

```csharp
await ctx.Docs.InsertBulkAsync([
    new DiffDoc { Name = "d1", MaybeCount = 2 },
    new DiffDoc { Name = "d2", MaybeCount = null },
    new DiffDoc { Name = "d3", MaybeCount = 1 },
    new DiffDoc { Name = "d4", MaybeCount = 3 }]);

ctx.Docs.AsQueryable().OrderBy(x => x.MaybeCount).Take(4).Select(x => x.MaybeCount);
// expected [null, 1, 2, 3], actual [1, 2, 3]
```

**Expected:** LINQ orders null first for an ascending `OrderBy` on `int?`, so the window of four
over four documents is `[null, 1, 2, 3]`.

**Actual:** three documents, `[1, 2, 3]`; d2 is not in the result at all although the window is as
large as the collection. The plain collection returns the expected sequence.

**Hypothesis (confirmed):** same root cause as family 1, reached through the ordering fast path.

- `BTreeQueryProvider` serves `OrderBy(indexedField).Skip(s).Take(n)` without a `Where` by reading
  `skip + take` entries straight from the index (`src/BLite.Core/Query/BTreeQueryProvider.cs:290-311`,
  `QueryIndexAsync(index, null, null, ascending, skip, take)`), after `IndexOptimizer.TryOptimizeOrderBy`
  found a B-tree index on the key property.
- The typed index has no entry for a null key (family 1), so a null-keyed document can never appear
  in the window. The documents are not sorted last or first; they are absent.
- The replay variant of the same fast path (operators after the window, `:315-325`) reads the
  prefix from the same index and has the same gap.

**Affected versions:** the fast path dates from `f83e0d5` (2026-03-17, v3.7.0); the symptom needs
the typed null-skip, which predates it. Reproduced on v5.2.3 / `0450b97`.

**Fix considerations:** follows the decision in family 1. If typed indexes write `NullSentinel`,
an ascending scan yields the null bucket first and a descending scan last, which is LINQ's order
for both directions (`NullSentinel` sorts before every encoded value by construction,
`IndexKey.cs:17`). If they keep skipping nulls, `TryOptimizeOrderBy` has to decline when the key
property is nullable (reference type or `Nullable<T>`), or the window has to be merged with the
null-keyed documents found by a scan.

---

## Observations outside the four families (not verified, not for filing yet)

- `DynamicCollection.BsonValueToIndexKey` encodes doubles with `BitConverter.GetBytes`
  (`DynamicCollection.cs:2725`), which is little-endian and not order-preserving, unlike
  `IndexKey(double)` on the typed path. Range queries on a dynamic double index may be mis-bounded;
  the differential catalogue only covers typed collections.
- `Compare` returns false for a stored type that has no branch (e.g. a `BsonType.Int32` value against
  a `string` target after a schema change), for every operator including `NotEqual`. Same mechanism
  as family 2, different trigger.

---

# Candidate issues from the B-tree model test

Status: **filed 2026-10-17 as #255 (B1), #256 (B2), #257 (B3) and #258 (B4).** Triage of the rows that `BTreeModelTests` reports as disagreeing with its
model on upstream `0450b97` (v5.2.3), in four new families (B1 to B4) and two that are already
filed. Every repro below was run as a Fact on `0450b97` (macOS, Arm64, .NET 10); the repros are
not part of the test suite, the rows listed under each family are. The shortest failing sequences
are the ones the test prints when the entry's issue is empty.

What agrees on every key generator, including the bytes 0x00 to 0x03 inside string keys: Insert,
Delete, TryFind and TryFindAsync, CountRange, Range forwards, backwards and async, cursor walks in
both directions, cursor windows, reads without the open transaction (isolation), the free list and
the reachable pages (no page both reachable and free, none reachable twice). These are the paths
the collections use. #212 does not show at this level: a raw `BTreeIndex` key has nothing appended
to it, so string keys stay prefix-free; the defect needs the document-id suffix that
`CollectionSecondaryIndex.CreateCompositeKey` adds.

Open issues checked on 2026-10-17 (`gh issue list --repo EntglDb/BLite --state all`, searched for
growth, reopen, file grows, GreaterThan, cursor, duplicates, NaN): none covers B1 to B4. #210
(descending ties on a typed collection) is the sibling of B2 and #249 (-0.0) the sibling of B3;
#237 and #249 are the two filed families.

**Registry entries.** Each draft is one family of `s_candidates` in
`tests/BLite.Tests/Oracles/BTreeModelTests.cs`. A row is `"<key generator> / <check>"`:

| Draft | Registry family label | Rows |
|---|---|---|
| B1, #255 | `BTreeIndex cursor primitives drop duplicates and fail past the last key` | 55 |
| B2, #256 | `equal keys come back in reverse insertion order` | 8 |
| B3, #257 | `a NaN with the sign bit clear sorts above +Infinity` | 1 |
| B4, #258 | `a reopen strands the unwritten rest of the last growth block` | 9 |
| #237 | `pages a rolled-back transaction allocated are never reclaimed` | 9 |
| #249 | `-0.0 and +0.0 get different index keys` | 1 |

---

## B1 (#255). `BTreeIndex.Equal`, `In`, `GreaterThan`, `LessThan`, `Between` and `StartsWith` miss duplicates and fail past the last key

**Rows:** `<generator> / Equal`, `/ In` and `/ In.Async` (8 non-unique generators each),
`/ GreaterThan`, `/ LessThan`, `/ Between` (all 9), `/ StartsWith` (4 string generators).

**Repro:**

```csharp
using var storage = new StorageEngine(path, PageFileConfig.Default);
var index = new BTreeIndex(storage, IndexOptions.CreateBTree("k"));
var txn = storage.BeginTransaction().TransactionId;
index.GreaterThan(IndexKey.Create(28), false, txn).Count();   // empty index: throws InvalidOperationException "Cursor is not valid."
index.Insert(IndexKey.Create(28), new DocumentLocation(1, 1), txn);
index.Insert(IndexKey.Create(28), new DocumentLocation(1, 2), txn);
index.Insert(IndexKey.Create(30), new DocumentLocation(1, 3), txn);
var k = IndexKey.Create(28);

index.Range(k, k, IndexDirection.Forward, txn).Count();       // 2 (correct, the control)
index.Equal(k, txn).Count();                                  // expected 2,      actual 1
index.In([k], txn).Count();                                   // expected 2,      actual 1
index.GreaterThan(k, orEqual: false, txn);                    // expected [30],   actual [28, 30]
index.Between(k, IndexKey.Create(30), false, true, txn);      // expected [30],   actual [28, 30]
index.LessThan(k, orEqual: true, txn);                        // expected [28, 28], actual [28]
index.LessThan(IndexKey.Create(31), orEqual: false, txn);     // expected [30, 28, 28], actual []
index.GreaterThan(IndexKey.Create(31), false, txn);           // expected [], throws InvalidOperationException
index.Between(IndexKey.Create(31), IndexKey.Create(40), true, true, txn); // expected [], throws

var s = new BTreeIndex(storage, IndexOptions.CreateBTree("s"));
foreach (var (v, slot) in new[] { ("a\u0003b", 4), ("\0", 5), ("ab", 6) })
    s.Insert(IndexKey.Create(v), new DocumentLocation(1, (ushort)slot), txn);
s.StartsWith("\0", txn);                                      // expected ["\0"], actual ["\0", "a\u0003b", "ab"]
s.StartsWith("b", txn);                                       // expected [],     throws InvalidOperationException
```

**Expected:** every entry that satisfies the primitive, as `Range` over the same bounds returns
them: all copies of a duplicated key, nothing (not an exception) when no key qualifies, and an
ordinal prefix test for `StartsWith`, as BLQL's `$startsWith` documents ("Prefix match
(ordinal)", README.md:1036).

**Actual:** three mechanisms, all in the primitives that sit on one `IBTreeCursor.Seek`
(`src/BLite.Core/Indexing/BTreeIndex.cs:567-677`).

**Mechanism (confirmed):**

- *Duplicates.* `Equal` (`BTreeIndex.cs:567`) and `In` (`:665`) yield `cursor.Current` once per
  key after `Seek`, which positions on the first copy (`BTreeCursor.cs:115`); the comment in `Equal`
  says as much ("Assuming unique for now"). `GreaterThan` with `orEqual: false` (`:580`) and
  `Between` with `startInclusive: false` (`:623`) step over one copy with a single `MoveNext`, so the
  other copies are returned. `LessThan` with `orEqual: true` (`:597`) starts at the first copy and
  walks backwards, so the later copies are never seen.
- *Past the last key.* When `Seek` finds no key at or after the bound, the cursor is invalid
  (`BTreeCursor.cs:151-152`) and `Current` throws (`BTreeCursor.cs:37`). `GreaterThan`, `Between`
  and `StartsWith` read `Current` in a `do ... while` without checking, so they throw on an empty
  index and for any bound above the last key; `LessThan` calls `MovePrev` on the invalid cursor,
  which returns false, so it returns nothing although every key qualifies.
- *Culture.* `StartsWith` stops at the first key for which `val.StartsWith(prefix)` is false
  (`BTreeIndex.cs:658`), a culture-sensitive comparison. With ICU (Linux, macOS, Windows on .NET 5+
  with ICU), U+0000 to U+0003 are ignorable, so `"a\u0003b".StartsWith("\0")` is true and the walk
  runs on. `Like` uses the same test only to stop its walk (`:715`) and then filters with a regex,
  so the `Like` rows agree.

None of these is called from `src/`: the collections use `Range`, `ScanRange`/`CountRange`,
`TryFind`, `TryFindFirst` and `Relocate`. They are public on `BTreeIndex`. Their `*Async` twins
(`BTreeIndex.cs:2134-2232`) are written over `RangeAsync` with an ordinal prefix test and agree
with the model on every generator (rows `Equal.Async`, `Between.Async`, `GreaterThan.Async`,
`LessThan.Async`, `StartsWith.Async`, `Like.Async`), except `InAsync` (`:2192`), which calls
`TryFindAsync` once per key and so returns one entry per duplicated key, like `In`.

**Affected versions:** the primitives are unchanged since the project was renamed (`4236d9e`,
2026-02-11, v1.0.0). Reproduced on v5.2.3 / `0450b97`.

**Fix considerations:** write the sync primitives the way their async twins already are: a `Range`
over the bounds, which handles duplicates across leaves (#213), empty leaves and the end of the
tree, with an exclusive bound skipping every entry equal to it (`BetweenAsync`, `:2164`). `In` and
`InAsync` become one `Range(k, k)` per key. `StartsWith(prefix)` is
the byte range from the prefix's key to the key after the prefix, the same boundary
`CollectionSecondaryIndex.CreateCompositeKeyBoundary` builds for #206, which is ordinal by
construction. Alternatively, mark them `[Obsolete]` if nothing outside the engine is meant to
use them.

---

## B2 (#256). Equal keys come back from a B-Tree in reverse insertion order, scanning forwards

**Rows:** `<generator> / Ties.Order` for the 8 non-unique generators. Every one of them fails first
on the **forward** scan (`Range(MinKey, MaxKey, Forward)`), at the first pair of equal keys.

**Sibling:** #210 reports the descending side on typed collections, where each key carries the
document id and only the backward scan reverses ties. This one is the B-Tree itself: the forward
scan already returns ties newest first, so a `DynamicCollection` index, whose keys carry no id,
reverses them in ascending order. The open fix for #210, PR #244 (`fix/210-desc-index-ties`),
changes `CollectionSecondaryIndex.Range` for backward scans only and does not touch `BTreeIndex`,
so it does not change these rows.

**Repro:**

```csharp
var index = new BTreeIndex(storage, IndexOptions.CreateBTree("k"));
var txn = storage.BeginTransaction().TransactionId;
for (ushort i = 1; i <= 3; i++) index.Insert(IndexKey.Create(28), new DocumentLocation(1, i), txn);
index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txn);   // slots 3, 2, 1

// The same through a DynamicCollection, whose index keys carry no document id:
var col = engine.GetOrCreateCollection("people");
await col.CreateIndexAsync("age", "idx_age", unique: false);
foreach (var n in new[] { "d1", "d2", "d3" })
    await col.InsertAsync(col.CreateDocument(["name", "age"], b => b.AddString("name", n).AddInt32("age", 5)));
col.QueryIndexAsync("idx_age", 5, 5);          // d3, d2, d1
col.QueryIndexAsync("idx_age", 5, 5, false);   // d1, d2, d3
```

**Expected:** equal keys in insertion order going forwards, as `Insert` itself describes it (an
entry equal to a separator goes "after any duplicates already there", `BTreeIndex.cs:145`) and as
a stable `OrderBy` over the same documents returns them.

**Actual:** within a leaf, each new copy goes before the copies already there, so a forward scan
returns ties newest first. Across a split the order is mixed: a run of 80 copies inserted in order
came back starting `#65, #64, #63, ...`.

**Mechanism (confirmed):** `InsertIntoLeaf` finds the insertion point with a lower-bound binary
search, `CompareRaw(newKey, entryKey) <= 0` (`BTreeIndex.cs:761`), which is the first copy of an
equal key. Before `4bd2a0b` the same method used `entries.FindIndex(e => e.Key > key)`, an upper
bound, which kept insertion order. Typed collections append the document id to every key
(`CollectionSecondaryIndex.cs:244`), so they never hold equal keys; `DynamicCollection` does not
(`DynamicCollection.cs:2716`), and `QueryIndexAsync` returns the raw scan order
(`DynamicCollection.cs:1079`).

**Affected versions:** since `4bd2a0b` (2026-04-11, "Refactor transactions, metrics and BTree
internals", first in v4.3.0-rc.0). Reproduced on v5.2.3 / `0450b97`.

**Fix considerations:** an upper-bound search (`< 0`) in `InsertIntoLeaf` restores insertion order
within a leaf; the split already keeps the order of the merged entries, so a run that crosses
leaves then stays in order too (`CopyMergedLeafEntries`, `BTreeIndex.cs:893`, copies the merged
entries in order; not verified by a test). Existing indexes keep their current order until rebuilt. Whether
ties have a defined order at all is the question #210 raises for descending scans; if the answer
is "no", the check `Ties.Order` should go instead, and README should say so.

---

## B3 (#257). A NaN with the sign bit clear sorts above `+Infinity` in an index on a double

**Rows:** `double / KeyOrder.NaN`.

**Sibling:** #249 (`-0.0` and `+0.0` get different keys) is the same encoding,
`IndexKey(double)`, and its text names NaN as a separate question; this draft is that question.
The row `double / KeyOrder` (the `-0.0` pair) is registered under #249 and kept apart from this one,
so the #249 fix clears it without clearing this one.

**Repro:**

```csharp
double zero = double.Parse("0");
double computed = zero / zero;             // 0x7FF8000000000000 on Arm64; double.NaN is 0xFFF8000000000000
IndexKey.Create(computed) > IndexKey.Create(double.PositiveInfinity);   // true
IndexKey.Create(computed) == IndexKey.Create(double.NaN);               // false

// DiffDoc has double Ratio with .HasIndex(x => x.Ratio).
await ctx.Docs.InsertBulkAsync([
    new DiffDoc { Name = "d1", Ratio = computed },
    new DiffDoc { Name = "d2", Ratio = 1.0 },
    new DiffDoc { Name = "d3", Ratio = double.NaN }]);
ctx.Docs.AsQueryable().OrderBy(x => x.Ratio).Take(3);              // expected d1, d3, d2; actual d3, d2, d1
ctx.Docs.AsQueryable().OrderByDescending(x => x.Ratio).Take(3);    // expected d2, d1, d3; actual d1, d2, d3
ctx.Docs.AsQueryable().Count(x => x.Ratio > 0.5);                  // expected 1;          actual 2
ctx.Docs.AsQueryable().Where(x => x.Ratio >= double.MaxValue);     // expected [];         actual [d1]
```

**Expected:** `double.CompareTo`, which LINQ-to-Objects and the unindexed collection use, orders
every NaN below every number and equal to every other NaN; every comparison operator with a NaN
operand is false in C#.

**Actual:** on Arm64, every NaN the program computes (`0.0 / 0.0`, `Math.Sqrt(-1)`, `∞ - ∞` all
give `0x7FF8000000000000`) sorts above `+Infinity`, while the constant `double.NaN` sorts below
`-Infinity`. An index-served ordering puts computed NaNs last, and index-served counts and ranges
with an upper-open bound include them. On x64, where computed NaNs carry the sign bit, the same
documents sort first; the order of a query depends on the CPU that wrote the value.

**Mechanism (confirmed):** `IndexKey(double)` (`src/BLite.Core/Indexing/IndexKey.cs:94-105`)
flips all bits of a value with the sign bit set and only the sign bit otherwise. A NaN's sign bit is
arbitrary, so the two NaN halves land below `-Infinity` (sign set) and above `+Infinity` (sign
clear), and every distinct payload is a distinct key. `Where(x => x.Ratio > 0.5)` itself returns
the right answer because it is post-filtered with the BSON predicate; `Count` is answered from the
index (`CountRange`) and is not.

**Affected versions:** since the sort-preserving double encoding in `f83e0d5` (2026-03-17, first
in v3.7.0). Reproduced on v5.2.3 / `0450b97` on Arm64; x64 not run.

**Fix considerations:** canonicalise in `IndexKey(double)` together with the `-0.0` fix of #249:
`if (double.IsNaN(value)) value = double.NaN;` before taking the bits gives every NaN the one key
below `-Infinity`, which is where `double.CompareTo` puts it. Range bounds then exclude NaN from
`> x` and `>= x`; whether `< x` should include it (C#: false, `CompareTo`: NaN is smallest) is the
same question the index already answers for `-Infinity`, and the BSON post-filter decides for
`Where`, not for `Count`. Existing indexes with a positive-NaN key need a rebuild.

---

## B4 (#258). Every reopen strands the unwritten rest of the last growth block

**Rows:** `<generator> / Pages.Leaked` for all 9 generators; each shrinks to a single `Reopen`.

**Repro:**

```csharp
for (int i = 0; i < 10; i++)
{
    using var db = new TestDbContext(path);                          // PageFileConfig.Default: 16 KB pages, 1 MB growth
    for (int j = 0; j < 20; j++)
        await db.PeopleV2.InsertAsync(new PersonV2 { Name = new string('x', 2000) + $"{i}/{j}", Age = i });
    Console.Write($"{new FileInfo(path).Length / 1024} ");            // after Dispose
}
// 3072 4096 5120 6144 7168 8192 9216 10240 11264 12288 (KiB), about 400 KB of documents
await reopened.Storage.TruncateToMinimumAsync();                     // still 12288 KiB
```

With a bare `StorageEngine` that writes one page per open, `PageCount` after each of ten cycles is
`66 129 193 257 321 385 449 513 577 641`: 64 pages, one growth block, per reopen.

**Expected:** a closed and reopened database allocates from where it left off; pages never handed
out stay available; the file grows with the data, not with the number of opens.

**Actual:** each open that writes anything new adds a growth block (`GrowthBlockSize`, 1 MB by
default, 512 KB for `Small`, 2 MB for `Large`) and the unused part of the previous one is lost: it
counts as allocated, is never handed out, is not on the free list and is not reclaimed by
`TruncateToMinimumAsync`. In the model test, a reopen right after the index is created leaves 126
of 256 pages neither reachable nor free.

**Mechanism (confirmed):**

- Allocation appends at `_nextPageId`, and the file grows ahead of it in whole blocks
  (`EnsureCapacityCore`, `src/BLite.Core/Storage/PageFile.cs:770-773`, `AlignToBlock` at `:369`);
  the pages between `_nextPageId` and the end of the file are allocated later, in this session.
- `Open` sets `_nextPageId` to the length of the file (`PageFile.cs:483`), so the next session
  starts allocating after the block's unwritten tail. Nothing records where the previous session
  stopped (the header page holds the free-list head, not the high-water mark).
- `TruncateToMinimumAsync` trims trailing pages only while their header says `Free`
  (`PageFile.cs:1347`); the stranded pages were never written and say `Empty`, so it stops at once.

**Affected versions:** `Open` has derived the next page from the file length since `4236d9e`
(v1.0.0, then with a fixed `InitialFileSize`); growth by blocks came with `237f9a9` (2026-02-24,
v1.8.0), from which every reopen strands up to one block. Reproduced on v5.2.3 / `0450b97`. An
encrypted file grows page by page (`PageFile.cs:770`) and is not affected.

**Fix considerations:** persist the high-water mark in the file header next to the free-list head
and restore `_nextPageId` from it on open; or, on open, walk back from the end over
pages whose header is `Empty` and either lower `_nextPageId` or push them onto the free list;
or truncate the file to `_nextPageId` on a clean `Dispose`. The second also repairs files that are
already affected, and `TruncateToMinimumAsync` could treat trailing `Empty` pages like `Free` ones.
In multi-process mode another process may have allocated past this one's view
(`PageFile.cs:1076-1087`), so the fix has to respect the shared allocation state.

---

# Candidate issues from the WAL crash test

Status: **filed 2026-10-17 as #260 (W1), #261 (W2) and #262 (W3).** Triage of the rows that `WalCrashTests` reports as failing on
upstream `0450b97` (v5.2.3): 8 of 24 rows, in three families (W1 to W3). Run on macOS, Arm64,
.NET 10, 6 seeds of 60 steps, 1,886 crash states (950 `ProcessCrash`, 687 `TornWrite`, 249
`PowerLoss`; 282 of the `ProcessCrash` states crash a recovery). The repros are the shortest
failing sequences the test prints when the entry's issue is empty; the crash point is named by the
recorded write after which the files were cut.

What holds in every crash state of `ProcessCrash` and `TornWrite`, including crashes during
checkpoints, WAL truncations, reopens, the closing `Dispose`, the first open and recovery itself:
the reopen succeeds, the collection is a committed state (scan, count, point lookups, both
secondary indexes), never older than the last acknowledged commit, no page is reachable twice or
both reachable and free, a second reopen changes nothing, and the rest of the sequence runs against
the model. The October fixes in this area hold: no freed page reused while a WAL image still
refers to it (#168, `e19035c`), no torn record replayed or skipped over (#198 is the multi-process
twin; single-process recovery stops at the first torn record, `WriteAheadLog.cs:1064-1240`), no
loss on a crash while `Dispose` waits for a pending-free checkpoint (#209/#242, `9b9c655`).
#197, #200 and #203 are multi-process and out of the test's scope.

Open issues checked on 2026-10-17 (`gh issue list --repo EntglDb/BLite --state all`, searched for
fsync, durab, flush, power, crash, torn, truncat, leak, free): none covers W1 to W3. The closest are
#237 (pages a rolled-back transaction allocated are never reclaimed, the sibling of W2) and #238
(the double-free guard that W3 undermines). Closed issues and PRs checked the same day (WAL, fsync,
flush, durability, WriteThrough, checkpoint, power loss, free page, free list): none discusses
commit fsync or power loss. #169 and #236 accept a crash leaking queued and commit-time frees as a
trade-off, which W2 does not dispute (it asks for reclamation and adds the index-root case, a
different mechanism); W3 is a remaining gap in the header invariant of #239, which #178's head
check does not see. Each issue carries a "Related" line.

**Registry entries.** Each draft is one family of `s_candidates` in
`tests/BLite.Tests/Oracles/WalCrashTests.cs`. A row is `"<crash mode> / <check>"`:

| Draft | Registry family label | Rows |
|---|---|---|
| W1, #260 | `the WAL is never fsynced: a power loss drops acknowledged commits and keeps the frees they made` | 4 |
| W2, #261 | `a crash leaks the pages that the interrupted work freed or allocated outside the WAL` | 3 |
| W3, #262 | `a crash between the two writes of PageFile.FreePage or AllocatePage leaves a free page off the free list` | 1 |

---

## W1 (#260). The WAL is never fsynced: a power loss drops acknowledged commits and corrupts documents

**Rows:** `PowerLoss / Durability` (108 of 249 crash states), `PowerLoss / Atomicity` (6),
`PowerLoss / Pages.Reachable` (6), `PowerLoss / Continue` (3).

**Repro (shortest sequences):**

```text
Durability, crash after the WAL flush of step 2:
    1. Insert(id=64, count=9, code=c15, payload=2500 chars)
    2. Update(id=64, count=6, code=c06, payload=0 chars)
  the collection is the state after the first open: document 64, inserted and acknowledged
  in step 1, is missing

Atomicity and Pages.Reachable, crash after FreePage(21) of step 3:
    1. Insert(id=74, count=3, code=c08, payload=18000 chars)
    2. Backup
    3. Upsert(id=74, count=7, code=null, payload=20 chars)
  document 74 is back in its step-1 version, but its payload differs at index 3899: expected
  'llll...', got '....' (zeros); page 24, on its overflow chain, has type Free
```

**Expected:** README "Transactions & ACID": "Durable: WAL ensures data safety even in power loss."
A commit that `CommitAsync` (or an auto-committing write) has acknowledged survives a power loss,
and whatever survives is a committed state.

**Actual:** after a power loss, only what the WAL had when the database was opened (or at the last
backup) survives in it. Every commit since then is lost unless a checkpoint copied its pages to the
page file. Worse, the pages those lost commits stopped using are already free, so the documents
that come back point at overflow pages that are on the free list (step 3 above) or have been handed
out again (the `Continue` row: a later insert overwrites them).

**Mechanism (confirmed):**

- A commit is acknowledged after `await _wal.FlushAsync()` (`StorageEngine.GroupCommit.cs:202`;
  the direct path `StorageEngine.Transactions.cs:445` says "Durability: ensure WAL is on disk").
  `WriteAheadLog.FlushAsync` calls `FileStream.FlushAsync` (`WriteAheadLog.cs:491`), which hands
  the 64 KB buffer to the OS and does not fsync: on .NET 6 to 10 `OSFileStreamStrategy.FlushAsync`
  returns `Task.CompletedTask`; only `Flush(flushToDisk: true)` calls `FlushToDisk`. The comment at
  `WriteAheadLog.cs:492-499` already doubts it. The WAL is opened without
  `FileOptions.WriteThrough` (`WriteAheadLog.cs:119-126`, removed in `f5802f9`). The only fsync of
  the WAL is in `WriteAheadLog.BackupAsync` (`:1002`).
- Page writes, by contrast, are durable at once: `PageFile.WritePageCore` flushes its view after
  every write (`PageFile.cs:712`, `:727`; `msync(MS_SYNC)` on Unix).
- So the order on disk is wrong wherever the engine acts on a commit: the group commit frees the
  pages the batch stopped using right after the (unsynced) flush
  (`StorageEngine.GroupCommit.cs:248`, `FreePagesOfCommittedTransaction`), and `FreePage` writes
  the Free header and the free-list head straight to the page file when the page has no WAL image
  (`StorageEngine.Memory.cs:105`, as after the backup's truncation in the repro). A power loss
  then keeps the free and drops the commit.
- The same holds for the WAL truncation: `TruncateAsync` (`WriteAheadLog.cs:1035-1037`) is not
  fsynced either, while the pages it releases are freed durably right after
  (`StorageEngine.Recovery.cs:272`, `:285`).

The seed `WalCrashTests.ack-before-wal-flush.patch` (no flush at all) shows the same corruption
under a process crash: what W1 does on a power loss, the seed does on any crash.

**Affected versions:** `f5802f9` (v1.0.0) dropped `WriteThrough` for performance but kept one
`Flush(flushToDisk: true)` per commit; `a4e052b` (v1.0.0) added the async `FlushAsync` path without
it (with the comment above), group commit (`d72b73a`, v3.6.2) was meant to amortise the fsync, and
`63465aa` (v4.0.0) removed the last synchronous commit path. So `CommitAsync` has not fsynced since
v1.0.0, and no commit path since v4.0.0. The frees that turn a lost commit into a corrupted document
are those of the current code: right after the commit (#233, `4d3e6b9`) or at the next truncation
(#168, `e19035c`), both merged after v5.2.3; before them pages were freed earlier still. Not
reproduced on hardware; the test models a power loss as "only fsynced WAL bytes and msynced pages
survive".

**Fix considerations:** fsync the WAL before acknowledging a batch (`Flush(flushToDisk: true)` or
`RandomAccess.FlushToDisk` in `WriteAheadLog.FlushAsync`; group commit already amortises it over a
batch), and before the pages a truncation releases are freed. On macOS, `fsync` does not flush the
drive cache; SQLite uses `F_FULLFSYNC` there. If an unsynced mode is wanted for speed, it should be
an option, documented as losing the last commits on a power loss, and it still has to fsync before
freeing pages a commit released, or those documents are corrupted rather than just older.

---

## W2 (#261). A crash leaks the pages that the interrupted work freed or allocated outside the WAL

**Rows:** `ProcessCrash / Pages.Leaked` (243 of 950), `TornWrite / Pages.Leaked` (107 of 687),
`PowerLoss / Pages.Leaked` (24 of 249).

**Repro (shortest sequences):**

```text
crash after WritePage(18) during the first open (collection creation), no steps:
  1 of the 35 pages is an index page that nothing reaches and that is not free: 18

crash at any write between a delete or shrinking update of a document with overflow pages and
the next WAL truncation, e.g. after a WAL flush of a later Insert:
  4 of the 48 pages are overflow pages that nothing reaches and that are not free: 32, 33, 34, 35
```

**Expected:** after recovery every page is reachable or free (or, if leaks are accepted, recovery
reclaims them; nothing does today, and `TruncateToMinimumAsync` trims only `Free` pages).

**Actual:** two ways a crash strands written pages:

- *Frees outside the WAL.* A page freed while the WAL holds an image of it is queued until the next
  truncation (`StorageEngine.Memory.cs:99-121`), and a page a transaction stopped using is freed
  only after its commit is durable (`FreePageOnCommit`, `:175`; freed at
  `StorageEngine.GroupCommit.cs:248`). A crash in between leaks them, and so does a crash between a
  truncation and `ReleaseTruncatedPages` (`StorageEngine.Recovery.cs:272`, `:285`). This is
  documented as the accepted trade-off (`StorageEngine.Memory.cs:87`, "A queued page is leaked,
  never reused unsafely, if the process dies first", and `:171`), but there is no issue and no
  reclamation. The window is long: without an explicit checkpoint the WAL is truncated only past
  16 MB (`MaxWalSize`) or 4 MB of queued frees, and every page queued until then is leaked by a
  crash.
- *Index roots of a collection being created.* `BTreeIndex`'s constructor allocates the root and
  writes it at once (`BTreeIndex.cs:37`, `:66`); the root id reaches the collection metadata only in
  the following `SaveMetadata` (`DocumentCollection.cs:885`, `CollectionIndexManager.cs:125-134`).
  A crash in between, during the first open, leaves the root orphaned and the next open allocates a
  new one.

**Mechanism (confirmed):** as above; frees and allocations are writes to the page file, not WAL
records, so recovery cannot redo or undo them.

**Affected versions:** the queued frees since #168 (`e19035c`) and the commit-time frees since
#233 (`4d3e6b9`), both merged after v5.2.3; roots written before the metadata since #151
(`e187bd6`, v5.2.0). #237 is the same class for a rollback instead of a crash. The leaks of
queued and commit-time frees are the accepted trade-off of #169 and #236 ("a crash still leaks
queued pages"); the issue asks for reclamation, the index roots are new.

**Fix considerations:** log frees in the WAL with the commit (a Free record replayed by recovery
after the commit's pages), which also closes W1's ordering; or let recovery rebuild the free list
from what the catalog, the indexes and the documents reach (the test's page accounting is such a
walk), which repairs leaks of any cause, #237 and #258 included; or create collections with their
roots in one transaction.

---

## W3 (#262). A crash between the two writes of `PageFile.FreePage` or `AllocatePage` leaves a free page off the free list

**Rows:** `ProcessCrash / Pages.FreeList` (46 of 950).

**Repro (shortest sequences):**

```text
crash after AllocatePage() = 21 wrote page 0, in step 4:
    1. Upsert(id=100, count=8, code=null, payload=18000 chars)
    2. Update(id=100, count=2, code=c12, payload=0 chars)
    3. Backup
    4. InsertBulk(id=22/..., id=87/..., id=4/..., id=8/...)
  1 page is marked free but not on the free list: 21

crash after FreePage(24) wrote page 24, in the closing Dispose of
    1. Upsert(id=100, count=8, code=null, payload=18000 chars)
    2. Update(id=100, count=2, code=c12, payload=0 chars)
  1 page is marked free but not on the free list: 24
```

**Expected:** a page carries a `Free` header exactly while it is on the free list, which the
double-free guard of #238 relies on (`PageFile.cs:1118-1127`, `IsFreePageCore` at `:1161`).

**Actual:** `FreePage` writes the page's Free header (`PageFile.cs:1144`) and then the free-list
head in page 0 (`:1150`); `AllocatePage` moves the head first (`:1061`) and clears the recycled
page's header after (`:1068`). Each write is durable at once (`PageFile.cs:727`), so a crash between
the two leaves a page with a Free header that the free list does not reach. It is never handed out
again, never freed again (the guard takes it for already free) and not reclaimed; when it is not the
last page, `TruncateToMinimumAsync` does not reach it either.

**Mechanism (confirmed):** the two writes, in that order, and no recovery step that checks the
free list against the headers.

**Affected versions:** the free list has lived in page headers since `4236d9e` (v1.0.0); the guard
that makes the stray header matter came with #238.

**Fix considerations:** write the two pages in an order that leaves a stray free page harmless
(for `FreePage`: page 0 last is what it does; for `AllocatePage`: clear the header before moving the
head, so a crash leaves a non-free page off the list, which is only a leak); or, cheaper and more
general, rebuild or check the free list on open after a crash (W2's walk); or make allocation and
free WAL records.

---

# Candidate issues from the DynamicCollection tests

Status: **not filed.** Triage of the rows that `DynamicDifferentialTests` and
`DynamicStoreModelTests` report on upstream `0450b97` (v5.2.3; the oracle branches change no file
under `src/`): 248 differential rows in eight families (D1 to D8) and 4 store-model rows in one
(D9), next to reproductions of #254 (48 rows), #234 (the scaling row) and the typed oracle's own
#247 and #248 (28 rows). Each repro below was run as a scratch Fact on that tree; the results
quoted are that run's. The repros are not part of the suite, the rows listed under each draft are.

A differential row is `"<way> <oracle>: <query>"`: the way is `indexed/api`, `indexed/blql`,
`plain/api` or `plain/blql` (the collection with an index on every queried field or without, through
`QueryIndexAsync`/`FindAsync` or through BLQL), the oracle `linq` or `typed`. A store-model row is
`"seed <n> / <check>"`.

What agrees, on all four ways and against both oracles: equality, ranges, `In`, `$mod`, `Or`,
`Not`, windows, `Count` and `Any` on Int32, Int64 and string fields; `== null` and `!= null`
through the null bucket (the dynamic index answers `nick == null` correctly, where the typed one
does not, #247); ordinal `$startsWith`, `$endsWith`, `$contains`; `QueryIndexAsync` with `DBNull`
bounds; the nested index through `QueryIndexAsync`. In the store model: every write, transaction,
checkpoint, backup and reopen, `CreateIndexAsync` and `DropIndex` with documents present, `VacuumAsync`
with and without `RebuildIndexes`, runs of 150 and 400 documents sharing one indexed value found and
deleted completely (#213 holds), and the pages after every commit, `DropIndex` and vacuum (#211 holds:
the old trees are freed).

The difference README.md:880 documents ("`DynamicCollection` indexes an explicit BSON `null` and
rejects a second one, whereas typed collections do not index `null` keys") changes which values a
unique index accepts, not any query answer: no differential row disagrees because of it. The store
model encodes it (a null code is an explicit null for ids that are multiples of 5 and a unique value).

Open and closed issues checked on 2026-10-17 (`gh issue list --repo EntglDb/BLite --state all`,
searched for BLQL nested, dot notation, nested path, ObjectId order, ObjectId sort, BLQL null,
lte null, Int64 precision, long double comparison, FirstOrDefault sort, BLQL First, BLQL Sort,
Decimal128 index, Boolean index, decimal DynamicCollection, QueryIndex DateTime, QueryIndexAsync,
BLQL index, constant of another type, BsonValueComparer, CreateIndexAsync unique, unique index
existing duplicates): none covers D1 to D9. The closest: #254 (open, the double key encoding; D6
and D7 are its siblings in the same key switch), #222 (closed, the typed twin of D6), #190 (open,
`OnIndexBuildFailure` for typed model indexes, the typed side of D9) and #213 and #211 (closed, hold).

**Registry entries.** D1 to D8 are families of `s_candidates` in
`tests/BLite.Tests/Oracles/DynamicDifferentialTests.cs`, D9 of the one in
`tests/BLite.Tests/Oracles/DynamicStoreModelTests.cs`:

| Draft | Registry family label | Rows |
|---|---|---|
| D1 | `BLQL cannot reach a nested field` | 48 |
| D2 | `BLQL orders ObjectIds by their little-endian bytes` | 60 |
| D3 | `BLQL First and FirstOrDefault ignore Sort and Skip` | 16 |
| D4 | `BLQL range operators match fields of another BSON type` | 4 |
| D5 | `BLQL compares two Int64 values through double` | 4 |
| D6 | `a numeric bound of another type than the stored values misses keys of a dynamic index` | 24 |
| D7 | `a dynamic index keys no Decimal128 or Boolean value` | 76 |
| D8 | `QueryIndexAsync rejects a DateTime bound` | 16 |
| D9 | `a failed unique CreateIndexAsync leaves the half-filled index registered` | 4 |
| #254 | `dynamic double keys are little-endian bytes` | 48 |
| #234 | `deleting N documents that share an indexed value is quadratic` | 1 |
| #247 | `the typed oracle: == null and a nullable top-N on an index find no null-keyed document` | 20 |
| #248 | `the typed oracle: != excludes null values` | 8 |

**Reproductions of filed issues.** #254: every range, sort and `== 0.0` on the indexed double field,
through `QueryIndexAsync` and BLQL (`ratio < 0`: expected 102 documents, got 0; `orderby ratio`
starts with `0, 0, ...` instead of `-1E+300`). For `ratio == 0.0` the typed collection misses the
same `-0.0` documents (#249), so only the LINQ rows of that query fail. #234:
`DuplicateDeletes_ScaleLikeNLogN` deletes 1,500 and 12,000 documents sharing one indexed value in
one `DeleteBulkAsync` on 4 KB pages: 57 ms and 2,250 ms (x38 to x40 over five runs), against 19 ms
and 200 ms (x10.5) for distinct values; N log N allows x10.3, the test fails above x20.6. #247 and
#248 are rows where the typed oracle is the one that is wrong, the dynamic answer agrees with LINQ.

---

## D1. BLQL filters and sorts cannot reach a nested field, though a dynamic index can be built on one

**Rows:** `indexed/blql` and `plain/blql`, both oracles: `(int)kind == 2`, `Count(kind == Blue)`,
`flag and kind == Red`, `kind != Blue`, `kind < Blue`, `kind == Black`, `kind == Blue`,
`kind == Red or kind == Black`, `kind >= Green`, `kind in list`, `three wheres`; `plain/blql`, both
oracles: `orderby kind`, `orderby kind skip 10 take 10`. The kind is stored as `meta.kind`.

**Repro:**

```csharp
var col = engine.GetOrCreateCollection("p");          // and once more after: await col.CreateIndexAsync("meta.kind");
foreach (var (n, k) in new[] { ("a", 2), ("b", 0), ("c", 2) })
    await col.InsertAsync(col.CreateDocument(["n", "meta", "kind"],
        b => b.AddString("n", n).AddDocument("meta", m => m.AddInt32("kind", k))));

col.Query(BlqlFilter.Eq("meta.kind", 2)).ToList();     // expected [a, c],    actual [] (indexed: [])
col.Query("{\"meta.kind\": 2}").ToList();              // expected [a, c],    actual []
col.Query(BlqlFilter.Ne("meta.kind", 2)).ToList();     // expected [b],       actual [a, b, c]
col.Query().OrderBy("meta.kind").ToList();             // expected [b, a, c], actual [a, b, c] (indexed: [b, c, a])
await col.QueryIndexAsync("idx_meta.kind", 2, 2).ToListAsync();   // [c, a], correct
```

**Expected:** a dotted path names the nested field in BLQL as it does in `CreateIndexAsync`
("Nested path index (dot-notation)", README.md:861-862; "dot-notation strings (`"address.city"`)
for schema-less collections", README.md:65) and in MQL.

**Actual:** BLQL sees a missing field: `$eq`, `$gt`, `$in` match nothing, `$ne` and `$nin` match
everything, and the in-memory sort keeps the scan order. With an index on the path, BLQL scans it
and then drops every document in the residual filter; only the index-served sort is right.

**Mechanism (confirmed):** every BLQL operator reads its field with `BsonDocument.TryGetValue`
(`CompareFilter.Matches`, `src/BLite.Core/Query/Blql/BlqlFilter.cs:315`; `InFilter`, `:408`; the
same for `$exists`, `$type`, `$regex`, the string and the array operators), and so does
`BlqlSort.ToComparison` (`BlqlSort.cs:53-54`). `BsonDocument.TryGetValue`
(`src/BLite.Bson/BsonDocument.cs:230`) looks up a top-level name. The dotted path is resolved only
by `DynamicCollection.TryGetNestedValue` (`DynamicCollection.cs:2759`), which the index paths use.
`GetScanSource` (`BlqlQuery.cs:302`) finds the nested index through `FindBTreeIndexForField`
(`DynamicCollection.cs:1091`), which is why the sort fast path (`BlqlQuery.cs:241-258`) orders by
it correctly while the filter does not match.

**Affected versions:** BLQL has read fields this way since it was added (`a86d458`, 2026-02-24,
v1.9.0). Reproduced on `0450b97` (v5.2.3).

**Fix considerations:** one path lookup for BLQL (`TryGetNestedValue` made shared, or a
`BsonDocument.TryGetPath`) used by every filter and by `BlqlSort`. MQL forbids dots in field names,
so a dotted name can always mean a path.

---

## D2. BLQL orders and compares ObjectIds by their little-endian bytes, not as `ObjectId.CompareTo`

**Rows:** `indexed/blql` and `plain/blql`, both oracles: `orderby id`, `orderby id desc`,
`orderby id take 50 where count > 5`, `orderby id desc skip 10 take 50 where name startswith p1`,
`OrderBy(id).Take(10).Sum(count)`, `Where(count > 5).OrderBy(id).Skip(2).Take(10).Sum(count)`, and
every ordering that breaks ties by id: `orderby count thenby id`, `orderby count desc thenby id`,
`orderby count thenby id take 7`, `orderby count thenby id take 20 where flag`,
`where flag orderby when thenby id`, `where name startswith p1 orderby count thenby id take 10`,
`OrderBy(count).ThenBy(id).ElementAt(10)`, `OrderBy(count).ThenBy(id).Take(5).Last()`,
`OrderBy(count).ThenBy(id).Select(id) (ordered)`.

**Repro:**

```csharp
var ids = new[] { new ObjectId(1, 0), new ObjectId(2, 0), new ObjectId(256, 0) };   // "t1", "t2", "t256"
// inserted with b.AddId(new BsonId(id))

ids[0].CompareTo(ids[2]) < 0;                                          // true
col.Query().OrderBy("_id").ToList();                                   // expected [t1, t2, t256], actual [t256, t1, t2]
col.Query(BlqlFilter.Gt("_id", BsonValue.FromObjectId(ids[0]))).ToList();  // expected [t2, t256], actual [t2]
await col.FindAllAsync().ToListAsync();                                // [t256, t1, t2] (unordered by contract)
```

**Expected:** one order for ObjectIds: `ObjectId.CompareTo` (timestamp, then the remaining 8
bytes, both unsigned; `src/BLite.Bson/ObjectId.cs:100-107`), which LINQ and the typed collection use
and which is creation order, as in MongoDB.

**Actual:** BLQL sorts and compares by bytes, and a little-endian timestamp makes timestamp 256
(`00 01 00 00`) sort before timestamp 1 (`01 00 00 00`). `OrderBy(id).Take(10).Sum(count)` picks
other documents and overflows.

**Mechanism (confirmed):** `BsonValueComparer.CompareObjectIds`
(`src/BLite.Core/Query/Blql/BsonValueComparer.cs:108-115`) compares what `ObjectId.WriteTo` writes,
and `WriteTo` writes the timestamp with `BitConverter.TryWriteBytes` (`ObjectId.cs:63`),
little-endian. `CompareTo` says "Compare timestamp (big-endian seconds)" and compares the value. The
primary index keys ObjectIds with the same bytes (`IndexKey(ObjectId)`, `IndexKey.cs:69`), so scans
come back in that order too; whether a typed range on an `ObjectId` key served by an index shows the
same was not checked.

**Affected versions:** the byte comparison since BLQL (`a86d458`, v1.9.0); the disagreement with
`CompareTo` since it was added (`609cb8a`, "Support ObjectId ordering", v4.0.1).

**Fix considerations:** compare ObjectIds with `ObjectId.CompareTo` in `BsonValueComparer`. That
alone fixes BLQL. Making the stored bytes big-endian (so that index order is creation order) is a
format change with a rebuild, and could join the v6 key-format bump (#212, #234).

---

## D3. BLQL `First` and `FirstOrDefault` ignore `Sort` and `Skip`

**Rows:** `indexed/blql` and `plain/blql`, both oracles: `OrderBy(count).ThenBy(id).First()`,
`OrderBy(count).ThenBy(id).Skip(3).First()`, `OrderByDescending(when).ThenBy(id).First()`,
`Where(count > 5).OrderBy(name).ThenBy(id).FirstOrDefault()` (e.g. expected #2, got #256, the first
document in storage order).

**Repro:**

```csharp
foreach (var (n, v) in new[] { ("a", 3), ("b", 1), ("c", 2) })
    await col.InsertAsync(col.CreateDocument(["n", "v"], b => b.AddString("n", n).AddInt32("v", v)));

col.Query().OrderBy("v").ToList();               // [b, c, a]
col.Query().OrderBy("v").FirstOrDefault();       // expected b, actual c (the first in storage order)
col.Query().OrderBy("v").Skip(2).First();        // expected a, actual c
```

**Expected:** the first document of the query as `ToList` returns it, which is what a sorted
`findOne` returns in MQL.

**Actual / mechanism (confirmed):** `FirstOrDefaultAsync` (`BlqlQuery.cs:185-196`), which `First`,
`FirstAsync` and `FirstOrDefault` call, iterates `GetScanSource` and returns the first match. It
never looks at `_sort` or `_skip`. `Count`, `CountAsync`, `Any` and `AnyAsync` ignore `Skip` and
`Take` as well; for `Count` the summary says so (`BlqlQuery.cs:165`), for `Any` nothing does. The
test runs those only without a window.

**Affected versions:** since BLQL (`a86d458`, v1.9.0: `FirstOrDefault` was a `Find` over the
filter); the current body is from `6364585` (v3.8.1-preview.2).

**Fix considerations:** with a sort or a skip set, `FirstOrDefaultAsync` takes the first item of
`AsAsyncEnumerable()` (which keeps the index-served sort); document or fix `Any` with a window.

---

## D4. BLQL range operators match fields of another BSON type: `null` under `$lt`, strings under `$gt`

**Rows:** `plain/blql`, both oracles: `maybe <= 0`, `maybe < 9.5 (double bound)` (e.g. expected 72
documents, got 178: every document whose `maybecount` is null). The indexed rows agree.

**Repro:**

```csharp
// m: null ("null"), -1 ("minus1"), 5 ("five"), "x" ("text")
col.Query(BlqlFilter.Lte("m", 0)).ToList();    // expected [minus1],       actual [minus1, null]
col.Query("{\"m\": {\"$lt\": 10}}").ToList();  // expected [minus1, five], actual [minus1, null, five]
col.Query(BlqlFilter.Gt("m", 0)).ToList();     // expected [five],         actual [five, text]
// after await col.CreateIndexAsync("m"): [minus1], [minus1, five], [five]
```

**Expected:** the same answer with and without an index, and MQL's type bracketing: `$lt`, `$gt`
and their `e` forms only match values of the operand's kind (numbers with numbers), which is also
what LINQ does with `int?` (`null <= 0` is false).

**Actual:** without an index, a null field is below every number and a string above it.

**Mechanism (confirmed):** `CompareFilter.CompareValues` (`BlqlFilter.cs:330`) applies the operator
to `BsonValueComparer.Compare`, a total order across types: null first
(`BsonValueComparer.cs:21-24`), then by `TypeOrder` (`:90`). With an index the scan stays within the
bound's key type (`QueryIndexAsync(null, upper)` starts after the null bucket,
`DynamicCollection.cs:1065`; string keys sort below the integer bound), and the residual filter can
only drop documents, so the indexed collection answers like LINQ.

**Affected versions:** the comparer since BLQL (`a86d458`, v1.9.0).

**Fix considerations:** `$gt`, `$gte`, `$lt`, `$lte` are false unless both values are of one
comparable kind (the numeric family, the date family, or the same type); `$eq`, `$ne` and sorting
keep the total order.

---

## D5. BLQL compares two Int64 values through `double`

**Rows:** `indexed/blql` and `plain/blql`, both oracles: `total > long.MaxValue - 1` (expected the
one document with `long.MaxValue`, got none).

**Repro:**

```csharp
// t: long.MaxValue ("max"), long.MaxValue - 1 ("max-1"), 2^53 + 1 ("2^53+1"), all Int64
col.Query(BlqlFilter.Gt("t", long.MaxValue - 1)).ToList();   // expected [max],   actual []
col.Query(BlqlFilter.Eq("t", long.MaxValue - 1)).ToList();   // expected [max-1], actual [max, max-1]
col.Query(BlqlFilter.Eq("t", 1L << 53)).ToList();            // expected [],      actual [2^53+1]
```

**Expected:** exact comparison of integers, as the comparer's own summary says: "values are compared
numerically: exactly (as `decimal`) when neither side is a Double, through `double` otherwise"
(`BsonValueComparer.cs:11`).

**Mechanism (confirmed):** the code takes the exact path only when one side is a Decimal128
(`BsonValueComparer.cs:29-33`); Int32 and Int64 pairs go through `ToDouble` (`:35`), which rounds
above 2^53. The index scan bounds are exact (`IndexKey(long)`), and the residual filter then drops
the right document, so the indexed collection is wrong too.

**Affected versions:** since BLQL (`a86d458`, v1.9.0).

**Fix considerations:** compare as `decimal` (or `long`) whenever neither side is a Double, as the
summary describes.

---

## D6. A numeric bound of another BSON type than the indexed values misses keys of a dynamic index

**Rows:** `indexed/api` and `indexed/blql`, both oracles: `count < 9.5 (double bound)`,
`count == 5.0 (double bound)`, `maybe < 9.5 (double bound)`, `total < 9.5 (double bound)`,
`Count(count < 9.5)`, `Where(count < 9.5).Max(count)` (e.g. `count < 9.5`: expected 273 documents,
got 1). The plain collection agrees with LINQ.

**Repro:**

```csharp
await col.CreateIndexAsync("c");   // c: -3, 1, 5, 9, 12 as Int32
col.Query(BlqlFilter.Lt("c", 9.5)).ToList();                   // expected [-3, 1, 5, 9], actual [] (no index: right)
col.Query(BlqlFilter.Eq("c", 5.0)).ToList();                   // expected [5],           actual []
col.Query(BlqlFilter.Lt("c", 5L)).ToList();                    // expected [-3, 1],       actual [-3]
await col.QueryIndexAsync("idx_c", null, 9.5).ToListAsync();   // expected [-3, 1, 5, 9], actual []
```

**Expected:** an index never changes the answer; BLQL's comparer treats all numbers as one family,
so `{c: 5.0}` matches `5` without the index.

**Mechanism (confirmed):** the bound is keyed in its own type. `CompareFilter.ToIndexObject`
(`BlqlFilter.cs:378`) passes a Double as `double` and an Int64 as `long`; `CreateIndexKeyFromObject`
(`DynamicCollection.cs:2804-2816`) keys a `double` as `BitConverter` bytes and a `long` as the 9-byte
`IndexKey(long)`, while the field's values are keyed as the 5-byte `IndexKey(int)` by
`BsonValueToIndexKey` (`:2716-2732`). Byte order across these encodings has no numeric meaning
(`$gte: 5L` happens to come out right in this data). The residual filter cannot add what the range
skipped. A collection whose documents store one field as different numeric types is affected the
same way without any cross-type bound.

**Affected versions:** `QueryIndexAsync` since `39e6c89` (v1.4.0); BLQL since it uses the index
(`6364585`, v3.8.1-preview.2).

**Fix considerations:** one order-preserving key encoding for every numeric BSON type in dynamic
indexes (`IndexKey(decimal)` covers Int32, Int64 and Decimal128 exactly; doubles would need a
combined encoding), done in the rebuild that #254 already requires; or convert the bound to each
numeric key type and scan each range. The typed collection solved its version by converting the bound
to the property's type (#222, `7e37f33`, PR #228).

---

## D7. A dynamic index keys no Decimal128 or Boolean value: index-served sorts return nothing

**Rows:** `indexed/api` and `indexed/blql`, both oracles: `orderby price`, `orderby price take 20`,
`orderby flag` (expected 400 documents, got 0); `indexed/api`, both oracles, 32 queries with a
`price` or `flag` bound (`QueryIndexAsync` throws): `price == 5m`, `price range [1, 10)`, `flag`,
`not flag`, `flag and count > 5`, `Count(flag)`, `Where(price < 1e19m).Sum(price)` and the rest of
the price and flag shapes.

**Repro:**

```csharp
await col.CreateIndexAsync("price");
await col.CreateIndexAsync("flag");
// a: 5m / true, b: 1m / false, c: 9.99m / true

col.Query().OrderBy("price").ToList();                              // expected [b, a, c], actual []
col.Query().OrderBy("flag").ToList();                               // expected b first,   actual []
await col.QueryIndexAsync("idx_price", null, null).ToListAsync();   // expected 3 documents, actual []
await col.QueryIndexAsync("idx_price", 5m, 5m).ToListAsync();       // ArgumentException: Cannot create IndexKey from type Decimal
await col.QueryIndexAsync("idx_flag", true, true).ToListAsync();    // ArgumentException: Cannot create IndexKey from type Boolean
col.Query(BlqlFilter.Eq("price", BsonValue.FromDecimal(5m))).ToList();  // [a], right (no index candidate, full scan)
```

**Expected:** `CreateIndexAsync` indexes the field's values (README "Index Management",
README.md:851) or refuses a type it cannot index; an index never makes a query return fewer documents.

**Actual:** `CreateIndexAsync` succeeds and the index stays empty for these values. BLQL's single-key
sort fast path (`BlqlQuery.cs:241-258`) iterates the empty index and returns nothing.

**Mechanism (confirmed):** `BsonValueToIndexKey` (`DynamicCollection.cs:2716-2732`) has no case for
Decimal128 or Boolean (`_ => null // Can't index this type as BTree key`), so `IndexInsert` skips
the document; `CreateIndexKeyFromObject` (`:2804-2816`) throws for `decimal` and `bool`. BLQL filters
escape only because `ToIndexObject` returns null for these types (`BlqlFilter.cs:378`).

**Affected versions:** the key switch since `26caa5a` / `39e6c89` (v1.4.0); the sort fast path since
`6364585` (v3.8.1-preview.2).

**Fix considerations:** key Decimal128 with `IndexKey(decimal)` (exact and order-preserving, the
typed encoding since #150) and Boolean as one byte; accept `decimal` and `bool` in
`CreateIndexKeyFromObject`. Existing indexes on such fields hold no entries, so a backfill of the
missing documents is enough. Until then the sort fast path should not use an index on a field whose
values it may not hold.

---

## D8. `QueryIndexAsync` rejects a `DateTime` bound

**Rows:** `indexed/api`, both oracles: `when == t5`, `when < t5`, `when <= T0`, `when > t20`,
`when >= t10 and < t20`, `when == T0.AddMinutes(5) (call)`, `when == t5 and maybe == null`,
`Count(when < t5)` (`ArgumentException`). BLQL on the same index agrees.

**Repro:**

```csharp
await col.CreateIndexAsync("when");   // m0, m5, m10: t0 plus 0, 5, 10 minutes
await col.QueryIndexAsync("idx_when", t0, t0.AddMinutes(5)).ToListAsync();
    // expected [m0, m5], actual ArgumentException: Cannot create IndexKey from type DateTime
await col.QueryIndexAsync("idx_when", new DateTimeOffset(t0).ToUnixTimeMilliseconds(),
    new DateTimeOffset(t0.AddMinutes(5)).ToUnixTimeMilliseconds()).ToListAsync();   // [m0, m5]
```

**Expected:** README.md:801 shows a date range: `orders.QueryIndex("idx_placed_at", minDate, maxDate)`.

**Mechanism (confirmed):** `CreateIndexKeyFromObject` (`DynamicCollection.cs:2804-2816`) has no
`DateTime` or `DateTimeOffset` case, although `BsonValueToIndexKey` keys both as Unix milliseconds
in an `IndexKey(long)` (`:2726-2730`) and BLQL converts its bounds the same way
(`BlqlFilter.cs:378`). A `long` bound is taken as milliseconds, which works by accident.

**Affected versions:** since `39e6c89` (v1.4.0).

**Fix considerations:** `DateTime` and `DateTimeOffset` cases keyed like the stored values. The
README sample also uses the old synchronous names (`QueryIndex`, `Near`, `Within`), now `*Async`.

---

## D9. A failed unique `CreateIndexAsync` leaves the half-filled index registered

**Rows:** `seed 3 / Index.UniqueBackfill`, `seed 4 / ...`, `seed 5 / ...`, `seed 6 / ...` of
`DynamicStoreModelTests`.

**Repro (shortest sequence, seed 4):**

```text
    1. DropIndex(idx_code)
    2. Insert(id=83, count=5, code=c31, payload=300 chars)
    3. Insert(id=21, count=7, code=c31, payload=300 chars)
    4. CreateIndex(code, unique)
  CreateIndex(code, unique) threw for values that are not unique and left 'idx_code' registered
```

```csharp
// a: code "x", b: code "y", c: code "x"
await col.CreateIndexAsync("code", unique: true);              // InvalidOperationException: Duplicate key violation (right)
col.ListIndexes();                                             // expected [], actual [idx_code]
await col.QueryIndexAsync("idx_code", "x", "x").ToListAsync(); // [] while FindAsync finds [a, c]
col.Query(BlqlFilter.Eq("code", "x")).ToList();                // [] (BLQL scans the empty index)
await col.InsertAsync(/* d with code "y" */);                  // accepted: a second "y" under a unique index
// dispose and reopen: ListIndexes() is [] again
```

**Expected:** a creation that fails leaves no index behind, so the caller can fix the data and
retry, and queries keep scanning.

**Actual:** until the next reopen the index is listed, answers nothing for the documents that
existed (so every BLQL query on the field returns nothing) and enforces nothing for them;
`CreateIndexAsync` on the field throws "already exists" until `DropIndex`.

**Mechanism (confirmed):** `CreateIndexAsync` (`DynamicCollection.cs:1944-1969`) registers the
index in `_secondaryIndexes` (`:1958`) before it fills it. The fill's `IndexInsert` (`:1964`) throws
from `BTreeIndex.Insert` on the second `"x"`, and nothing catches it: the entry stays, the fill's
transaction is neither committed nor rolled back, and `PersistIndexMetadata` (`:1968`) is never
reached. `CreateVectorIndexAsync` and `CreateSpatialIndexAsync` have the same shape (`:1990`/`:1996`,
`:2022`/`:2028`) but do not throw on duplicates. In the test, the page checks pass after the run
drops the index, so the half tree's written pages are freed by `DropIndex`.

**Affected versions:** the shape of `CreateIndexAsync` since `26caa5a` (v1.4.0).

**Fix considerations:** fill the new tree in its transaction first, register and persist it after
the commit; on failure roll the transaction back and free the tree's pages, as the rebuild in
`VacuumAsync` does (`DynamicCollection.cs:1862-1880`). Checking for duplicates before writing would
let it throw `BLiteValidationException` naming the duplicated value. #190 asks the same question for
typed model indexes.
