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

Status: **not filed.** Triage of the rows that `BTreeModelTests` reports as disagreeing with its
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
| B1 | `BTreeIndex cursor primitives drop duplicates and fail past the last key` | 55 |
| B2 | `equal keys come back in reverse insertion order` | 8 |
| B3 | `a NaN with the sign bit clear sorts above +Infinity` | 1 |
| B4 | `a reopen strands the unwritten rest of the last growth block` | 9 |
| #237 | `pages a rolled-back transaction allocated are never reclaimed` | 9 |
| #249 | `-0.0 and +0.0 get different index keys` | 1 |

---

## B1. `BTreeIndex.Equal`, `In`, `GreaterThan`, `LessThan`, `Between` and `StartsWith` miss duplicates and fail past the last key

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

## B2. Equal keys come back from a B-Tree in reverse insertion order, scanning forwards

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

## B3. A NaN with the sign bit clear sorts above `+Infinity` in an index on a double

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

## B4. Every reopen strands the unwritten rest of the last growth block

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
