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
