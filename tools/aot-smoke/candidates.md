# Candidate issues from the Native AOT smoke test

Status: **filed** on 2026-10-17: A1 as #273, A2 as #274, A3, A4, A6 and A7 together as #275 (one
cause, `#pragma warning disable IL...` in BLite.Core), A5 as #276; the registry's Issue column
links each row. Triage of what `tools/aot-smoke/run.sh` reports on `test/oracle-aot` (upstream
`0450b97`, v5.2.3, plus the oracle-test branches, which change nothing under `src/`),
osx-arm64, .NET SDK 10.0.401, ILCompiler 10.0.12, on 2026-10-17. Seven candidates, two of them
functional failures (A1 under Native AOT only, A2 under both builds), five about the warnings the
publish reports where AOT_LIMITATIONS.md promises none, or promises them at the call site only.

Six of the seven come from one pattern: BLite.Core silences the trim and AOT analyzers with
`#pragma warning disable IL2026 / IL3050 / IL2070` around reflection and `Expression.Compile()`
instead of annotating or restructuring the code. The pragma satisfies the library's own build
(`IsAotCompatible` plus `TreatWarningsAsErrors`), which is the check AOT_LIMITATIONS.md relies on
("BLite.Core's own build validates annotations"), but it is a Roslyn directive: ILC analyses the
IL again in the consumer's publish and reports every one of those sites there.
`git grep -n 'pragma warning disable IL' -- src/BLite.Core` lists 13 of them.

Open and closed issues checked on 2026-10-17 (`gh issue list --repo EntglDb/BLite --state all
--search AOT`): #45 (the original IL2104/IL3053 report, closed by #47) and #50 (AOT-safe query
pipeline, closed). Neither covers a candidate below; nothing covers encryption with rolled-back
transactions.

Affected versions are read from `git describe --contains` of the commit that introduced the code
path; the tool was only run on the tree above.

**Registry.** `run.sh` reads the table below: a publish warning that contains a `warning` row's
text (a `linq-warning` row: only among the warnings the Consumer.Linq publish adds), or a failed
section (a dump line starting with `!!`) that contains a `dump` row's text, is reported as `KNOWN ... [known candidate: A<n>]` instead of failing the run. A section that a `dump`
row matches is not compared beyond the failure until the candidate is fixed. When a fix lands,
delete the candidate's rows in the same PR; a stale row matches nothing and costs nothing, so
check with `grep` that the fix removed what it names.

| Id | Kind | Match | Issue | Summary |
|---|---|---|---|---|
| A1 | dump | `BSON Key 'name' not found in dictionary cache` | #273 | typed writes fail under Native AOT: the generated mapper reports no keys |
| A1 | warning | `UsedKeys.get: Using member` | #273 | the reflection behind it, hidden by a pragma |
| A1 | warning | `BsonSchemaGenerator.GetCollectionItemType` | #273 | the same path, one call further |
| A2 | dump | `AuthenticationTagMismatchException` | #274 | encrypted database: a page allocated by a transaction that never commits cannot be read |
| A3 | warning | `DocumentDbContext.CreateCollection<` | #275 | every source-generated DbContext warns in its generated InitializeCollections |
| A3 | warning | `DocumentDbContext.CreateSessionCollection<` | #275 | and in its generated Set<TId, T> |
| A4 | warning | `ValueConverterRegistry..cctor` | #275 | ValueConverterRegistry.Empty calls the annotated constructor, hidden by a pragma |
| A5 | warning | `BLite.Caching.BLiteDistributedCache.` | #276 | BLite.Caching's typed helpers use reflection-based System.Text.Json, unannotated |
| A5 | dump | `Reflection-based serialization has been disabled` | #276 | and throw in every app with PublishAot or PublishTrimmed |
| A6 | linq-warning | `BLite.Core.Query.BTreeQueryProvider` | #275 | the LINQ provider's internals warn inside BLite.Core, hidden by pragmas |
| A6 | linq-warning | `BLite.Core.Query.BTreeQueryable` | #275 | |
| A6 | linq-warning | `BLite.Core.Query.EnumerableRewriter.VisitMethodCall` | #275 | |
| A6 | linq-warning | `BLite.Core.Query.BsonExpressionEvaluator.TryEvaluate` | #275 | |
| A6 | linq-warning | `BLite.Core.Query.BsonExpressionEvaluator.HasFieldInHierarchy` | #275 | |
| A6 | linq-warning | `BLite.Core.Query.BTreeExpressionVisitor.AddPredicate` | #275 | |
| A7 | linq-warning | `PersonalDataResolver.ReferencesOrIsAssembly` | #275 | typed Watch is annotated but not documented, and warns inside BLite.Core |

---

## A1. Typed collections cannot write under Native AOT: the generated mapper reports no BSON keys (#273)

**API:** every write of a `DocumentCollection<TId, T>` from a source-generated `DocumentDbContext`
(`InsertAsync`, `InsertBulkAsync`, `UpdateAsync`, `UpsertAsync`), the core of what
AOT_LIMITATIONS.md calls the fully safe path ("Generated mapper (no private setters) ✅").

**What the tool reports:** section `typed collection: keys from the generated mapper` fails under
AOT only, at the first insert:

```
!! typed collection: keys from the generated mapper: System.InvalidOperationException: BSON Key 'name' not found in dictionary cache. Ensure all keys are registered before serialization.
   at BLite.Bson.BsonSpanWriter.WriteElementHeader(BsonType, String)
   at AotSmoke.AotDbContext_Model_Mappers.AotSmoke_StoreDocMapper.Serialize(StoreDoc, BsonSpanWriter)
   at BLite.Core.Collections.DocumentCollection`2.SerializeWithRetry(T, Byte[]&)
```

and the publish carries the warning that predicts it:

```
src/BLite.Core/Collections/BaseMappers.cs(38): Trim analysis warning IL2026: BLite.Core.Collections.DocumentMapperBase`2.UsedKeys.get:
  Using member 'DocumentMapperBase`2<TId,T>.GetSchema()' which has 'RequiresUnreferencedCodeAttribute' ...
src/BLite.Core/Collections/BsonSchemaGenerator.cs(178): Trim analysis warning IL2070: BsonSchemaGenerator.GetCollectionItemType(Type): ...
```

**Repro:** the section itself (`tools/aot-smoke/Consumer/Program.cs`, `MapperKeysAsync`): a new
database, a context whose entity has a string property, one `InsertAsync`. Under JIT it passes.

**Mechanism:** `DocumentCollection` registers the mapper's keys in its constructor
(`src/BLite.Core/Collections/DocumentCollection.cs:889`, `_storage.RegisterKeys(_mapper.UsedKeys)`).
The source generator does not override `UsedKeys` (no occurrence in `src/BLite.SourceGenerators`),
so the base implementation runs: `UsedKeys => GetSchema().GetAllKeys()`
(`src/BLite.Core/Collections/BaseMappers.cs:37-39`), and `GetSchema()` is
`BsonSchemaGenerator.FromType<T>()`, which walks `Type.GetProperties()`. The call is annotated
`[RequiresUnreferencedCode]` and the use is wrapped in `#pragma warning disable IL2026`, so BLite
builds clean; under Native AOT the properties' metadata is trimmed, the schema is empty, no key is
registered and the generated `Serialize` throws on its first `WriteString`.

**Affected versions:** since `9fdae7c` (#47, v4.2.0), the release that declared `IsAotCompatible`.
Before it, Native AOT publishing was not claimed.

**Workaround (what the Consumer does for the rest of its sequences):** register the keys on the
engine before the context opens: `storage.RegisterKeys(["_id", "name", ...]);
new MyDbContext(storage)`. With that, the 300-step store sequence is byte-identical under JIT and AOT.

**Fix considerations:** the generator knows every BSON field name it writes (it emits the
`WriteString("name", ...)` calls), so it can emit `public override IEnumerable<string> UsedKeys
=> new[] { "_id", "name", ... };` per mapper, including nested mappers' keys. That removes the
reflection from the safe path and the warning with it; `GetSchema()` stays annotated for the
callers that want a schema. A Native AOT publish of a consumer in CI would have caught it on #47.

---

## A2. Encrypted database: a page allocated by a transaction that never commits cannot be read (#274)

Not an AOT defect: both builds fail identically. The smoke test found it because it runs the store
model's sequence on an encrypted context, which no oracle test does.

**API:** `DocumentDbContext(path, CryptoOptions)` (or an engine with `AesGcmCryptoProvider`), any
typed collection, and a caller-owned transaction that allocates a data page and then does not
commit.

**What the tool reports:** section `encryption at rest: store sequence` fails under both builds:

```
!! encryption at rest: store sequence: System.Security.Cryptography.AuthenticationTagMismatchException: The computed authentication tag did not match the input authentication tag.
   at BLite.Core.Encryption.AesGcmCryptoProvider.Decrypt(UInt32 pageId, ...)
   at BLite.Core.Storage.PageFile.ReadPageHeaderCore(UInt32 pageId, Span`1 destination)
   at BLite.Core.Collections.DocumentCollection`2.FindPageWithSpace(Int32 requiredBytes, ITransaction transaction)
```

**Repro:** three shapes, each from an empty encrypted database, each passing without encryption:

```
1. Begin   2. Insert(id=1, 20 chars)   3. Rollback   4. Insert(id=2)            -> the insert throws (FindPageWithSpace)
1. Begin   2. Insert(id=1, 20 chars)   3. Backup, then open the backup          -> the open throws (RebuildFreeSpaceIndex)
1. Begin   2. Insert(id=1, 20 chars)   3. dispose the context, reopen it        -> the open throws (RebuildFreeSpaceIndex)
```

The third shape means an encrypted database that is closed, or whose process dies, while a
transaction that allocated a page is open cannot be opened again. A first committed insert before
`Begin` avoids all three (the transaction then reuses the existing page instead of allocating one).

**Expected:** the rolled-back or uncommitted page is never visible: the insert goes to a valid page,
the backup and the reopened database open, as they do without encryption.

**Mechanism:** `AllocateNewDataPage` (`src/BLite.Core/Collections/DocumentCollection.cs:1926`)
allocates the page id at once (`AllocateCollectionPage`) but queues the initialised page header as a
write of the transaction (`OperationType.AllocatePage`), so the page reaches the file only on commit.
The id stays in `_currentDataPage` and in the collection's page list after a rollback. The fast path
of `FindPageWithSpace` (`DocumentCollection.cs:1905-1909`, "Page not yet in FSI (e.g., after a
rollback recovery): read header from disk") and `RebuildFreeSpaceIndex` on open then read that
page's header from the file. Without encryption the never-written page reads as zeros, a header
with no free space, and is skipped; with encryption `PageFile.ReadPageHeaderCore`
(`src/BLite.Core/Storage/PageFile.cs:676`) decrypts the whole page, and zeros fail AES-GCM
authentication.

**Affected versions:** encryption since `c2bfec6` (#100, v5.0.0); the FSI fast path since `4c56f63`
(#54, v4.2.1). So v5.0.0 to v5.2.3.

**Fix considerations:** either treat a page that was never written (all zero on disk, or beyond the
last committed size) as free/empty in the encrypted read path, or keep page allocation inside the
transaction (release the id on rollback, do not persist it in the page list before commit). The
store model would catch a regression once it runs once per seed on an encrypted context; adding
`PageFileConfig` with a crypto provider as a mode of `StoreModelTests` is the cheapest guard.

---

## A3. Every source-generated DbContext warns: its generated code calls annotated methods (#275)

**API:** any `partial class X : DocumentDbContext` with collection properties.

**What the tool reports:** in the Consumer publish (and at build time from the analyzer):

```
.../AotSmoke.AotDbContext_Model.Mappers.g.cs(190): AOT analysis warning IL3050: AotSmoke.AotDbContext.InitializeCollections():
  Using member 'BLite.Core.DocumentDbContext.CreateCollection<Int32,StoreDoc>(IDocumentMapper`2<Int32,StoreDoc>)' which has 'RequiresDynamicCodeAttribute' ...
.../AotSmoke.AotDbContext_Model.Mappers.g.cs(190): Trim analysis warning IL2026: ... the same member ... 'RequiresUnreferencedCodeAttribute' ...
```

and at build time also IL2026/IL3050 at `.g.cs(224)` for `CreateSessionCollection` in the
generated `Set<TId, T>()`.

**Mechanism:** `DocumentDbContext.CreateCollection` and `CreateSessionCollection` carry
`[RequiresDynamicCode]` and `[RequiresUnreferencedCode]` (`src/BLite.Core/DocumentDbContext.cs:291-292`
and `:431-432`, since `9fdae7c`, #47) because a model may declare value converters or index key
selectors that compile expressions. The generator emits the calls in `InitializeCollections()` and
`Set<TId, T>()` (`src/BLite.SourceGenerators/MapperGenerator.cs:184-206`) with no annotation and no
suppression, so the warning lands in every consumer's generated code, attributed to a file the
consumer cannot edit. The summary table of AOT_LIMITATIONS.md lists the generated mapper as
"✅ Yes, fully safe"; the document does not mention the context.

At runtime the warned path works under AOT for the Consumer's model (two `HasIndex`, no converter):
the 300-step sequence passes. So the annotation is broader than the risk, which is a property of
the model (converters, expression-compiled key selectors), not of creating a collection.

**Affected versions:** since `9fdae7c` (#47, v4.2.0).

**Fix considerations:** annotate the generated `InitializeCollections` and `Set` the same way
(moves the warning to the user's constructor call, honest but noisy), or narrow the annotation to
the parts that compile expressions (`ValueConverterRegistry`, index key selectors from
`CreateIndexAsync`) and keep `CreateCollection` unannotated when the model has none, which is
what A4 and A1 need too. Either way AOT_LIMITATIONS.md should say what a generated context costs.

---

## A4. `ValueConverterRegistry.Empty` calls the annotated constructor, hidden by a pragma (#275)

**API:** every typed collection (the static field initialises with the type).

**What the tool reports:**

```
src/BLite.Core/Metadata/ValueConverterRegistry.cs(21): AOT analysis warning IL3050: BLite.Core.Metadata.ValueConverterRegistry..cctor():
  Using member 'ValueConverterRegistry.ValueConverterRegistry(Dictionary`2<String,Type>)' which has 'RequiresDynamicCodeAttribute' ...
src/BLite.Core/Metadata/ValueConverterRegistry.cs(21): Trim analysis warning IL2026: ... the same ...
```

**Mechanism:** `public static readonly ValueConverterRegistry Empty = new(new Dictionary<string, Type>());`
(`src/BLite.Core/Metadata/ValueConverterRegistry.cs:20-22`) runs the annotated constructor with an
empty dictionary, which never reaches `Expression.Compile()`, and `#pragma warning disable IL3050,
IL2026` hides that from the build. ILC cannot see that the loop is empty and warns in every
consumer's publish. Harmless at runtime; a warning AOT_LIMITATIONS.md says the safe path does not
produce.

**Affected versions:** the field since `4de5919` (v3.1.0); the warning since the annotations of
`9fdae7c` (#47, v4.2.0).

**Fix considerations:** a private parameterless constructor for `Empty` that only creates the empty
dictionary, so nothing annotated is called; then delete the pragma.

---

## A5. BLite.Caching's typed helpers use reflection-based System.Text.Json and are not annotated (#276)

**API:** `BLiteDistributedCache.Get<T>`, `Set<T>`, `GetAsync<T>`, `SetAsync<T>`, `GetOrSet<T>`,
`GetOrSetAsync<T>` (`IBLiteCache`). BLite.Caching is not mentioned in AOT_LIMITATIONS.md and is not
`IsAotCompatible` (it targets `net8.0` only).

**What the tool reports:** four warnings in the Consumer publish, attributed to BLite.Caching:

```
src/BLite.Caching/BLiteDistributedCache.cs(91): AOT analysis warning IL3050: BLite.Caching.BLiteDistributedCache.Get<T>(String):
  Using member 'System.Text.Json.JsonSerializer.Deserialize<T>(ReadOnlySpan`1<Byte>,JsonSerializerOptions)' ...
src/BLite.Caching/BLiteDistributedCache.cs(91): Trim analysis warning IL2026: ...
src/BLite.Caching/BLiteDistributedCache.cs(96): AOT analysis warning IL3050: BLite.Caching.BLiteDistributedCache.Set<T>(...): ... SerializeToUtf8Bytes ...
src/BLite.Caching/BLiteDistributedCache.cs(96): Trim analysis warning IL2026: ...
```

and section `BLite.Caching` fails under both builds at the first `Set<T>`:

```
!! BLite.Caching: System.InvalidOperationException: Reflection-based serialization has been disabled for this application. ...
```

The byte-array API (`Get`, `Set`, `Remove`, async variants) works under AOT.

**Mechanism:** `Get<T>` and `Set<T>` call `JsonSerializer.Deserialize<T>(bytes)` and
`JsonSerializer.SerializeToUtf8Bytes(value)` with no `JsonTypeInfo` (`src/BLite.Caching/BLiteDistributedCache.cs:91`
and `:96`). With `PublishAot` or `PublishTrimmed` set, the SDK turns
`JsonSerializerIsReflectionEnabledByDefault` off, which is why the JIT run of the same project fails
too: any trimmed or AOT application that uses the typed helpers throws at runtime, with no
call-site warning because the methods are not annotated (the warnings land inside BLite.Caching).

**Affected versions:** since BLite.Caching was added (`b028cfb`, around v3.2.0).

**Fix considerations:** overloads taking `JsonTypeInfo<T>` (the System.Text.Json source-generation
path), and `[RequiresUnreferencedCode]`/`[RequiresDynamicCode]` on the reflection-based ones so the
caller gets the warning. Adding `net10.0` with `IsAotCompatible` to BLite.Caching would make its
own build check this.

---

## A6. The LINQ provider's internals warn inside BLite.Core, hidden by pragmas (#275)

**API:** `AsQueryable()` and the LINQ terminals, which are annotated and documented as not AOT-safe.

**What the tool reports:** the documented promise holds at the call site (`LinqCalls.cs:19` gets
IL2026 and IL3050), but the Consumer.Linq publish also carries 11 warnings inside BLite.Core that the
Consumer publish does not have, for example:

```
src/BLite.Core/Query/BTreeQueryProvider.cs(48): AOT analysis warning IL3050: BTreeQueryProvider`2.<>c.<CreateQuery>b__3_0(Type): Using member 'MethodInfo.MakeGenericMethod(Type[])' ...
src/BLite.Core/Query/BTreeQueryProvider.cs(95): AOT analysis warning IL3050: BTreeQueryProvider`2.<>c__DisplayClass9_0`1.<Execute>b__0(): ...
src/BLite.Core/Query/BTreeQueryable.cs(526): AOT analysis warning IL3050: BTreeQueryable`1.<GetAsyncEnumerator>d__60.MoveNext(): ...
src/BLite.Core/Query/EnumerableRewriter.cs(89): AOT analysis warning IL3050 / Trim analysis warning IL2060: EnumerableRewriter.VisitMethodCall(...): MakeGenericMethod ...
src/BLite.Core/Query/BsonExpressionEvaluator.cs(814): AOT analysis warning IL3050: BsonExpressionEvaluator.TryEvaluate(Expression): Expression.Lambda ...
src/BLite.Core/Query/BsonExpressionEvaluator.cs(739): Trim analysis warning IL2070: BsonExpressionEvaluator.HasFieldInHierarchy(Type,String): ...
src/BLite.Core/Query/BTreeExpressionVisitor.cs(114): AOT analysis warning IL3050: BTreeExpressionVisitor.AddPredicate(LambdaExpression): Expression.Lambda ...
```

**Mechanism:** the `IQueryProvider` and `IAsyncEnumerable` members (`CreateQuery`, `Execute`,
`GetAsyncEnumerator`) implement interfaces whose members are not annotated, so they cannot carry
`[RequiresDynamicCode]` without an IL2046 mismatch; the files suppress the analyzer instead with
file-wide pragmas (`src/BLite.Core/Query/BTreeQueryProvider.cs:30` and `:81`,
`BTreeQueryable.cs:5`, `EnumerableRewriter.cs:12` and `:51`, `BsonExpressionEvaluator.cs:4` and
`:736`, `BTreeExpressionVisitor.cs:4`, `BsonProjectionCompiler.cs:7`). ILC still reports them. The
user already has the call-site warning, so these add noise in a library assembly, the outcome the
annotations of #47 were meant to remove (AOT_LIMITATIONS.md, Overview point 1). The
`BsonExpressionEvaluator` fallback (`Expression.Lambda(expression).Compile().DynamicInvoke()`,
`:814-815`) is the one known AOT-suspect spot on the evaluation path of a query.

**Affected versions:** since `9fdae7c` (#47, v4.2.0).

**Fix considerations:** `[UnconditionalSuppressMessage]` with a justification ("reachable only
through AsQueryable, which carries RequiresDynamicCode") on the members whose only entry is an
annotated API: it is visible to ILC, unlike the pragma. The `BsonExpressionEvaluator` fallback
deserves its own decision: interpret instead of compile, or annotate the evaluator.

---

## A7. Typed `Watch` is annotated but not in AOT_LIMITATIONS.md, and warns inside BLite.Core (#275)

**API:** `DocumentCollection<TId, T>.Watch(WatchOptions?)` and `Watch(bool)`, the typed CDC
subscription. (The dynamic `DynamicCollection.Watch` is unannotated and passes under AOT: section
`dynamic collection` delivers the same 66 events under both builds.)

**What the tool reports:** the call site warns as an annotated API should (`LinqCalls.cs:25`,
IL2026), which the document does not announce: AOT_LIMITATIONS.md lists LINQ, index creation,
schema generation and value converters only. And the publish adds one warning inside BLite.Core:

```
src/BLite.Core/GDPR/PersonalDataResolver.cs(153): Trim analysis warning IL2026: BLite.Core.GDPR.PersonalDataResolver.ReferencesOrIsAssembly(Assembly,Assembly):
  Using member 'System.Reflection.Assembly.GetReferencedAssemblies()' ...
```

**Mechanism:** `Watch` is annotated `[RequiresUnreferencedCode("Watch resolves personal-data fields
via PersonalDataResolver which uses reflection.")]` (`src/BLite.Core/Collections/DocumentCollection.cs:4578`
and `:4634`); `PersonalDataResolver.ReferencesOrIsAssembly` wraps `GetReferencedAssemblies()` in
`#pragma warning disable IL2026` (`src/BLite.Core/GDPR/PersonalDataResolver.cs:152-154`). The other
GDPR entry points (`GdprEngineExtensions`, `GdprDocumentDbContextExtensions`, `PersonalDataMetadataCache`)
are annotated too and also missing from the document.

**Affected versions:** the annotation since the CDC payload masking (GDPR) work; the pragma since
`9fdae7c` (#47, v4.2.0) or later.

**Fix considerations:** list typed `Watch` and the GDPR APIs in AOT_LIMITATIONS.md (with the
workaround: `DynamicCollection.Watch`, or `Watch` with a payload mask that needs no reflection), and
replace the pragma with `[UnconditionalSuppressMessage]` or an annotation.
