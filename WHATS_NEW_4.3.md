# What's New in BLite 4.3

BLite 4.3 is a major feature release that introduces an AOT-safe query pipeline, caller-owned transactions, a built-in metrics subsystem, a persistent free-space index, BTree internal optimizations, ValueTask migration, and AOT-safe source-generated accessors. This document describes each feature in detail.

---

## Table of Contents

- [AOT-Safe Query Pipeline](#aot-safe-query-pipeline)
  - [Phase 1: BSON-Level Predicate Compilation](#phase-1-bson-level-predicate-compilation)
  - [Phase 2: Index Planning and Predicate Building](#phase-2-index-planning-and-predicate-building)
  - [Phase 3: Query Interface and Interceptors](#phase-3-query-interface-and-interceptors)
- [Generated Filter Classes](#generated-filter-classes)
- [Caller-Owned Transactions](#caller-owned-transactions)
- [Metrics Subsystem](#metrics-subsystem)
- [Persistent Free-Space Index](#persistent-free-space-index)
- [BTree Internal Optimizations](#btree-internal-optimizations)
- [ValueTask Migration](#valuetask-migration)
- [AOT-Safe Accessors and Constructor Selection](#aot-safe-accessors-and-constructor-selection)
- [Opt-in LINQ Interceptors](#opt-in-linq-interceptors)

---

## AOT-Safe Query Pipeline

BLite 4.3 introduces a three-phase query pipeline that eliminates `Expression.Compile()` at runtime, making the full query path compatible with Native AOT and trimming. Instead of building and compiling LINQ expression trees at query time, BLite now translates predicates directly into BSON-level reader delegates that operate on raw binary data — zero reflection, zero dynamic code generation.

### Phase 1: BSON-Level Predicate Compilation

The `BsonExpressionEvaluator` is the foundation of the AOT-safe pipeline. It accepts a `LambdaExpression` and produces a `BsonReaderPredicate` — a delegate that evaluates a filter directly against raw BSON bytes without deserializing the document into a CLR object.

**Key method:**

```csharp
BsonReaderPredicate? TryCompile<T>(LambdaExpression expression,
    ValueConverterRegistry? converters = null,
    IReadOnlyDictionary<string, ushort>? fieldOrdinals = null);
```

The method returns `null` when the expression contains patterns it cannot translate (e.g., field-to-field comparisons or unsupported method calls), allowing callers to fall back gracefully.

**Supported expression patterns:**

| Category | Examples |
|---|---|
| Binary comparisons | `x.Age > 30`, `x.Name == "Alice"`, `x.Price <= 99.99m` |
| Logical operators | `x.IsActive && x.Age > 18`, `x.A \|\| x.B` |
| Boolean members | `x.IsActive` (implicitly `== true`) |
| Null checks | `x.Email == null`, `x.Email != null` |
| String methods | `x.Name.Contains("li")`, `x.Name.StartsWith("A")`, `x.Name.EndsWith("ce")` |
| Static string helpers | `string.IsNullOrEmpty(x.Name)`, `string.IsNullOrWhiteSpace(x.Name)` |
| IN operator | `ids.Contains(x.Id)`, `Enumerable.Contains(tags, x.Tag)` — uses `HashSet<T>` for O(1) per-document |
| Nullable members | `x.Score.HasValue`, `x.Score.Value > 5` |
| Enum comparisons | Stored as `Int32`/`Int64` in BSON, compared at the integer level |
| CompareTo | `x.Age.CompareTo(30) > 0` |
| Closure captures | Evaluated once at plan-time, not per-document |

**Supported BSON primitive types:**

`int`, `long`, `double`, `decimal`, `bool`, `string`, `DateTime`, `DateTimeOffset`, `ObjectId`, `TimeSpan`, `Guid`, `DateOnly`, `TimeOnly`

**Additional compilation methods:**

- `TryCompileInverse<T>(...)` — produces a logically negated predicate, used internally by `AllAsync` to perform an early-exit O(1) inverted scan rather than evaluating every document.
- `CreateFieldProjector<TResult>(string fieldName)` — creates a delegate that reads a single BSON field by name and returns a typed CLR value, used by the Min/Max aggregation path.

### Phase 2: Index Planning and Predicate Building

Phase 2 introduces a typed, composable API for building query execution plans that can leverage B-Tree indexes when available and fall back to full scans otherwise.

#### BsonPredicateBuilder

A static factory for constructing `BsonReaderPredicate` delegates without LINQ expressions:

```csharp
// Equality and comparison
var pred = BsonPredicateBuilder.Gt("age", 30);
var pred = BsonPredicateBuilder.Between("price", 10.0, 50.0);

// String operations
var pred = BsonPredicateBuilder.Contains("name", "Ali");
var pred = BsonPredicateBuilder.StartsWith("email", "admin@");

// Null checks
var pred = BsonPredicateBuilder.IsNull("deletedAt");
var pred = BsonPredicateBuilder.IsNotNull("email");

// IN operator (HashSet-backed)
var pred = BsonPredicateBuilder.In("status", new[] { "active", "pending" });

// Combinators
var combined = BsonPredicateBuilder.And(
    BsonPredicateBuilder.Gt("age", 18),
    BsonPredicateBuilder.Eq("isActive", true)
);
var negated = BsonPredicateBuilder.Not(pred);
```

Every method returns a `BsonReaderPredicate` — a delegate that takes a `BsonSpanReader` and returns `bool`. No reflection, no expression trees.

#### IndexQueryPlan

An `IndexQueryPlan` describes *how* a query should execute — either via a B-Tree index range/point lookup, or via a full BSON scan:

```csharp
public sealed class IndexQueryPlan
{
    public enum PlanKind { IndexRange, IndexIn, Scan }

    public PlanKind Kind { get; }
    public string? IndexName { get; }
    public BsonReaderPredicate? ScanPredicate { get; }
    public BsonReaderPredicate? ResiduePredicate { get; }

    // Factory methods
    public static IndexQueryPlan Scan(BsonReaderPredicate predicate);
    public static IndexQueryPlan ForRange(string indexName, object? min, object? max);
    public static IndexQueryPlan ForIn(string indexName, IReadOnlyList<IndexKey> keys);

    // Composition — attach a residue predicate to an index plan
    public IndexQueryPlan And(BsonReaderPredicate residue);
}
```

When a B-Tree index covers the filter, the plan records `IndexRange` or `IndexIn` and the engine traverses only the relevant B-Tree leaf pages. When the plan includes a residue predicate (e.g., a strict `>` after an inclusive index range `>=`), the residue is evaluated per-document on the BSON bytes returned by the index scan.

#### IndexPlanBuilder

A fluent builder used by generated filter classes to construct plans from index metadata:

```csharp
public sealed class IndexPlanBuilder
{
    public IndexPlanBuilder(string indexName);

    public IndexQueryPlan Exact(IndexKey key);
    public IndexQueryPlan Range(IndexKey min, IndexKey max);
    public IndexQueryPlan In(IEnumerable<IndexKey> keys);
    public IndexMinMax First();  // O(log n) minimum boundary read
    public IndexMinMax Last();   // O(log n) maximum boundary read
}
```

#### IndexMinMax and BsonAggregator

For Min/Max queries, BLite can take two paths:

1. **O(log n) B-Tree boundary read** — when an index exists, `First()` or `Last()` walks to the leftmost or rightmost leaf of the B-Tree.
2. **Full BSON scan fallback** — when no index is available, BLite scans all documents using a `BsonAggregator`.

```csharp
// With an index — O(log n)
var minPlan = IndexMinMax.ForIndex("idx_Age", isMin: true);

// Without an index — full scan fallback
var minPlan = IndexMinMax.Scan(BsonAggregator.Min("age"));
var maxPlan = IndexMinMax.Scan(BsonAggregator.Max("age"));
```

`BsonAggregator` also supports `Sum` and `Average` for BSON-level field aggregation.

### Phase 3: Query Interface and Interceptors

Phase 3 wires everything together by adding `IndexQueryPlan` and `IndexMinMax` overloads to `IBLiteQueryable<T>`.

#### New IBLiteQueryable overloads

All query terminal methods now accept an `IndexQueryPlan` parameter that drives execution through the AOT-safe pipeline:

```csharp
// Get live index metadata
IEnumerable<CollectionIndexInfo> indexes = db.People.AsQueryable().GetIndexes();

// Build a plan and execute
var plan = PersonFilter.AgeGt(30, indexes);
List<Person> results = await db.People.AsQueryable().ToListAsync(plan);
Person? first       = await db.People.AsQueryable().FirstOrDefaultAsync(plan);
int count           = await db.People.AsQueryable().CountAsync(plan);
bool any            = await db.People.AsQueryable().AnyAsync(plan);
Person[] array      = await db.People.AsQueryable().ToArrayAsync(plan);

// Min/Max via O(log n) B-Tree boundary
var readOnlyIndexes = db.People.AsQueryable().GetIndexes().ToReadOnlyList();
int minAge = await db.People.AsQueryable().MinAsync<int>(PersonFilter.AgeMin(readOnlyIndexes));
int maxAge = await db.People.AsQueryable().MaxAsync<int>(PersonFilter.AgeMax(readOnlyIndexes));

// ForEachAsync
await db.People.AsQueryable().ForEachAsync(plan, person => Console.WriteLine(person.Name));
```

**Full list of new overloads on `IBLiteQueryable<T>`:**

| Method | Description |
|---|---|
| `GetIndexes()` | Returns live `CollectionIndexInfo` for the underlying collection |
| `ToListAsync(IndexQueryPlan, CancellationToken)` | Materializes matching documents into a `List<T>` |
| `FirstOrDefaultAsync(IndexQueryPlan, CancellationToken)` | Returns first match or `null` |
| `FirstAsync(IndexQueryPlan, CancellationToken)` | Returns first match or throws `InvalidOperationException` |
| `SingleOrDefaultAsync(IndexQueryPlan, CancellationToken)` | Returns single match or `null`; throws if multiple |
| `SingleAsync(IndexQueryPlan, CancellationToken)` | Returns single match; throws if none or multiple |
| `AnyAsync(IndexQueryPlan, CancellationToken)` | Returns `true` if at least one match exists |
| `CountAsync(IndexQueryPlan, CancellationToken)` | Returns the number of matching documents |
| `LongCountAsync(IndexQueryPlan, CancellationToken)` | Returns the count as `long` |
| `ToArrayAsync(IndexQueryPlan, CancellationToken)` | Materializes matching documents into a `T[]` |
| `ForEachAsync(IndexQueryPlan, Action<T>, CancellationToken)` | Invokes an action for each match without materialization |
| `MinAsync<TResult>(IndexMinMax, CancellationToken)` | O(log n) min value via B-Tree or BSON scan fallback |
| `MaxAsync<TResult>(IndexMinMax, CancellationToken)` | O(log n) max value via B-Tree or BSON scan fallback |

All overloads are also available as extension methods on `IQueryable<T>` via `BLiteQueryableExtensions`, enabling fluent usage without explicit interface casts.

#### Internal: IBTreeQueryCore\<T\>

The AOT pipeline introduces `IBTreeQueryCore<T>`, an internal interface that exposes the raw scan and aggregation methods without requiring the `TId` generic parameter:

```csharp
internal interface IBTreeQueryCore<T>
{
    IAsyncEnumerable<T> ScanAsync(BsonReaderPredicate predicate, CancellationToken ct = default);
    IAsyncEnumerable<T> ScanAsync(IndexQueryPlan plan, CancellationToken ct = default);
    IEnumerable<CollectionIndexInfo> GetIndexes();
    Task<int> CountAsync(CancellationToken ct = default);
    ValueTask<TResult> MinBoundaryAsync<TResult>(IndexMinMax plan, CancellationToken ct = default);
    ValueTask<TResult> MaxBoundaryAsync<TResult>(IndexMinMax plan, CancellationToken ct = default);
}
```

---

## Generated Filter Classes

The source generator now emits a `{Entity}Filter` static class for each entity registered in a `DocumentDbContext`. These classes provide a typed, AOT-safe API for constructing query plans without writing raw predicate builder calls.

**Example — generated `PersonFilter`:**

```csharp
// String property (unindexed) — returns BsonReaderPredicate
PersonFilter.NameEq("Alice")
PersonFilter.NameContains("li")
PersonFilter.NameStartsWith("A")
PersonFilter.NameEndsWith("ce")
PersonFilter.NameIsNull()
PersonFilter.NameIsNotNull()

// Numeric property (indexed) — returns IndexQueryPlan
var indexes = db.People.AsQueryable().GetIndexes();
PersonFilter.AgeEq(30, indexes)       // Exact B-Tree lookup or scan fallback
PersonFilter.AgeGt(30, indexes)       // Range [30, MaxKey] + residue Gt(30)
PersonFilter.AgeGte(30, indexes)      // Range [30, MaxKey]
PersonFilter.AgeLt(30, indexes)       // Range [MinKey, 30] + residue Lt(30)
PersonFilter.AgeLte(30, indexes)      // Range [MinKey, 30]
PersonFilter.AgeBetween(20, 40, indexes)  // Range [20, 40]
PersonFilter.AgeIn(new[] {10, 30, 50}, indexes)  // Multi-point B-Tree lookup

// Min/Max — O(log n) B-Tree boundary or BsonAggregator fallback
var readOnlyIndexes = indexes.ToReadOnlyList();
PersonFilter.AgeMin(readOnlyIndexes)  // IndexMinMax via First()
PersonFilter.AgeMax(readOnlyIndexes)  // IndexMinMax via Last()
```

**How it works:**

Each generated filter method checks whether a matching B-Tree index exists for the target property:
- If a compatible index is found, it constructs an `IndexQueryPlan` using `IndexPlanBuilder` for index-accelerated execution.
- If no index is found, it falls back to `IndexQueryPlan.Scan(BsonPredicateBuilder.Op(...))` for a full BSON scan with the appropriate predicate.

The fallback is automatic and transparent — the same filter method works regardless of whether an index has been created.

**Generation rules:**

| Property type | Generated operators |
|---|---|
| String | `Eq`, `Contains`, `StartsWith`, `EndsWith`, `IsNull`, `IsNotNull` |
| Numeric (int, long, double, decimal) | `Eq`, `Gt`, `Gte`, `Lt`, `Lte`, `Between`, `In` |
| Indexed numeric | All numeric operators + `Min`, `Max` (via `IndexMinMax`) |

---

## Caller-Owned Transactions

BLite 4.3 fundamentally changes the transaction model. Previously, transactions were implicitly acquired and held by the engine or session. Now, transactions are **caller-owned**: you create them, pass them to collection methods, and control their lifetime explicitly.

### Transaction Interface

```csharp
public interface ITransaction : IDisposable
{
    ulong TransactionId { get; }
    TransactionState State { get; }  // Active, Preparing, Committed, Aborted

    ValueTask CommitAsync(CancellationToken ct = default);
    ValueTask RollbackAsync();
    void AddWrite(WriteOperation operation);
    Task<bool> PrepareAsync();  // Two-phase commit first phase

    event Action? OnRollback;
}

public enum TransactionState : byte
{
    Active, Preparing, Committed, Aborted
}
```

### New Collection Overloads

Every CRUD method on `IDocumentCollection<TId, T>` now accepts an optional `ITransaction?` parameter. When `null`, the operation auto-commits in its own ephemeral transaction. When a transaction is provided, the operation participates in that transaction and commits only when you call `CommitAsync()`.

```csharp
// Auto-commit mode (no transaction argument, backward-compatible)
await db.People.InsertAsync(person);

// Explicit transaction
using var txn = db.BeginTransaction();
await db.People.InsertAsync(person, txn);
await db.People.UpdateAsync(otherPerson, txn);
await txn.CommitAsync();

// Async transaction creation
using var txn = await db.BeginTransactionAsync();
await db.People.InsertAsync(person, txn);
await db.SaveChangesAsync(txn);
```

**Full list of overloaded methods:**

| Method | Signature |
|---|---|
| `InsertAsync` | `ValueTask<TId> InsertAsync(T entity, ITransaction? transaction, CancellationToken ct)` |
| `InsertBulkAsync` | `ValueTask<List<TId>> InsertBulkAsync(IEnumerable<T> entities, ITransaction? transaction, CancellationToken ct)` |
| `FindByIdAsync` | `ValueTask<T?> FindByIdAsync(TId id, ITransaction? transaction, CancellationToken ct)` |
| `FindAllAsync` | `IAsyncEnumerable<T> FindAllAsync(ITransaction? transaction, CancellationToken ct)` |
| `FindAsync` | `IAsyncEnumerable<T> FindAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction, CancellationToken ct)` |
| `FindOneAsync` | `Task<T?> FindOneAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction, CancellationToken ct)` |
| `UpdateAsync` | `ValueTask<bool> UpdateAsync(T entity, ITransaction? transaction, CancellationToken ct)` |
| `UpdateBulkAsync` | `ValueTask<int> UpdateBulkAsync(IEnumerable<T> entities, ITransaction? transaction, CancellationToken ct)` |
| `DeleteAsync` | `ValueTask<bool> DeleteAsync(TId id, ITransaction? transaction, CancellationToken ct)` |
| `DeleteBulkAsync` | `ValueTask<int> DeleteBulkAsync(IEnumerable<TId> ids, ITransaction? transaction, CancellationToken ct)` |
| `ScanAsync` | `IAsyncEnumerable<T> ScanAsync(BsonReaderPredicate pred, ITransaction? transaction, CancellationToken ct)` |
| `QueryIndexAsync` | `IAsyncEnumerable<T> QueryIndexAsync(..., ITransaction? transaction, ...)` |

All existing overloads without the `ITransaction` parameter continue to work unchanged — they delegate to the new overloads with `transaction: null`.

### DbContext Transaction API

```csharp
public interface IDocumentDbContext
{
    ITransaction? CurrentTransaction { get; }
    ITransaction BeginTransaction();
    ValueTask<ITransaction> BeginTransactionAsync(CancellationToken ct = default);
    ValueTask SaveChangesAsync(CancellationToken ct = default);
    ValueTask SaveChangesAsync(ITransaction transaction, CancellationToken ct = default);
}
```

### Defensive Rollback

When an auto-commit operation fails (e.g., a page write error), BLite now performs a defensive rollback to ensure the engine is not left with a partially applied transaction.

---

## Metrics Subsystem

BLite 4.3 includes a built-in, opt-in metrics subsystem designed for zero-allocation on the hot path. It provides real-time counters for transactions, checkpoints, group commits, and per-collection CRUD operations with latency tracking.

### Enabling Metrics

```csharp
using var db = new MyDbContext("data.db");
db.EnableMetrics();  // Opt-in, idempotent — safe to call multiple times
```

Before `EnableMetrics()` is called, `GetMetrics()` returns `null`.

### Capturing a Snapshot

```csharp
MetricsSnapshot? snap = db.GetMetrics();
if (snap is not null)
{
    Console.WriteLine($"Inserts: {snap.InsertsTotal}");
    Console.WriteLine($"Avg Insert Latency: {snap.AvgInsertLatencyUs:F1} µs");
    Console.WriteLine($"Tx Commits: {snap.TransactionCommitsTotal}");
    Console.WriteLine($"Avg Commit Latency: {snap.AvgCommitLatencyUs:F1} µs");

    // Per-collection breakdown
    foreach (var (name, col) in snap.Collections)
    {
        Console.WriteLine($"  [{name}] Inserts={col.InsertCount} " +
                          $"Updates={col.UpdateCount} Deletes={col.DeleteCount}");
    }
}
```

### Observing Metrics Over Time

```csharp
IObservable<MetricsSnapshot> stream = db.WatchMetrics(interval: TimeSpan.FromSeconds(1));
stream.Subscribe(snap =>
{
    Console.WriteLine($"[{snap.SnapshotTimestamp:HH:mm:ss}] " +
                      $"Inserts={snap.InsertsTotal} Queries={snap.QueriesTotal}");
});
```

### MetricsSnapshot

A `MetricsSnapshot` is an immutable point-in-time copy of all counters:

| Property | Type | Description |
|---|---|---|
| `TransactionBeginsTotal` | `long` | Total transaction begin events |
| `TransactionCommitsTotal` | `long` | Total committed transactions |
| `TransactionRollbacksTotal` | `long` | Total rolled-back transactions |
| `AvgCommitLatencyUs` | `double` | Average commit latency in microseconds |
| `GroupCommitBatchesTotal` | `long` | Total group commit batches written |
| `GroupCommitAvgBatchSize` | `double` | Average documents per group commit batch |
| `CheckpointsTotal` | `long` | Total checkpoint events |
| `AvgCheckpointLatencyUs` | `double` | Average checkpoint latency in microseconds |
| `InsertsTotal` | `long` | Total document inserts |
| `UpdatesTotal` | `long` | Total document updates |
| `DeletesTotal` | `long` | Total document deletes |
| `FindsTotal` | `long` | Total `FindById`-style lookups |
| `QueriesTotal` | `long` | Total scan/query operations (`FindAll`, `Scan`, `FindOne`, `Count`) |
| `AvgInsertLatencyUs` | `double` | Average insert latency in microseconds |
| `AvgUpdateLatencyUs` | `double` | Average update latency in microseconds |
| `AvgDeleteLatencyUs` | `double` | Average delete latency in microseconds |
| `AvgQueryLatencyUs` | `double` | Average query latency in microseconds |
| `Collections` | `IReadOnlyDictionary<string, CollectionMetricsSnapshot>` | Per-collection breakdown |
| `SnapshotTimestamp` | `DateTimeOffset` | When the snapshot was captured |

### MetricEventType

```csharp
public enum MetricEventType : byte
{
    TransactionBegin    = 0,
    TransactionCommit   = 1,
    TransactionRollback = 2,
    Checkpoint          = 3,
    GroupCommitBatch    = 4,
    CollectionInsert    = 5,
    CollectionUpdate    = 6,
    CollectionDelete    = 7,
    CollectionFind      = 8,
    CollectionQuery     = 9,
}
```

### Architecture

The metrics pipeline is designed for minimal interference with the database hot path:

1. **Event creation** — A `MetricEvent` struct is created on the stack with a `ValueStopwatch`-derived microsecond latency.
2. **Publishing** — The event is written to an unbounded `Channel<MetricEvent>` via `TryWrite()` — a single-word write that never blocks.
3. **Background aggregation** — A dedicated background task drains the channel and updates atomic counters via `Interlocked.Add`.
4. **Snapshot** — `GetMetrics()` reads all counters atomically and returns an immutable `MetricsSnapshot`.

The `ValueStopwatch` is a zero-allocation `readonly struct` that reads `Stopwatch.GetTimestamp()` and converts to microseconds using precomputed tick conversion factors.

---

## Persistent Free-Space Index

BLite 4.3 replaces the volatile in-memory `_freeSpaceMap` (`Dictionary<uint, ushort>`) with a 16-bucket `FreeSpaceIndex` that provides O(1) page allocation and solves the cold-start page reuse problem.

### The Problem

In BLite 4.2, the free-space map was populated only during the lifetime of the current process. When the database was reopened, the map was empty and the engine would allocate new pages even though existing pages had ample free space, causing file bloat over time.

### The Solution

The `FreeSpaceIndex` is a 16-bucket structure where each bucket covers an equal range of free-byte values. On cold start, BLite now runs `RebuildFreeSpaceIndex()`, which reads only the 24-byte header of each data page (via `ReadPageHeader`, using a `stackalloc byte[24]` — zero heap allocation) and registers the page in the appropriate bucket.

**Bucket architecture:**

- Page size usable space (page size minus 24-byte header) is divided into 16 equal ranges.
- Each bucket is a growable `uint[]` array of page IDs.
- A companion `Dictionary<uint, ushort>` tracks the exact free bytes per page for O(1) bucket-move decisions.

**Key operations:**

| Operation | Complexity | Description |
|---|---|---|
| `FindPage(requiredBytes)` | O(1) amortized | Scans from the lowest bucket that fits, returns the first unlocked page |
| `FindPage(requiredBytes, Func<uint, bool> isPageLocked)` | O(1) amortized | Same, but skips pages locked by other transactions |
| `Update(pageId, freeBytes)` | O(1) steady-state | Moves the page to the correct bucket if the bucket changed |
| Cold-start rebuild | O(n) | Reads 24 bytes per data page, zero heap allocation per page |

The `FindPage` overloads accept an optional `Func<uint, bool>` or `Func<uint, ulong, bool>` delegate to skip pages locked by concurrent transactions without requiring the free-space index to know about the storage engine's locking internals.

---

## BTree Internal Optimizations

The `BTreeIndex` implementation received significant internal improvements for better cache locality, reduced allocations, and faster traversal.

### MaxEntriesPerNode Reduction

`MaxEntriesPerNode` was lowered to **64**. A smaller fan-out means shallower trees for moderate-size collections and reduces the amount of data read per node traversal, improving CPU cache utilization.

### Binary Search in Leaf Nodes

Leaf-level key lookup and insertion now use `BinarySearchLeaf()` — an O(log 64) binary search within a leaf node, replacing the previous linear scan. This is particularly impactful for nodes at or near capacity.

### Stackalloc Page Buffers

B-Tree node reads now use stack-allocated buffers where possible:

```csharp
Span<byte> pageBuffer = stackalloc byte[_storage.PageSize];
ReadPage(leafPageId, txnId, pageBuffer);
```

This eliminates `ArrayPool` rent/return overhead on the hot path.

### Offset-Table Approach

Leaf entries are now stored with a prefix offset table that enables O(1) random access to any entry within a node. Previously, reaching the Nth entry required scanning all N-1 preceding variable-length entries sequentially.

### In-Place Data Shifting

Split, borrow, and underflow operations now use in-place `Span<byte>` copies instead of allocating temporary buffers for shifting entry data within a node page.

---

## ValueTask Migration

All core async APIs have been migrated from `Task`/`Task<T>` to `ValueTask`/`ValueTask<T>`. This reduces allocation pressure for operations that commonly complete synchronously — which is the majority case for in-memory indexed lookups, cached reads, and auto-commit transactions.

### Affected Interfaces

**IDocumentCollection\<TId, T\>:**

```csharp
ValueTask<TId> InsertAsync(T entity, CancellationToken ct = default);
ValueTask<List<TId>> InsertBulkAsync(IEnumerable<T> entities, CancellationToken ct = default);
ValueTask<T?> FindByIdAsync(TId id, CancellationToken ct = default);
ValueTask<bool> UpdateAsync(T entity, CancellationToken ct = default);
ValueTask<int> UpdateBulkAsync(IEnumerable<T> entities, CancellationToken ct = default);
ValueTask<bool> DeleteAsync(TId id, CancellationToken ct = default);
ValueTask<int> DeleteBulkAsync(IEnumerable<TId> ids, CancellationToken ct = default);
```

**IDocumentDbContext:**

```csharp
ValueTask<ITransaction> BeginTransactionAsync(CancellationToken ct = default);
ValueTask SaveChangesAsync(CancellationToken ct = default);
ValueTask SaveChangesAsync(ITransaction transaction, CancellationToken ct = default);
```

**ITransaction:**

```csharp
ValueTask CommitAsync(CancellationToken ct = default);
ValueTask RollbackAsync();
```

### Migration Notes

This is a **breaking API change** for callers that stored the return value in a `Task<T>` variable. Code using `await` directly requires no changes:

```csharp
// Works in both 4.2 and 4.3 — no change needed
await collection.InsertAsync(entity);
var entity = await collection.FindByIdAsync(id);

// 4.2: Task<ObjectId> task = collection.InsertAsync(entity);
// 4.3: ValueTask<ObjectId> task = collection.InsertAsync(entity);
// If storing in a variable, the type changes.
```

---

## AOT-Safe Accessors and Constructor Selection

The source generator now produces AOT-compatible property setters and constructor invocations using .NET 8's `[UnsafeAccessor]` attribute, eliminating the need for `Expression.Compile()` or reflection-based property setting during deserialization.

### UnsafeAccessor-Based Code Generation

On .NET 8+, the generated mapper code uses `[UnsafeAccessor]` for three scenarios:

**Private/init-only property setters:**

```csharp
[UnsafeAccessor(UnsafeAccessorKind.Method, Name = "set_Email")]
private static extern void __UnsafeSetter_Email(MyEntity obj, string value);
```

**Private backing fields (DDD pattern):**

```csharp
[UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_items")]
private static extern ref List<Item> __UnsafeField_Items(MyEntity obj);
```

**Non-public constructors:**

```csharp
[UnsafeAccessor(UnsafeAccessorKind.Constructor)]
private static extern MyEntity __CreateInstance(string name, int age);
```

On `netstandard2.1`, the source generator emits reflection-based fallbacks using `MethodInfo.Invoke`. These paths are not AOT-safe but provide backward compatibility.

### Constructor Selection Algorithm

The source generator now uses a structured algorithm (`SelectConstructor`) to choose the best constructor for deserialization:

1. **Visibility priority:** `protected` > `public` > `internal` > `private`
2. **Parameter count:** Fewer parameters preferred (parameterless constructors are ideal)
3. **Parameter matching:** All constructor parameters must match a property name (case-insensitive)

When no viable constructor is found, the generator emits diagnostic `BLITE010` and falls back to `RuntimeHelpers.GetUninitializedObject` (field initializers will not run in that case).

**EntityInfo metadata:**

```csharp
// null = no viable ctor → GetUninitializedObject fallback
// empty list = parameterless ctor
// non-empty = N-parameter ctor with parameter→property mappings
List<ConstructorParameterInfo>? SelectedConstructorParameters { get; set; }
bool SelectedConstructorIsPublic { get; set; }
```

### EntityAnalyzer Improvements

The `EntityAnalyzer` was improved to handle reference assemblies more accurately. It now only treats external properties as having a private backing setter when the getter is compiler-generated, avoiding incorrect classification of computed or expression-bodied properties as auto-properties.

---

## Opt-in LINQ Interceptors

BLite 4.3 includes experimental support for C# 13 interceptors that automatically transform LINQ query call sites into AOT-safe `IndexQueryPlan` execution paths at compile time.

### Enabling Interceptors

Add the following to your `.csproj`:

```xml
<PropertyGroup>
    <BLiteEnableInterceptors>true</BLiteEnableInterceptors>
</PropertyGroup>
```

### Requirements

- **C# 13** (`LangVersion` set to `13`, `preview`, or `latest`)
- **.NET 9** or later target framework

### How It Works

When enabled, the `MapperGenerator` in `BLite.SourceGenerators` scans for LINQ terminal call sites (e.g., `ToListAsync`, `FirstOrDefaultAsync`, `CountAsync`, `AnyAsync`, `ToArrayAsync`, `ForEachAsync`) in your source code and emits `[InterceptsLocation]`-annotated methods that reroute the call through the AOT-safe `ScanAsync(IndexQueryPlan)` path.

```csharp
// Your code — written as standard LINQ:
var adults = await db.People.AsQueryable()
    .Where(p => p.Age >= 18)
    .ToListAsync();

// What the interceptor does at compile time:
// 1. Compiles the Where predicate to a BsonReaderPredicate via TryCompile<Person>
// 2. Wraps it in an IndexQueryPlan.Scan(predicate)
// 3. Calls collection.ScanAsync(plan) instead of Expression.Compile()
```

**Safety guardrails:**

- Chained `.Where()` calls are detected: the interceptor requires the source argument to be a `ConstantExpression` to avoid silently discarding an inner `Where` predicate.
- When the predicate cannot be compiled to BSON level (e.g., field-to-field comparison), the interceptor does not fire and the standard Expression fallback is used.

### Why Opt-in?

Interceptors are an experimental C# feature and require recent compiler and runtime versions. They are disabled by default to maintain broad compatibility. The `BLite.SourceGenerators.props` file, bundled in the NuGet package, exposes the `BLiteEnableInterceptors` property as a `CompilerVisibleProperty` so the source generator can read it during compilation.
