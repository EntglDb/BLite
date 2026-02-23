# BLite — AI Agent Context

> Last updated: 2026-02-23 (v1.7.0)
> Keep this file up to date after every development session (see `.agent/rules.md`).

---

## What is BLite?

**BLite** is an embedded, ACID-compliant, document-oriented database built from scratch for .NET 10.
Key design goals: zero reflection, zero allocation, compile-time type-safety via Roslyn Source Generators.

- NuGet: `BLite` (meta-package), `BLite.Core`, `BLite.Bson`, `BLite.SourceGenerators`
- License: MIT
- Repository: https://github.com/EntglDb/BLite
- Website: `website/` (Vue 3 + Vite, docs in `website/src/views/docs/*.vue`)

---

## Solution Structure (`BLite.slnx`)

```
src/
  BLite/                   # Meta-package (re-exports Core + Bson + SourceGenerators)
  BLite.Core/              # Storage engine, collections, indexes, WAL, LINQ, CDC
  BLite.Bson/              # C-BSON: span-based reader/writer, BsonDocument, BsonJsonConverter
  BLite.SourceGenerators/  # Roslyn generator: compile-time BSON mapper generation
  BLite.Demo/              # Usage examples
  BLite.Benchmark/         # BenchmarkDotNet benchmarks
  BLite.Debug/             # Debug/diagnostic helpers
  BLite.CheckpointTest/    # Manual checkpoint stress test

BLite.Shared/              # Shared test infrastructure (MockEntities + TestDbContext)

tests/
  BLite.Tests/             # xUnit test suite (~364 tests, 1 skipped)
```

---

## Core Concepts

### Two Access Modes

| Mode | Entry point | When to use |
|------|-------------|-------------|
| **Typed / Embedded** | `DocumentDbContext` | Compile-time types, Source Generators, full LINQ |
| **Schema-less / Dynamic** | `BLiteEngine` | Server mode, no compile-time types, `BsonDocument` |

Both share the same kernel: `StorageEngine`, BTreeIndex, C-BSON, WAL.

### DocumentDbContext (typed mode)

```csharp
// 1. Define context
public partial class MyDb : DocumentDbContext
{
    public DocumentCollection<ObjectId, User> Users { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<User>()
            .ToCollection("users")
            .HasIndex(u => u.Age);
    }
}

// 2. Use
using var db = new MyDb("mydb.db");
db.Users.Insert(new User { Name = "Alice", Age = 30 });
var users = db.Users.AsQueryable().Where(u => u.Age > 25).ToList();
```

- `partial class` required for Source Generator integration
- Source Generator generates `InitializeCollections()` and all BSON mappers at compile time
- Key types supported: `ObjectId`, `int`, `long`, `string`, `Guid`, custom value objects with converter

### BLiteEngine (schema-less mode)

```csharp
using var engine = new BLiteEngine("mydb.db");
var col = engine.GetOrCreateCollection("users");

var doc = BsonDocument.Empty;
doc["name"] = new BsonValue("Alice");
col.Insert(doc);
```

- Returns `BsonDocument` / `BsonId` — no generics
- `DynamicCollection` exposes the same CRUD + index API as typed collections
- Can use `BsonJsonConverter` to import/export JSON

---

## BLite.Bson

### Key types
- `BsonDocument` — dictionary of `string → BsonValue`, BSON-encoded
- `BsonValue` — discriminated union of all BSON types
- `BsonId` — wrapper for document IDs (ObjectId / int / long / Guid / string)
- `ObjectId` — 12-byte timestamp+random identifier (similar to MongoDB)
- `BsonSpanReader` / `BsonSpanWriter` — zero-allocation span-based serialization
- `BsonSchemaGenerator` — registers all field names at startup into the key dictionary

### BsonJsonConverter (v1.7.0)

JSON ↔ BSON conversion using `System.Text.Json` (no extra dependencies).

```csharp
// JSON → BsonDocument
BsonDocument doc = BsonJsonConverter.FromJson(jsonString, usedKeys);

// BsonDocument → JSON
string json = BsonJsonConverter.ToJson(doc);
```

Type mapping:
- `null` → `BsonValue.Null`
- `bool` → `BsonValue.Boolean`
- number → `BsonValue.Int32` / `Int64` / `Double`
- string → `BsonValue.String` (auto-detects ISO-8601 → DateTime, UUID → Guid)
- object → nested `BsonDocument`
- array → `BsonValue.Array`
- `"_id"` field → `BsonId`

Useful in `DynamicAPI`/`BLiteEngine` scenarios for ingesting JSON payloads.

### BSON key dictionary

All field names must be pre-registered before serialization via  
`BsonSchemaGenerator → GetSchema() → GetAllKeys()`.  
Unregistered keys throw `InvalidOperationException("BSON Key 'x' not found in dictionary cache")`.  
**Both `"id"` and `"_id"` are always emitted** for any property named `Id`  
(root entities use `"_id"` as the document key; nested types use `"id"`).

---

## Source Generator (`BLite.SourceGenerators`)

### Files
- `EntityAnalyzer.cs` — Roslyn symbol analysis, builds `EntityInfo`
- `MapperGenerator.cs` — orchestrates multi-pass analysis
- `CodeGenerator.cs` — emits C# mapper code
- `Models/EntityInfo.cs` — metadata model (properties, key info, flags)

### Key behavior
- Activated by `partial class MyDb : DocumentDbContext`
- Generates: `IDocumentMapper<TId, T>`, `SerializeFields`, `Deserialize(ref BsonSpanReader)`
- `EntityInfo.IsNestedTypeMapper = true` → nested embedded type (not a root collection):
  - Does NOT derive from root-entity base mapper
  - Serializes `[Key] Id` as `"id"` (not `"_id"`)
- Supported property types: all primitives, enums (int/byte/long backing), temporal types  
  (`DateTimeOffset`, `TimeSpan`, `DateOnly`, `TimeOnly`), `ObjectId`, `Guid`,  
  `List<T>`, `T[]`, `ICollection<T>`, `IEnumerable<T>`, `HashSet<T>`, `Dictionary<string, T>`,  
  nullable variants, nested objects, collections of nested objects
- Excluded: computed/getter-only properties, constructor logic

---

## Indexing

### Secondary indexes (B-Tree)
```csharp
modelBuilder.Entity<Person>().HasIndex(p => p.Age);
modelBuilder.Entity<Order>().HasIndex(o => o.Status); // enum supported (v1.7.0)
```

### Vector index (HNSW)
```csharp
modelBuilder.Entity<VectorItem>()
    .HasVectorIndex(x => x.Embedding, dimensions: 1536, metric: VectorMetric.Cosine);
```

### Spatial index (R-Tree)
```csharp
modelBuilder.Entity<Store>()
    .HasSpatialIndex(x => x.Location);
```

Enum index fix (v1.7.0): `ConvertToIndexKey` uses `Convert.ToInt64(value)` for boxed enum values.

---

## LINQ / Query Engine

```csharp
// Auto-uses B-Tree index when available
db.Users.AsQueryable()
    .Where(u => u.Age > 25 && u.Name.StartsWith("A"))
    .OrderBy(u => u.Age)
    .Take(10)
    .ToList();

// Projection push-down (v1.5.0): T never allocated
db.Users.AsQueryable()
    .Where(u => u.Active)
    .Select(u => new { u.Name, u.Email })
    .ToList();

// Async LINQ
await db.Users.AsQueryable()
    .Where(u => u.Age > 18)
    .ToListAsync(ct);
```

---

## Transactions

```csharp
using var tx = db.BeginTransaction();
db.Users.Insert(user);
db.Orders.Insert(order);
tx.Commit();
// tx.Rollback() on exception — WAL ensures ACID
```

---

## CDC (Change Data Capture)

```csharp
db.Users.Watch(change => {
    Console.WriteLine($"{change.OperationType}: {change.DocumentId}");
});
```

---

## Test Infrastructure

### Projects
- `BLite.Shared/` — shared between all test projects
  - `MockEntities.cs` — all entity types used in tests
  - `TestDbContext.cs` — `partial class` with all collections + `OnModelCreating`
  - `TestExtendedDbContext.cs` — extended context for inheritance tests

### Key MockEntities (in `BLite.Shared/MockEntities.cs`)
| Entity | Key type | Notes |
|--------|----------|-------|
| `User` | `ObjectId` | Basic entity |
| `ComplexUser` | `ObjectId` | Nested objects, collections |
| `Person` | `int` | Has `Age` index |
| `VectorEntity` | `ObjectId` | Has `float[]` Embedding, vector index |
| `GeoEntity` | `ObjectId` | Has `(double,double)` Location, spatial index |
| `EnumEntity` | `ObjectId` | Has `Status` enum property + enum index |
| `TemporalEntity` | `ObjectId` | `DateTimeOffset`, `TimeSpan`, `DateOnly`, `TimeOnly` |
| `ContactInfo` | `int` ([Key] Id) | Nested type with Key-decorated Id (v1.7.0) |
| `PersonWithContact` | `ObjectId` | Has `ContactInfo` + `List<ContactInfo>` (v1.7.0) |
| `Employee` | `ObjectId` | Self-referencing circular ref test |

### Test files (`tests/BLite.Tests/`)
Notable files:
- `BsonSchemaTests.cs` — verifies dual `"id"`/`"_id"` schema generation
- `NestedObjectWithIdTests.cs` — 7 tests for nested `[Key] Id` fix (v1.7.0)
- `EnumIndexTests.cs` — enum secondary index (v1.7.0)
- `EnumSerializationTests.cs` — enum BSON serialization
- `VectorSearchTests.cs` — HNSW search
- `GeospatialTests.cs` — R-Tree proximity/bbox
- `AdvancedQueryTests.cs` — LINQ push-down, projections
- `CdcTests.cs` / `CdcScalabilityTests.cs` — CDC (1 test skipped: `Test_Cdc_Slow_Subscriber`)
- `TransactionTests` / `AsyncConcurrencyTests.cs` — ACID + concurrency

Run all tests: `dotnet test` from repo root.  
Current result: **364 passed, 0 failed, 1 skipped**.

---

## Website (`website/`)

Vue 3 + Vite. Docs pages: `website/src/views/docs/*.vue`

| File | Content |
|------|---------|
| `GettingStarted.vue` | Installation, first context, basic CRUD |
| `CRUD.vue` | Full CRUD API reference |
| `Generators.vue` | Source Generator: supported types, nested objects, limitations |
| `Querying.vue` | LINQ, indexes, push-down, async |
| `DynamicAPI.vue` | `BLiteEngine` / `DynamicCollection`, schema-less mode, `BsonJsonConverter` |
| `Transactions.vue` | ACID, WAL, transaction API |
| `CDC.vue` | Change Data Capture |
| `Spatial.vue` | R-Tree spatial index |
| `Converters.vue` | Custom key converters, `BsonJsonConverter` |
| `Architecture.vue` | Internals: storage, B-Tree, WAL, HNSW |
| `Benchmarks.vue` | Performance numbers |
| `Comparisons.vue` | vs SQLite, LiteDB, etc. |
| `Installation.vue` | NuGet packages |

---

## Version History (recent)

| Version | Date | Highlights |
|---------|------|------------|
| **1.7.0** | 2026-02-23 | `BsonJsonConverter` (JSON↔BSON); nested `[Key] Id` fix; enum index fix |
| 1.6.2 | 2026-02-23 | Document metadata page overflow fix |
| 1.6.1 | 2026-02-22 | Full enum support in Source Generator |
| 1.6.0 | 2026-02-22 | `RegisterKeys`/`GetKeyMap` public on `BLiteEngine` |
| 1.5.0 | 2026-02-22 | Projection push-down, `IBLiteQueryable<T>`, async LINQ, `DynamicCollection` CRUD |
| 1.4.0 | 2026-02-21 | Full async read path, `BLiteEngine`, `DynamicCollection` |
| 1.3.0 | 2026-02-19 | Temporal types (`DateTimeOffset`, `TimeSpan`, `DateOnly`, `TimeOnly`) |
| 1.2.0 | 2026-02-19 | Private/init setters, circular refs, N-N relationships |
| 1.1.0 | 2026-02-18 | `Set<TId,T>` on `DocumentDbContext` |
