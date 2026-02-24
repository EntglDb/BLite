# ⚡ BLite
### High-Performance BSON Database Engine for .NET 10

[![NuGet](https://img.shields.io/nuget/v/BLite?label=nuget&color=red)](https://www.nuget.org/packages/BLite)
[![NuGet Downloads](https://img.shields.io/nuget/dt/BLite?label=downloads)](https://www.nuget.org/packages/BLite)
[![Buy Me a Coffee](https://img.shields.io/badge/sponsor-Buy%20Me%20a%20Coffee-ffdd00?logo=buy-me-a-coffee&logoColor=black)](https://buymeacoffee.com/lucafabbriu)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)
![Platform](https://img.shields.io/badge/platform-.NET%2010-purple)
![Status](https://img.shields.io/badge/status-active%20development-orange)

**BLite** is an embedded, ACID-compliant, document-oriented database built from scratch for **maximum performance** and **zero allocation**. It leverages modern .NET features like `Span<T>`, `Memory<T>`, and Source Generators to eliminate runtime overhead.

> **Note**: Currently targets **.NET 10** to maximize performance with `Span<T>` and modern hardware intrinsics. Future support for `.netstandard2.1` is being evaluated.

---

## 🚀 Why BLite?

Most embedded databases for .NET are either wrappers around C libraries (SQLite, RocksDB) or legacy C# codebases burdened by heavy GC pressure.

**BLite is different:**
- **Zero Allocation**: I/O and interaction paths use `Span<byte>` and `stackalloc`. No heap allocations for reads/writes.
- **Type-Safe**: No reflection. All serialization code is generated at compile-time.
- **Developer Experience**: Full LINQ provider (`IQueryable`) that feels like Entity Framework but runs on bare metal.
- **Reliable**: Full ACID transactions with Write-Ahead Logging (WAL) and Snapshot Isolation.

---

## ✨ Key Features

### 🚄 Zero-Allocation Architecture
- **Span-based I/O**: The entire pipeline, from disk to user objects, utilizes `Span<T>` to avoid copying memory.
- **Memory-Mapped Files**: OS-level paging and caching for blazing fast access.

### 🧠 Powerful Query Engine (LINQ)
Write queries naturally using LINQ. The engine automatically translates them to optimized B-Tree lookups.

```csharp
// Automatic Index Usage
var users = collection.AsQueryable()
    .Where(x => x.Age > 25 && x.Name.StartsWith("A"))
    .OrderBy(x => x.Age)
    .Take(10)
    .AsEnumerable(); // Executed efficiently on the engine
```

- **Optimized**: Uses B-Tree indexes for `=`, `>`, `<`, `Between`, and `StartsWith`.
- **Hybrid Execution**: Combines storage-level optimization with in-memory LINQ to Objects.
- **Advanced Features**: Full support for `GroupBy`, `Join`, `Select` (including anonymous types), and Aggregations (`Count`, `Sum`, `Min`, `Max`, `Average`).

### 🔍 Advanced Indexing
- **B-Tree Indexes**: Logarithmic time complexity for lookups.
- **Composite Indexes**: Support for multi-column keys.
- **Vector Search (HNSW)**: Fast similarity search for AI embeddings using Hierarchical Navigable Small World algorithm.

### 🔎 BLQL — BLite Query Language
MQL-inspired query language for schema-less (`DynamicCollection`) scenarios. Filter, sort, project, and page `BsonDocument` results using either a fluent C# API or JSON strings — no compile-time types required.

```csharp
// JSON string entry-point (MQL-style)
var docs = col.Query("""{ "status": "active", "age": { "$gt": 18 } }""")
    .Sort("""{ "name": 1 }""")
    .Skip(0).Take(20)
    .ToList();

// Fluent C# API
var docs = col.Query()
    .Filter(BlqlFilter.And(
        BlqlFilter.Eq("status", "active"),
        BlqlFilter.Gt("age", 18)))
    .OrderBy("name")
    .Project(BlqlProjection.Include("name", "email"))
    .ToList();
```

- **All MQL operators**: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$type`, `$regex`.
- **Logical combinators**: `$and`, `$or`, `$nor`, `$not`, implicit AND for multiple top-level fields.
- **Security-hardened**: Unknown `$` operators (e.g. `$where`, `$expr`) throw `FormatException`. ReDoS protected via `NonBacktracking` regex engine.

### 🤖 AI-Ready Vector Search
BLite natively supports vector embeddings and fast similarity search.

```csharp
// 1. Configure vector index on float[] property
modelBuilder.Entity<VectorItem>()
    .HasVectorIndex(x => x.Embedding, dimensions: 1536, metric: VectorMetric.Cosine);

// 2. Perform fast similarity search
var results = db.Items.AsQueryable()
    .VectorSearch(x => x.Embedding, queryVector, k: 5)
    .ToList();
```

### 🌍 High-Performance Geospatial Indexing
BLite features a built-in R-Tree implementation for lightning-fast proximity and bounding box searches.

- **Zero-Allocation**: Uses coordinate tuples `(double, double)` and `Span`-based BSON arrays.
- **LINQ Integrated**: Search naturally using `.Near()` and `.Within()`.

```csharp
// 1. Configure spatial index (uses R-Tree internally)
modelBuilder.Entity<Store>()
    .HasSpatialIndex(x => x.Location);

// 2. Proximity Search (Find stores within 5km)
var stores = db.Stores.AsQueryable()
    .Where(s => s.Location.Near((45.4642, 9.1899), 5.0))
    .ToList();

// 3. Bounding Box Search
var area = db.Stores.AsQueryable()
    .Where(s => s.Location.Within((45.0, 9.0), (46.0, 10.0)))
    .ToList();
```

### 🆔 Custom ID Converters (ValueObjects)
Native support for custom primary key types using `ValueConverter<TModel, TProvider>`. Configure them easily via the Fluent API.

```csharp
// 1. Define your ValueObject and Converter
public record OrderId(string Value);
public class OrderIdConverter : ValueConverter<OrderId, string> { ... }

// 2. Configure in OnModelCreating
modelBuilder.Entity<Order>()
    .Property(x => x.Id)
    .HasConversion<OrderIdConverter>();

// 3. Use it naturally
var order = collection.FindById(new OrderId("ORD-123"));
```

### 📡 Change Data Capture (CDC)
Real-time event streaming for database changes with transactional consistency.

- **Zero-Allocation**: Events are only captured when watchers exist; no overhead when disabled.
- **Transactional**: Events fire only after successful commit, never on rollback.
- **Scalable**: Uses Channel-per-subscriber architecture to support thousands of concurrent listeners.

```csharp
// Watch for changes in a collection
using var subscription = db.People.Watch(capturePayload: true)
    .Subscribe(e => 
    {
        Console.WriteLine($"{e.Type}: {e.DocumentId}");
        if (e.Entity != null) 
            Console.WriteLine($"  Name: {e.Entity.Name}");
    });

// Perform operations - events fire after commit
db.People.Insert(new Person { Id = 1, Name = "Alice" });
```

### 🛡️ Transactions & ACID
- **Atomic**: Multi-document transactions.
- **Durable**: WAL ensures data safety even in power loss.
- **Isolated**: Snapshot isolation allowing concurrent readers and writers.
- **Thread-Safe**: Protected with `SemaphoreSlim` to prevent race conditions in concurrent scenarios.
- **Async-First**: Full async/await support across reads, writes, and transactions — with proper `CancellationToken` propagation throughout the entire stack (B-Tree traversal → page I/O → `RandomAccess.ReadAsync` on OS level).
- **Implicit Transactions**: Use `SaveChanges()` / `SaveChangesAsync()` for automatic transaction management (like EF Core).

### ⚡ Async Read Operations

All read paths have a true async counterpart — cancellation is propagated all the way down to OS-level `RandomAccess.ReadAsync` (IOCP on Windows).

```csharp
// FindById — async primary-key lookup via B-Tree
var order = await db.Orders.FindByIdAsync(id, ct);

// FindAll — async streaming (IAsyncEnumerable)
await foreach (var order in db.Orders.FindAllAsync(ct))
    Process(order);

// FindAsync — async predicate scan (IAsyncEnumerable)
await foreach (var order in db.Orders.FindAsync(o => o.Status == "shipped", ct))
    Process(order);

// LINQ — full async materialisation
var shipped = await db.Orders
    .AsQueryable()
    .Where(o => o.Status == "shipped")
    .ToListAsync(ct);

// Async aggregates
int count = await db.Orders.AsQueryable().CountAsync(ct);
bool any  = await db.Orders.AsQueryable().AnyAsync(o => o.Total > 500, ct);
bool all  = await db.Orders.AsQueryable().AllAsync(o => o.Currency == "EUR", ct);

// First/Single helpers
var first  = await db.Orders.AsQueryable().FirstOrDefaultAsync(o => o.Status == "pending", ct);
var single = await db.Orders.AsQueryable().SingleOrDefaultAsync(o => o.Id == id, ct);

// Materialise to array
var arr = await db.Orders.AsQueryable().ToArrayAsync(ct);

// SaveChanges is also async
await db.SaveChangesAsync(ct);
```

**Available async read methods on `DocumentCollection<TId, T>`:**

| Method | Description |
|:-------|:------------|
| `FindByIdAsync(id, ct)` | Primary-key lookup via B-Tree; returns `ValueTask<T?>` |
| `FindAllAsync(ct)` | Full collection streaming; returns `IAsyncEnumerable<T>` |
| `FindAsync(predicate, ct)` | Async predicate scan; returns `IAsyncEnumerable<T>` |
| `AsQueryable().ToListAsync(ct)` | LINQ pipeline materialized as `Task<List<T>>` |
| `AsQueryable().ToArrayAsync(ct)` | LINQ pipeline materialized as `Task<T[]>` |
| `AsQueryable().FirstOrDefaultAsync(ct)` | First match or `null` |
| `AsQueryable().SingleOrDefaultAsync(ct)` | Single match or `null`; throws on duplicates |
| `AsQueryable().CountAsync(ct)` | Element count |
| `AsQueryable().AnyAsync(predicate, ct)` | Short-circuits on first match |
| `AsQueryable().AllAsync(predicate, ct)` | Returns `false` on first non-match |

### 🔌 Intelligent Source Generation
- **Zero Reflection**: Mappers are generated at compile-time for zero overhead.
- **Nested Objects & Collections**: Full support for complex graphs, deep nesting, and ref struct handling.
- **Robust Serialization**: Correctly handles nested objects, collections, and complex type hierarchies.
- **Lowercase Policy**: BSON keys are automatically persisted as `lowercase` for consistency.
- **Custom Overrides**: Use `[BsonProperty]` or `[JsonPropertyName]` for manual field naming.

#### ✅ Supported Scenarios

The source generator handles a wide range of modern C# patterns:

| Feature | Support | Description |
| :--- | :---: | :--- |
| **Property Inheritance** | ✅ | Properties from base classes are automatically included in serialization |
| **Private Setters** | ✅ | Properties with `private set` are correctly deserialized using Expression Trees |
| **Init-Only Setters** | ✅ | Properties with `init` are supported via runtime compilation |
| **Private Constructors** | ✅ | Deserialization works even without parameterless public constructor |
| **Advanced Collections** | ✅ | `IEnumerable<T>`, `ICollection<T>`, `IList<T>`, `HashSet<T>`, and more |
| **Nullable Value Types** | ✅ | `ObjectId?`, `int?`, `DateTime?` are correctly serialized/deserialized |
| **Nullable Collections** | ✅ | `List<T>?`, `string?` with proper null handling |
| **Unlimited Nesting** | ✅ | Deeply nested object graphs with circular reference protection |
| **Self-Referencing** | ✅ | Entities can reference themselves (e.g., `Manager` property in `Employee`) |
| **N-N Relationships** | ✅ | Collections of ObjectIds for efficient document referencing |

#### ❌ Limitations & Design Choices

| Scenario | Status | Reason |
| :--- | :---: | :--- |
| **Computed Properties** | ⚠️ Excluded | Getter-only properties without backing fields are intentionally skipped (e.g., `FullName => $"{First} {Last}"`) |
| **Constructor Logic** | ⚠️ Bypassed | Deserialization uses `FormatterServices.GetUninitializedObject()` to avoid constructor execution |
| **Constructor Validation** | ⚠️ Not Executed | Validation logic in constructors won't run during deserialization - use Data Annotations instead |

> **💡 Best Practice**: For relationships between entities, prefer **referencing** (storing ObjectIds) over **embedding** (full nested objects) to avoid data duplication and maintain consistency. See tests in `CircularReferenceTests.cs` for implementation patterns.

### 🏷️ Supported Attributes
BLite supports standard .NET Data Annotations for mapping and validation:

| Attribute | Category | Description |
| :--- | :--- | :--- |
| `[Table("name")]` | Mapping | Sets the collection name. Supports `Schema="s"` for `s.name` grouping. |
| `[Column("name")]` | Mapping | Maps property to a specific BSON field name. |
| `[Column(TypeName="...")]`| Mapping | Handles special types (e.g., `geopoint` for coordinate tuples). |
| `[Key]` | Identity | Explicitly marks the primary key (maps to `_id`). |
| `[NotMapped]` | Mapping | Excludes property from BSON serialization. |
| `[Required]` | Validation | Ensures string is not null/empty or nullable type is not null. |
| `[StringLength(max)]` | Validation | Validates string length (supports `MinimumLength`). |
| `[MaxLength(n)]` | Validation | Validates maximum string length. |
| `[MinLength(n)]` | Validation | Validates minimum string length. |
| `[Range(min, max)]` | Validation | Validates numeric values stay within the specified range. |

> [!IMPORTANT]
> Validation attributes (`[Required]`, `[Range]`, etc.) throw a `System.ComponentModel.DataAnnotations.ValidationException` during serialization if rules are violated.

---

## 📚 Documentation

📖 **[Official Documentation → blitedb.com/docs/getting-started](https://blitedb.com/docs/getting-started)**

For in-depth technical details, see the complete specification documents:

- **[RFC.md](RFC.md)** - Full architectural specification covering storage engine, indexing, transactions, WAL protocol, and query processing
- **[C-BSON.md](C-BSON.md)** - Detailed wire format specification for BLite's Compressed BSON format, including hex dumps and performance analysis

---

## 📦 Quick Start

### 1. Installation
```
dotnet add package BLite
```

### 2. Basic Usage

```csharp
// 1. Define your Entities
public class User 
{ 
    public ObjectId Id { get; set; } 
    public string Name { get; set; } 
}

// 2. Define your DbContext (Source Generator will produce InitializeCollections)
public partial class MyDbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, User> Users { get; set; } = null!;

    public MyDbContext(string path) : base(path) 
    {
        InitializeCollections();
    }
}

// 3. Use with Implicit Transactions (Recommended)
using var db = new MyDbContext("mydb.db");

// Operations are tracked automatically
db.Users.Insert(new User { Name = "Alice" });
db.Users.Insert(new User { Name = "Bob" });

// Commit all changes at once
db.SaveChanges();

// 4. Query naturally with LINQ
var results = db.Users.AsQueryable()
    .Where(u => u.Name.StartsWith("A"))
    .AsEnumerable();

// 5. Or use explicit transactions for fine-grained control
using (var txn = db.BeginTransaction())
{
    db.Users.Insert(new User { Name = "Charlie" });
    txn.Commit(); // Explicit commit
}
```

---

## � Schema-less API (BLiteEngine / DynamicCollection)

When compile-time types are not available — server-side query processing, scripting, migrations, or interop scenarios — BLite exposes a **fully schema-less BSON API** via `BLiteEngine` and `DynamicCollection`.

Both paths share the **same kernel**: StorageEngine, B-Tree, WAL, Vector / Spatial indexes.

### Entry Point

```csharp
using var engine = new BLiteEngine("data.db");

// Open (or create) a schema-less collection
var orders = engine.GetOrCreateCollection("orders", BsonIdType.ObjectId);

// List all collections
IReadOnlyList<string> names = engine.ListCollections();

// Drop a collection
engine.DropCollection("orders");
```

### Insert

```csharp
// Build a BsonDocument using the engine's field-name dictionary
var doc = orders.CreateDocument(
    ["status", "total", "currency"],
    b => b
        .Set("status",   "pending")
        .Set("total",    199.99)
        .Set("currency", "EUR"));

BsonId id = orders.Insert(doc);

// Async variant
BsonId id = await orders.InsertAsync(doc, ct);

// Bulk insert (single transaction)
List<BsonId> ids = orders.InsertBulk([doc1, doc2, doc3]);
List<BsonId> ids = await orders.InsertBulkAsync([doc1, doc2, doc3], ct);
```

### Read

```csharp
// Primary-key lookup
BsonDocument? doc = orders.FindById(id);
BsonDocument? doc = await orders.FindByIdAsync(id, ct);

// Full scan
foreach (var d in orders.FindAll()) { ... }
await foreach (var d in orders.FindAllAsync(ct)) { ... }

// Predicate filter
var pending = orders.Find(d => d.GetString("status") == "pending");
await foreach (var d in orders.FindAsync(d => d.GetString("status") == "pending", ct)) { ... }

// Zero-copy predicate scan (BsonSpanReader — no heap allocation per document)
var pending = orders.Scan(reader =>
{
    // Read "status" field directly from the BSON bytes
    if (reader.TryReadString("status", out var status))
        return status == "shipped";
    return false;
});

// B-Tree range query on a secondary index
var recent = orders.QueryIndex("idx_placed_at", minDate, maxDate);

// Vector similarity search
var similar = orders.VectorSearch("idx_embedding", queryVector, k: 10);

// Geospatial proximity / bounding box
var nearby = orders.Near("idx_location", (45.46, 9.18), radiusKm: 5.0);
var inArea  = orders.Within("idx_location", (45.0, 9.0), (46.0, 10.0));

// Count
int total = orders.Count();
```

### Update & Delete

```csharp
bool updated = orders.Update(id, newDoc);
bool deleted = orders.Delete(id);

// Async (collection-level)
bool updated = await orders.UpdateAsync(id, newDoc, ct);
bool deleted = await orders.DeleteAsync(id, ct);

// Bulk (single transaction)
int updatedCount = orders.UpdateBulk([(id1, doc1), (id2, doc2)]);
int deletedCount = orders.DeleteBulk([id1, id2, id3]);

// Bulk async
int updatedCount = await orders.UpdateBulkAsync([(id1, doc1), (id2, doc2)], ct);
int deletedCount = await orders.DeleteBulkAsync([id1, id2, id3], ct);

// or via engine shortcuts (async)
await engine.UpdateAsync("orders", id, newDoc, ct);
await engine.DeleteAsync("orders", id, ct);
int u = await engine.UpdateBulkAsync("orders", [(id1, d1), (id2, d2)], ct);
int d = await engine.DeleteBulkAsync("orders", [id1, id2], ct);
```

### Index Management

```csharp
// B-Tree secondary index
orders.CreateIndex("status");                         // default name = "idx_status"
orders.CreateIndex("placed_at", unique: false);

// Unique index
orders.CreateIndex("order_number", unique: true);

// Vector index (HNSW)
orders.CreateVectorIndex("embedding", dimensions: 1536, metric: VectorMetric.Cosine);

// Spatial index (R-Tree)
orders.CreateSpatialIndex("location");

// Introspect
IReadOnlyList<string> indexes = orders.ListIndexes();

// Drop
orders.DropIndex("idx_status");
```

### Reading BsonDocument fields

```csharp
BsonDocument? doc = orders.FindById(id);
if (doc is not null)
{
    string status   = doc.GetString("status");
    double total    = doc.GetDouble("total");
    BsonId docId    = doc.Id;
}
```

### When to use which API

| | `DocumentDbContext` | `BLiteEngine` |
|:---|:---|:---|
| **Type safety** | ✅ Compile-time | ❌ Runtime `BsonDocument` |
| **Source generators** | ✅ Zero reflection | — |
| **LINQ** | ✅ Full `IQueryable` | ❌ |
| **BLQL** | ❌ | ✅ JSON string queries |
| **Schema-less / dynamic** | ❌ | ✅ |
| **Server / scripting mode** | ❌ | ✅ |
| **Performance** | ✅ Max (generated mappers) | ✅ Near-identical (same kernel) |
| **Shared storage** | ✅ | ✅ Same file |

---

## 🔎 BLQL — BLite Query Language

BLQL is a **BLite Query Language** for `DynamicCollection` — the schema-less counterpart of LINQ for `DocumentDbContext`. Inspired by MQL (MongoDB Query Language), it lets you filter, sort, project, and page `BsonDocument` results using **JSON strings** or a **fluent C# API**, with no compile-time type information required.

### Entry Points

```csharp
using BLite.Core.Query.Blql;

// 1. JSON string filter (MQL-style)
var docs = col.Query("""{ "status": "active", "age": { "$gt": 18 } }""")
    .Sort("""{ "name": 1 }""")
    .Skip(0).Take(20)
    .ToList();

// 2. Programmatic filter
var docs = col.Query()
    .Filter(BlqlFilter.Eq("status", "active").AndAlso(BlqlFilter.Gt("age", 18)))
    .OrderByDescending("createdAt")
    .Project(BlqlProjection.Include("name", "email", "createdAt"))
    .ToList();
```

### Supported Filter Operators

| JSON syntax | C# equivalent | Description |
|:---|:---|:---|
| `{ "f": value }` | `BlqlFilter.Eq("f", v)` | Equality |
| `{ "f": { "$ne": v } }` | `BlqlFilter.Ne("f", v)` | Not equal |
| `{ "f": { "$gt": v } }` | `BlqlFilter.Gt("f", v)` | Greater than |
| `{ "f": { "$gte": v } }` | `BlqlFilter.Gte("f", v)` | Greater than or equal |
| `{ "f": { "$lt": v } }` | `BlqlFilter.Lt("f", v)` | Less than |
| `{ "f": { "$lte": v } }` | `BlqlFilter.Lte("f", v)` | Less than or equal |
| `{ "f": { "$in": [...] } }` | `BlqlFilter.In("f", ...)` | Value in set |
| `{ "f": { "$nin": [...] } }` | `BlqlFilter.Nin("f", ...)` | Value not in set |
| `{ "f": { "$exists": true } }` | `BlqlFilter.Exists("f")` | Field exists |
| `{ "f": { "$type": 16 } }` | `BlqlFilter.Type("f", BsonType.Int32)` | BSON type check |
| `{ "f": { "$regex": "^Al" } }` | `BlqlFilter.Regex("f", "^Al")` | Regex match |
| `{ "$and": [...] }` | `BlqlFilter.And(...)` | Logical AND |
| `{ "$or": [...] }` | `BlqlFilter.Or(...)` | Logical OR |
| `{ "$nor": [...] }` | `BlqlFilter.Nor(...)` | Logical NOR |
| `{ "$not": {...} }` | `BlqlFilter.Not(...)` | Logical NOT |

Multiple top-level fields in one JSON object produce an implicit AND:
```json
{ "status": "active", "age": { "$gt": 18 } }
```

### Sorting

```csharp
// JSON sort (1 = ascending, -1 = descending)
var results = col.Query(filter)
    .Sort("""{ "lastName": 1, "age": -1 }""")  // multi-key sort
    .ToList();

// Fluent sort
var results = col.Query(filter)
    .OrderBy("lastName")
    .ToList();
```

### Projection

```csharp
// Include only specified fields
var results = col.Query(filter)
    .Project(BlqlProjection.Include("name", "email"))
    .ToList();

// Exclude specified fields
var results = col.Query(filter)
    .Project(BlqlProjection.Exclude("password", "__internal"))
    .ToList();
```

### Paging & Terminal Methods

```csharp
var page = col.Query(filter)
    .OrderBy("createdAt")
    .Skip(20).Take(10)     // or .Limit(10)
    .ToList();

// Single document
BsonDocument? doc = col.Query(filter).FirstOrDefault();

// Aggregates
int total = col.Query(filter).Count();
bool any  = col.Query(filter).Any();
bool none = col.Query(filter).None();

// Async streaming
await foreach (var doc in col.Query(filter).AsAsyncEnumerable(ct))
    Process(doc);
```

### Security

The JSON parser is **hardened against BLQL-injection**:
- Unknown `$` operators at root level (`$where`, `$expr`, `$function`, …) → `FormatException`.
- `$regex` patterns are compiled with `RegexOptions.NonBacktracking` (ReDoS-safe).
- `$exists` requires a strict boolean — wrong types throw `FormatException`.
- Deeply nested JSON (> 64 levels) is rejected by `System.Text.Json` before evaluation.

---

## �🗺️ Roadmap & Status

We are actively building the core. Here is where we stand:

- ✅ **Core Storage**: Paged I/O, WAL, Transactions with thread-safe concurrent access.
- ✅ **BSON Engine**: Zero-copy Reader/Writer with lowercase policy.
- ✅ **Indexing**: B-Tree implementation.
- ✅ **Vector Search**: HNSW implementation for Similarity Search.
- ✅ **Geospatial Indexing**: Optimized R-Tree with zero-allocation tuple API.
- ✅ **Query Engine**: Hybrid execution (Index/Scan + LINQ to Objects).
- ✅ **Advanced LINQ**: GroupBy, Joins, Aggregations, Complex Projections.
- ✅ **Async I/O**: True async reads and writes — `FindByIdAsync`, `FindAllAsync` (`IAsyncEnumerable<T>`), `ToListAsync`/`ToArrayAsync`/`CountAsync`/`AnyAsync`/`AllAsync`/`FirstOrDefaultAsync`/`SingleOrDefaultAsync` for LINQ pipelines, `SaveChangesAsync`. `CancellationToken` propagates to `RandomAccess.ReadAsync` (IOCP on Windows).
- ✅ **Source Generators**: Auto-map POCO/DDD classes with robust nested objects, collections, and ref struct support.
- ✅ **Projection Push-down**: SELECT (and WHERE+SELECT) lambdas compile to a single-pass raw-BSON reader — `T` is never instantiated. `IBLiteQueryable<T>` preserves the async chain across all LINQ operators.
- ✅ **BLQL**: MQL-inspired query language for `DynamicCollection` — filter, sort, project and page `BsonDocument` results from JSON strings or via a fluent C# API. Security-hardened against injection and ReDoS.

## 🔮 Future Vision

### 1. Advanced Querying & Specialized Indices
- **Graph Traversals**:
  - Specialized index for "links" (Document IDs) for $O(1)$ navigation without full scans.

### 2. CDC & Event Integration
- **BSON Change Stream**: "Log Miner" that decodes WAL entries and emits structured events.
- **Internal Dispatcher**: Keeps specialized indices updated automatically via CDC.

### 3. Performance & Optimization
- **Portability**: Evaluate `.netstandard2.1` support for broader compatibility (Unity, MAUI, etc.).

---

## 🤝 Contributing

We welcome contributions! This is a great project to learn about database internals, B-Trees, and high-performance .NET.

### How to Build
1. **Clone**: `git clone https://github.com/mrdevrobot/BLite.git`
2. **Build**: `dotnet build`
3. **Test**: `dotnet test` (We have comprehensive tests for Storage, Indexing, and LINQ).

### Areas to Contribute
- **Missing LINQ Operators**: Help us implement additional `IQueryable` functions.
- **Benchmarks**: Help us prove `BLite` is faster than the competition.
- **Documentation**: Examples, Guides, and Wiki.

---

## 📝 License

Licensed under the MIT License. Use it freely in personal and commercial projects.


