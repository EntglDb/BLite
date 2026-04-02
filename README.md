# ⚡ BLite
### High-Performance BSON Database Engine for .NET

[![NuGet](https://img.shields.io/nuget/v/BLite?label=nuget&color=red)](https://www.nuget.org/packages/BLite)
[![NuGet Downloads](https://img.shields.io/nuget/dt/BLite?label=downloads)](https://www.nuget.org/packages/BLite)
[![Buy Me a Coffee](https://img.shields.io/badge/sponsor-Buy%20Me%20a%20Coffee-ffdd00?logo=buy-me-a-coffee&logoColor=black)](https://buymeacoffee.com/lucafabbriu)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)
![Platform](https://img.shields.io/badge/platform-.NET%2010%20%7C%20netstandard2.1-purple)
![Status](https://img.shields.io/badge/status-active%20development-orange)

**BLite** is an embedded, ACID-compliant, document-oriented database built from scratch for **maximum performance** and **zero allocation**. It leverages modern .NET features like `Span<T>`, `Memory<T>`, and Source Generators to eliminate runtime overhead.

> **Compatibility**: Targets **net10.0** and **netstandard2.1** — works with .NET 5+, Unity, Xamarin, MAUI, and any netstandard2.1-compatible runtime.

---

> [!IMPORTANT]
> **v4.0.0 — Breaking Change: Async-Only CRUD API**
> Synchronous data methods (`Insert`, `Update`, `Delete`, `FindById`, `FindAll`, `Find`, `InsertBulk`, `UpdateBulk`, `DeleteBulk`, `Count`) have been **removed** from `DocumentCollection<TId, T>` and `DynamicCollection`. Only `*Async` variants are available. Update all call sites to use `await InsertAsync(...)`, `await FindByIdAsync(...)`, etc., and `SaveChangesAsync()` as the commit path.

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
// 1. Declare a secondary index in OnModelCreating:
//    modelBuilder.Entity<User>().HasIndex(x => x.Name);

// 2. Query — the engine translates StartsWith into a B-Tree range scan on 'name'.
//    The Age predicate is evaluated in-memory on the already-filtered result set.
var users = await collection.AsQueryable()
    .Where(x => x.Age > 25 && x.Name.StartsWith("A"))
    .OrderBy(x => x.Age)
    .Take(10)
    .ToListAsync(); // → B-Tree range scan on 'name' + in-memory Age filter
```

- **Optimized**: Uses B-Tree indexes for `=`, `>`, `<`, `Between`, and `StartsWith`.
- **Hybrid Execution**: Combines storage-level optimization with in-memory LINQ to Objects.
- **Advanced Features**: Full support for `GroupBy`, `Join`, `Select` (including anonymous types), and Aggregations (`Count`, `Sum`, `Min`, `Max`, `Average`).

### 🔍 Advanced Indexing
- **B-Tree Indexes**: Logarithmic time complexity for lookups.
- **Composite Indexes**: Support for multi-column keys.
- **Nested Property Indexes**: Index on embedded sub-object fields using lambda expressions (`x => x.Address.City`) for typed collections, or dot-notation strings (`"address.city"`) for schema-less collections. Intermediate null values are safely skipped.
- **Vector Search (HNSW)**: Fast similarity search for AI embeddings using Hierarchical Navigable Small World algorithm.

#### 🏷️ Secondary Indexes on Nested Properties (Typed Collections)

Configure secondary indexes on embedded sub-object properties using a standard lambda path in `OnModelCreating`:

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    // Index on a top-level property
    modelBuilder.Entity<Customer>()
        .HasIndex(x => x.Email);

    // Index on a nested property — dot-notation path is inferred automatically
    modelBuilder.Entity<Customer>()
        .HasIndex(x => x.Address.City);

    // Deeper nesting is supported too
    modelBuilder.Entity<Order>()
        .HasIndex(x => x.Shipping.Address.PostalCode);
}

// Indexed range query — B-Tree hit on "address.city"
var italianCustomers = await db.Customers.AsQueryable()
    .Where(c => c.Address.City == "Milan")
    .ToListAsync(); // → B-Tree index hit on "address.city"

// Single-document equality lookup — fast path directly via index Seek (v4.x)
var customer = await db.Customers.FindOneAsync(c => c.Email == "alice@example.com");
```

> **Note**: If an intermediate property is `null` (e.g. `Address` is `null`) the record is simply skipped by the indexer — no exception is thrown.

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

- **Comparison**: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$type`, `$regex`.
- **String**: `$startsWith`, `$endsWith`, `$contains` — ordinal comparison, no regex interpretation.
- **Array**: `$elemMatch` (scalar and document arrays), `$size`, `$all`.
- **Arithmetic**: `$mod` — modulo check with zero-divisor protection at parse time.
- **Logical**: `$and`, `$or`, `$nor`, `$not` (top-level) and `$not` (field-level condition negation). Implicit AND for multiple top-level fields.
- **Geospatial**: `$geoWithin` (bounding box) and `$geoNear` (Haversine radius in km).
- **Vector**: `$nearVector` — index-accelerated ANN search via HNSW.
- **Security-hardened**: Unknown `$` operators throw `FormatException`. Every operator validates its JSON type. `$mod` divisor=0 rejected at parse time. ReDoS protected via `NonBacktracking`. 252 dedicated security tests.

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

> **v3.6.2 HNSW correctness**: the HNSW implementation received a full correctness pass — `AllocateNode` overflow, neighbor link integrity (`LinkPageChain`), `SelectNeighbors` heuristic (keep closest, not farthest), random level distribution (`mL = 1/ln(M)`), and persistence across database close/reopen are all fixed.

#### 🛠️ Vector Source Configuration (RAG Optimization)
For sophisticated RAG (Retrieval-Augmented Generation) scenarios, BLite allows you to define a **Vector Source Configuration** directly on the collection metadata. This configuration specifies which BSON fields should be used to build the input text for your embedding model.

```csharp
// Define which fields to include in the normalized text for embedding
var config = new VectorSourceConfig()
    .Add("title",   weight: 2.0)   // Boost important fields
    .Add("content", weight: 1.0)
    .Add("tags",    weight: 0.5);

// Set it on a collection
engine.SetVectorSource("documents", config);

// Use TextNormalizer to build the text from any BsonDocument
string text = TextNormalizer.BuildEmbeddingText(doc, config);
// -> "TITLE [Boost: 2.0] ... CONTENT ... TAGS [Boost: 0.5] ..."
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
await db.People.InsertAsync(new Person { Id = 1, Name = "Alice" });

// v3.6.0 — DynamicCollection.Watch() is also supported
using var dynSub = engine.GetOrCreateCollection("orders").Watch()
    .Subscribe(e => Console.WriteLine($"{e.Type}: {e.DocumentId}"));
```

### 🛡️ Transactions & ACID
- **Atomic**: Multi-document transactions.
- **Durable**: WAL ensures data safety even in power loss.
- **Isolated**: Snapshot isolation allowing concurrent readers and writers.
- **Thread-Safe**: Protected with `SemaphoreSlim` to prevent race conditions in concurrent scenarios.
- **Async-Only**: All CRUD operations on `DocumentCollection<TId, T>` and `DynamicCollection` are exclusively async/await — no blocking synchronous methods. Eliminates accidental blocking calls on thread-pool threads.
- **Implicit Transactions**: Use `SaveChangesAsync()` for automatic transaction management.

### � Native TimeSeries
A dedicated `PageType.TimeSeries` — an append-only page format optimised for high-throughput time-ordered data. Introduced natively in **1.12**; the typed `DocumentDbContext` fluent API (`HasTimeSeries`) was added in **3.3.0**.

- **No background threads**: pruning fires transparently on insert (every 1 000 docs or 5 min).
- **Page-level granularity**: entire expired pages are freed in a single pass — O(freed pages), not O(all documents).
- **Transparent reads**: `FindAll()`, BLQL queries, and B-Tree lookups work unchanged.

```csharp
// Enable on any DynamicCollection
var sensors = engine.GetOrCreateCollection("sensors");
sensors.SetTimeSeries("timestamp", TimeSpan.FromDays(7));
engine.Commit();

// Insert as normal — routing to TS pages is automatic
var doc = sensors.CreateDocument(
    ["deviceId", "temperature", "timestamp"],
    b => b.Set("deviceId", "sensor-42")
          .Set("temperature", 23.5)
          .Set("timestamp", DateTime.UtcNow));
await sensors.InsertAsync(doc);

// Force prune immediately (useful in tests)
sensors.ForcePrune();
```

#### Typed API (`DocumentDbContext` — added in 3.3.0)

Configure a typed collection as TimeSeries in `OnModelCreating` using `HasTimeSeries`:

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    modelBuilder.Entity<SensorReading>()
        .ToCollection("sensor_readings")
        .HasTimeSeries(r => r.Timestamp, retention: TimeSpan.FromDays(7));
}

// Insert as normal — routing to TS pages is automatic
await db.SensorReadings.InsertAsync(new SensorReading
{
    SensorId  = "sensor-42",
    Value     = 23.5,
    Timestamp = DateTime.UtcNow
});
await db.SaveChangesAsync();

// Force prune (useful in tests / maintenance)
db.SensorReadings.ForcePrune();
```

### �🔄 Hot Backup
BLite supports hot backups of live databases without blocking readers. The engine uses a combination of the commit lock and WAL checkpointing to ensure the backup is a fully consistent, standalone database file.

```csharp
// 1. Embedded mode (DocumentDbContext)
await db.BackupAsync("backups/mydb-2026-02-25.blite", cancellationToken);

// 2. Schema-less mode (BLiteEngine)
await engine.BackupAsync("backups/mydb-backup.blite");
```

### ⚡ Async Read Operations

All read paths have a true async counterpart — cancellation is propagated all the way down to OS-level `RandomAccess.ReadAsync` (IOCP on Windows).

> [!IMPORTANT]
> **Choosing the right query API — performance matters**
>
> | API | Complexity | Index used? | When to use |
> |:----|:----------:|:-----------:|:------------|
> | `FindByIdAsync(id)` | O(log N) | ✅ Primary key | Always the fastest path for single-document lookup by ID |
> | `FindOneAsync(x => x.Field == value)` | O(log N) | ✅ Secondary (equality) | Single-document equality lookup on an indexed field — direct index `Seek`, bypasses the full LINQ pipeline |
> | `AsQueryable().Where(x => x.Field == value).ToListAsync()` | O(log N + results) | ✅ Secondary (range/eq) | Multi-document queries, range scans, `StartsWith`, composite predicates |
> | `FindAsync(predicate)` | **O(N)** | ❌ **None** | Full collection scan — only use when no suitable index exists or for small collections |
> | `FindAllAsync()` | O(N) | ❌ None | Full streaming scan — suitable for batch processing of entire collections |
>
> **Rule**: always declare a `HasIndex` on the fields you query by equality or range. Without an index, any query degrades to O(N).

```csharp
// ── Fastest: primary-key lookup (O(log N)) ────────────────────────────────
var order = await db.Orders.FindByIdAsync(id, ct);

// ── Fast: single-doc equality via index Seek — bypasses full LINQ pipeline ─
// Requires: modelBuilder.Entity<Order>().HasIndex(x => x.OrderNumber, unique: true)
var order = await db.Orders.FindOneAsync(o => o.OrderNumber == "ORD-9999", ct);

// ── Indexed multi-doc query — B-Tree scan O(log N + results) ──────────────
// Requires: modelBuilder.Entity<Order>().HasIndex(x => x.Status)
var shipped = await db.Orders
    .AsQueryable()
    .Where(o => o.Status == "shipped")
    .ToListAsync(ct);

// ── Full scan — use only when no index applies ────────────────────────────
// ⚠️ O(N): reads every document in the collection.
await foreach (var order in db.Orders.FindAsync(o => o.Notes != null, ct))
    Process(order);

// ── Full collection stream ────────────────────────────────────────────────
await foreach (var order in db.Orders.FindAllAsync(ct))
    Process(order);

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

**Available async methods on `DocumentCollection<TId, T>` (all CRUD operations are async-only since v4.0.0):**

| Method | Description |
|:-------|:------------|
| `FindByIdAsync(id, ct)` | Primary-key lookup via B-Tree — O(log N); returns `ValueTask<T?>` |
| `FindOneAsync(predicate, ct)` | Single-document equality fast path — O(log N) on an indexed field; returns `ValueTask<T?>` |
| `FindAllAsync(ct)` | Full collection streaming — O(N); returns `IAsyncEnumerable<T>` |
| `FindAsync(predicate, ct)` | **Full scan** — O(N), no index involvement; returns `IAsyncEnumerable<T>`. Prefer `AsQueryable().Where(...)` for indexed fields |
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
| **Self-Referencing** | ✅ | Entities can reference themselves (e.g., `Manager` property in `Employee`). Schema generation is recursion-safe — cycles are detected and terminated automatically |
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

### � `IDocumentCollection<TId, T>` Abstraction *(v3.5.0)*
Typed collections implement the `IDocumentCollection<TId, T>` interface — a clean contract covering async CRUD, bulk operations, and LINQ. This makes constructor injection and unit-test mocking straightforward without binding to the concrete `DocumentCollection` class.

```csharp
// Inject or mock via the interface
public class OrderService
{
    private readonly IDocumentCollection<ObjectId, Order> _orders;

    public OrderService(IDocumentCollection<ObjectId, Order> orders) 
        => _orders = orders;

    public async Task PlaceAsync(Order o) { await _orders.InsertAsync(o); }
}
```

### �🗝️ Embedded Key-Value Store
BLite 3.2.0 ships a persistent key-value store **co-located in the same database file** — no extra process, no extra file. Access it via `IBLiteKvStore` on any `BLiteEngine` or `DocumentDbContext`.

- **Raw bytes**: values are `byte[]` / `ReadOnlySpan<byte>` — serialize however you like.
- **Optional TTL**: per-entry expiry with lazy purge (`PurgeExpired()`) or auto-purge on open.
- **Prefix scan**: enumerate all keys with a given prefix.
- **Atomic batches**: set + delete multiple keys under a single lock acquisition.

```csharp
using var engine = new BLiteEngine("data.db");
IBLiteKvStore kv = engine.KvStore;

// Write (optional TTL)
kv.Set("session:abc", Encoding.UTF8.GetBytes("payload"), TimeSpan.FromHours(1));

// Read
byte[]? value = kv.Get("session:abc");

// Exists / Delete
bool exists = kv.Exists("session:abc");
kv.Delete("session:abc");

// Refresh expiry without rewriting value
kv.Refresh("session:abc", TimeSpan.FromHours(2));

// Prefix scan
IEnumerable<string> sessionKeys = kv.ScanKeys("session:");

// Atomic batch (one lock)
kv.Batch()
  .Set("k1", data1)
  .Set("k2", data2, TimeSpan.FromMinutes(30))
  .Delete("k3")
  .Execute();

// Options (passed to BLiteEngine / DocumentDbContext constructor)
var options = new BLiteKvOptions
{
    DefaultTtl         = TimeSpan.FromDays(1),
    PurgeExpiredOnOpen = true
};
using var db = new MyDbContext("app.db", options);
IBLiteKvStore kv = db.KvStore;
```

### 🚀 BLite.Caching — `IDistributedCache`
`BLite.Caching` wraps the embedded KV store as a fully compliant **`IDistributedCache`** — drop it in anywhere you'd use Redis or SQL Server cache, with zero external dependencies.

```
dotnet add package BLite.Caching
```

```csharp
// ASP.NET Core DI registration
builder.Services.AddBLiteDistributedCache("cache.db");

// Optionally with KV options
builder.Services.AddBLiteDistributedCache("cache.db", new BLiteKvOptions
{
    DefaultTtl         = TimeSpan.FromMinutes(30),
    PurgeExpiredOnOpen = true
});
```

The package also exposes `IBLiteCache` — a typed superset of `IDistributedCache`:

```csharp
// Typed helpers (uses System.Text.Json internally)
await cache.SetAsync("user:42", myUser, new DistributedCacheEntryOptions
{
    SlidingExpiration = TimeSpan.FromMinutes(20)
});

User? user = await cache.GetAsync<User>("user:42");

// GetOrSet — built-in thundering-herd protection (per-key SemaphoreSlim)
User user = await cache.GetOrSetAsync("user:42",
    factory: async ct => await db.LoadUserAsync(42, ct),
    options: new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1) });
```

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

// 3. Use with Async Implicit Transactions (Recommended)
using var db = new MyDbContext("mydb.db");

// Operations are tracked automatically
await db.Users.InsertAsync(new User { Name = "Alice" });
await db.Users.InsertAsync(new User { Name = "Bob" });

// Commit all changes at once
await db.SaveChangesAsync();

// 4. Query naturally with LINQ (async)
var results = await db.Users.AsQueryable()
    .Where(u => u.Name.StartsWith("A"))
    .ToListAsync();

// 5. Or use explicit transactions for fine-grained control
using (var txn = db.BeginTransaction())
{
    await db.Users.InsertAsync(new User { Name = "Charlie" });
    await txn.CommitAsync(); // Explicit async commit
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

BsonId id = await orders.InsertAsync(doc, ct);

// Bulk insert (single transaction)
List<BsonId> ids = await orders.InsertBulkAsync([doc1, doc2, doc3], ct);
```

### Read

```csharp
// Primary-key lookup
BsonDocument? doc = await orders.FindByIdAsync(id, ct);

// Full scan
await foreach (var d in orders.FindAllAsync(ct)) { ... }

// Predicate filter
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
int total = await orders.CountAsync(ct);
```

### Update & Delete

```csharp
bool updated = await orders.UpdateAsync(id, newDoc, ct);
bool deleted = await orders.DeleteAsync(id, ct);

// Bulk (single transaction)
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

// Nested path index (dot-notation) — indexes a field inside an embedded document
orders.CreateIndex("shipping.city");                  // indexes doc["shipping"]["city"]
orders.CreateIndex("customer.address.zip");           // arbitrary depth; null intermediates skipped

// Vector index (HNSW) — supports nested paths too
orders.CreateVectorIndex("embedding", dimensions: 1536, metric: VectorMetric.Cosine);
orders.CreateVectorIndex("meta.embedding", dimensions: 768, metric: VectorMetric.Cosine);

// Spatial index (R-Tree) — supports nested paths too
orders.CreateSpatialIndex("location");
orders.CreateSpatialIndex("store.location");

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

## 🔌 BLiteSession — Per-Connection Isolation *(v3.8.0)*

When a single `BLiteEngine` is shared across multiple concurrent clients (e.g. inside a custom server layer), `BLiteSession` provides **per-connection isolated transaction contexts**. Each session carries its own transaction state so independent callers cannot interfere with each other.

Open a session with `engine.OpenSession()`. Disposing the session automatically rolls back any uncommitted transaction.

```csharp
using var engine = new BLiteEngine("data.db");

// One session per connected client / per request
using var session = engine.OpenSession();

// Begin an explicit transaction scoped to this session
using var txn = session.BeginTransaction();
try
{
    await session.InsertAsync("orders",   orderDoc,   ct);
    await session.InsertAsync("invoices", invoiceDoc, ct);
    await session.CommitAsync(ct);
}
catch
{
    session.Rollback(); // or disposed automatically
    throw;
}

// Convenience CRUD (auto-commit each call)
BsonId id  = await session.InsertAsync("users", userDoc, ct);
BsonDocument? doc = await session.FindByIdAsync("users", id, ct);

// Access collections scoped to this session
var col = session.GetOrCreateCollection("events");
col.Insert(eventDoc);
```

> **BLiteSession API** (selected): `BeginTransaction()`, `CommitAsync()`, `Rollback()`, `GetOrCreateCollection(name)`, `GetCollection(name)`, `InsertAsync/InsertBulkAsync`, `FindByIdAsync/FindAllAsync/FindAsync`, `UpdateAsync/UpdateBulkAsync`, `DeleteAsync/DeleteBulkAsync`.

---

## 📂 Multi-File Storage Layout *(v3.8.0)*

BLite 3.8.0 introduces an optional **multi-file storage layout** designed for server deployments where each database should keep its WAL, indexes and collection data in separate files rather than a single monolithic `.db` file.

```csharp
using BLite.Core.Storage;

// Build a server-style config — WAL, index and collection data go to separate files
var config = PageFileConfig.Server("data/mydb.db");
// → WAL:         data/wal/mydb.wal
// → Index file:  data/mydb.idx
// → Collections: data/collections/mydb/<collection>.col

using var engine = new BLiteEngine("data/mydb.db", config);
```

`PageFileConfig.Server()` accepts an optional base config to control page size:

```csharp
var config = PageFileConfig.Server("data/mydb.db", PageFileConfig.Large); // 32 KB pages
```

### BLiteMigration — Single ↔ Multi-File Migration

`BLiteMigration` migrates an existing database between layouts without data loss:

```csharp
// Migrate from single-file to server multi-file layout
BLiteMigration.ToMultiFile(
    sourcePath:   "data/mydb.db",
    targetConfig: PageFileConfig.Server("data/mydb.db"));

// Migrate back to a single file
BLiteMigration.ToSingleFile(
    sourcePath:   "data/mydb.db",
    sourceConfig: PageFileConfig.Server("data/mydb.db"),
    targetPath:   "export/mydb-single.db");
```

Both methods preserve documents, KV entries (including TTL expiry times), and index definitions.

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

**Comparison & field tests**

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
| `{ "f": { "$regex": "^Al" } }` | `BlqlFilter.Regex("f", "^Al")` | Regex (NonBacktracking) |

**String operators**

| JSON syntax | C# equivalent | Description |
|:---|:---|:---|
| `{ "f": { "$startsWith": "Al" } }` | `BlqlFilter.StartsWith("f", "Al")` | Prefix match (ordinal) |
| `{ "f": { "$endsWith": ".com" } }` | `BlqlFilter.EndsWith("f", ".com")` | Suffix match (ordinal) |
| `{ "f": { "$contains": "foo" } }` | `BlqlFilter.Contains("f", "foo")` | Substring match (ordinal) |

**Array operators**

| JSON syntax | C# equivalent | Description |
|:---|:---|:---|
| `{ "f": { "$elemMatch": { "$gt": 80 } } }` | `BlqlFilter.ElemMatch("f", BlqlFilter.Gt("f", 80))` | Any element satisfies condition |
| `{ "f": { "$size": 3 } }` | `BlqlFilter.Size("f", 3)` | Array has exact length |
| `{ "f": { "$all": ["a", "b"] } }` | `BlqlFilter.All("f", ...)` | Array contains all values |

**Arithmetic**

| JSON syntax | C# equivalent | Description |
|:---|:---|:---|
| `{ "f": { "$mod": [3, 0] } }` | `BlqlFilter.Mod("f", 3, 0)` | `field % divisor == remainder` |

**Logical**

| JSON syntax | C# equivalent | Description |
|:---|:---|:---|
| `{ "$and": [...] }` | `BlqlFilter.And(...)` | Logical AND |
| `{ "$or": [...] }` | `BlqlFilter.Or(...)` | Logical OR |
| `{ "$nor": [...] }` | `BlqlFilter.Nor(...)` | Logical NOR |
| `{ "$not": {...} }` | `BlqlFilter.Not(...)` | Top-level NOT |
| `{ "f": { "$not": { "$gt": 0 } } }` | `BlqlFilter.Not(BlqlFilter.Gt("f", 0))` | Field-level condition negation |

**Geospatial**

| JSON syntax | C# equivalent | Description |
|:---|:---|:---|
| `{ "loc": { "$geoWithin": { "$box": [[minLon,minLat],[maxLon,maxLat]] } } }` | `BlqlFilter.GeoWithin("loc", minLon, minLat, maxLon, maxLat)` | Point inside bounding box |
| `{ "loc": { "$geoNear": { "$center": [lon,lat], "$maxDistance": km } } }` | `BlqlFilter.GeoNear("loc", lon, lat, km)` | Point within radius (Haversine) |

**Vector search**

| JSON syntax | C# equivalent | Description |
|:---|:---|:---|
| `{ "emb": { "$nearVector": { "$vector": [...], "$k": 10, "$metric": "cosine" } } }` | `BlqlFilter.NearVector("emb", vector, k: 10)` | HNSW ANN similarity search |

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
- Unknown `$` operators (`$where`, `$expr`, `$function`, …) → `FormatException` — never passed through.
- Every operator validates its JSON type (e.g. `$startsWith` requires string, `$mod` requires `[divisor, remainder]`) → `FormatException` on mismatch.
- `$mod` with divisor `0` is rejected at parse time, preventing `DivideByZeroException` at evaluation.
- `$regex` compiled with `RegexOptions.NonBacktracking` (ReDoS-safe). String operators (`$startsWith`, `$endsWith`, `$contains`) use ordinal comparison — regex metacharacters are literals.
- Deeply nested JSON (> 64 levels) is rejected by `System.Text.Json` before evaluation.
- **252 security tests** covering type-confusion, division-by-zero, deep nesting DoS, large `$in`/`$all` array DoS, and vector dimension bombing.

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
- ✅ **Source Generators**: Auto-map POCO/DDD classes with robust nested objects, collections, and ref struct support. Self-referencing types (recursive cycles) are handled safely.
- ✅ **Nested Property Indexes**: Index on embedded sub-object fields via lambda paths (`x => x.Address.City`) for typed collections and dot-notation strings (`"address.city"`) for schema-less collections. Null intermediates skipped.
- ✅ **Projection Push-down**: SELECT (and WHERE+SELECT) lambdas compile to a single-pass raw-BSON reader — `T` is never instantiated. `IBLiteQueryable<T>` preserves the async chain across all LINQ operators.
- ✅ **BLQL**: MQL-inspired query language for `DynamicCollection` — filter, sort, project and page `BsonDocument` results from JSON strings or via a fluent C# API. Full operator set: comparison, string (`$startsWith`, `$endsWith`, `$contains`), array (`$elemMatch`, `$size`, `$all`), arithmetic (`$mod`), logical, geospatial (`$geoWithin`, `$geoNear`), and vector (`$nearVector`). Security-hardened against injection, ReDoS, and division-by-zero.
- ✅ **Native TimeSeries**: Dedicated `PageType.TimeSeries` (12) with append-only layout, `LastTimestamp` header field and automatic retention-based pruning. Triggered on insert — no background threads. `SetTimeSeries()`, `ForcePrune()`, `IsTimeSeries`, `GetTimeSeriesConfig()` on `DynamicCollection`. Studio UI: TimeSeries tab, TS badge in sidebar.
- ✅ **Page Compaction on Delete**: Intra-page space is reclaimed on every delete — live documents are packed toward the top of the page, `FreeSpaceEnd` is updated and the free-space map is refreshed immediately. Deleted bytes are reusable without a VACUUM pass.
- ✅ **Typed TimeSeries (DocumentDbContext)**: `HasTimeSeries(x => x.Timestamp, retention)` fluent API on `EntityTypeBuilder<T>`. Configure a typed `DocumentDbContext` collection as a TimeSeries source from `OnModelCreating`. `ForcePrune()` available on `DocumentCollection<TId, T>`.
- ✅ **Auto ID Fallback for `string` and `Guid` (v3.4.0)**: primary keys of type `string` are auto-generated as CUID-style strings and `Guid` keys use `Guid.NewGuid()` — no manual ID assignment required on insert. Fixed index navigation for number-based indexes.
- ✅ **`IDocumentCollection<TId, T>` Abstraction (v3.5.0)**: typed collections implement `IDocumentCollection<TId, T>` — a clean interface covering CRUD, LINQ, async, and bulk operations (`Update`, `UpdateBulk`, `Delete`, `DeleteBulk`). Enables constructor injection and mocking without coupling to the concrete `DocumentCollection` class.
- ✅ **CDC Watch on `DynamicCollection` (v3.6.0)**: `DynamicCollection.Watch()` adds real-time change streams to the schema-less API — previously only available on typed `DocumentCollection<TId, T>`.
- ✅ **HNSW Vector Search Correctness (v3.6.2)**: full correctness pass — fixes `AllocateNode` overflow, neighbor link integrity, `SelectNeighbors` heuristic, random level distribution (`mL = 1/ln(M)`), and index persistence across close/reopen. 12 dedicated edge-case tests added.
- ✅ **OLAP GroupBy Push-down (v3.7.0)**: aggregate terminal operators (`Count`, `Sum`, `Min`, `Max`, `Average`) are pushed down to the storage layer via `BTreeQueryProvider.TryBsonAggregate<TResult>`, eliminating unnecessary document materialization for large scans.
- ✅ **BLiteSession — Per-Connection Isolation (v3.8.0)**: `BLiteEngine.OpenSession()` returns a `BLiteSession` with its own isolated transaction context. Multiple sessions on the same engine run independent concurrent transactions. Disposing a session rolls back any uncommitted transaction automatically.
- ✅ **Multi-File Storage Layout (v3.8.0)**: `PageFileConfig.Server(dbPath)` configures separate files for WAL, index data, and per-collection data. `BLiteMigration.ToMultiFile()` / `ToSingleFile()` migrate existing databases between layouts, preserving all documents, KV entries (including TTL), and index definitions.
- ✅ **Non-Blocking Checkpoints (v3.8.0)**: checkpoint and metadata writes are deferred to avoid blocking the hot path. `PageFile` uses `Lazy<T>` for collection file initialization and `ReaderWriterLockSlim` to fix concurrent read/write races.
- ✅ **Async-Only CRUD API (v4.0.0 — Breaking Change)**: Synchronous data methods (`Insert`, `Update`, `Delete`, `FindById`, `FindAll`, `Find`, and all bulk variants) have been **removed** from `DocumentCollection<TId, T>` and `DynamicCollection`. All data operations are now exclusively async — this eliminates accidental blocking calls on thread-pool threads, simplifies the internal code paths, and enforces correct async usage throughout the entire stack.

## 🔮 Future Vision

### 1. Advanced Querying & Specialized Indices
- **Graph Traversals**:
  - Specialized index for "links" (Document IDs) for $O(1)$ navigation without full scans.

### 2. CDC & Event Integration
- **BSON Change Stream**: "Log Miner" that decodes WAL entries and emits structured events.
- **Internal Dispatcher**: Keeps specialized indices updated automatically via CDC.

### 3. Performance & Optimization
- **Portability**: ✅ `.netstandard2.1` support shipped in v2.0 — compatible with Unity, MAUI, Xamarin, and .NET 5+.

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

## � Acknowledgements

Special thanks to the community members who helped improve BLite:

- **[@LeoYang6](https://github.com/LeoYang6)** — For identifying and benchmarking real-world performance bottlenecks, directly driving the zero-allocation read path optimisations in BLite 4.x.

---

## �📝 License

Licensed under the MIT License. Use it freely in personal and commercial projects.


