# ⚡ BLite
### High-Performance BSON Database Engine for .NET 10

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
- **Async-First**: Full async/await support with proper `CancellationToken` handling.
- **Implicit Transactions**: Use `SaveChanges()` / `SaveChangesAsync()` for automatic transaction management (like EF Core).

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

For in-depth technical details, see the complete specification documents:

- **[RFC.md](RFC.md)** - Full architectural specification covering storage engine, indexing, transactions, WAL protocol, and query processing
- **[C-BSON.md](C-BSON.md)** - Detailed wire format specification for BLite's Compressed BSON format, including hex dumps and performance analysis

---

## 📦 Quick Start

### 1. Installation
*Coming soon to NuGet...*

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

## 🗺️ Roadmap & Status

We are actively building the core. Here is where we stand:

- ✅ **Core Storage**: Paged I/O, WAL, Transactions with thread-safe concurrent access.
- ✅ **BSON Engine**: Zero-copy Reader/Writer with lowercase policy.
- ✅ **Indexing**: B-Tree implementation.
- ✅ **Vector Search**: HNSW implementation for Similarity Search.
- ✅ **Geospatial Indexing**: Optimized R-Tree with zero-allocation tuple API.
- ✅ **Query Engine**: Hybrid execution (Index/Scan + LINQ to Objects).
- ✅ **Advanced LINQ**: GroupBy, Joins, Aggregations, Complex Projections.
- ✅ **Async I/O**: Full `async`/`await` support with proper `CancellationToken` handling.
- ✅ **Source Generators**: Auto-map POCO/DDD classes with robust nested objects, collections, and ref struct support.

## 🔮 Future Vision

### 1. Advanced Querying & Specialized Indices
- **Graph Traversals**:
  - Specialized index for "links" (Document IDs) for $O(1)$ navigation without full scans.

### 2. CDC & Event Integration
- **BSON Change Stream**: "Log Miner" that decodes WAL entries and emits structured events.
- **Internal Dispatcher**: Keeps specialized indices updated automatically via CDC.

### 3. Performance & Optimization
- **Projection Engine**: Read only specific fields from disk (via BSON offsets) without full document deserialization.
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

