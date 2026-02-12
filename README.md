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

### 🛡️ Transactions & ACID
- **Atomic**: Multi-document transactions.
- **Durable**: WAL ensures data safety even in power loss.
- **Isolated**: Snapshot isolation allowing concurrent readers and writers.

### 🔌 Intelligent Source Generation
- **Zero Reflection**: Mappers are generated at compile-time for zero overhead.
- **Nested Objects & Collections**: Full support for complex graphs and deep nesting.
- **Lowercase Policy**: BSON keys are automatically persisted as `lowercase` for consistency.
- **Custom Overrides**: Use `[BsonProperty]` or `[JsonPropertyName]` for manual field naming.

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

// 3. Use naturally
using var db = new MyDbContext("mydb.db");
db.Users.Insert(new User { Name = "Alice" });

var results = db.Users.AsQueryable()
    .Where(u => u.Name == "Alice")
    .AsEnumerable();
```

---

## 🗺️ Roadmap & Status

We are actively building the core. Here is where we stand:

- ✅ **Core Storage**: Paged I/O, WAL, Transactions.
- ✅ **BSON Engine**: Zero-copy Reader/Writer with lowercase policy.
- ✅ **Indexing**: B-Tree implementation.
- ✅ **Vector Search**: HNSW implementation for Similarity Search.
- ✅ **Query Engine**: Hybrid execution (Index/Scan + LINQ to Objects).
- ✅ **Advanced LINQ**: GroupBy, Joins, Aggregations, Complex Projections.
- ✅ **Async I/O**: Full `async`/`await` support for CRUD and Bulk operations.
- ✅ **Source Generators**: Auto-map POCO/DDD classes (Nested Objects, Collections, Value Objects).

## 🔮 Future Vision

### 1. Advanced Querying & Specialized Indices
- **Geospatial (The Location Layer)**:
  - R-Tree or S2 Geometry index for GeoJSON coordinates.
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

