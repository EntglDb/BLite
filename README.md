# ⚡ DocumentDb
### High-Performance BSON Database Engine for .NET 10

![Build Status](https://img.shields.io/badge/build-passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)
![Platform](https://img.shields.io/badge/platform-.NET%2010-purple)
![Status](https://img.shields.io/badge/status-active%20development-orange)

**DocumentDb** is an embedded, ACID-compliant, document-oriented database built from scratch for **maximum performance** and **zero allocation**. It leverages modern .NET features like `Span<T>`, `Memory<T>`, and Source Generators to eliminate runtime overhead.

---

## 🚀 Why DocumentDb?

Most embedded databases for .NET are either wrappers around C libraries (SQLite, RocksDB) or legacy C# codebases burdened by heavy GC pressure.

**DocumentDb is different:**
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
- **Smart Scans**: Fallback to **Raw BSON Scanning** (predicates evaluated on raw bytes) when no index is available.

### 🔍 Advanced Indexing
- **B-Tree Indexes**: Logarithmic time complexity for lookups.
- **Composite Indexes**: Support for multi-column keys.
- **Prefix Compression**: Efficient storage for string keys.

### 🛡️ Transactions & ACID
- **Atomic**: Multi-document transactions.
- **Durable**: WAL ensures data safety even in power loss.
- **Isolated**: Snapshot isolation allowing concurrent readers and writers.

---

## 📦 Quick Start

### 1. Installation
*Coming soon to NuGet...*

### 2. Basic Usage

```csharp
// 1. Initialize Engine
using var storage = new StorageEngine("mydb.data");

// 2. Define your Data
public class User 
{ 
    public ObjectId Id { get; set; } 
    public string Name { get; set; } 
}

// 3. Get Collection (Mapper auto-generated via Source Generator)
var users = new DocumentCollection<User>(storage, new UserMapper(), "users");

// 4. Index & Query
users.EnsureIndex(u => u.Name, "idx_name");

users.Insert(new User { Name = "Alice" });

var results = users.AsQueryable()
    .Where(u => u.Name == "Alice")
    .ToList();
```

---

## �️ Roadmap & Status

We are actively building the core. Here is where we stand:

- ✅ **Core Storage**: Paged I/O, WAL, Transactions.
- ✅ **BSON Engine**: Zero-copy Reader/Writer.
- ✅ **Indexing**: B-Tree implementation.
- ✅ **Query Engine**: Basic LINQ (Where, OrderBy, Select, Page).
- 🚧 **Advanced LINQ**: GroupBy, Joins, Aggregations (Coming Soon).
- 🚧 **Source Generators**: Auto-map POCO/DDD classes (Nested Objects, Collections, Value Objects).
- 🚧 **Async I/O**: `ToListAsync`, `FirstAsync` (Coming Soon).

## 🔮 Future Vision

### 1. Advanced Querying & Specialized Indices
- **Vector Search (The AI Layer)**:
  - HNSW (Hierarchical Navigable Small World) index for fast similarity searches.
  - Store embeddings as `BinData` within BSON, indexed in memory-mapped structures.
- **Geospatial (The Location Layer)**:
  - R-Tree or S2 Geometry index for GeoJSON coordinates.
- **Graph Traversals**:
  - Specialized index for "links" (Document IDs) for $O(1)$ navigation without full scans.

### 2. CDC & Event Integration
- **BSON Change Stream**: "Log Miner" that decodes WAL entries and emits structured events.
- **Internal Dispatcher**: Keeps Vector/Spatial indices updated automatically via CDC.

### 3. Performance & Optimization
- **Projection Engine**: Read only specific fields from disk (via BSON offsets) without full document deserialization.

---

## 🤝 Contributing

We welcome contributions! This is a great project to learn about database internals, B-Trees, and high-performance .NET.

### How to Build
1. **Clone**: `git clone https://github.com/mrdevrobot/DocumentDb.git`
2. **Build**: `dotnet build`
3. **Test**: `dotnet test` (We have comprehensive tests for Storage, Indexing, and LINQ).

### Areas to Contribute
- **Missing LINQ Operators**: Help us implement `GroupBy`, `Count`, or `Join`.
- **Benchmarks**: Help us prove `DocumentDb` is faster than the competition.
- **Documentation**: Examples, Guides, and Wiki.

---

## 📝 License

Licensed under the MIT License. Use it freely in personal and commercial projects.

