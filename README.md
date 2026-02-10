# DocumentDb - High-Performance BSON Database Engine

An embedded, ACID-compliant, document-oriented database for .NET 10, built for maximum performance and minimum allocation.

## 🚀 Key Features

### 1. Zero-Allocation Architecture
- **Span-based I/O**: Operations use `Span<byte>` and `Memory<byte>` throughout the stack.
- **No Reflection**: Serialization is handled at compile-time via **Source Generators**.
- **Memory-Mapped Files**: Efficient page-based storage engine with OS-level caching.

### 2. Powerful Query Engine (LINQ)
Full `IQueryable<T>` support seamlessly integrated with the storage engine.
```csharp
// Automatic Index Usage
var users = collection.AsQueryable()
    .Where(x => x.Age > 25 && x.Name.StartsWith("A"))
    .OrderBy(x => x.Age)
    .Take(10)
    .ToList();
```
- **Index Optimization**: Automatically selects indexes for Equality, Range (`>`, `<`), `Between`, and Prefix (`StartsWith`) queries.
- **Scan Optimization**: Fallback queries use **Raw BSON Scanning**, evaluating predicates on bytes without object deserialization.
- **Full Capabilities**: Supports `Select` (Projections), `Skip`/`Take` (Pagination), and Sorting.

### 3. Advanced Indexing
- **B-Tree Indexes**: Logarithmic lookup time for primary and secondary keys.
- **Composite Indexes**: Multi-column indexing support.
- **Range & Prefix**: Efficiently handles range queries and string prefix matching.

### 4. Transactions & ACID
- **WAL (Write-Ahead Logging)**: Ensures durability and crash recovery.
- **Snapshot Isolation**: Readers do not block writers; writers do not block readers.
- **Atomic Operations**: Support for multi-document transactions.

### 5. Large Document Support
- **Overflow Pages**: Handles documents exceeding the 16KB page limit.
- **Adaptive Buffering**: Smart memory management for large object serialization (64KB -> 2MB -> 16MB).

## 🏗️ Architecture Stack

### `DocumentDb.Bson`
Core serialization library.
- `BsonSpanReader`: Low-level, zero-copy BSON parser.
- `BsonSpanWriter`: Stack-allocated BSON writer.

### `DocumentDb.Core`
The database engine.
- `PageFile`: Paged storage abstraction (4KB/8KB/16KB pages).
- `DocumentCollection`: Typed interface for document management.
- `BTreeIndex`: The backbone of data retrieval.

### `DocumentDb.Generators`
Source generators that produce `IBsonMapper<T>` implementations at compile time, eliminating runtime reflection cost.

## 📦 Quick Start

```csharp
// 1. Configure Storage
var storage = new StorageEngine("mydb.data");

// 2. Define Document & Mapper
public class User { public ObjectId Id { get; set; } public string Name { get; set; } }

// 3. Open Collection
var users = new DocumentCollection<User>(storage, new UserMapper(), "users");

// 4. Indexing & Querying
users.EnsureIndex(u => u.Name, "idx_name");

users.Insert(new User { Name = "Alice" });

var results = users.AsQueryable()
    .Where(u => u.Name == "Alice")
    .ToList();
```

## 📊 Performance Philosophy
- **Minimize GC Pressure**: Reuse buffers, avoid temporary objects.
- **Locality of Reference**: B-Tree structure maximizes cache hits.
- **Asynchronous I/O**: Non-blocking disk operations where critical.

## 📝 License
MIT License

