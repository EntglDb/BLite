# RFC-BLite: High-Performance Embedded Document Database for .NET

**Status:** Draft  
**Version:** 0.1.0  
**Date:** February 2026  
**Authors:** BLite Development Team

---

## Abstract

This document specifies **BLite**, a high-performance embedded document-oriented database engine for .NET 10. BLite is designed from the ground up for **zero-allocation performance**, leveraging modern .NET features including `Span<T>`, Memory-Mapped Files, and Source Generators. The database implements a custom **C-BSON** (Compressed BSON) format that achieves 30-60% storage reduction compared to standard BSON while maintaining full type compatibility.

Key innovations include:
- **Zero-allocation I/O** via `Span<byte>` and `stackalloc`
- **C-BSON format** with field name compression (2-byte IDs vs. variable-length strings)
- **Page-based storage** with memory-mapped file I/O
- **Multiple index types:** B+Tree, R-Tree (geospatial), and HNSW (vector similarity)
- **ACID transactions** with Write-Ahead Logging and Snapshot Isolation
- **Compile-time code generation** for zero-reflection serialization

---

## 1. Introduction

### 1.1 Motivation

Most embedded databases for .NET fall into two categories:

1. **Native wrappers** (SQLite, RocksDB): High performance but with interop overhead and GC pressure from marshalling
2. **Managed implementations** (LiteDB, others): Pure C# but burdened by reflection, excessive allocations, and legacy design

BLite bridges this gap by leveraging **.NET 10's modern performance features** while remaining a pure managed implementation:

- `Span<T>` and `Memory<T>` for zero-copy I/O
- Source Generators for zero-reflection serialization
- Memory-Mapped Files for OS-level page caching
- Stack allocation (`stackalloc`) for ephemeral buffers

### 1.2 Scope

This RFC specifies:
- **Storage Engine:** Page file format, WAL protocol, transaction semantics
- **C-BSON Format:** Wire format, schema management, key compression
- **Indexing:** B+Tree, R-Tree, and HNSW implementations
- **Query Engine:** LINQ provider and hybrid execution model
- **Code Generation:** Mapper generation rules and attribute support

### 1.3 Terminology

**MUST**, **SHOULD**, **MAY**: As defined in RFC 2119

- **Page:** Fixed-size block of data (default 16KB)
- **Slot:** Variable-size entry within a slotted page
- **C-BSON:** Compressed BSON with field ID compression
- **WAL:** Write-Ahead Log for durability
- **MBR:** Minimum Bounding Rectangle (for R-Tree)
- **HNSW:** Hierarchical Navigable Small World (vector search algorithm)

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    BLite Architecture                    │
├─────────────────────────────────────────────────────────┤
│  ┌───────────────┐  ┌───────────────┐  ┌─────────────┐ │
│  │ LINQ Provider  │  │ Source Gen    │  │ Collections │ │
│  │  (Queryable)   │  │  (Mappers)    │  │  (DbContext)│ │
│  └───────┬───────┘  └───────┬───────┘  └──────┬──────┘ │
│          │                   │                  │        │
│  ┌───────▼─────────────────────────────────────▼──────┐ │
│  │          Index Layer (B-Tree, R-Tree, HNSW)        │ │
│  └───────┬─────────────────────────────────────┬──────┘ │
│          │                                      │        │
│  ┌───────▼──────────────────────────────────────▼─────┐ │
│  │         Storage Engine (Pages, Transactions)       │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────┐ │ │
│  │  │   PageFile   │  │     WAL      │  │ FreeList │ │ │
│  │  └──────────────┘  └──────────────┘  └──────────┘ │ │
│  └────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────┐ │
│  │       C-BSON (Span-based Reader/Writer)            │ │
│  └────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────┐ │
│  │   OS Memory-Mapped Files (Kernel Page Cache)       │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

### 2.1 Storage Layer

The storage layer manages page-based I/O using memory-mapped files:

- **PageFile:** Fixed-size pages (8KB, 16KB, or 32KB)
- **Page Types:** Header, Data, Index, Vector, Spatial, Dictionary, Schema, Overflow, Free
- **Free List:** Linked list of reusable pages

### 2.2 C-BSON Layer

Provides zero-allocation BSON serialization/deserialization:

- **BsonSpanWriter:** Writes C-BSON to `Span<byte>`
- **BsonSpanReader:** Reads C-BSON from `ReadOnlySpan<byte>`
- **Schema Management:** Field name → ID mapping

Full specification: See `C-BSON.md`

### 2.3 Index Layer

Three specialized index types:

- **B+Tree:** General-purpose sorted index (range queries, equality)
- **R-Tree:** Geospatial index (proximity, bounding box queries)
- **HNSW:** Vector similarity search (k-NN, ANN)

### 2.4 Query Layer

LINQ-to-BLite provider:

- Translates LINQ expressions to index operations
- Hybrid execution: Index-based filtering + in-memory LINQ to Objects
- Supports `Where`, `OrderBy`, `Skip`, `Take`, `GroupBy`, `Join`, aggregations

### 2.5 Transaction Layer

ACID guarantees via:

- **Atomicity:** All-or-nothing commits
- **Consistency:** Schema validation
- **Isolation:** Snapshot isolation (MVCC-like)
- **Durability:** Write-Ahead Logging (WAL)

---

## 3. Storage Engine Specification

### 3.1 Page File Format

#### 3.1.1 Page Sizes

BLite supports 3 predefined page sizes:

| Configuration | Page Size | Use Case                        |
|:--------------|:----------|:--------------------------------|
| **Small**     | 8 KB      | Embedded, tiny documents        |
| **Default**   | 16 KB     | General purpose (InnoDB-like)   |
| **Large**     | 32 KB     | Big documents (MongoDB-like)    |

**Rationale:** 16KB aligns with Linux page cache and InnoDB defaults, balancing fragmentation vs. overhead.

#### 3.1.2 File Layout

```
┌─────────────────────────────────────────────────────────┐
│  Page 0: File Header                                    │
│    [PageHeader (32)]                                    │
│    [Database Version (4)]                               │
│    [Page Size (4)]                                      │
│    [First Free Page ID (4)] ← Free list head            │
│    [Dictionary Root Page ID (4)]                        │
│    [Reserved (remaining)]                               │
├─────────────────────────────────────────────────────────┤
│  Page 1: Collection Metadata                            │
│    [SlottedPageHeader (24)]                             │
│    [Collection Schemas...]                              │
├─────────────────────────────────────────────────────────┤
│  Page 2+: Data, Index, Vector, Spatial, Dictionary...  │
└─────────────────────────────────────────────────────────┘
```

### 3.2 Page Header Format

All pages start with a **32-byte header**:

```
Offset  Size  Field               Description
------  ----  ------------------  --------------------------
0       4     PageId              Page number (0-indexed)
4       1     PageType            Enum (see §3.3)
5       2     FreeBytes           Unused space in page
7       4     NextPageId          Linked list pointer
11      8     TransactionId       Last modifying transaction
19      4     Checksum            CRC32 of page data
23      4     DictionaryRootPageId (Page 0 only)
27      5     Reserved            Future use
```

**Total:** 32 bytes

**Implementation:**
```csharp
[StructLayout(LayoutKind.Explicit, Size = 32)]
public struct PageHeader
{
    [FieldOffset(0)]  public uint PageId;
    [FieldOffset(4)]  public PageType PageType;
    [FieldOffset(5)]  public ushort FreeBytes;
    [FieldOffset(7)]  public uint NextPageId;
    [FieldOffset(11)] public ulong TransactionId;
    [FieldOffset(19)] public uint Checksum;
    [FieldOffset(23)] public uint DictionaryRootPageId;
}
```

### 3.3 Page Types

| Value | Type        | Purpose                                  |
|:------|:------------|:-----------------------------------------|
| 0     | Empty       | Uninitialized                            |
| 1     | Header      | Page 0 (file header)                     |
| 2     | Collection  | Schema and collection metadata           |
| 3     | Data        | Document storage (slotted page)          |
| 4     | Index       | B+Tree node                              |
| 5     | FreeList    | Deprecated (unused)                      |
| 6     | Overflow    | Continuation of large documents          |
| 7     | Dictionary  | String interning for C-BSON keys         |
| 8     | Schema      | Schema versioning                        |
| 9     | Vector      | HNSW index node                          |
| 10    | Free        | Reusable page (linked via NextPageId)    |
| 11    | Spatial     | R-Tree node                              |

### 3.4 Slotted Page Structure

Data pages use a **slotted page** design for variable-size documents:

```
┌─────────────────────────────────────────────────────────┐
│  [SlottedPageHeader (24)]                               │
│    PageId, PageType, SlotCount, FreeSpaceStart,         │
│    FreeSpaceEnd, NextOverflowPage, TransactionId        │
├─────────────────────────────────────────────────────────┤
│  [Slot Array (grows down)]                              │
│    ┌──────────────────────────────────┐                 │
│    │ Slot 0: [Offset, Length, Flags]  │ (8 bytes)       │
│    │ Slot 1: [Offset, Length, Flags]  │                 │
│    │ Slot 2: [Offset, Length, Flags]  │                 │
│    │ ...                               │                 │
│    └──────────────────────────────────┘                 │
│                 ↓                                        │
│      [Free Space]                                        │
│                 ↑                                        │
│  [Data Area (grows up)]                                 │
│    ┌──────────────────────────────────┐                 │
│    │ ... Document N C-BSON bytes ...  │                 │
│    │ ... Document 2 C-BSON bytes ...  │                 │
│    │ ... Document 1 C-BSON bytes ...  │                 │
│    │ ... Document 0 C-BSON bytes ...  │                 │
│    └──────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────┘
```

#### SlottedPageHeader (24 bytes)

```
Offset  Size  Field               Description
------  ----  ------------------  --------------------------
0       4     PageId              
4       1     PageType            (= 3 for Data pages)
8       2     SlotCount           Number of slots
10      2     FreeSpaceStart      Offset where slots end
12      2     FreeSpaceEnd        Offset where data begins
14      4     NextOverflowPage    For large documents
18      4     TransactionId       
22      2     Reserved
```

#### SlotEntry (8 bytes)

```
Offset  Size  Field               Description
------  ----  ------------------  --------------------------
0       2     Offset              Byte offset to document data
2       2     Length              Document length in bytes
4       4     Flags               SlotFlags enum
```

**SlotFlags:**
- `None = 0`: Active slot
- `Deleted = 1`: Slot marked for reuse
- `HasOverflow = 2`: Document continues in overflow pages
- `Compressed = 4`: Reserved for future compression

### 3.5 DocumentLocation

Documents are addressed by **(PageId, SlotIndex)** tuple:

```csharp
public readonly struct DocumentLocation
{
    public uint PageId { get; init; }      // 4 bytes
    public ushort SlotIndex { get; init; } // 2 bytes
}
// Total: 6 bytes when serialized
```

**Used in:**
- Index entries (key → location mapping)
- Overflow page chains
- Internal references

### 3.6 Free Page Management

Deleted pages form a **linked list** for reuse:

1. `PageHeader.PageType = Free`
2. `PageHeader.NextPageId` points to next free page
3. Page 0's `NextPageId` points to **head of free list**

**Allocation:**
```
if (freeListHead != 0):
    pageId = freeListHead
    freeListHead = ReadPage(pageId).NextPageId
    UpdatePage0(freeListHead)
else:
    pageId = nextPageId++
    ExpandFile()
```

**Deallocation:**
```
WritePage(pageId, Free with next=freeListHead)
freeListHead = pageId
UpdatePage0(freeListHead)
```

---

## 4. C-BSON Format Specification

C-BSON (Compressed BSON) is BLite's wire format. See the dedicated **`C-BSON.md`** document for complete specification.

**Key points:**

- **Element header:** `[1 byte type][2 byte field ID]` (vs. BSON's `[1 byte type][N byte name\0]`)
- **Schema-based:** Field names mapped to `ushort` IDs via `ConcurrentDictionary`
- **Storage savings:** 30-60% reduction for typical schemas
- **Type compatible:** Uses standard BSON type codes and value encoding

**Example:**

```
Standard BSON element: [0x02]['em','ai','l','\0'][value] = 6 bytes overhead
C-BSON element:        [0x02][0x03, 0x00][value]          = 3 bytes overhead
Savings: 50%
```

---

## 5. Indexing Specifications

### 5.1 B+Tree Index

#### 5.1.1 Node Structure

B+Tree nodes are stored in **Index pages (PageType = 4)**:

```
┌─────────────────────────────────────────────────────────┐
│  [PageHeader (32)]                                      │
├─────────────────────────────────────────────────────────┤
│  [BTreeNodeHeader (20)]                                 │
│    PageId, IsLeaf, EntryCount, ParentPageId,            │
│    NextLeafPageId, PrevLeafPageId                       │
├─────────────────────────────────────────────────────────┤
│  [Entries...]                                           │
│    For Leaf Nodes:                                      │
│      ┌────────────────────────────────────┐             │
│      │ IndexKey   (variable)              │             │
│      │ DocumentLocation (6 bytes)         │             │
│      └────────────────────────────────────┘             │
│    For Internal Nodes:                                  │
│      ┌────────────────────────────────────┐             │
│      │ IndexKey (variable)                │             │
│      │ ChildPageId (4 bytes)              │             │
│      └────────────────────────────────────┘             │
└─────────────────────────────────────────────────────────┘
```

#### 5.1.2 BTreeNodeHeader (20 bytes)

```csharp
public struct BTreeNodeHeader
{
    public uint PageId;              // 4 bytes
    public bool IsLeaf;              // 1 byte
    public ushort EntryCount;        // 2 bytes
    public uint ParentPageId;        // 4 bytes
    public uint NextLeafPageId;      // 4 bytes (leaf only)
    public uint PrevLeafPageId;      // 4 bytes (leaf only)
}
```

#### 5.1.3 IndexKey

Supports composite keys with multiple types:

```csharp
public struct IndexKey : IComparable<IndexKey>
{
    public object[] Values { get; set; } // Multi-column support
    public int CompareTo(IndexKey other) { ... }
}
```

**Serialization:**
- Each value serialized as C-BSON element
- Supports: String, Int32, Int64, Double, ObjectId, DateTime, Guid

#### 5.1.4 Operations

**Insert:** O(log n)
- Traverse to leaf
- Insert key-location pair
- Split if full (B+Tree standard split)

**Search:** O(log n)
- Binary search in nodes
- Equality: Single lookup
- Range: Scan linked leaf nodes

**Delete:** O(log n)
- Mark entry as deleted
- Lazy compaction on split

### 5.2 R-Tree Index (Geospatial)

#### 5.2.1 Node Structure

R-Tree nodes use **Spatial pages (PageType = 11)**:

```
┌─────────────────────────────────────────────────────────┐
│  [PageHeader (32)]                                      │
├─────────────────────────────────────────────────────────┤
│  [SpatialPageHeader (16)]                               │
│    IsLeaf, Level, EntryCount, ParentPageId              │
├─────────────────────────────────────────────────────────┤
│  [Entries...] (38 bytes each)                           │
│    ┌──────────────────────────────────────┐             │
│    │ MBR (GeoBox): 4 × double = 32 bytes  │             │
│    │   MinLat, MinLon, MaxLat, MaxLon     │             │
│    │ Pointer: DocumentLocation = 6 bytes  │             │
│    └──────────────────────────────────────┘             │
│  (For internal nodes: Pointer = ChildPageId)            │
└─────────────────────────────────────────────────────────┘
```

#### 5.2.2 GeoBox (Minimum Bounding Rectangle)

```csharp
public struct GeoBox
{
    public double MinLat { get; set; }
    public double MinLon { get; set; }
    public double MaxLat { get; set; }
    public double MaxLon { get; set; }

    public bool Intersects(GeoBox other) { ... }
    public bool Contains((double, double) point) { ... }
    public GeoBox ExpandTo(GeoBox other) { ... }
}
```

#### 5.2.3 Operations

**Insert:** O(log n)
- Choose subtree with minimal MBR expansion
- Insert and update MBRs up the tree
- Split using quadratic algorithm

**Search (Proximity):**
```
Query: Find points within radius R of (lat, lon)
1. Convert to GeoBox: (lat-R, lon-R) to (lat+R, lon+R)
2. Traverse R-Tree, pruning non-intersecting branches
3. For leaf entries: Calculate exact distance
4. Return sorted by distance
```

**Search (Bounding Box):**
```
Query: Find points within box (minLat, minLon, maxLat, maxLon)
1. Create GeoBox
2. Traverse R-Tree, returning intersecting leaf entries
```

### 5.3 HNSW Index (Vector Similarity)

#### 5.3.1 Node Structure

HNSW nodes use **Vector pages (PageType = 9)**:

```
┌─────────────────────────────────────────────────────────┐
│  [PageHeader (32)]                                      │
├─────────────────────────────────────────────────────────┤
│  [VectorPageHeader (16)]                                │
│    Dimensions, MaxM, NodeSize, NodeCount                │
├─────────────────────────────────────────────────────────┤
│  [Nodes...] (variable size)                             │
│    ┌──────────────────────────────────────┐             │
│    │ DocumentLocation (6 bytes)           │             │
│    │ MaxLevel (1 byte)                    │             │
│    │ Vector (dimensions × 4 bytes)        │             │
│    │ Links Level 0 (2M × 6 bytes)         │             │
│    │ Links Level 1-15 (M × 6 bytes each)  │             │
│    └──────────────────────────────────────┘             │
└─────────────────────────────────────────────────────────┘
```

#### 5.3.2 HNSW Parameters

- **M:** Max bidirectional links per level (typically 16)
- **Dimensions:** Vector dimensionality (e.g., 1536 for OpenAI embeddings)
- **ef_construction:** Quality parameter during build (typically 200)
- **ef_search:** Quality parameter during search (typically 50)

#### 5.3.3 Vector Similarity Metrics

```csharp
public enum VectorMetric
{
    Cosine,      // cos(θ) = dot(a,b) / (||a|| × ||b||)
    Euclidean,   // √Σ(ai - bi)²
    DotProduct   // Σ(ai × bi)
}
```

#### 5.3.4 Operations

**Insert:** O(log n) expected
- Assign random level (exponential distribution)
- Starting from top level, greedily descend
- At each level, add bidirectional links to nearest M neighbors

**Search (k-NN):**
```
Query: Find k nearest neighbors to query vector
1. Start at entry point (top level)
2. Greedily search to local minimum at each level
3. At level 0, maintain priority queue of k candidates
4. Expand candidate set with ef_search parameter
5. Return top k by similarity
```

---

## 6. Transaction and WAL Specification

### 6.1 Transaction Model

BLite implements **Snapshot Isolation** (SI):

- **Read transactions:** See consistent snapshot as of transaction start
- **Write transactions:** Accumulate changes in-memory, commit atomically
- **Conflict detection:** Last-write-wins (optimistic concurrency)

### 6.2 Write-Ahead Log (WAL)

BLite implements a **full WAL** for durability and crash recovery:

#### WAL Entry Format

```
┌─────────────────────────────────────────────────────────┐
│  WAL Entry Format                                       │
├─────────────────────────────────────────────────────────┤
│  [Record Type: 1 byte]                                  │
│    0x01 = Begin                                          │
│    0x02 = Write                                          │
│    0x03 = Commit                                         │
│    0x04 = Abort                                          │
│    0x05 = Checkpoint                                     │
├─────────────────────────────────────────────────────────┤
│  For Begin/Commit/Abort:                                │
│    [Transaction ID: 8 bytes]                             │
│    [Timestamp: 8 bytes (Unix ms)]                        │
│    Total: 17 bytes                                       │
├─────────────────────────────────────────────────────────┤
│  For Write:                                              │
│    [Transaction ID: 8 bytes]                             │
│    [PageId: 4 bytes]                                     │
│    [After Image Length: 4 bytes]                         │
│    [After Image: variable bytes]                         │
│    Total: 17 + AfterImage.Length                         │
└─────────────────────────────────────────────────────────┘
```

#### WAL Protocol

**Write Path:**
```
1. Begin Transaction → WriteBeginRecord(txnId)
2. Modify Data       → WriteDataRecord(txnId, pageId, afterImage)
3. Commit            → WriteCommitRecord(txnId) + Flush()
```

**Recovery Path:**
```csharp
var records = wal.ReadAll();
foreach (var record in records)
{
    if (record.Type == WalRecordType.Write && IsCommitted(record.TransactionId))
    {
        pageFile.WritePage(record.PageId, record.AfterImage);
    }
}
```

#### Implementation Details

**Zero-Allocation Writes:**
```csharp
// Synchronous (stack allocated)
Span<byte> buffer = stackalloc byte[17];
buffer[0] = (byte)WalRecordType.Begin;
BitConverter.TryWriteBytes(buffer[1..9], transactionId);
BitConverter.TryWriteBytes(buffer[9..17], timestamp);
_walStream.Write(buffer);

// Asynchronous (pooled)
var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
try
{
    // ... write to buffer
    await _walStream.WriteAsync(buffer.AsMemory(0, totalSize), ct);
}
finally
{
    ArrayPool<byte>.Shared.Return(buffer);
}
```

**Durability Guarantee:**
```csharp
public void Flush()
{
    _walStream?.Flush(flushToDisk: true); // Force OS fsync
}
```

**Checkpoint and Truncate:**
```csharp
// After applying WAL to pages:
pageFile.Flush();        // Ensure pages on disk
wal.Truncate();          // Remove committed WAL records
wal.Flush();             // Sync truncation
```

### 6.3 ACID Guarantees

- **Atomicity:** WAL ensures all-or-nothing commits
- **Consistency:** Schema validation before commit
- **Isolation:** Snapshot isolation (MVCC-like)
- **Durability:** WAL flush on commit

---

## 7. Query Processing

### 7.1 LINQ Provider

BLite implements `IQueryable<T>` via custom query provider:

```csharp
var results = db.Users.AsQueryable()
    .Where(u => u.Age > 25 && u.City == "NYC")
    .OrderBy(u => u.Name)
    .Take(10)
    .ToList();
```

**Translation:**
1. Expression tree → BTree query plan
2. Index selection: Choose index on `Age` or `City`
3. Index scan: Retrieve candidate DocumentLocations
4. Post-filter: Apply remaining predicates in-memory
5. Materialize: Read documents, deserialize to objects

### 7.2 Index Selection Algorithm

```
For WHERE clause:
1. Extract equality and range predicates
2. Score each index by predicate coverage
3. Select index with highest score
4. Use index scan if selective, else full scan
```

**Example:**
```sql
WHERE Age > 25 AND City = "NYC"
```

Index options:
- Index on `Age` → Range scan (potentially many results)
- Index on `City` → Equality lookup (likely fewer results)
- **Choose:** `City` index, post-filter `Age > 25`

### 7.3 Hybrid Execution Model

BLite combines **index-based** and **in-memory** execution:

1. **Index Phase:** Use B-Tree/R-Tree to filter candidates
2. **Materialization:** Read documents from pages
3. **LINQ to Objects:** Apply complex predicates, projections, aggregations

**Benefits:**
- Leverage index selectivity
- Support full LINQ semantics
- Avoid building complex query execution engine

---

## 8. Source Generation Protocol

### 8.1 Mapper Generation

BLite uses **Roslyn Source Generators** to produce zero-reflection mappers:

**Input:**
```csharp
public class User
{
    public ObjectId Id { get; set; }
    public string Name { get; set; }
    public int Age { get; set; }
}

public partial class MyDbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, User> Users { get; set; }
}
```

**Generated:**
```csharp
namespace MyApp.Mappers
{
    public class UserMapper : ObjectIdMapperBase<User>
    {
        public override string CollectionName => "users";

        public override int Serialize(User entity, BsonSpanWriter writer)
        {
            var start = writer.BeginDocument();
            writer.WriteObjectId("_id", entity.Id);
            writer.WriteString("name", entity.Name);
            writer.WriteInt32("age", entity.Age);
            writer.EndDocument(start);
            return writer.Position;
        }

        public override User Deserialize(BsonSpanReader reader)
        {
            var user = new User();
            reader.ReadDocumentSize();
            while (reader.Remaining > 0)
            {
                var type = reader.ReadBsonType();
                if (type == BsonType.EndOfDocument) break;
                var name = reader.ReadElementHeader();
                switch (name)
                {
                    case "_id": user.Id = reader.ReadObjectId(); break;
                    case "name": user.Name = reader.ReadString(); break;
                    case "age": user.Age = reader.ReadInt32(); break;
                    default: reader.SkipValue(type); break;
                }
            }
            return user;
        }
    }
}
```

### 8.2 Attribute Support

See **Data Annotations Support** section in README and related documentation.

Supported attributes:
- `[Table(Name, Schema)]` → Collection name mapping
- `[Column(Name, TypeName)]` → Field name and special type mapping
- `[Key]` → Primary key identification
- `[NotMapped]` → Exclusion from serialization
- `[Required]`, `[StringLength]`, `[Range]` → Validation

### 8.3 Code Generation Rules

1. **Lowercase policy:** BSON field names are lowercase by default
2. **Attribute override:** `[BsonProperty]`, `[JsonPropertyName]`, `[Column]` override default names
3. **Nested objects:** Recursively analyzed and mapped
4. **Collections:** Arrays and `IEnumerable<T>` mapped to BSON arrays
5. **Value types:** Primitives, enums, `DateTime`, `ObjectId`, `Guid` handled natively

---

## 9. Security Considerations

### 9.1 Data Integrity

- **Checksums:** CRC32 on page headers (planned: extend to full pages)
- **WAL:** Ensures consistency even on crash
- **Schema validation:** Prevents type mismatches

### 9.2 Concurrent Access

**Multi-Threading:** ✅ **Fully Supported**
- **Thread-safe writes:** Multiple threads can write concurrently within the same process
- **Internal synchronization:** `SemaphoreSlim` for critical sections, `ConcurrentDictionary` for shared state
- **WAL coordination:** Commit lock ensures serializable WAL writes
- **Page cache:** Thread-safe access to memory-mapped pages

**Multi-Process:** ❌ **Not Supported**
- **Exclusive file lock:** `FileShare.None` prevents multiple processes from opening the same database
- **Rationale:** Simplifies consistency guarantees, avoids complex inter-process coordination
- **Future:** May support via cooperative locking or server mode (HTTP API)

### 9.3 Injection Attacks

- **No SQL injection:** No query language, only type-safe LINQ
- **Schema-validated:** All operations type-checked at compile-time

---

## 10. Performance Considerations

### 10.1 Zero-Allocation Design

**Stack allocation:**
```csharp
Span<byte> buffer = stackalloc byte[16384]; // Page buffer on stack
var writer = new BsonSpanWriter(buffer, keyMap);
```

**No boxing:**
- Value types remain unboxed throughout serialization
- No reflection or dynamic invocation

**Pooling:**
```csharp
var buffer = ArrayPool<byte>.Shared.Rent(pageSize);
try { /* use buffer */ }
finally { ArrayPool<byte>.Shared.Return(buffer); }
```

### 10.2 Memory-Mapped Files

**Benefits:**
- OS kernel manages page cache
- Zero-copy reads (map file → process memory)
- Prefetching and read-ahead by OS

**Tradeoffs:**
- Limited to single process (file lock)
- Windows vs. Linux differences in `mmap` behavior

### 10.3 Cache Efficiency

**Compact C-BSON:**
- More documents per 16KB page
- Better CPU cache utilization
- Reduced TLB misses

---

## 11. Implementation Notes

### 11.1 .NET 10 Requirements

BLite targets **.NET 10** to leverage:
- `Span<T>` and `ref struct` for zero-copy I/O
- Source Generators (Roslyn)
- `MemoryMarshal` for efficient struct serialization
- Improved JIT optimizations

**Future:** Evaluate `.NET Standard 2.1` for broader compatibility.

### 11.2 Platform Considerations

- **Windows:** Full support via MemoryMappedFile
- **Linux/macOS:** Supported, potential differences in `mmap` behavior
- **Endianness:** Little-endian assumed (matches x86/x64/ARM)

### 11.3 Future Compatibility

**Planned enhancements:**
- **Compression:** Page-level or document-level LZ4
- **Encryption:** AES-256 for data-at-rest
- **Multi-process:** Via cooperative locking or server mode

---

## 12. References

### 12.1 BSON and Formats

- [BSON Specification v1.1](http://bsonspec.org/)
- [MongoDB BSON Types](https://www.mongodb.com/docs/manual/reference/bson-types/)

### 12.2 Database Internals

- *Database Internals* by Alex Petrov (O'Reilly, 2019)
- [B+ Trees (Wikipedia)](https://en.wikipedia.org/wiki/B%2B_tree)
- [R-Trees (Guttman, 1984)](http://www-db.deis.unibo.it/courses/SI-LS/papers/Gut84.pdf)

### 12.3 Vector Search

- [HNSW Algorithm (Malkov & Yashunin, 2018)](https://arxiv.org/abs/1603.09320)
- [Faiss Library (Facebook AI)](https://github.com/facebookresearch/faiss)

### 12.4 Standards

- [RFC 2119: Key words for RFCs](https://www.ietf.org/rfc/rfc2119.txt)
- [IEEE 754: Floating Point Arithmetic](https://ieeexplore.ieee.org/document/8766229)

---

## Appendix A: Page Format Diagrams

### A.1 Slotted Page Visual

```
Offset:  0                                              16383
         ┌────────────────────────────────────────────────┐
     24  │ [Header: 24 bytes]                             │
         ├────────────────────────────────────────────────┤
         │ [Slot 0: Offset=16320, Len=64, Flags=0]        │
         │ [Slot 1: Offset=16256, Len=64, Flags=0]        │
         │ [Slot 2: Offset=16192, Len=64, Flags=0]        │
      56 │ ← FreeSpaceStart                               │
         │                                                 │
         │ ~~~~~~~~~~~~~~~~ Free Space ~~~~~~~~~~~~~~~~~~~ │
         │                                                 │
  16192  │ ← FreeSpaceEnd                                 │
         │ [Document 2 data: 64 bytes]                    │
         │ [Document 1 data: 64 bytes]                    │
         │ [Document 0 data: 64 bytes]                    │
  16384  └────────────────────────────────────────────────┘
```

### A.2 B+Tree Node Visual

```
Internal Node:
┌──────────────────────────────────────────────────────┐
│ [PageHeader 32] [BTreeNodeHeader 20]                 │
├──────────────────────────────────────────────────────┤
│ Key1 | ChildPtr1                                      │
│ Key2 | ChildPtr2                                      │
│ Key3 | ChildPtr3                                      │
│ ...                                                   │
└──────────────────────────────────────────────────────┘

Leaf Node:
┌──────────────────────────────────────────────────────┐
│ [PageHeader 32] [BTreeNodeHeader 20]                 │
├──────────────────────────────────────────────────────┤
│ Key1 | DocumentLocation1                             │
│ Key2 | DocumentLocation2                             │
│ Key3 | DocumentLocation3                             │
│ ...                                                   │
│ NextLeafPageId → [next leaf]                         │
└──────────────────────────────────────────────────────┘
```

---

## Appendix B: C-BSON Hex Dump

See `C-BSON.md`, Section "Hex Dump Examples" for detailed wire format examples.

---

## Appendix C: Transaction State Machine

```
┌─────────────┐
│  Idle       │
└──────┬──────┘
       │ BeginTransaction()
       ▼
┌─────────────┐
│  Active     │ ← Accumulate writes in memory
└──┬───────┬──┘
   │       │ Rollback()
   │       └────────────┐
   │ Commit()           │
   ▼                    ▼
┌─────────────┐   ┌─────────────┐
│ Committing  │   │  Aborted    │
│ (WAL flush) │   └─────────────┘
└──────┬──────┘
       │ Flush complete
       ▼
┌─────────────┐
│ Committed   │
└─────────────┘
```

---

**End of RFC-BLite Specification**
