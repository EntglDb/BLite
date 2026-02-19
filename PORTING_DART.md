# BLite Porting to Dart — Feasibility Analysis

This document analyzes the feasibility of porting BLite to the Dart language, targeting Flutter mobile, desktop, server, and IoT platforms.

## 1. Feature Mapping: .NET → Dart

| BLite Feature (.NET) | Dart Equivalent | Feasibility | Notes |
|---|---|---|---|
| **Source Generators** (Roslyn incremental) | `build_runner` + `source_gen` / Dart Macros | ✅ Feasible | `build_runner` is mature; Dart Macros are experimental but promising |
| **BSON Serialization** (BsonSpanReader/Writer) | `dart:typed_data` (ByteData, Uint8List) | ✅ Feasible | Dart has solid binary buffer support |
| **ref struct** (zero-allocation reader/writer) | ❌ No equivalent | ⚠️ Partial | Dart lacks value types and ref structs; mitigate with pooling and ByteData views |
| **Span\<byte\> / Memory\<byte\>** | `Uint8List`, `ByteData`, `BytesBuilder` | ✅ Feasible | Different APIs but functionally equivalent |
| **File I/O** (pages, random access) | `dart:io` (RandomAccessFile) | ✅ Feasible | Full support on desktop/server; limited on web |
| **Concurrency** (lock, SemaphoreSlim, async) | `Isolate`, `async/await`, `Zone` | ⚠️ Different model | Dart is single-threaded + isolates (no shared memory) |
| **Generics with constraints** | Dart generics (less powerful) | ⚠️ Partial | No `where T : struct`, no specialization |
| **NativeAOT / Trimming** | `dart compile exe`, Flutter AOT | ✅ Natural | Dart is AOT-first in Flutter |
| **DbContext pattern** | Custom implementation | ✅ Feasible | No built-in equivalent but straightforward pattern |
| **Attributes** (`[BCollection]`, `[BIndex]`) | Annotations (`@BCollection()`) | ✅ Feasible | Dart annotations + code generation is well-established |
| **ACID Transactions** | Custom implementation on `RandomAccessFile` | ✅ Feasible | Need WAL or journal-based approach |
| **B-Tree / Hash / R-Tree Indexes** | Custom implementation | ✅ Feasible | Pure algorithmic code, no platform dependency |
| **Change Data Capture** | `Stream<ChangeEvent>` | ✅ Feasible | Dart Streams are a natural fit for CDC |
| **Vector Search Index** | Custom implementation | ✅ Feasible | Math operations work the same |

## 2. Proposed Architecture

```
blite/                           # Meta-package (pub.dev)
├── blite_annotations/           # Shared annotations
│   ├── b_collection.dart        # @BCollection()
│   ├── b_document.dart          # @BDocument()
│   ├── b_index.dart             # @BIndex()
│   ├── b_id.dart                # @BId()
│   └── b_ignore.dart            # @BIgnore()
├── blite_bson/                  # BSON serialization engine
│   ├── bson_writer.dart         # Equivalent of BsonSpanWriter
│   ├── bson_reader.dart         # Equivalent of BsonSpanReader
│   ├── bson_types.dart          # ObjectId, BsonDocument, etc.
│   └── bson_value.dart          # Type-safe BSON value wrapper
├── blite_core/                  # Storage engine
│   ├── database.dart            # BLiteDatabase
│   ├── collection.dart          # DocumentCollection<T>
│   ├── db_context.dart          # DocumentDbContext base class
│   ├── storage/
│   │   ├── page_file.dart       # Page-based file storage
│   │   ├── storage_engine.dart  # Core storage engine
│   │   └── wal.dart             # Write-ahead log
│   ├── indexing/
│   │   ├── btree_index.dart     # B-Tree index
│   │   ├── hash_index.dart      # Hash index
│   │   ├── rtree_index.dart     # R-Tree spatial index
│   │   └── vector_index.dart    # Vector search index
│   ├── query/
│   │   ├── query_builder.dart   # Fluent query API
│   │   └── index_optimizer.dart # Query plan optimization
│   └── cdc/
│       └── change_stream.dart   # Change Data Capture via Dart Streams
└── blite_generator/             # Code generator (build_runner)
    ├── mapper_generator.dart    # Generates serialize/deserialize
    ├── context_generator.dart   # Generates DbContext implementations
    └── index_generator.dart     # Generates index key extractors
```

## 3. API Design Example

### Entity Definition

```dart
import 'package:blite_annotations/blite_annotations.dart';

@BCollection()
class Customer {
  @BId()
  final String? id;

  final String name;
  final String email;

  @BIndex()
  final String taxCode;

  final Address? address;
  final List<Tag> tags;

  Customer({
    this.id,
    required this.name,
    required this.email,
    required this.taxCode,
    this.address,
    this.tags = const [],
  });
}

@BDocument()
class Address {
  final String street;
  final String city;
  final String zip;

  Address({required this.street, required this.city, required this.zip});
}
```

### DbContext

```dart
import 'package:blite_annotations/blite_annotations.dart';

@BLiteContext()
abstract class AppDbContext {
  @BCollectionRef()
  late final DocumentCollection<Customer> customers;
}
```

### Usage

```dart
Future<void> main() async {
  final db = await AppDbContext.open('myapp.db');

  final customer = Customer(
    name: 'Mario Rossi',
    email: 'mario@example.com',
    taxCode: 'RSSMRA80A01H501Z',
    address: Address(street: 'Via Roma 1', city: 'Milano', zip: '20100'),
    tags: [Tag(name: 'premium')],
  );

  await db.customers.insert(customer);

  final found = await db.customers.findByTaxCode('RSSMRA80A01H501Z');

  final all = await db.customers.find((c) => c.city == 'Milano');

  await db.close();
}
```

## 4. Key Challenges

### 🔴 Critical: No ref struct / Value Types

Dart has no value types. BLite's `BsonSpanReader` and `BsonSpanWriter` are `ref struct` in .NET to avoid heap allocations. Mitigation strategies:

- **Object pooling** for reader/writer instances
- **ByteData views** on Uint8List (zero-copy slicing)
- Dart VM is optimized for small, short-lived objects — GC pressure may be acceptable
- **Benchmark early** to validate performance assumptions

### 🟡 Moderate: Concurrency Model

Dart uses **Isolates** with no shared memory. Concurrent database writes require a different pattern:

- Dedicate a **single isolate** as the database engine (like an in-process server)
- Communicate via `SendPort` / `ReceivePort`
- This pattern is well-established in Flutter (`sqflite`, `drift`, `isar`)
- Advantage: no lock contention, no data races by design

### 🟡 Moderate: Code Generation Performance

`build_runner` is powerful but **slower** than Roslyn Source Generators. Incremental builds help, but large projects may feel the cost. **Dart Macros** (currently experimental) promise better performance and tighter language integration. Strategy:

- Launch with `build_runner` for maximum compatibility
- Migrate to Macros when they stabilize

### 🟢 Advantage: Web Platform

Dart compiles to JavaScript/WASM. A web-compatible BLite could use:

- **IndexedDB** as the storage backend (instead of `RandomAccessFile`)
- Same API surface for Flutter Web apps
- This would be a major differentiator over desktop-only alternatives

## 5. Competitive Landscape in Dart

| Library | Type | Code Gen | AOT | Pure Dart | Active |
|---|---|---|---|---|---|
| **BLite (Dart)** | Document DB | ✅ | ✅ | ✅ | — |
| **Hive** | Key-Value | ✅ (adapters) | ✅ | ✅ | ⚠️ Maintenance mode |
| **Isar** | Document DB | ✅ | ✅ | ❌ (native FFI) | ⚠️ Uncertain |
| **ObjectBox** | Document DB | ✅ | ⚠️ | ❌ (native FFI) | ✅ Active |
| **sqflite** | SQL | ❌ | ✅ | ❌ (SQLite FFI) | ✅ Active |
| **Drift** | SQL (type-safe) | ✅ | ✅ | ❌ (SQLite FFI) | ✅ Active |

**BLite Dart would be the only embedded document database in pure Dart** — no FFI, no native libraries, no platform-specific builds. This is a significant competitive advantage for Flutter developers who want maximum portability.

## 6. BLite + EntglDb in Dart

The combination would be even more compelling in the Dart/Flutter ecosystem:

```
┌──────────────────────────────────────────────────┐
│              Flutter / Dart App                   │
├──────────────────┬───────────────────────────────┤
│   BLite (Dart)   │   EntglDb (Dart)              │
│   Local Storage  │   P2P Sync & Distribution     │
│   BSON Engine    │   libp2p / WebRTC             │
│   Zero-config    │   CRDT Merge                  │
└──────────────────┴───────────────────────────────┘
```

Dart already has libraries for **libp2p** and **WebRTC**, making the P2P networking layer feasible. Target scenarios:

- **Multi-device sync** — Flutter apps on phones, tablets, and desktops stay in sync without a server
- **Edge computing** — IoT devices replicate data across a mesh network
- **Collaborative apps** — multiple users share data in real-time with conflict resolution
- **Offline-first** — every node owns its data; no single point of failure

## 7. Verdict

| Aspect | Assessment |
|---|---|
| **Technical feasibility** | ✅ **Yes, feasible** |
| **Estimated effort** | 🟡 **Medium-high** (3–6 months for MVP) |
| **Market value** | 🟢 **High** — no pure-Dart document DB with BSON exists |
| **Natural target** | Flutter mobile + desktop + IoT |
| **Key risk** | BSON performance without value types |
| **Key advantage** | Zero native dependencies — runs everywhere Dart runs, including web |

### Recommendation

The porting is feasible and would occupy a **unique position** in the Dart/Flutter ecosystem. The competitive advantage is clear: a **100% pure Dart** embedded document database with BSON serialization, source-generated mappers, and optional P2P distribution via EntglDb. No FFI, no native builds, no platform-specific headaches.

**Suggested approach:**
1. Start with `blite_bson` — port the BSON engine first and benchmark it
2. Build `blite_core` storage engine using `RandomAccessFile`
3. Add `blite_generator` using `build_runner`
4. Validate with a Flutter demo app
5. Add web support (IndexedDB backend) as a differentiator
6. Port EntglDb for P2P capabilities
