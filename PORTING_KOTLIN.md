# BLite Porting to Kotlin — Feasibility Analysis

This document analyzes the feasibility of porting BLite to Kotlin, targeting Android, JVM server, Kotlin Multiplatform (KMP), and Kotlin/Native platforms.

## 1. Feature Mapping: .NET → Kotlin

| BLite Feature (.NET) | Kotlin Equivalent | Feasibility | Notes |
|---|---|---|---|
| **Source Generators** (Roslyn incremental) | **KSP** (Kotlin Symbol Processing) | ✅ Excellent | KSP is mature, fast, and officially supported by Google/JetBrains |
| **BSON Serialization** (BsonSpanReader/Writer) | `ByteBuffer`, `ByteArray`, `okio.Buffer` | ✅ Feasible | Multiple high-quality buffer APIs available |
| **ref struct** (zero-allocation) | `@JvmInline value class` (JVM) | ⚠️ Partial | Inline classes avoid boxing on JVM; no equivalent on KMP/Native |
| **Span\<byte\> / Memory\<byte\>** | `ByteBuffer` (JVM), `ByteArray` (common) | ✅ Feasible | JVM has NIO ByteBuffer with slicing; KMP uses ByteArray |
| **File I/O** (pages, random access) | `RandomAccessFile` (JVM), `okio` (KMP) | ✅ Feasible | JVM has full support; KMP via okio or expect/actual |
| **Concurrency** (lock, async) | **Coroutines** + `Mutex` + `Channel` | ✅ Excellent | Kotlin coroutines are arguably better than C# async/await |
| **Generics with constraints** | Reified generics (inline functions), `where` | ✅ Good | `reified` type parameters give runtime type access without reflection |
| **NativeAOT / Trimming** | Kotlin/Native (LLVM), GraalVM native-image | ✅ Feasible | Kotlin/Native compiles to native binaries via LLVM |
| **DocumentDbContext** | `DocumentDatabase` (Room-inspired) | ✅ Excellent | Room's `@Database` pattern is universally known in Kotlin/Android |
| **Annotations** (`[BCollection]`, `[BIndex]`) | Kotlin annotations (`@BCollection`) | ✅ Feasible | First-class annotation support, processed by KSP |
| **ACID Transactions** | Custom WAL implementation | ✅ Feasible | Same approach as .NET version |
| **B-Tree / Hash / R-Tree Indexes** | Custom implementation | ✅ Feasible | Pure algorithmic code |
| **Change Data Capture** | `Flow<ChangeEvent>` | ✅ Excellent | Kotlin Flow is a perfect fit for CDC streams |
| **Vector Search Index** | Custom implementation | ✅ Feasible | Math operations identical |

## 2. Why Kotlin is a Strong Fit

### 2.1 KSP > build_runner

Kotlin Symbol Processing (KSP) is the **most direct equivalent** of Roslyn Source Generators in any language:

| Aspect | Roslyn Source Generators | KSP | Dart build_runner |
|---|---|---|---|
| Speed | ✅ Incremental, fast | ✅ Incremental, fast | ⚠️ Slower |
| IDE integration | ✅ Real-time | ✅ Real-time | ⚠️ Requires manual run |
| Official support | Microsoft | Google / JetBrains | Dart team |
| Multiplatform | .NET only | ✅ KMP-compatible | Dart only |
| Maturity | Very mature | Mature (1.0+) | Very mature |

KSP generates Kotlin source files at compile time — same paradigm as BLite's Roslyn generators. The migration would be almost 1:1 conceptually.

### 2.2 Coroutines are a Natural Fit

BLite's async API maps beautifully to Kotlin coroutines:

```csharp
// C# BLite
await db.Customers.InsertAsync(customer);
var results = await db.Customers.FindAsync(q => q.Name == "Mario");
```

```kotlin
// Kotlin BLite
db.customers.insert(customer)  // suspend function, no await needed
val results = db.customers.find { it.name == "Mario" }
```

Kotlin coroutines are **structured** — cancellation, timeouts, and scoping come for free. The CDC feature maps to `Flow`:

```kotlin
db.customers.changes()  // returns Flow<ChangeEvent<Customer>>
    .filter { it.type == ChangeType.INSERT }
    .collect { event -> println("New customer: ${event.entity.name}") }
```

### 2.3 Kotlin Multiplatform (KMP)

KMP is the biggest strategic advantage. A single codebase can target:

| Platform | Runtime | File I/O | Status |
|---|---|---|---|
| **Android** | JVM (ART) | `java.io.RandomAccessFile` | ✅ Primary target |
| **JVM Server** | JVM (HotSpot/GraalVM) | `java.io.RandomAccessFile` | ✅ Full support |
| **iOS** | Kotlin/Native (LLVM) | POSIX `fopen`/`fread` | ✅ Via expect/actual |
| **macOS** | Kotlin/Native (LLVM) | POSIX | ✅ Via expect/actual |
| **Linux** | Kotlin/Native (LLVM) | POSIX | ✅ Via expect/actual |
| **Windows** | Kotlin/Native (LLVM) | Win32 API / POSIX | ⚠️ Experimental |
| **WASM** | Kotlin/WASM | ❌ No file I/O | ⚠️ In-memory only |

Using `expect`/`actual` declarations:

```kotlin
// commonMain
expect class PlatformFile {
    fun read(position: Long, buffer: ByteArray, length: Int): Int
    fun write(position: Long, buffer: ByteArray, length: Int)
    fun flush()
    fun close()
}

// jvmMain
actual class PlatformFile(path: String) {
    private val raf = RandomAccessFile(path, "rw")
    actual fun read(position: Long, buffer: ByteArray, length: Int): Int { ... }
    // ...
}

// nativeMain
actual class PlatformFile(path: String) {
    private val fd = fopen(path, "r+b")
    actual fun read(position: Long, buffer: ByteArray, length: Int): Int { ... }
    // ...
}
```

## 3. Proposed Architecture

```
blite-kotlin/                        # Root project
├── blite-annotations/               # Shared annotations (KMP common)
│   └── commonMain/
│       └── annotations.kt          # @BCollection, @BDocument, @BIndex, @BId, @BIgnore
├── blite-bson/                      # BSON engine (KMP common)
│   ├── commonMain/
│   │   ├── BsonWriter.kt           # Write BSON to ByteArray
│   │   ├── BsonReader.kt           # Read BSON from ByteArray
│   │   ├── BsonTypes.kt            # ObjectId, BsonValue, etc.
│   │   └── BsonDocument.kt         # Dynamic BSON document
│   └── jvmMain/
│       └── BsonWriterJvm.kt        # ByteBuffer-optimized overloads
├── blite-core/                      # Storage engine (KMP)
│   ├── commonMain/
│   │   ├── BLiteDatabase.kt        # Database entry point
│   │   ├── DocumentCollection.kt   # Collection<T>
│   │   ├── DocumentDatabase.kt      # Base database class (Room-inspired)
│   │   ├── storage/
│   │   │   ├── PageFile.kt         # expect class for file I/O
│   │   │   ├── StorageEngine.kt    # Core engine
│   │   │   └── Wal.kt              # Write-ahead log
│   │   ├── indexing/
│   │   │   ├── BTreeIndex.kt
│   │   │   ├── HashIndex.kt
│   │   │   ├── RTreeIndex.kt
│   │   │   └── VectorSearchIndex.kt
│   │   ├── query/
│   │   │   ├── QueryBuilder.kt     # DSL-based query API
│   │   │   └── IndexOptimizer.kt
│   │   └── cdc/
│   │       └── ChangeFlow.kt       # CDC via Kotlin Flow
│   ├── jvmMain/
│   │   └── storage/
│   │       └── PageFileJvm.kt      # actual class using RandomAccessFile
│   └── nativeMain/
│       └── storage/
│           └── PageFileNative.kt   # actual class using POSIX I/O
└── blite-ksp/                       # KSP code generator
    └── jvmMain/
        ├── BLiteSymbolProcessor.kt  # Main KSP processor
        ├── MapperGenerator.kt       # Generates serializer/deserializer
        ├── DatabaseGenerator.kt     # Generates DocumentDatabase impl
        └── IndexGenerator.kt        # Generates index key extractors
```

## 4. API Design Example

### Entity Definition

```kotlin
import com.blite.annotations.*

@BCollection
data class Customer(
    @BId val id: String? = null,
    val name: String,
    val email: String,
    @BIndex val taxCode: String,
    val address: Address? = null,
    val tags: List<Tag> = emptyList()
)

@BDocument
data class Address(
    val street: String,
    val city: String,
    val zip: String
)
```

### Database

In the Kotlin/Android ecosystem, the concept equivalent to .NET's `DbContext` is Room's `@Database`. Every Kotlin/Android developer recognizes this pattern instantly. BLite Kotlin adopts the same naming convention — `DocumentDatabase` with `@BLiteDatabase` — while keeping the API simpler (no DAO layer, direct collection access, like EF Core).

| Concept | EF Core (.NET) | Room (Android) | BLite .NET | BLite Kotlin |
|---|---|---|---|---|
| Base class | `DbContext` | `RoomDatabase()` | `DocumentDbContext` | `DocumentDatabase` |
| Annotation | None (convention) | `@Database` | `[BLiteContext]` | `@BLiteDatabase` |
| Data access | `DbSet<T>` properties | DAO interfaces | `DocumentCollection<T>` | `DocumentCollection<T>` |
| Code gen | Runtime reflection | KSP (compile-time) | Roslyn (compile-time) | KSP (compile-time) |
| Intermediate layer | No | Yes (DAO) | No | No |

The key insight: we take **Room's familiarity** (`@Database` + abstract class + KSP) but keep **EF Core's simplicity** (collections directly on the class, no DAO boilerplate). The word "Database" is preferred over "Context" because in Android, `Context` refers to `android.content.Context` — using it would create confusion.

```kotlin
import com.blite.annotations.*

@BLiteDatabase
abstract class AppDatabase : DocumentDatabase() {
    abstract val customers: DocumentCollection<Customer>
    abstract val products: DocumentCollection<Product>
}
```

### Usage

```kotlin
suspend fun main() {
    val db = AppDatabase.open("myapp.db")

    val customer = Customer(
        name = "Mario Rossi",
        email = "mario@example.com",
        taxCode = "RSSMRA80A01H501Z",
        address = Address("Via Roma 1", "Milano", "20100"),
        tags = listOf(Tag("premium"))
    )

    db.customers.insert(customer)

    // Query by index
    val found = db.customers.findByTaxCode("RSSMRA80A01H501Z")

    // Query with DSL
    val milanese = db.customers.find {
        where { it.address?.city eq "Milano" }
        orderBy { it.name.asc() }
        limit(10)
    }

    // CDC with Flow
    db.customers.changes()
        .filter { it.type == ChangeType.INSERT }
        .collect { println("New: ${it.entity.name}") }

    db.close()
}
```

### Kotlin DSL Advantage

Kotlin's DSL capabilities allow for an extremely ergonomic query API:

```kotlin
// Type-safe query builder using Kotlin DSL
val results = db.customers.find {
    where {
        (it.name startsWith "Mar") and (it.tags contains "premium")
    }
    orderBy { it.name.asc() }
    skip(20)
    limit(10)
}

// Transaction DSL
db.transaction {
    customers.insert(newCustomer)
    customers.update(existingCustomer)
    products.delete(obsoleteProduct.id)
    // auto-commit on success, auto-rollback on exception
}

// Open database — familiar to any Room developer
val db = AppDatabase.open("myapp.db")
// vs Room:
// val db = Room.databaseBuilder(context, AppDatabase::class.java, "myapp.db").build()
```

This is arguably **more ergonomic than the C# version** thanks to Kotlin's receiver lambdas and infix functions.

## 5. Key Challenges

### 🟡 Moderate: KMP File I/O Abstraction

File I/O must be abstracted via `expect`/`actual` for each platform target. This is well-understood but adds maintenance surface:

- **JVM**: `java.io.RandomAccessFile` — fast, proven, memory-mappable
- **Native**: POSIX `fopen`/`fread`/`fwrite` via `kotlinx.cinterop`
- **WASM**: No file system — in-memory only (or IndexedDB via JS interop)

**Mitigation**: Use [okio](https://square.github.io/okio/) which already provides multiplatform file I/O. This adds a dependency but dramatically reduces platform code.

### 🟡 Moderate: Value Types on Native

While JVM has `@JvmInline value class`, Kotlin/Native doesn't inline value classes the same way. The BSON reader/writer won't have the zero-allocation guarantee of .NET's `ref struct` on all platforms. Mitigation:

- **JVM**: `@JvmInline value class` wrapping `ByteBuffer` position
- **Native**: Class instances (Kotlin/Native has efficient allocation and GC)
- **Benchmark both** to verify acceptable performance

### 🟢 Advantage: No Reflection with KSP

Like Roslyn Source Generators, KSP runs at **compile time**. Generated mappers are plain Kotlin functions — no reflection needed at runtime. This means:

- Compatible with Kotlin/Native (which has limited reflection)
- Compatible with GraalVM native-image
- Compatible with R8/ProGuard on Android
- **Same zero-reflection philosophy as BLite .NET**

### 🟢 Advantage: JVM Ecosystem Access

On JVM targets, BLite Kotlin can leverage:

- `java.nio.MappedByteBuffer` for memory-mapped file I/O
- `java.util.concurrent.locks.ReentrantReadWriteLock` for fine-grained locking
- `FileChannel` with `force()` for durability guarantees
- Mature profiling and monitoring tools

## 6. Competitive Landscape in Kotlin/Android

| Library | Type | Code Gen | KMP | Pure Kotlin | No Native | Active |
|---|---|---|---|---|---|---|
| **BLite (Kotlin)** | Document DB | ✅ KSP | ✅ | ✅ | ✅ | — |
| **Room** | SQL (Android) | ✅ KSP | ❌ Android only | ❌ (SQLite) | ❌ | ✅ Active |
| **ObjectBox** | Document DB | ✅ | ⚠️ Partial | ❌ (C++ core) | ❌ | ✅ Active |
| **Realm Kotlin** | Document DB | ✅ KSP | ✅ | ❌ (C++ core) | ❌ | ⚠️ Deprecated |
| **SQLDelight** | SQL (type-safe) | ✅ | ✅ | ❌ (SQLite) | ❌ | ✅ Active |
| **Paper** | Key-Value | ❌ | ❌ | ❌ (Kryo) | ✅ | ⚠️ Maintenance |
| **Multiplatform Settings** | Key-Value | ❌ | ✅ | ✅ | ✅ | ✅ Active |

**BLite Kotlin would be unique**: the only KMP document database with no native dependencies, KSP-generated mappers, and full multiplatform support. Realm Kotlin SDK has been deprecated by MongoDB in favor of Atlas Device SDK, leaving a significant gap in the KMP document DB space.

## 7. BLite + EntglDb in Kotlin

```
┌──────────────────────────────────────────────────┐
│         Android / KMP / Server App                │
├──────────────────┬───────────────────────────────┤
│  BLite (Kotlin)  │  EntglDb (Kotlin)             │
│  Local Storage   │  P2P Sync & Distribution      │
│  BSON Engine     │  Ktor / WebRTC / libp2p       │
│  KSP Mappers     │  CRDT Merge                   │
└──────────────────┴───────────────────────────────┘
```

Kotlin has excellent networking libraries:

- **Ktor** — multiplatform HTTP/WebSocket client and server
- **WebRTC** — available on Android and JVM
- **kotlinx.coroutines** — structured concurrency for sync orchestration

Target scenarios:

- **Android apps** syncing data across devices without a backend
- **KMP apps** (Android + iOS + Desktop) with shared data layer and P2P sync
- **JVM microservices** with embedded storage and peer replication
- **Edge/IoT** — Kotlin/Native on Linux ARM devices

## 8. Kotlin vs Dart: Which to Port First?

| Factor | Dart | Kotlin |
|---|---|---|
| **Code gen quality** | ⚠️ build_runner (slower) | ✅ KSP (fast, incremental) |
| **Concurrency** | ⚠️ Isolates (no shared memory) | ✅ Coroutines (shared memory, structured) |
| **Value types** | ❌ None | ⚠️ Partial (JVM inline classes) |
| **Multiplatform** | Flutter (mobile, desktop, web) | KMP (Android, iOS, JVM, Native, WASM) |
| **Market gap** | 🟢 No pure-Dart document DB | 🟢 No pure-Kotlin KMP document DB (Realm deprecated) |
| **Ecosystem size** | Flutter developers | Android + JVM + KMP developers |
| **API ergonomics** | Good | ✅ Excellent (DSL, coroutines, data classes) |
| **Porting difficulty** | Medium-high | Medium |

**Recommendation**: Kotlin offers a **smoother porting path** thanks to KSP's close alignment with Roslyn Source Generators, coroutines mapping naturally to async/await, and the JVM's ByteBuffer being closer to Span\<byte\> than Dart's Uint8List. The deprecation of Realm Kotlin SDK also creates an immediate market opportunity.

## 9. Verdict

| Aspect | Assessment |
|---|---|
| **Technical feasibility** | ✅ **Yes, highly feasible** |
| **Estimated effort** | 🟢 **Medium** (2–5 months for MVP) — slightly easier than Dart |
| **Market value** | 🟢 **Very high** — Realm deprecated, no KMP pure-Kotlin alternative |
| **Natural target** | Android, KMP multiplatform, JVM server |
| **Key risk** | KMP native target maturity (Windows still experimental) |
| **Key advantage** | KSP ≈ Roslyn generators; coroutines ≈ async/await; Kotlin DSL for queries |

### Recommendation

Kotlin is arguably the **best target for a BLite port** among all non-.NET languages. The language features (KSP, coroutines, DSL builders, data classes, inline classes) align closely with BLite's architecture. The **deprecation of Realm Kotlin SDK** creates a rare window of opportunity — the Kotlin/Android ecosystem needs a modern, pure-Kotlin document database.

**Suggested approach:**
1. Start with `blite-bson` — port BSON engine as a KMP common module
2. Build `blite-core` with JVM-first storage (RandomAccessFile), then add Native targets
3. Implement `blite-ksp` — KSP processor generating mapper classes
4. Validate with an Android demo app
5. Add iOS target via Kotlin/Native
6. Port EntglDb for P2P sync
7. Optional: GraalVM native-image support for server deployments
