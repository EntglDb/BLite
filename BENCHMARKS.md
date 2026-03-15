# BLite Performance Benchmarks

> **Last Updated:** March 14, 2026  
> **Platform:** Windows 11, Intel Core i7-13800H (14 cores), .NET 10.0.4

---

## Overview

BLite is designed for **zero-allocation, high-performance** document operations. These benchmarks compare BLite against **LiteDB 5.0.21**, **SQLite+JSON** (`Microsoft.Data.Sqlite` + `System.Text.Json`), and **Couchbase Lite 4.0.3** using BenchmarkDotNet with identical, realistic document workloads.

### Key Takeaways

✅ **4.9x faster** single insert vs LiteDB, **21.7x faster** vs SQLite+JSON  
✅ **2.8x faster** batch insert vs LiteDB, **5.8x faster** vs CouchbaseLite  
✅ **6.3x faster** FindById vs LiteDB, **10x faster** vs SQLite+JSON  
✅ **3.6x faster** full collection scan vs LiteDB  
✅ **3.4x less** memory allocated during scan vs LiteDB  
✅ **-51%** batch insert allocation (31 MB vs 64 MB) after WAL page buffer reuse  

---

## Document Schema

All benchmarks use a **realistic e-commerce `CustomerOrder` document** (~1–2 KB per document):

```
CustomerOrder
├── Id (string, ObjectId)
├── OrderNumber, PlacedAt, Status, Currency
├── Subtotal, TaxAmount, Total
├── Customer (CustomerContact)
│   ├── FullName, Email, Phone
│   └── BillingAddress (PostalAddress)
│       └── Street, City, ZipCode, Country
├── Shipping (ShippingInfo)
│   ├── Carrier, TrackingNumber, EstimatedDelivery
│   └── Destination (PostalAddress)
├── Lines (List<OrderLine> — 5 items)
│   └── each: Sku, ProductName, Quantity, UnitPrice, Subtotal, Tags[3]
├── Notes (List<OrderNote> — 2 items)
│   └── each: Author, Text, CreatedAt
└── Tags (List<string>)
```

Status distribution across 1000 documents: ~25% each (`pending`, `confirmed`, `shipped`, `delivered`).

---

## Insert Performance

### Single Document Insert

| Engine | Mean | Ratio | Allocated |
|:-------|-----:|------:|----------:|
| **BLite** | **128.0 μs** | baseline | 215.46 KB |
| LiteDB | 623.3 μs | 4.91x slower | 56.99 KB |
| SQLite+JSON | 2,758.3 μs | 21.71x slower | 27.04 KB |
| CouchbaseLite | 346.2 μs | 2.72x slower | 61.24 KB |

### Batch Insert (1000 Documents, 1 Transaction)

| Engine | Mean | Ratio | Allocated | Gen0 |
|:-------|-----:|------:|----------:|-----:|
| **BLite** | **10,841 μs** | baseline | **31,289 KB** | 2000 |
| LiteDB | 30,142 μs | 2.80x slower | 33,126 KB | 3000 |
| SQLite+JSON | 18,576 μs | 1.72x slower | 6,311 KB | 0 |
| CouchbaseLite | 63,021 μs | 5.84x slower | 32,548 KB | 2000 |

> **Note on SQLite allocations:** SQLite reports minimal managed allocations because it delegates work to its native C library. Unmanaged memory is not captured by BenchmarkDotNet. BLite allocations are fully measured (100% managed code).

> **Note on BLite batch allocation:** improved from 64,160 KB to 31,242 KB (-51%) by reusing WAL cache page buffers on repeated writes within the same transaction (introduced March 2026).

---

## Read Performance

*Results from `DefaultJob` (standard BenchmarkDotNet configuration — most reliable).*

### FindById — Primary Key Lookup

| Engine | Mean | Ratio | Allocated |
|:-------|-----:|------:|----------:|
| **BLite** | **2.823 μs** | baseline | 5.80 KB |
| LiteDB | 17.813 μs | 6.31x slower | 45.14 KB |
| SQLite+JSON | 28.262 μs | 10.01x slower | 9.34 KB |
| CouchbaseLite | 18.494 μs | 6.55x slower | 9.77 KB |

### Scan — Filter by Field (`Status = "shipped"`, ~250 of 1000 results)

| Engine | Mean | Ratio | Allocated |
|:-------|-----:|------:|----------:|
| **BLite** | **1,971 μs** | baseline | 5,091 KB |
| LiteDB | 7,129 μs | 3.62x slower | 17,296 KB |
| SQLite+JSON | 5,872 μs | 2.98x slower | 7,804 KB |
| CouchbaseLite | 5,527 μs | 2.81x slower | 2,221 KB |

---

## Serialization Performance

*Standalone BSON vs JSON serialization (no I/O), measured on a complex nested object.*

### Single Object

| Operation | BSON (BLite) | JSON (System.Text.Json) | Speedup |
|:----------|-------------:|------------------------:|--------:|
| **Serialize** | **1.42 μs** | 3.43 μs | **2.4x** |
| **Deserialize** | **3.34 μs** | 7.01 μs | **2.1x** |

| Metric | BSON | JSON |
|:-------|-----:|-----:|
| Serialize Allocated | **0 B** | 1,880 B |
| Deserialize Allocated | 5,704 B | 6,600 B |

### Bulk (10,000 Objects)

| Operation | BSON (BLite) | JSON | Speedup |
|:----------|-------------:|-----:|--------:|
| **Serialize** | **14.99 ms** | 21.40 ms | **1.43x** |
| **Deserialize** | **18.92 ms** | 42.96 ms | **2.27x** |

| Metric | BSON | JSON |
|:-------|-----:|-----:|
| Serialize Allocated | **0 B** | 19.19 MB |
| Deserialize Allocated | 57.98 MB | 66.94 MB |

---

## Architecture Highlights

### Why BLite is Faster

1. **C-BSON Format** — Field name compression (2-byte IDs vs full string keys)
2. **Zero-Copy I/O** — Direct `Span<byte>` operations
3. **Memory Pooling** — `ArrayPool` for buffer reuse, WAL page buffer in-place reuse
4. **Stack Allocation** — `stackalloc` for temporary buffers
5. **Source Generators** — Compile-time serialization, no reflection at runtime
6. **Group Commit** — Background writer batches N concurrent commits into one `fsync`, amortising disk flush cost

### LiteDB vs BLite

LiteDB uses reflection-based BSON serialization and a B+ tree storage engine. BLite's source-generated mappers and append-only WAL-based storage eliminate per-document reflection overhead, yielding the measured 3.6–6.3x advantage on read/write operations.

### CouchbaseLite vs BLite

CouchbaseLite 4.0.3 is a mobile/edge document database with a managed .NET wrapper over a native C++ core. Despite native code, BLite outperforms it by 2.7–5.8x across all benchmarks. CouchbaseLite's allocation advantage in the scan benchmark (2,221 KB vs 5,091 KB) reflects its native heap usage being outside .NET measurement.

---

## Benchmark Environment

```
BenchmarkDotNet v0.15.8
OS: Windows 11 (10.0.22631.6649/23H2/2023Update/SunValley3)
CPU: 13th Gen Intel Core i7-13800H @ 2.50GHz — 14 physical cores, 20 logical cores
Runtime: .NET 10.0.4 (X64 RyuJIT x86-64-v3)
```

### Engine Versions

| Engine | Version | Notes |
|:-------|:--------|:------|
| **BLite** | current | Source-generated mappers, `TestDbContext` |
| LiteDB | 5.0.21 | `Connection=direct` |
| SQLite | Microsoft.Data.Sqlite 10.0.2 | Dapper 2.1.66, JSON blobs |
| CouchbaseLite | 4.0.3 | `Couchbase.Lite` + `Couchbase.Lite.Support.NetDesktop` |

---

## Running Benchmarks

```bash
git clone https://github.com/EntglDb/BLite.git
cd BLite

# Full BenchmarkDotNet run (all engines, all categories)
dotnet run -c Release --project tests/BLite.Benchmark

# Filter to a specific category
dotnet run -c Release --project tests/BLite.Benchmark -- --filter "*Insert*"
dotnet run -c Release --project tests/BLite.Benchmark -- --filter "*Read*"

# Results will be in:
# BenchmarkDotNet.Artifacts/results/
```

---

## Interpreting Results

- **μs (microseconds)** — Execution time (lower is better)
- **Allocated** — Managed .NET memory allocated per operation
- **Gen0/Gen1** — Garbage collection pressure
- **Ratio** — Performance relative to BLite baseline (lower = closer to BLite)
- **DefaultJob** — Standard BenchmarkDotNet multi-iteration run (most reliable)
- **InProcess** — Same process, avoids startup cost (used for Insert benchmarks)

---

## Detailed Raw Output

### Insert Benchmarks

```
| Method                                     | Mean        | Error        | StdDev      | Ratio         | Gen0      | Allocated   |
|------------------------------------------- |------------:|-------------:|------------:|--------------:|----------:|------------:|
| 'BLite – Batch Insert (1000)'              | 10,841.0 μs |    282.66 μs |   815.55 μs |      baseline | 2000.0000 | 31288.73 KB |
| 'LiteDB – Batch Insert (1000)'             | 30,142.4 μs |  2,121.31 μs | 6,254.72 μs |  2.80x slower | 3000.0000 | 33125.82 KB |
| 'SQLite+JSON – Batch Insert (1000, 1 Txn)' | 18,576.3 μs |  1,056.73 μs | 3,014.91 μs |  1.72x slower |         - |  6311.38 KB |
| 'CouchbaseLite – Batch Insert (1000)'      | 63,020.5 μs |  1,307.46 μs | 3,751.36 μs |  5.84x slower | 2000.0000 |  32548.2 KB |
|                                            |             |              |             |               |           |             |
| 'BLite – Single Insert'                    |    128.0 μs |      3.96 μs |    11.54 μs |      baseline |         - |   215.46 KB |
| 'LiteDB – Single Insert'                   |    623.3 μs |     27.42 μs |    80.85 μs |  4.91x slower |         - |    56.99 KB |
| 'SQLite+JSON – Single Insert'              |  2,758.3 μs |    116.00 μs |   338.39 μs | 21.71x slower |         - |    27.04 KB |
| 'CouchbaseLite – Single Insert'            |    346.2 μs |     13.55 μs |    38.65 μs |  2.72x slower |         - |    61.24 KB |
```

### Read Benchmarks (DefaultJob)

```
| Method                                            | Mean         | Error      | StdDev     | Ratio         | Gen0      | Gen1     | Allocated   |
|-------------------------------------------------- |-------------:|-----------:|-----------:|--------------:|----------:|---------:|------------:|
| 'BLite – FindById'                                |     2.823 μs |  0.0512 μs |  0.0526 μs |      baseline |    0.4730 |   0.0038 |      5.8 KB |
| 'LiteDB – FindById'                               |    17.813 μs |  0.3508 μs |  0.4562 μs |  6.31x slower |    3.6621 |   0.1221 |    45.14 KB |
| 'SQLite+JSON – FindById'                          |    28.262 μs |  0.2647 μs |  0.2476 μs | 10.01x slower |    0.7324 |        - |     9.34 KB |
| 'CouchbaseLite – FindById'                        |    18.494 μs |  0.2937 μs |  0.2747 μs |  6.55x slower |    0.7935 |   0.2441 |     9.77 KB |
|                                                   |              |            |            |               |           |          |             |
| 'BLite – Scan by Status'                          | 1,971.119 μs | 39.2086 μs | 54.9650 μs |      baseline |  414.0625 | 246.0938 |  5090.54 KB |
| 'LiteDB – Scan by Status'                         | 7,129.015 μs | 137.795 μs | 147.439 μs |  3.62x slower | 1406.2500 | 460.9375 |  17295.5 KB |
| 'SQLite+JSON – Scan by Status (full deserialize)' | 5,872.312 μs |  83.121 μs |  73.685 μs |  2.98x slower |  632.8125 | 562.5000 |  7803.59 KB |
| 'CouchbaseLite – Scan by Status'                  | 5,527.266 μs |  88.942 μs |  78.845 μs |  2.81x slower |  179.6875 |  85.9375 |  2220.98 KB |
```

---

## License

BLite is licensed under the MIT License. See [LICENSE](LICENSE.txt) for details.

