# BLite Performance Benchmarks

> **Last Updated:** February 21, 2026  
> **Platform:** Windows 11, Intel Core i7-13800H (14 cores), .NET 10.0.2

---

## Overview

BLite is designed for **zero-allocation, high-performance** document operations. These benchmarks compare BLite against **LiteDB 5.0.21** and **SQLite+JSON** (`Microsoft.Data.Sqlite` + `System.Text.Json`) using BenchmarkDotNet with identical, realistic document workloads.

### Key Takeaways

✅ **3.1x faster** batch insert vs LiteDB, **2.9x faster** vs SQLite+JSON  
✅ **5.6x faster** single insert vs LiteDB, **33x faster** vs SQLite+JSON  
✅ **2.6x faster** FindById vs LiteDB, **9.9x faster** vs SQLite+JSON  
✅ **3.5x faster** full collection scan vs LiteDB  
✅ **3.4x less** memory allocated during scan vs LiteDB  

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
| **BLite** | **134.7 μs** | baseline | 114.53 KB |
| LiteDB | 704.3 μs | 5.57x slower | 57.82 KB |
| SQLite+JSON | 4,209.1 μs | 33.27x slower | 11.02 KB |

### Batch Insert (1000 Documents, 1 Transaction)

| Engine | Mean | Ratio | Allocated |
|:-------|-----:|------:|----------:|
| **BLite** | **8,797 μs** | baseline | 52,930 KB |
| LiteDB | 27,376 μs | 3.15x slower | 33,000 KB |
| SQLite+JSON | 25,128 μs | 2.89x slower | 6,295 KB |

> **Note on SQLite allocations:** SQLite reports minimal managed allocations because it delegates work to its native C library. Unmanaged memory is not captured by BenchmarkDotNet. BLite allocations are fully measured (100% managed code).

---

## Read Performance

*Results from `DefaultJob` (standard BenchmarkDotNet configuration — most reliable).*

### FindById — Primary Key Lookup

| Engine | Mean | Ratio | Allocated |
|:-------|-----:|------:|----------:|
| **BLite** | **6.368 μs** | baseline | 6.60 KB |
| LiteDB | 16.466 μs | 2.59x slower | 44.44 KB |
| SQLite+JSON | 62.807 μs | 9.87x slower | 9.34 KB |

### Scan — Filter by Field (`Status = "shipped"`, ~250 of 1000 results)

| Engine | Mean | Ratio | Allocated |
|:-------|-----:|------:|----------:|
| **BLite** | **3,433 μs** | baseline | 5,090 KB |
| LiteDB | 11,953 μs | 3.51x slower | 17,295 KB |
| SQLite+JSON | 9,677 μs | 2.84x slower | 7,803 KB |

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
3. **Memory Pooling** — `ArrayPool` for buffer reuse
4. **Stack Allocation** — `stackalloc` for temporary buffers
5. **Source Generators** — Compile-time serialization, no reflection at runtime

### LiteDB vs BLite

LiteDB uses reflection-based BSON serialization and a B+ tree storage engine. BLite's source-generated mappers and append-only WAL-based storage eliminate per-document reflection overhead, yielding the measured 2.6–5.6x advantage on read/write operations.

---

## Benchmark Environment

```
BenchmarkDotNet v0.15.8
OS: Windows 11 (10.0.22631.6495/23H2/2023Update/SunValley3)
CPU: 13th Gen Intel Core i7-13800H @ 2.50GHz — 14 physical cores, 20 logical cores
Runtime: .NET 10.0.2 (X64 RyuJIT x86-64-v3)
```

### Engine Versions

| Engine | Version | Notes |
|:-------|:--------|:------|
| **BLite** | current | Source-generated mappers, `TestDbContext` |
| LiteDB | 5.0.21 | `Connection=direct` |
| SQLite | Microsoft.Data.Sqlite 10.0.2 | Dapper 2.1.66, JSON blobs |

---

## Running Benchmarks

```bash
git clone https://github.com/EntglDb/BLite.git
cd BLite

# Full BenchmarkDotNet run (all engines, all categories)
dotnet run -c Release --project src/BLite.Benchmark

# Quick manual run (no BDN overhead, ~10s)
dotnet run -c Release --project src/BLite.Benchmark -- manual

# Results will be in:
# src/BLite.Benchmark/BenchmarkDotNet.Artifacts/results/
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
| Method                                     | Mean        | Error       | StdDev      | Ratio         | Gen0      | Gen1      | Allocated   |
|------------------------------------------- |------------:|------------:|------------:|--------------:|----------:|----------:|------------:|
| 'BLite – Batch Insert (1000)'              |  8,797.0 μs |   352.75 μs |   971.57 μs |      baseline | 4000.0000 | 1000.0000 | 52930.39 KB |
| 'LiteDB – Batch Insert (1000)'             | 27,376.7 μs | 2,337.11 μs | 6,854.34 μs |  3.15x slower | 2000.0000 |         - | 33000.42 KB |
| 'SQLite+JSON – Batch Insert (1000, 1 Txn)' | 25,128.5 μs | 2,442.94 μs | 7,203.05 μs |  2.89x slower |         - |         - |   6295.2 KB |
|                                            |             |             |             |               |           |           |             |
| 'BLite – Single Insert'                    |    134.7 μs |    10.88 μs |    31.74 μs |      baseline |         - |         - |   114.53 KB |
| 'LiteDB – Single Insert'                   |    704.3 μs |    46.31 μs |   136.54 μs |  5.57x slower |         - |         - |    57.82 KB |
| 'SQLite+JSON – Single Insert'              |  4,209.1 μs |   162.09 μs |   472.83 μs | 33.27x slower |         - |         - |    11.02 KB |
```

### Read Benchmarks (DefaultJob)

```
| Method                                            | Mean          | Error       | StdDev      | Ratio         | Gen0      | Gen1     | Allocated   |
|-------------------------------------------------- |--------------:|------------:|------------:|--------------:|----------:|---------:|------------:|
| 'BLite – FindById'                                |      6.368 μs |   0.1267 μs |   0.2081 μs |      baseline |    0.5379 |   0.0038 |      6.6 KB |
| 'LiteDB – FindById'                               |     16.466 μs |   0.3149 μs |   0.3093 μs |  2.59x slower |    3.5400 |   0.1221 |    44.44 KB |
| 'SQLite+JSON – FindById'                          |     62.807 μs |   1.5688 μs |   4.6255 μs |  9.87x slower |    0.7324 |        - |     9.34 KB |
|                                                   |               |             |             |               |           |          |             |
| 'BLite – Scan by Status'                          |  3,433.450 μs |  91.2274 μs | 268.9861 μs |      baseline |  414.0625 | 214.8438 |  5090.54 KB |
| 'LiteDB – Scan by Status'                         | 11,953.181 μs | 234.5464 μs | 434.7467 μs |  3.51x slower | 1406.2500 | 468.7500 |  17295.5 KB |
| 'SQLite+JSON – Scan by Status (full deserialize)' |  9,677.939 μs | 213.9870 μs | 630.9455 μs |  2.84x slower |  632.8125 | 562.5000 |  7803.53 KB |
```

---

## License

BLite is licensed under the MIT License. See [LICENSE](LICENSE.txt) for details.

