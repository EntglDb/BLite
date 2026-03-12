# BLite Performance Benchmarks

> **Last Updated:** March 12, 2026  
> **Platform:** Windows 11, Intel Core i7-13800H (14 cores), .NET 10.0.4

---

## Overview

BLite is designed for **zero-allocation, high-performance** document operations. These benchmarks compare BLite against **LiteDB 5.0.21** and **SQLite+JSON** (`Microsoft.Data.Sqlite` + `System.Text.Json`) using BenchmarkDotNet with identical, realistic document workloads.

### Key Takeaways

✅ **4.7x faster** single insert vs LiteDB, **31.5x faster** vs SQLite+JSON  
✅ **1.77x faster** batch insert vs LiteDB, **1.78x faster** vs SQLite+JSON  
✅ **5.8x faster** FindById vs LiteDB, **9.5x faster** vs SQLite+JSON  
✅ **3.3x faster** full collection scan vs LiteDB  
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
| **BLite** | **115.6 μs** | baseline | 134.43 KB |
| LiteDB | 541.5 μs | 4.72x slower | 56.09 KB |
| SQLite+JSON | 3,608.8 μs | 31.46x slower | 10.02 KB |

### Batch Insert (1000 Documents, 1 Transaction)

| Engine | Mean | Ratio | Allocated | Gen0 |
|:-------|-----:|------:|----------:|-----:|
| **BLite** | **9,113 μs** | baseline | **31,242 KB** | 2000 |
| LiteDB | 16,117 μs | 1.77x slower | 33,491 KB | 2000 |
| SQLite+JSON | 16,196 μs | 1.78x slower | 6,294 KB | 0 |

> **Note on SQLite allocations:** SQLite reports minimal managed allocations because it delegates work to its native C library. Unmanaged memory is not captured by BenchmarkDotNet. BLite allocations are fully measured (100% managed code).

> **Note on BLite batch allocation:** improved from 64,160 KB to 31,242 KB (-51%) by reusing WAL cache page buffers on repeated writes within the same transaction (introduced March 2026).

---

## Read Performance

*Results from `DefaultJob` (standard BenchmarkDotNet configuration — most reliable).*

### FindById — Primary Key Lookup

| Engine | Mean | Ratio | Allocated |
|:-------|-----:|------:|----------:|
| **BLite** | **3.005 μs** | baseline | 6.46 KB |
| LiteDB | 17.327 μs | 5.77x slower | 45.65 KB |
| SQLite+JSON | 28.425 μs | 9.46x slower | 9.33 KB |

### Scan — Filter by Field (`Status = "shipped"`, ~250 of 1000 results)

| Engine | Mean | Ratio | Allocated |
|:-------|-----:|------:|----------:|
| **BLite** | **2,115 μs** | baseline | 5,090 KB |
| LiteDB | 7,001 μs | 3.31x slower | 17,295 KB |
| SQLite+JSON | 6,156 μs | 2.91x slower | 7,803 KB |

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

LiteDB uses reflection-based BSON serialization and a B+ tree storage engine. BLite's source-generated mappers and append-only WAL-based storage eliminate per-document reflection overhead, yielding the measured 2.6–5.6x advantage on read/write operations.

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
| SQLite | Microsoft.Data.Sqlite 10.0.4 | Dapper 2.1.66, JSON blobs |

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
| Method                                     | Mean        | Error     | StdDev      | Ratio         | Gen0      | Allocated   |
|------------------------------------------- |------------:|----------:|------------:|--------------:|----------:|------------:|
| 'BLite – Batch Insert (1000)'              |  9,113.9 μs | 180.54 μs |   481.90 μs |      baseline | 2000.0000 | 31242.16 KB |
| 'LiteDB – Batch Insert (1000)'             | 16,117.3 μs | 318.41 μs |   692.20 μs |  1.77x slower | 2000.0000 | 33491.02 KB |
| 'SQLite+JSON – Batch Insert (1000, 1 Txn)' | 16,196.6 μs | 432.15 μs | 1,239.91 μs |  1.78x slower |         - |  6294.49 KB |
|                                            |             |           |             |               |           |             |
| 'BLite – Single Insert'                    |    115.6 μs |   3.71 μs |    10.60 μs |      baseline |         - |   134.43 KB |
| 'LiteDB – Single Insert'                   |    541.5 μs |  22.38 μs |    64.21 μs |  4.72x slower |         - |    56.09 KB |
| 'SQLite+JSON – Single Insert'              |  3,608.8 μs | 116.51 μs |   334.28 μs | 31.46x slower |         - |    10.02 KB |
```

### Read Benchmarks (DefaultJob)

```
| Method                                            | Mean         | Error      | StdDev     | Ratio        | Gen0      | Gen1     | Allocated   |
|-------------------------------------------------- |-------------:|-----------:|-----------:|-------------:|----------:|---------:|------------:|
| 'BLite – FindById'                                |     3.005 μs |  0.0508 μs |  0.0475 μs |     baseline |    0.5264 |   0.0038 |     6.46 KB |
| 'LiteDB – FindById'                               |    17.327 μs |  0.2773 μs |  0.2315 μs | 5.77x slower |    3.6621 |   0.1221 |    45.65 KB |
| 'SQLite+JSON – FindById'                          |    28.425 μs |  0.3271 μs |  0.3059 μs | 9.46x slower |    0.7324 |        - |     9.33 KB |
|                                                   |              |            |            |              |           |          |             |
| 'BLite – Scan by Status'                          | 2,115.120 μs | 41.9366 μs | 80.7975 μs |     baseline |  414.0625 | 214.8438 |  5090.54 KB |
| 'LiteDB – Scan by Status'                         | 7,001.699 μs | 116.994 μs | 156.184 μs | 3.31x slower | 1406.2500 | 515.6250 |  17295.5 KB |
| 'SQLite+JSON – Scan by Status (full deserialize)' | 6,156.223 μs | 118.903 μs | 127.225 μs | 2.91x slower |  632.8125 | 562.5000 |  7803.58 KB |
```

---

## License

BLite is licensed under the MIT License. See [LICENSE](LICENSE.txt) for details.

