# BLite Performance Benchmarks

> **Last Updated:** March 17, 2026  
> **Platform:** Windows 11, Intel Core i7-13800H (14 cores), .NET 10.0.4

---

## Overview

BLite is designed for **zero-allocation, high-performance** document operations. These benchmarks compare BLite against **LiteDB 5.0.21**, **SQLite+JSON** (`Microsoft.Data.Sqlite` + `System.Text.Json`), **Couchbase Lite 4.0.3**, and **DuckDB 1.3.0** using BenchmarkDotNet with identical, realistic document workloads.

### Key Takeaways

✅ **5.0x faster** single insert vs LiteDB, **~45x faster** vs SQLite+JSON, **171x faster** vs DuckDB  
✅ **1.9x faster** batch insert vs LiteDB, **6.5x faster** vs CouchbaseLite, **25.7x faster** vs DuckDB  
✅ **5.6x faster** FindById vs LiteDB, **9.6x faster** vs SQLite+JSON, **2,843x faster** vs DuckDB  
✅ **3.4x faster** full collection scan vs LiteDB, **11.6x faster** vs DuckDB  
✅ **3.4x less** memory allocated during scan vs LiteDB  
✅ **OLAP Top-N**: 6.5x faster vs LiteDB, **480x faster** vs DuckDB  
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
| **BLite** | **164.6 μs** | baseline | 215.79 KB |
| LiteDB | 826.9 μs | 5.06x slower | 57.4 KB |
| SQLite+JSON | 7,380.1 μs | 45.15x slower | 27.36 KB |
| CouchbaseLite | 466.6 μs | 2.85x slower | 61.57 KB |
| DuckDB | 28,218.2 μs | 172.64x slower | 27.84 KB |

### Batch Insert (1000 Documents, 1 Transaction)

| Engine | Mean | Ratio | Allocated | Gen0 |
|:-------|-----:|------:|----------:|-----:|
| **BLite** | **14,086 μs** | baseline | **31,312 KB** | 2000 |
| LiteDB | 27,047 μs | 1.93x slower | 32,993 KB | 2000 |
| SQLite+JSON | 25,008 μs | 1.78x slower | 6,296 KB | 0 |
| CouchbaseLite | 91,217 μs | 6.51x slower | 32,548 KB | 2000 |
| DuckDB | 361,785 μs | 25.80x slower | 4,512 KB | 0 |

> **Note on SQLite/DuckDB allocations:** These engines delegate work to native C/C++ libraries; unmanaged memory is not captured by BenchmarkDotNet. BLite allocations are fully measured (100% managed code).

> **Note on BLite batch allocation:** improved from 64,160 KB to 31,312 KB (-51%) by reusing WAL cache page buffers on repeated writes within the same transaction (introduced March 2026).

---

## Read Performance

*Results from `DefaultJob` (standard BenchmarkDotNet configuration — most reliable).*

### FindById — Primary Key Lookup

| Engine | Mean | Ratio | Allocated |
|:-------|-----:|------:|----------:|
| **BLite** | **3.980 μs** | baseline | 7.16 KB |
| LiteDB | 22.257 μs | 5.60x slower | 47.8 KB |
| SQLite+JSON | 38.249 μs | 9.63x slower | 9.34 KB |
| CouchbaseLite | 23.374 μs | 5.88x slower | 9.77 KB |
| DuckDB | 11,296 μs | 2,843x slower | 12.59 KB |

### Scan — Filter by Field (`Status = "shipped"`, ~250 of 1000 results)

| Engine | Mean | Ratio | Allocated |
|:-------|-----:|------:|----------:|
| **BLite** | **2,502 μs** | baseline | 5,091 KB |
| LiteDB | 8,455 μs | 3.38x slower | 17,296 KB |
| SQLite+JSON | 8,046 μs | 3.22x slower | 7,804 KB |
| CouchbaseLite | 10,463 μs | 4.19x slower | 2,221 KB |
| DuckDB | 28,986 μs | 11.60x slower | 1,956 KB |

---

## OLAP Performance

*Results from `DefaultJob`. Dataset: 1,000 `CustomerOrder` documents.*

> **Note on SQLite aggregation:** SQLite's `SUM/AVG/COUNT` runs inside its native C library and outperforms BLite's in-engine aggregation for that specific case. BLite loads all matching documents into managed memory and aggregates in-process, whereas SQLite executes the expression directly on its internal storage. DuckDB shows similar columnar-scan advantages for pure aggregates but is penalized heavily by its connection overhead on fine-grained document lookups.

### Aggregate — SUM / AVG / COUNT over all orders

| Engine | Mean | Ratio | Allocated |
|:-------|-----:|------:|----------:|
| **BLite** | **10,644 μs** | baseline | 7,334 KB |
| LiteDB | 247,462 μs | 23.35x slower | 206,174 KB |
| SQLite+JSON | 509 μs | **20.98x faster** | 1.45 KB |
| CouchbaseLite | 18,192 μs | 1.72x slower | 5.04 KB |
| DuckDB | 11,843 μs | 1.12x slower | 5.09 KB |

### GroupBy Status

| Engine | Mean | Ratio | Allocated |
|:-------|-----:|------:|----------:|
| **BLite** | **7,176 μs** | baseline | 4,673 KB |
| LiteDB | 250,021 μs | 34.92x slower | 206,289 KB |
| SQLite+JSON | 34,680 μs | 4.84x slower | 2.5 KB |
| CouchbaseLite | 41,567 μs | 5.81x slower | 8.45 KB |
| DuckDB | 11,586 μs | 1.62x slower | 7.06 KB |

### Range Scan — `Total > threshold` (full deserialize, ~1000 docs)

| Engine | Mean | Ratio | Allocated |
|:-------|-----:|------:|----------:|
| **BLite** | **71,713 μs** | baseline | 40,286 KB |
| LiteDB | 164,970 μs | 2.31x slower | 162,097 KB |
| SQLite+JSON | 107,775 μs | 1.51x slower | 61,734 KB |
| CouchbaseLite | 257,702 μs | 3.60x slower | 70,180 KB |
| DuckDB | 108,461 μs | 1.52x slower | 61,738 KB |

### Top-10 by Total (ORDER BY + TAKE, index-assisted in BLite)

| Engine | Mean | Ratio | Allocated |
|:-------|-----:|------:|----------:|
| **BLite** | **27.98 μs** | baseline | 62.34 KB |
| LiteDB | 181.20 μs | 6.49x slower | 221.51 KB |
| SQLite+JSON | 130.55 μs | 4.68x slower | 80.10 KB |
| CouchbaseLite | 396.23 μs | 14.19x slower | 92.62 KB |
| DuckDB | 13,423 μs | 480.71x slower | 83.45 KB |

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

LiteDB uses reflection-based BSON serialization and a B+ tree storage engine. BLite's source-generated mappers and append-only WAL-based storage eliminate per-document reflection overhead, yielding the measured 3.4–5.6x advantage on read/write operations and a 23x advantage on in-process OLAP aggregation.

### CouchbaseLite vs BLite

CouchbaseLite 4.0.3 is a mobile/edge document database with a managed .NET wrapper over a native C++ core. Despite native code, BLite outperforms it by 2.8–6.5x across all benchmarks. CouchbaseLite's allocation advantage in the scan benchmark (2,221 KB vs 5,091 KB) reflects its native heap usage being outside .NET measurement.

### DuckDB vs BLite

DuckDB 1.3.0 is an analytical (OLAP) in-process database optimised for columnar scans. It is competitive in pure aggregation (SUM/AVG within ~1.1x of BLite) but incurs severe overhead for fine-grained document workloads: 2,843x slower on `FindById` and 480x slower on ordered Top-N queries. BLite's WAL-indexed sequential pages and source-generated BSON deserialization give it far lower latency on the point-lookup and ordered retrieval patterns typical in edge and server-side document stores.

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
| DuckDB | 1.3.0 | `DuckDB.NET.Data.Full`, in-process |

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
dotnet run -c Release --project tests/BLite.Benchmark -- --filter "*Olap*"

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
BenchmarkDotNet v0.15.8, Windows 11 (10.0.22631.6649/23H2)
13th Gen Intel Core i7-13800H 2.50GHz, 1 CPU, 20 logical and 14 physical cores
.NET SDK 10.0.200
  [Host] : .NET 10.0.4 (10.0.4, 10.0.426.12010), X64 RyuJIT x86-64-v3
Job=InProcess  Toolchain=InProcessEmitToolchain  InvocationCount=1  UnrollFactor=1

| Method                                     | Mean         | Error        | StdDev       | Median       | Ratio          | RatioSD | Gen0      | Allocated   | Alloc Ratio |
|------------------------------------------- |-------------:|-------------:|-------------:|-------------:|---------------:|--------:|----------:|------------:|------------:|
| 'BLite – Batch Insert (1000)'              |  14,086.5 μs |    351.58 μs |    980.06 μs |  13,901.6 μs |       baseline |         | 2000.0000 | 31312.04 KB |             |
| 'LiteDB – Batch Insert (1000)'             |  27,047.5 μs |  1,375.92 μs |  3,903.25 μs |  26,135.1 μs |   1.93x slower |   0.31x | 2000.0000 | 32992.59 KB |  1.05x more |
| 'SQLite+JSON – Batch Insert (1000, 1 Txn)' |  25,008.3 μs |    989.31 μs |  2,790.36 μs |  24,721.7 μs |   1.78x slower |   0.23x |         - |  6295.63 KB |  4.97x less |
| 'CouchbaseLite – Batch Insert (1000)'      |  91,217.0 μs |  4,156.77 μs | 11,926.55 μs |  89,641.9 μs |   6.51x slower |   0.95x | 2000.0000 | 32548.36 KB |  1.04x more |
| 'DuckDB – Batch Insert (1000, 1 Txn)'      | 361,784.5 μs | 18,187.04 μs | 51,593.60 μs | 350,384.5 μs |  25.80x slower |   4.05x |         - |  4511.74 KB |  6.94x less |
|                                            |              |              |              |              |                |         |           |             |             |
| 'BLite – Single Insert'                    |     164.6 μs |      5.12 μs |     14.35 μs |     160.6 μs |       baseline |         |         - |   215.79 KB |             |
| 'LiteDB – Single Insert'                   |     826.9 μs |     36.10 μs |    104.14 μs |     807.1 μs |   5.06x slower |   0.76x |         - |     57.4 KB |  3.76x less |
| 'SQLite+JSON – Single Insert'              |   7,380.1 μs |    821.52 μs |  2,383.39 μs |   7,690.0 μs |  45.15x slower |  15.01x |         - |    27.36 KB |  7.89x less |
| 'CouchbaseLite – Single Insert'            |     466.6 μs |      9.48 μs |     27.50 μs |     464.0 μs |   2.85x slower |   0.29x |         - |    61.57 KB |  3.50x less |
| 'DuckDB – Single Insert'                   |  28,218.2 μs |  1,868.60 μs |  5,450.80 μs |  26,832.2 μs | 172.64x slower |  36.10x |         - |    27.84 KB |  7.75x less |
```

### Read Benchmarks (DefaultJob)

```
BenchmarkDotNet v0.15.8, Windows 11 (10.0.22631.6649/23H2)
13th Gen Intel Core i7-13800H 2.50GHz, 1 CPU, 20 logical and 14 physical cores
.NET SDK 10.0.200
  [Host]     : .NET 10.0.4 (10.0.4, 10.0.426.12010), X64 RyuJIT x86-64-v3
  DefaultJob : .NET 10.0.4 (10.0.4, 10.0.426.12010), X64 RyuJIT x86-64-v3

| Method                                            | Job        | Mean          | Error         | StdDev        | Median        | Ratio            | RatioSD | Gen0      | Gen1     | Allocated   | Alloc Ratio |
|-------------------------------------------------- |----------- |--------------:|--------------:|--------------:|--------------:|-----------------:|--------:|----------:|---------:|------------:|------------:|
| 'BLite – FindById'                                | DefaultJob |      3.980 μs |     0.0789 μs |     0.1716 μs |      4.013 μs |         baseline |         |    0.5798 |        - |     7.16 KB |             |
| 'LiteDB – FindById'                               | DefaultJob |     22.257 μs |     0.4769 μs |     1.3837 μs |     22.458 μs |     5.60x slower |   0.43x |    3.7842 |   0.1221 |     47.8 KB |  6.67x more |
| 'SQLite+JSON – FindById'                          | DefaultJob |     38.249 μs |     0.7547 μs |     1.3018 μs |     38.365 μs |     9.63x slower |   0.53x |    0.7324 |        - |     9.34 KB |  1.30x more |
| 'CouchbaseLite – FindById'                        | DefaultJob |     23.374 μs |     0.4588 μs |     0.7792 μs |     23.197 μs |     5.88x slower |   0.32x |    0.7935 |   0.2441 |     9.77 KB |  1.36x more |
| 'DuckDB – FindById'                               | DefaultJob | 11,295.998 μs |   233.3239 μs |   665.6863 μs | 11,216.298 μs | 2,843.30x slower | 208.46x |         - |        - |    12.59 KB |  1.76x more |
|                                                   |            |               |               |               |               |                  |         |           |          |             |             |
| 'BLite – Scan by Status'                          | DefaultJob |  2,501.830 μs |    48.9194 μs |    73.2202 μs |  2,484.271 μs |         baseline |         |  414.0625 | 234.3750 |   5090.6 KB |             |
| 'LiteDB – Scan by Status'                         | DefaultJob |  8,455.092 μs |   167.2175 μs |   265.2246 μs |  8,445.819 μs |     3.38x slower |   0.14x | 1406.2500 | 453.1250 | 17295.58 KB |  3.40x more |
| 'SQLite+JSON – Scan by Status (full deserialize)' | DefaultJob |  8,046.062 μs |   224.5704 μs |   655.0824 μs |  7,817.766 μs |     3.22x slower |   0.28x |  625.0000 | 578.1250 |  7803.74 KB |  1.53x more |
| 'CouchbaseLite – Scan by Status'                  | DefaultJob | 10,462.934 μs |   846.4545 μs | 2,495.7905 μs | 10,489.571 μs |     4.19x slower |   1.00x |  171.8750 |  78.1250 |   2221.2 KB |  2.29x less |
| 'DuckDB – Scan by Status'                         | DefaultJob | 28,986.217 μs | 1,567.3954 μs | 4,420.8667 μs | 29,267.287 μs |    11.60x slower |   1.79x |  125.0000 |        - |  1955.95 KB |  2.60x less |
```

### OLAP Benchmarks (DefaultJob)

```
BenchmarkDotNet v0.15.8, Windows 11 (10.0.22631.6649/23H2)
13th Gen Intel Core i7-13800H 2.50GHz, 1 CPU, 20 logical and 14 physical cores
.NET SDK 10.0.200
  [Host]     : .NET 10.0.4 (10.0.4, 10.0.426.12010), X64 RyuJIT x86-64-v3
  DefaultJob : .NET 10.0.4 (10.0.4, 10.0.426.12010), X64 RyuJIT x86-64-v3

| Method                                    | Job        | Mean          | Error        | StdDev        | Median        | Ratio          | RatioSD | Gen0       | Gen1      | Gen2      | Allocated    | Alloc Ratio     |
|------------------------------------------ |----------- |--------------:|-------------:|--------------:|--------------:|---------------:|--------:|-----------:|----------:|----------:|-------------:|----------------:|
| 'BLite – Aggregate SUM/AVG/COUNT'         | DefaultJob |  10,643.86 μs |   247.087 μs |    716.844 μs |  10,603.66 μs |       baseline |         |   593.7500 |   31.2500 |         - |   7334.04 KB |                 |
| 'LiteDB – Aggregate SUM/AVG/COUNT'        | DefaultJob | 247,461.69 μs | 5,966.543 μs | 17,592.489 μs | 244,969.32 μs |  23.35x slower |   2.27x | 17333.3333 | 6666.6667 |  666.6667 | 206173.73 KB |    28.112x more |
| 'SQLite+JSON – Aggregate SUM/AVG/COUNT'   | DefaultJob |     509.04 μs |    10.409 μs |     30.529 μs |     510.16 μs |  20.98x faster |   1.88x |          - |         - |         - |      1.45 KB | 5,047.082x less |
| 'CouchbaseLite – Aggregate SUM/AVG/COUNT' | DefaultJob |  18,191.89 μs |   425.131 μs |  1,253.510 μs |  18,215.24 μs |   1.72x slower |   0.16x |          - |         - |         - |      5.04 KB | 1,455.438x less |
| 'DuckDB – Aggregate SUM/AVG/COUNT'        | DefaultJob |  11,842.81 μs |   236.413 μs |    523.876 μs |  11,866.65 μs |   1.12x slower |   0.09x |          - |         - |         - |      5.09 KB | 1,439.812x less |
|                                           |            |               |              |               |               |                |         |            |           |           |              |                 |
| 'BLite – GroupBy Status'                  | DefaultJob |   7,176.27 μs |   142.638 μs |    349.894 μs |   7,243.22 μs |       baseline |         |   343.7500 |   78.1250 |         - |   4672.83 KB |                 |
| 'LiteDB – GroupBy Status'                 | DefaultJob | 250,021.10 μs | 4,986.877 μs | 14,308.287 μs | 252,199.80 μs |  34.92x slower |   2.62x | 17000.0000 | 6000.0000 |  500.0000 | 206288.69 KB |    44.146x more |
| 'SQLite+JSON – GroupBy Status'            | DefaultJob |  34,679.96 μs |   692.770 μs |  1,686.298 μs |  34,741.35 μs |   4.84x slower |   0.33x |          - |         - |         - |       2.5 KB | 1,869.132x less |
| 'CouchbaseLite – GroupBy Status'          | DefaultJob |  41,567.46 μs |   821.700 μs |  1,984.499 μs |  41,801.95 μs |   5.81x slower |   0.40x |          - |         - |         - |      8.45 KB |   553.305x less |
| 'DuckDB – GroupBy Status'                 | DefaultJob |  11,585.76 μs |   231.578 μs |    653.169 μs |  11,589.94 μs |   1.62x slower |   0.12x |          - |         - |         - |      7.06 KB |   662.281x less |
|                                           |            |               |              |               |               |                |         |            |           |           |              |                 |
| 'BLite – Range Total > threshold'         | DefaultJob |  71,712.79 μs | 1,425.493 μs |  3,303.797 μs |  71,964.09 μs |       baseline |         |  4000.0000 | 3857.1429 |  714.2857 |  40285.54 KB |                 |
| 'LiteDB – Range Total > threshold'        | DefaultJob | 164,969.57 μs | 3,265.118 μs |  5,544.419 μs | 165,251.00 μs |   2.31x slower |   0.13x | 13750.0000 | 5500.0000 |  750.0000 | 162096.87 KB |      4.02x more |
| 'SQLite+JSON – Range Total > threshold'   | DefaultJob | 107,774.78 μs | 2,133.891 μs |  4,902.971 μs | 107,737.74 μs |   1.51x slower |   0.10x |  6000.0000 | 5800.0000 | 1000.0000 |  61733.64 KB |      1.53x more |
| 'CouchbaseLite – Range Total > threshold' | DefaultJob | 257,702.11 μs | 6,434.046 μs | 18,460.485 μs | 251,890.85 μs |   3.60x slower |   0.31x |  6000.0000 | 3500.0000 |  500.0000 |  70180.21 KB |      1.74x more |
| 'DuckDB – Range Total > threshold'        | DefaultJob | 108,461.34 μs | 2,074.253 μs |  2,769.069 μs | 108,139.56 μs |   1.52x slower |   0.08x |  6000.0000 | 5800.0000 | 1000.0000 |  61737.61 KB |      1.53x more |
|                                           |            |               |              |               |               |                |         |            |           |           |              |                 |
| 'BLite – Top-10 by Total'                 | DefaultJob |      27.98 μs |     0.553 μs |      1.237 μs |      27.99 μs |       baseline |         |     5.0659 |    0.5798 |         - |     62.34 KB |                 |
| 'LiteDB – Top-10 by Total'                | DefaultJob |     181.20 μs |    17.140 μs |     48.345 μs |     170.58 μs |   6.49x slower |   1.75x |    18.0664 |    2.6855 |         - |    221.51 KB |      3.55x more |
| 'SQLite+JSON – Top-10 by Total'           | DefaultJob |     130.55 μs |     7.035 μs |     20.521 μs |     128.13 μs |   4.68x slower |   0.76x |     6.3477 |    1.2207 |         - |      80.1 KB |      1.29x more |
| 'CouchbaseLite – Top-10 by Total'         | DefaultJob |     396.23 μs |     7.855 μs |     15.868 μs |     394.43 μs |  14.19x slower |   0.84x |     7.3242 |    1.9531 |         - |     92.62 KB |      1.49x more |
| 'DuckDB – Top-10 by Total'                | DefaultJob |  13,422.82 μs |   257.336 μs |    436.976 μs |  13,404.78 μs | 480.71x slower |  26.11x |          - |         - |         - |     83.45 KB |      1.34x more |
```

---

## License

BLite is licensed under the MIT License. See [LICENSE](LICENSE.txt) for details.

