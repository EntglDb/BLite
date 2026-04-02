# BLite Benchmarks

Performance benchmarks comparing BLite with LiteDB, SQLite, DuckDB, and Couchbase Lite using BenchmarkDotNet.

## Quick Start

```bash
# Run all benchmarks
dotnet run -c Release --project tests/BLite.Benchmark

# Run specific benchmark category
dotnet run -c Release --project tests/BLite.Benchmark --filter "*Insert*"
dotnet run -c Release --project tests/BLite.Benchmark --filter "*Read*"
dotnet run -c Release --project tests/BLite.Benchmark --filter "*RealWorld*"
```

## Benchmark Categories

### Insert Benchmarks
- **Single Insert** - Insert one document (baseline comparison)
- **Batch Insert** - Insert 1,000 documents in a single transaction

### Read Benchmarks
- **FindById** - Lookup single document by primary key

### Real-World Read Benchmarks (`RealWorldReadBenchmarks`)
Simulates a photo-library workload (5,000 photos across 20 folders). Directly
modelled after the community benchmark by [@LeoYang6](https://github.com/LeoYang6)
that drove the zero-allocation read path optimisations in v4.x.

- **1-to-1** — `FindOneAsync` by unique `FilePath` index (100 queries/iteration): exercises the direct B-Tree `Seek` fast path introduced in v4.x.
- **1-to-N** — `FindAsync` by non-unique `SourceId` index (2 queries × ~250 results each): exercises indexed range scan vs. full LiteDB scan.

### Serialization Benchmarks
- **Single** - Serialize/Deserialize one object (BSON vs JSON)
- **Batch** - Serialize/Deserialize 10,000 objects in a loop

## Output

Results are exported to `BenchmarkDotNet.Artifacts/results/`:
- **Markdown** (`*.md`) - GitHub-flavored tables
- **HTML** (`*.html`) - Interactive reports
- **JSON** (`*.json`) - Raw data

## Comparing with SQLite

All benchmarks include SQLite comparisons using:
- **Dapper** for object mapping
- **System.Text.Json** for serialization
- **WAL mode** for fair transaction comparison

## Manual Benchmark

For quick sanity checks without full BenchmarkDotNet overhead:

```bash
dotnet run -c Release --project src/BLite.Benchmark manual
```

## Requirements

- .NET 10 SDK
- Release build (`-c Release`) for accurate results
- Close other applications to minimize interference

## Interpreting Results

- **Mean** - Average execution time
- **Error** - Standard error of the mean
- **StdDev** - Standard deviation
- **Allocated** - Memory allocated (Gen 0/1/2 collections)
- **Ratio** - Performance relative to baseline (SQLite)

Lower is better for all metrics.
