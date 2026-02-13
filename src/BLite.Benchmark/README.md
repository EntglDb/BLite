# BLite Benchmarks

Performance benchmarks comparing BLite with SQLite using BenchmarkDotNet.

## Quick Start

```bash
# Run all benchmarks
dotnet run -c Release --project src/BLite.Benchmark

# Run specific benchmark category
dotnet run -c Release --project src/BLite.Benchmark --filter "*Insert*"
dotnet run -c Release --project src/BLite.Benchmark --filter "*Read*"
dotnet run -c Release --project src/BLite.Benchmark --filter "*Serialization*"
```

## Benchmark Categories

### Insert Benchmarks
- **Single Insert** - Insert one document (baseline comparison)
- **Batch Insert** - Insert 1,000 documents in a single transaction

### Read Benchmarks
- **FindById** - Lookup single document by primary key

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
