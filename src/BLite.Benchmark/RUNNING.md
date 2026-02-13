# Running BLite Benchmarks

## Prerequisites
```bash
# Ensure you're in the BLite root directory
cd c:\github\BLite

# Clean previous builds
dotnet clean src\BLite.Benchmark\BLite.Benchmark.csproj
```

## Run All Benchmarks
```bash
dotnet run -c Release --project src\BLite.Benchmark
```

This will:
- Run all benchmark categories (Insert, Read, Serialization)
- Compare BLite vs SQLite
- Generate markdown and HTML reports
- Output to `BenchmarkDotNet.Artifacts/results/`

## Run Specific Categories

### Insert Benchmarks Only
```bash
dotnet run -c Release --project src\BLite.Benchmark --filter "*InsertBenchmarks*"
```

### Read Benchmarks Only
```bash
dotnet run -c Release --project src\BLite.Benchmark --filter "*ReadBenchmarks*"
```

### Serialization Benchmarks Only
```bash
dotnet run -c Release --project src\BLite.Benchmark --filter "*Serialization*"
```

## Expected Output Files

After running, check:
```
BenchmarkDotNet.Artifacts/
├── results/
│   ├── BLite.Benchmark.InsertBenchmarks-report.md
│   ├── BLite.Benchmark.ReadBenchmarks-report.md
│   ├── BLite.Benchmark.SerializationBenchmarks-report.md
│   ├── *.html (interactive reports)
│   └── *.json (raw data)
```

## What to Send Back

After running all benchmarks, send me:
1. The `.md` files from `BenchmarkDotNet.Artifacts/results/`
2. Or a screenshot of the console summary

I'll use these to:
- Create a `/docs/benchmarks` page on the website
- Add `BENCHMARKS.md` to the repository root
- Link from README.md

## Tips

- **Close other apps** for consistent results
- **Run multiple times** and use the median run
- **Use Release build** (`-c Release`) - Debug builds are ~10x slower
- Expected total runtime: **5-10 minutes** for all benchmarks
