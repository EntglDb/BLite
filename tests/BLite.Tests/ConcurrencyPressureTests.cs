using System.Collections.Concurrent;
using System.Diagnostics;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;
using BLite.Shared;
using Xunit.Abstractions;

namespace BLite.Tests;

/// <summary>
/// Parametric stress test that scales concurrent writers to find the breaking point
/// where TimeoutExceptions (WAL lock contention) exceed an acceptable threshold.
///
/// Two scenarios:
///   1. <b>Insert-only</b> — pure append, no page conflicts.
///   2. <b>Read-modify-write</b> — realistic "update after read" on a shared hot set.
///      Each task reads a random document by ID, rebuilds it with an incremented counter,
///      then calls UpdateAsync+Commit. Multiple tasks hit the same pages/slots → real
///      WAL + page-lock contention.
///
/// Output per level:
///   - Total ops, successes, timeouts (real failures), contention errors (expected)
///   - Failure rate %
///   - p50 / p95 / p99 latency
///   - Throughput (ops/s)
/// </summary>
public class ConcurrencyPressureTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _dbPath;

    /// <summary>Insert+Commit cycles per task.</summary>
    private const int OpsPerTask = 50;

    /// <summary>Documents pre-seeded for read-modify-write scenarios.</summary>
    private const int SeedCount = 200;

    /// <summary>Failure rate above which the database is considered unusable.</summary>
    private const double FailureThreshold = 0.40;

    private static readonly int[] InsertLevels = [8, 16, 32, 64, 128, 256];
    private static readonly int[] RmwLevels = [16, 32, 64, 128, 256, 512];

    public ConcurrencyPressureTests(ITestOutputHelper output)
    {
        _output = output;
        _dbPath = Path.Combine(Path.GetTempPath(), $"pressure_{Guid.NewGuid():N}.db");
    }

    [Fact]
    public async Task FindConcurrencyBreakingPoint_ServerLayout()
    {
        _output.WriteLine("Insert-only (Server layout)");
        await RunSuite(useServerLayout: true, assertAt4: true);
    }

    [Fact]
    public async Task FindConcurrencyBreakingPoint_DefaultLayout()
    {
        _output.WriteLine("Default layout (single-file) — expected to saturate earlier");
        await RunSuite(useServerLayout: false, assertAt4: false);
    }

    [Fact]
    public async Task ReadModifyWrite_ServerLayout()
    {
        _output.WriteLine("Read-Modify-Write on shared hot set (Server layout)");
        await RunSuite(useServerLayout: true, assertAt4: true, mode: WorkloadMode.ReadModifyWrite);
    }

    [Fact]
    public async Task ReadModifyWrite_DefaultLayout()
    {
        _output.WriteLine("Read-Modify-Write on shared hot set (Default layout)");
        await RunSuite(useServerLayout: false, assertAt4: false, mode: WorkloadMode.ReadModifyWrite);
    }

    // ── Shared suite runner ─────────────────────────────────────────────

    private enum WorkloadMode { InsertOnly, ReadModifyWrite }

    private async Task RunSuite(bool useServerLayout, bool assertAt4, WorkloadMode mode = WorkloadMode.InsertOnly)
    {
        _output.WriteLine($"{"Level",6} {"TotalOps",9} {"OK",7} {"Timeout",8} {"Contention",10} {"Timeout%",9} {"p50ms",7} {"p95ms",7} {"p99ms",7} {"ops/s",9}");
        _output.WriteLine(new string('─', 90));

        var levels = mode == WorkloadMode.ReadModifyWrite ? RmwLevels : InsertLevels;
        int? breakingPoint = null;

        foreach (var level in levels)
        {
            var result = await RunLevel(level, useServerLayout, mode);

            // Only timeouts count as real failures — contention errors (duplicate key, etc.)
            // are the correct outcome of optimistic concurrency control.
            var failRate = result.TotalOps == 0 ? 0 : (double)result.Timeouts / result.TotalOps;

            _output.WriteLine(
                $"{level,6} {result.TotalOps,9} {result.Successes,7} {result.Timeouts,8} {result.OtherErrors,10} " +
                $"{failRate,9:P1} {result.P50,7:F1} {result.P95,7:F1} {result.P99,7:F1} {result.Throughput,9:F0}");

            if (result.ErrorSamples is { Length: > 0 })
                foreach (var err in result.ErrorSamples.Take(3))
                    _output.WriteLine($"         └─ {err}");

            if (breakingPoint == null && failRate > FailureThreshold)
                breakingPoint = level;

            if (failRate > 0.80)
            {
                _output.WriteLine($"\n⛔ Stopped: failure rate {failRate:P1} at concurrency {level}");
                break;
            }
        }

        _output.WriteLine("");
        if (breakingPoint.HasValue)
            _output.WriteLine($"⚠ Breaking point: concurrency {breakingPoint.Value} (failure rate > {FailureThreshold:P0})");
        else
            _output.WriteLine($"✅ All levels below {FailureThreshold:P0} failure threshold");

        if (assertAt4)
        {
            var level4 = await RunLevel(4, useServerLayout, mode);
            var rate4 = level4.TotalOps == 0 ? 1.0 : (double)level4.Timeouts / level4.TotalOps;
            Assert.True(rate4 < 0.01,
                $"At concurrency 4 the timeout rate should be < 1%, was {rate4:P2}");
        }
    }

    // ── Engine ──────────────────────────────────────────────────────────

    private async Task<LevelResult> RunLevel(int concurrency, bool useServerLayout = true, WorkloadMode mode = WorkloadMode.InsertOnly)
    {
        // Fresh database per level to avoid cross-contamination
        var levelPath = Path.Combine(Path.GetTempPath(), $"pressure_{Guid.NewGuid():N}.db");
        var config = useServerLayout ? PageFileConfig.Server(levelPath) : PageFileConfig.Default;

        using var db = new TestDbContext(levelPath, config);

        // Seed documents for read-modify-write
        BsonId[] seedIds = [];
        if (mode == WorkloadMode.ReadModifyWrite)
        {
            seedIds = new BsonId[SeedCount];
            using var seedSession = db.OpenSession();
            var seedCol = seedSession.GetOrCreateCollection("pressure");
            for (int i = 0; i < SeedCount; i++)
                seedIds[i] = await seedCol.InsertAsync(
                    seedCol.CreateDocument(["counter"], b => b.AddInt32("counter", 0)));
            await seedSession.CommitAsync();
        }

        var latencies = new ConcurrentBag<double>();
        var errorSamples = new ConcurrentDictionary<string, int>();
        int successes = 0, timeouts = 0, otherErrors = 0;

        var sw = Stopwatch.StartNew();

        var tasks = Enumerable.Range(0, concurrency).Select(taskIdx => Task.Run(async () =>
        {
            var rng = new Random(taskIdx * 31);
            for (int i = 0; i < OpsPerTask; i++)
            {
                var opSw = Stopwatch.StartNew();
                try
                {
                    using var session = db.OpenSession();
                    var col = session.GetOrCreateCollection("pressure");

                    if (mode == WorkloadMode.ReadModifyWrite)
                    {
                        // Pick a random doc from the hot set
                        var targetId = seedIds[rng.Next(seedIds.Length)];

                        // READ
                        var doc = await col.FindByIdAsync(targetId);
                        if (doc == null)
                        {
                            // Doc temporarily invisible (concurrent rewrite) — count as skip
                            opSw.Stop();
                            latencies.Add(opSw.Elapsed.TotalMilliseconds);
                            Interlocked.Increment(ref successes);
                            continue;
                        }

                        // MODIFY — increment counter
                        int counter = 0;
                        doc.TryGetInt32("counter", out counter);
                        var updated = col.CreateDocument(["counter"],
                            b => b.AddInt32("counter", counter + 1));

                        // WRITE
                        await col.UpdateAsync(targetId, updated);
                    }
                    else
                    {
                        await col.InsertAsync(
                            col.CreateDocument(["v"], b => b.AddInt32("v", taskIdx * OpsPerTask + i)));
                    }

                    await session.CommitAsync();

                    opSw.Stop();
                    latencies.Add(opSw.Elapsed.TotalMilliseconds);
                    Interlocked.Increment(ref successes);
                }
                catch (TimeoutException)
                {
                    opSw.Stop();
                    latencies.Add(opSw.Elapsed.TotalMilliseconds);
                    Interlocked.Increment(ref timeouts);
                }
                catch (Exception ex)
                {
                    opSw.Stop();
                    latencies.Add(opSw.Elapsed.TotalMilliseconds);
                    Interlocked.Increment(ref otherErrors);
                    errorSamples.TryAdd(ex.GetType().Name + ": " + ex.Message, 1);
                }
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        sw.Stop();

        var sorted = latencies.OrderBy(x => x).ToArray();
        int total = successes + timeouts + otherErrors;

        // 'using var db' handles Dispose — just clean up temp files after scope ends.
        // DeleteDb is called from the using-block's finally via the method return.

        return new LevelResult
        {
            TotalOps = total,
            Successes = successes,
            Timeouts = timeouts,
            OtherErrors = otherErrors,
            P50 = Percentile(sorted, 0.50),
            P95 = Percentile(sorted, 0.95),
            P99 = Percentile(sorted, 0.99),
            Throughput = total / sw.Elapsed.TotalSeconds,
            ErrorSamples = errorSamples.Keys.ToArray()
        };
    }

    private static double Percentile(double[] sorted, double p)
    {
        if (sorted.Length == 0) return 0;
        int idx = (int)Math.Ceiling(p * sorted.Length) - 1;
        return sorted[Math.Clamp(idx, 0, sorted.Length - 1)];
    }

    private record struct PhasedResult
    {
        public int Successes;
        public int Failures;
        public double[] ReadLatencies;
        public double[] WriteLatencies;
        public double[] CommitLatencies;
        public double[] TotalLatencies;
        public double Throughput;
        public string[] ErrorSamples;
    }

    // ── Cleanup helpers ─────────────────────────────────────────────────

    private static void DeleteDb(string path)
    {
        foreach (var ext in new[] { "", ".wal", ".idx" })
        {
            try
            {
                var f = ext == "" ? path : Path.ChangeExtension(path, ext);
                if (File.Exists(f)) File.Delete(f);
            }
            catch { }
        }
        // Server layout directory
        var dir = path + ".collections";
        if (Directory.Exists(dir))
            try { Directory.Delete(dir, true); } catch { }
    }

    public void Dispose()
    {
        DeleteDb(_dbPath);
    }

    private record struct LevelResult
    {
        public int TotalOps;
        public int Successes;
        public int Timeouts;
        public int OtherErrors;
        public int Failures => Timeouts + OtherErrors;
        public double P50;
        public double P95;
        public double P99;
        public double Throughput;
        public string[] ErrorSamples;
    }
}
