using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;
using BLite.Tests;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace BLite.Benchmark;

/// <summary>
/// BLite Server layout — concurrent workload throughput comparison.
///
/// Each benchmark method spawns <see cref="ThreadCount"/> parallel tasks that share a single
/// <see cref="DocumentDbContext"/>. Every task opens its own <see cref="BLiteSession"/> via
/// <see cref="DocumentDbContext.OpenSession"/> so transactions are fully independent.
///
/// Layout note
/// ───────────
/// Only the <b>Server</b> layout (per-collection page files) is benchmarked here.
/// The Default (single-file) layout is intentionally designed for embedded, single-caller
/// scenarios and is not suitable for high-concurrency workloads: all collections share one
/// PageFile and therefore one <c>ReaderWriterLockSlim</c>, which serialises concurrent writers
/// and results in severe contention (and potential stalls) at ThreadCount &gt; 1.
/// See LayoutBenchmarks for a single-threaded Default vs Server comparison.
///
/// Operations (per task, per iteration)
/// ─────────────────────────────────────
///   Insert  – <see cref="OpsPerTask"/> inserts, all committed in one session transaction
///   Read    – <see cref="OpsPerTask"/> FindById lookups against seeded data
///   Mixed   – alternating insert / FindById (half-half), committed once per task
/// </summary>
[InProcess]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[HtmlExporter]
[JsonExporterAttribute.Full]
public class ConcurrencyBenchmarks
{
    /// <summary>Documents inserted per task per benchmark call.</summary>
    private const int OpsPerTask = 50;

    /// <summary>Documents pre-seeded into each context for read benchmarks.</summary>
    private const int SeedDocs = 1_000;

    private const string CollectionName = "bench_orders";

    /// <summary>Thread count to use for parallel tasks. BDN varies this across benchmark cases.</summary>
    [Params(1, 2, 4, 8)]
    public int ThreadCount { get; set; }

    private string _serverPath = null!;
    private TestDbContext _serverCtx = null!;

    // IDs seeded — used by read / mixed tasks
    private BsonId[] _readIdsServer = null!;

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    [GlobalSetup]
    public void GlobalSetup()
    {
        var temp = AppContext.BaseDirectory;
        var id = Guid.NewGuid().ToString("N");

        _serverPath = Path.Combine(temp, $"ccbench_srv_{id}.db");

        _serverCtx = new TestDbContext(_serverPath, PageFileConfig.Server(_serverPath));

        _readIdsServer = Seed(_serverCtx).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _serverCtx?.Dispose();
        InsertBenchmarks.DeleteServerDb(_serverPath);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>Seeds <see cref="SeedDocs"/> documents and returns their IDs.</summary>
    private static async Task<BsonId[]> Seed(TestDbContext ctx)
    {
        using var session = ctx.OpenSession();
        var col = session.GetOrCreateCollection(CollectionName);
        var ids = new BsonId[SeedDocs];
        for (int i = 0; i < SeedDocs; i++)
            ids[i] = await col.InsertAsync(col.CreateDocument(["v"], b => b.AddInt32("v", i)));
        await session.CommitAsync();
        return ids;
    }

    // ── Server layout benchmarks ───────────────────────────────────────────────

    [Benchmark(Description = "Server – Concurrent Insert")]
    [BenchmarkCategory("Concurrency_Insert")]
    public async Task Server_Insert() => await RunInsert(_serverCtx);

    [Benchmark(Description = "Server – Concurrent Read")]
    [BenchmarkCategory("Concurrency_Read")]
    public async Task Server_Read() => await RunRead(_serverCtx, _readIdsServer);

    [Benchmark(Description = "Server – Concurrent Mixed")]
    [BenchmarkCategory("Concurrency_Mixed")]
    public async Task Server_Mixed() => await RunMixed(_serverCtx, _readIdsServer);

    // ── Workload runners ──────────────────────────────────────────────────────

    private async Task RunInsert(TestDbContext ctx)
    {
        var tasks = Enumerable.Range(0, ThreadCount).Select(t => Task.Run(async () =>
        {
            using var session = ctx.OpenSession();
            var col = session.GetOrCreateCollection(CollectionName);
            for (int i = 0; i < OpsPerTask; i++)
                await col.InsertAsync(col.CreateDocument(["v"], b => b.AddInt32("v", t * OpsPerTask + i)));
            await session.CommitAsync();
        })).ToArray();
        await Task.WhenAll(tasks);
    }

    private async Task RunRead(TestDbContext ctx, BsonId[] ids)
    {
        var tasks = Enumerable.Range(0, ThreadCount).Select(t => Task.Run(async () =>
        {
            using var session = ctx.OpenSession();
            var col = session.GetOrCreateCollection(CollectionName);
            for (int i = 0; i < OpsPerTask; i++)
                await col.FindByIdAsync(ids[(t * OpsPerTask + i) % ids.Length]);
            // reads don't require a commit; Dispose() handles rollback of the read tx
        })).ToArray();
        await Task.WhenAll(tasks);
    }

    private async Task RunMixed(TestDbContext ctx, BsonId[] ids)
    {
        var tasks = Enumerable.Range(0, ThreadCount).Select(t => Task.Run(async() =>
        {
            using var session = ctx.OpenSession();
            var col = session.GetOrCreateCollection(CollectionName);
            for (int i = 0; i < OpsPerTask; i++)
            {
                if (i % 2 == 0)
                    await col.InsertAsync(col.CreateDocument(["v"], b => b.AddInt32("v", t * OpsPerTask + i)));
                else
                    await col.FindByIdAsync(ids[(t * OpsPerTask + i) % ids.Length]);
            }
            await session.CommitAsync();
        })).ToArray();
        await Task.WhenAll(tasks);
    }
}
