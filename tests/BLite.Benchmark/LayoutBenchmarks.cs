using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BLite.Core.Query;
using BLite.Core.Storage;
using BLite.Shared;
using BLite.Tests;
using System.IO;

namespace BLite.Benchmark;

/// <summary>
/// BLite single-file vs multi-file layout comparison.
///
/// Layouts under test
/// ──────────────────
///   Default – 16 KB pages, single file  (standard embedded layout)
///   Server  – 16 KB pages, multi-file   (separate WAL + index + per-collection files)
///
/// Operations
/// ──────────
///   Layout_Insert_Single  – insert one document into a fresh empty database
///   Layout_Insert_Bulk    – insert 1 000 documents into a fresh empty database
///   Layout_Read_FindById  – primary-key lookup in a stable 1 000-document collection
///   Layout_Read_Scan      – full collection scan filtered by Status (~250 hits / 1 000)
///
/// State management
/// ────────────────
///   Read benchmarks  → stable contexts created once in GlobalSetup.
///   Write benchmarks → fresh empty DBs created per-iteration via targeted IterationSetup.
/// </summary>
[SimpleJob(launchCount: 2, warmupCount: 5, iterationCount: 10, id: "Layout")]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[HtmlExporter]
[JsonExporterAttribute.Full]
public class LayoutBenchmarks
{
    private const int    DocCount   = 1_000;
    private const int    BatchSize  = 1_000;
    private const string ScanStatus = "shipped"; // ~25 % of documents

    // ── Stable read contexts (GlobalSetup / GlobalCleanup) ────────────────

    private string        _rDefaultPath = null!;
    private string        _rServerPath  = null!;
    private TestDbContext _rDefaultCtx  = null!;
    private TestDbContext _rServerCtx   = null!;
    private string        _targetId     = null!;

    // ── Ephemeral write contexts (IterationSetup / IterationCleanup) ──────

    private string        _wDefaultPath = null!;
    private string        _wServerPath  = null!;
    private TestDbContext _wDefaultCtx  = null!;
    private TestDbContext _wServerCtx   = null!;

    private CustomerOrder[] _batchData   = Array.Empty<CustomerOrder>();
    private CustomerOrder   _singleOrder = null!;

    // ── Lifecycle ─────────────────────────────────────────────────────────

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        var temp = AppContext.BaseDirectory;
        var id   = Guid.NewGuid().ToString("N");

        _rDefaultPath = Path.Combine(temp, $"layout_rd_def_{id}.db");
        _rServerPath  = Path.Combine(temp, $"layout_rd_srv_{id}.db");

        var orders = Enumerable.Range(0, DocCount).Select(BenchmarkDataFactory.CreateOrder).ToArray();
        _targetId = orders[DocCount / 2].Id;

        _rDefaultCtx = new TestDbContext(_rDefaultPath);
        await _rDefaultCtx.CustomerOrders.InsertBulkAsync(orders);

        _rServerCtx = new TestDbContext(_rServerPath, PageFileConfig.Server(_rServerPath));
        await _rServerCtx.CustomerOrders.InsertBulkAsync(orders);

        _batchData   = Enumerable.Range(0, BatchSize).Select(BenchmarkDataFactory.CreateOrder).ToArray();
        _singleOrder = BenchmarkDataFactory.CreateOrder(999_999);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _rDefaultCtx?.Dispose();
        if (File.Exists(_rDefaultPath)) File.Delete(_rDefaultPath);
        _rServerCtx?.Dispose();
        InsertBenchmarks.DeleteServerDb(_rServerPath);
    }

    [IterationSetup(Targets = new[]
    {
        nameof(Default_Insert_Single), nameof(Default_Insert_Bulk),
        nameof(Server_Insert_Single),  nameof(Server_Insert_Bulk)
    })]
    public void WriteIterationSetup()
    {
        var temp = AppContext.BaseDirectory;
        _wDefaultPath = Path.Combine(temp, $"layout_wr_def_{Guid.NewGuid():N}.db");
        _wServerPath  = Path.Combine(temp, $"layout_wr_srv_{Guid.NewGuid():N}.db");
        _wDefaultCtx  = new TestDbContext(_wDefaultPath);
        _wServerCtx   = new TestDbContext(_wServerPath, PageFileConfig.Server(_wServerPath));
    }

    [IterationCleanup(Targets = new[]
    {
        nameof(Default_Insert_Single), nameof(Default_Insert_Bulk),
        nameof(Server_Insert_Single),  nameof(Server_Insert_Bulk)
    })]
    public void WriteIterationCleanup()
    {
        _wDefaultCtx?.Dispose(); _wDefaultCtx = null!;
        if (File.Exists(_wDefaultPath)) File.Delete(_wDefaultPath);
        _wServerCtx?.Dispose(); _wServerCtx = null!;
        // Force a full GC cycle so any FileStream finalizers inside StorageEngine
        // flush before we attempt to delete the collections directory on Windows.
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        InsertBenchmarks.DeleteServerDb(_wServerPath);
    }

    // ── BLite Default (single-file, 16 KB pages) ─────────────────────────

    [Benchmark(Description = "Default – Insert Single")]
    [BenchmarkCategory("Layout_Insert_Single")]
    public async Task Default_Insert_Single() => await _wDefaultCtx.CustomerOrders.InsertAsync(_singleOrder);

    [Benchmark(Description = "Default – Insert Bulk (1 000)")]
    [BenchmarkCategory("Layout_Insert_Bulk")]
    public async Task Default_Insert_Bulk() => await _wDefaultCtx.CustomerOrders.InsertBulkAsync(_batchData);

    [Benchmark(Description = "Default – FindById")]
    [BenchmarkCategory("Layout_Read_FindById")]
    public async Task<CustomerOrder?> Default_FindById() => await _rDefaultCtx.CustomerOrders.FindByIdAsync(_targetId);

    [Benchmark(Description = "Default – Scan by Status")]
    [BenchmarkCategory("Layout_Read_Scan")]
    public async Task<List<CustomerOrder>> Default_Scan()
        => await _rDefaultCtx.CustomerOrders.AsQueryable().Where(o => o.Status == ScanStatus).ToListAsync();

    // ── BLite Server (multi-file, 16 KB pages, separate WAL + index) ─────

    [Benchmark(Description = "Server – Insert Single")]
    [BenchmarkCategory("Layout_Insert_Single")]
    public async Task Server_Insert_Single() => await _wServerCtx.CustomerOrders.InsertAsync(_singleOrder);

    [Benchmark(Description = "Server – Insert Bulk (1 000)")]
    [BenchmarkCategory("Layout_Insert_Bulk")]
    public async Task Server_Insert_Bulk() => await _wServerCtx.CustomerOrders.InsertBulkAsync(_batchData);

    [Benchmark(Description = "Server – FindById")]
    [BenchmarkCategory("Layout_Read_FindById")]
    public async Task<CustomerOrder?> Server_FindById() => await _rServerCtx.CustomerOrders.FindByIdAsync(_targetId);

    [Benchmark(Description = "Server – Scan by Status")]
    [BenchmarkCategory("Layout_Read_Scan")]
    public async Task<List<CustomerOrder>> Server_Scan()
        => await _rServerCtx.CustomerOrders.AsQueryable().Where(o => o.Status == ScanStatus).ToListAsync();
}
