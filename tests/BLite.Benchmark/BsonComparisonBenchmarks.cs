using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;
using BLite.Shared;
using BLite.Tests;
using System.IO;
using System.Threading;

namespace BLite.Benchmark;

/// <summary>
/// Focused benchmark comparing BLite typed (DocumentCollection via Source Generator)
/// against BLite BSON dynamic (DynamicCollection with BsonDocument).
///
/// Run with:
///   dotnet run -c Release -- --filter *BsonComparison*
///
/// Use this benchmark to measure the baseline and the impact of each optimization step
/// without the overhead of setting up LiteDB / SQLite / CouchbaseLite / DuckDB.
/// </summary>
[InProcess]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[HtmlExporter]
[JsonExporterAttribute.Full]
public class BsonComparisonBenchmarks
{
    private const int BatchSize = 1000;

    private string _typedPath    = "";
    private string _bsonPath     = "";

    private TestDbContext?         _ctx        = null;
    private BLiteEngine?           _bsonEngine = null;
    private DynamicCollection?     _bsonCol    = null;

    private CustomerOrder?               _singleOrder    = null;
    private CustomerOrder[]              _batchOrders    = Array.Empty<CustomerOrder>();
    private BLite.Bson.BsonDocument?     _singleBsonDoc  = null;
    private BLite.Bson.BsonDocument[]    _batchBsonDocs  = Array.Empty<BLite.Bson.BsonDocument>();

    [GlobalSetup]
    public void GlobalSetup()
    {
        _singleOrder  = BenchmarkDataFactory.CreateOrder(0);
        _batchOrders  = Enumerable.Range(0, BatchSize)
                                  .Select(BenchmarkDataFactory.CreateOrder)
                                  .ToArray();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        var temp = AppContext.BaseDirectory;
        var id   = Guid.NewGuid().ToString("N");

        // ── Typed (DocumentCollection) ──────────────────────────────────────
        _typedPath = Path.Combine(temp, $"bench_cmp_typed_{id}.db");
        if (File.Exists(_typedPath)) File.Delete(_typedPath);
        _ctx = new TestDbContext(_typedPath);

        // ── BSON Dynamic (DynamicCollection) ────────────────────────────────
        _bsonPath = Path.Combine(temp, $"bench_cmp_bson_{id}.db");
        if (File.Exists(_bsonPath)) File.Delete(_bsonPath);
        _bsonEngine = new BLiteEngine(_bsonPath);
        _bsonCol    = _bsonEngine.GetOrCreateCollection("orders");

        // Pre-build BsonDocuments (doc construction cost excluded from benchmark)
        _singleBsonDoc = BenchmarkDataFactory.CreateBsonDocument(0, _bsonEngine);
        _batchBsonDocs = Enumerable.Range(0, BatchSize)
                                   .Select(i => BenchmarkDataFactory.CreateBsonDocument(i, _bsonEngine))
                                   .ToArray();
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        try
        {
            _ctx?.Dispose();
            _bsonEngine?.Dispose();
            _bsonEngine = null;
            _bsonCol    = null;

            Thread.Sleep(50);

            if (File.Exists(_typedPath)) File.Delete(_typedPath);
            if (File.Exists(_bsonPath))  File.Delete(_bsonPath);
        }
        catch (Exception ex) { Console.WriteLine($"Cleanup warning: {ex.Message}"); }
    }

    // ──── Single Insert ──────────────────────────────────────────────────────

    [Benchmark(Baseline = true, Description = "BLite Typed – Single Insert")]
    [BenchmarkCategory("Insert_Single")]
    public void Typed_Insert_Single() => _ctx!.CustomerOrders.Insert(_singleOrder!);

    [Benchmark(Description = "BLite BSON – Single Insert")]
    [BenchmarkCategory("Insert_Single")]
    public void Bson_Insert_Single() => _bsonCol!.Insert(_singleBsonDoc!);

    // ──── Batch Insert (1 000) ───────────────────────────────────────────────

    [Benchmark(Baseline = true, Description = "BLite Typed – Batch Insert (1000)")]
    [BenchmarkCategory("Insert_Batch")]
    public void Typed_Insert_Batch() => _ctx!.CustomerOrders.InsertBulk(_batchOrders);

    [Benchmark(Description = "BLite BSON – Batch Insert (1000)")]
    [BenchmarkCategory("Insert_Batch")]
    public void Bson_Insert_Batch() => _bsonCol!.InsertBulk(_batchBsonDocs);

    // ──── FindById ───────────────────────────────────────────────────────────

    [Benchmark(Baseline = true, Description = "BLite Typed – FindById")]
    [BenchmarkCategory("FindById")]
    public CustomerOrder? Typed_FindById()
    {
        var id = _ctx!.CustomerOrders.Insert(_singleOrder!);
        return _ctx!.CustomerOrders.FindById(id);
    }

    [Benchmark(Description = "BLite BSON – FindById")]
    [BenchmarkCategory("FindById")]
    public BLite.Bson.BsonDocument? Bson_FindById()
    {
        var id = _bsonCol!.Insert(_singleBsonDoc!);
        return _bsonCol!.FindById(id);
    }
}
