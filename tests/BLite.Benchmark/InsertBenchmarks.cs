using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BLite.Bson;
using BLite.Shared;
using BLite.Tests;
using Dapper;
using LiteDB;
using Microsoft.Data.Sqlite;
using System.IO;
using System.Text.Json;

namespace BLite.Benchmark;

[InProcess]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[HtmlExporter]
[JsonExporterAttribute.Full]
public class InsertBenchmarks
{
    private const int BatchSize = 1000;

    private string _docDbPath = "";
    private string _sqlitePath = "";
    private string _sqliteConnString = "";
    private string _litePath = "";

    private TestDbContext?  _ctx    = null;
    private LiteDatabase?  _liteDb = null;

    private CustomerOrder[]  _batchData   = Array.Empty<CustomerOrder>();
    private CustomerOrder?   _singleOrder = null;

    [GlobalSetup]
    public void Setup()
    {
        var temp = AppContext.BaseDirectory;
        var id   = Guid.NewGuid().ToString("N");
        _docDbPath         = Path.Combine(temp, $"bench_docdb_{id}.db");
        _sqlitePath        = Path.Combine(temp, $"bench_sqlite_{id}.db");
        _sqliteConnString  = $"Data Source={_sqlitePath}";
        _litePath          = Path.Combine(temp, $"bench_lite_{id}.db");

        _singleOrder = BenchmarkDataFactory.CreateOrder(0);
        _batchData   = Enumerable.Range(0, BatchSize)
                                 .Select(BenchmarkDataFactory.CreateOrder)
                                 .ToArray();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        if (File.Exists(_docDbPath)) File.Delete(_docDbPath);
        _ctx = new TestDbContext(_docDbPath);

        if (File.Exists(_sqlitePath)) File.Delete(_sqlitePath);
        using var conn = new SqliteConnection(_sqliteConnString);
        conn.Open();
        conn.Execute("CREATE TABLE Orders (Id TEXT PRIMARY KEY, Data TEXT NOT NULL)");

        if (File.Exists(_litePath)) File.Delete(_litePath);
        _liteDb = new LiteDatabase($"Filename={_litePath};Connection=direct");
    }

    [IterationCleanup]
    public void Cleanup()
    {
        try
        {
            _ctx?.Dispose();
            _liteDb?.Dispose();
            SqliteConnection.ClearAllPools();
            System.Threading.Thread.Sleep(50);
            if (File.Exists(_docDbPath)) File.Delete(_docDbPath);
            if (File.Exists(_sqlitePath)) File.Delete(_sqlitePath);
            if (File.Exists(_litePath))   File.Delete(_litePath);
        }
        catch (Exception ex) { Console.WriteLine($"Cleanup warning: {ex.Message}"); }
    }

    // ──── Single Insert ──────────────────────────────────────────────

    [Benchmark(Baseline = true, Description = "BLite – Single Insert")]
    [BenchmarkCategory("Insert_Single")]
    public void BLite_Insert_Single()  => _ctx!.CustomerOrders.Insert(_singleOrder!);

    [Benchmark(Description = "LiteDB – Single Insert")]
    [BenchmarkCategory("Insert_Single")]
    public void LiteDb_Insert_Single() => _liteDb!.GetCollection<CustomerOrder>("orders").Insert(_singleOrder!);

    [Benchmark(Description = "SQLite+JSON – Single Insert")]
    [BenchmarkCategory("Insert_Single")]
    public void Sqlite_Insert_Single()
    {
        using var conn = new SqliteConnection(_sqliteConnString);
        conn.Open();
        conn.Execute("INSERT INTO Orders (Id, Data) VALUES (@Id, @Data)",
            new { _singleOrder!.Id, Data = System.Text.Json.JsonSerializer.Serialize(_singleOrder) });
    }

    // ──── Batch Insert (1000) ──────────────────────────────────────

    [Benchmark(Baseline = true, Description = "BLite – Batch Insert (1000)")]
    [BenchmarkCategory("Insert_Batch")]
    public void BLite_Insert_Batch()  => _ctx!.CustomerOrders.InsertBulk(_batchData);

    [Benchmark(Description = "LiteDB – Batch Insert (1000)")]
    [BenchmarkCategory("Insert_Batch")]
    public void LiteDb_Insert_Batch() => _liteDb!.GetCollection<CustomerOrder>("orders").InsertBulk(_batchData);

    [Benchmark(Description = "SQLite+JSON – Batch Insert (1000, 1 Txn)")]
    [BenchmarkCategory("Insert_Batch")]
    public void Sqlite_Insert_Batch()
    {
        using var conn = new SqliteConnection(_sqliteConnString);
        conn.Open();
        using var txn = conn.BeginTransaction();
        foreach (var o in _batchData)
            conn.Execute("INSERT INTO Orders (Id, Data) VALUES (@Id, @Data)",
                new { o.Id, Data = System.Text.Json.JsonSerializer.Serialize(o) }, transaction: txn);
        txn.Commit();
    }
}
