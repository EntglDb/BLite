using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BLite.Shared;
using BLite.Tests;
using Dapper;
using LiteDB;
using Microsoft.Data.Sqlite;
using System.IO;
using System.Text.Json;

namespace BLite.Benchmark;

[SimpleJob]
[InProcess]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[HtmlExporter]
[JsonExporterAttribute.Full]
public class ReadBenchmarks
{
    private const int DocCount   = 1000;
    private const string ScanStatus = "shipped"; // ~250 hits out of 1000

    private string _docDbPath = null!;
    private string _sqlitePath = null!;
    private string _sqliteConnString = null!;
    private string _litePath = null!;

    private TestDbContext _ctx    = null!;
    private LiteDatabase  _liteDb = null!;

    private string _targetId = null!;

    [GlobalSetup]
    public void Setup()
    {
        var temp = AppContext.BaseDirectory;
        var id   = Guid.NewGuid().ToString("N");
        _docDbPath        = Path.Combine(temp, $"bench_read_docdb_{id}.db");
        _sqlitePath       = Path.Combine(temp, $"bench_read_sqlite_{id}.db");
        _sqliteConnString = $"Data Source={_sqlitePath}";
        _litePath         = Path.Combine(temp, $"bench_read_lite_{id}.db");

        foreach (var p in new[] { _docDbPath, _sqlitePath, _litePath })
            if (File.Exists(p)) File.Delete(p);

        var orders = Enumerable.Range(0, DocCount)
                               .Select(BenchmarkDataFactory.CreateOrder)
                               .ToArray();

        // 1. BLite
        _ctx = new TestDbContext(_docDbPath);
        _ctx.CustomerOrders.InsertBulk(orders);

        // 2. SQLite + JSON
        using var conn = new SqliteConnection(_sqliteConnString);
        conn.Open();
        conn.Execute("CREATE TABLE Orders (Id TEXT PRIMARY KEY, Data TEXT NOT NULL)");
        using var txn = conn.BeginTransaction();
        foreach (var o in orders)
            conn.Execute("INSERT INTO Orders (Id, Data) VALUES (@Id, @Data)",
                new { o.Id, Data = System.Text.Json.JsonSerializer.Serialize(o) }, transaction: txn);
        txn.Commit();

        // 3. LiteDB
        _liteDb = new LiteDatabase($"Filename={_litePath};Connection=direct");
        var col = _liteDb.GetCollection<CustomerOrder>("orders");
        col.InsertBulk(orders);

        // Middle document as lookup target
        _targetId = orders[DocCount / 2].Id;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _ctx?.Dispose();
        _liteDb?.Dispose();
        SqliteConnection.ClearAllPools();
        foreach (var p in new[] { _docDbPath, _sqlitePath, _litePath })
            if (File.Exists(p)) File.Delete(p);
    }

    // ──── FindById (primary key lookup) ──────────────────────────────

    [Benchmark(Baseline = true, Description = "BLite – FindById")]
    [BenchmarkCategory("FindById")]
    public CustomerOrder? BLite_FindById() => _ctx.CustomerOrders.FindById(_targetId);

    [Benchmark(Description = "LiteDB – FindById")]
    [BenchmarkCategory("FindById")]
    public CustomerOrder? LiteDb_FindById() => _liteDb.GetCollection<CustomerOrder>("orders").FindById(_targetId);

    [Benchmark(Description = "SQLite+JSON – FindById")]
    [BenchmarkCategory("FindById")]
    public CustomerOrder Sqlite_FindById()
    {
        using var conn = new SqliteConnection(_sqliteConnString);
        conn.Open();
        var json = conn.QueryFirstOrDefault<string>(
            "SELECT Data FROM Orders WHERE Id = @Id", new { Id = _targetId });
        return json is null ? throw new InvalidOperationException()
                           : System.Text.Json.JsonSerializer.Deserialize<CustomerOrder>(json)!;
    }

    // ──── Scan (linear scan with status filter, ~250 results out of 1000) ───

    [Benchmark(Baseline = true, Description = "BLite – Scan by Status")]
    [BenchmarkCategory("Scan")]
    public List<CustomerOrder> BLite_Scan()
        => _ctx.CustomerOrders.Find(x => x.Status == ScanStatus).ToList();

    [Benchmark(Description = "LiteDB – Scan by Status")]
    [BenchmarkCategory("Scan")]
    public List<CustomerOrder> LiteDb_Scan()
        => _liteDb.GetCollection<CustomerOrder>("orders")
                  .Find(x => x.Status == ScanStatus).ToList();

    [Benchmark(Description = "SQLite+JSON – Scan by Status (full deserialize)")]
    [BenchmarkCategory("Scan")]
    public List<CustomerOrder> Sqlite_Scan()
    {
        using var conn = new SqliteConnection(_sqliteConnString);
        conn.Open();
        return conn.Query<string>("SELECT Data FROM Orders")
                   .Select(json => System.Text.Json.JsonSerializer.Deserialize<CustomerOrder>(json)!)
                   .Where(o => o.Status == ScanStatus)
                   .ToList();
    }
}
