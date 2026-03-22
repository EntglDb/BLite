using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BLite.Core.Storage;
using BLite.Shared;
using BLite.Tests;
using Couchbase.Lite;
using Dapper;
using DuckDB.NET.Data;
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

    private string _docDbPath      = null!;
    private string _docDbServerPath = null!;
    private string _sqlitePath      = null!;
    private string _sqliteConnString = null!;
    private string _litePath = null!;
    private string _cblDir = null!;
    private string _duckPath = null!;

    private TestDbContext _ctx       = null!;
    private TestDbContext _serverCtx = null!;
    private LiteDatabase  _liteDb    = null!;
    private Database      _cblDb  = null!;
    private Couchbase.Lite.Collection _cblCol = null!;

    private string _targetId = null!;

    [GlobalSetup]
    public void Setup()
    {
        var temp = AppContext.BaseDirectory;
        var id   = Guid.NewGuid().ToString("N");
        _docDbPath        = Path.Combine(temp, $"bench_read_docdb_{id}.db");
        _docDbServerPath  = Path.Combine(temp, $"bench_read_docdb_server_{id}.db");
        _sqlitePath       = Path.Combine(temp, $"bench_read_sqlite_{id}.db");
        _sqliteConnString = $"Data Source={_sqlitePath}";
        _litePath         = Path.Combine(temp, $"bench_read_lite_{id}.db");
        _cblDir           = Path.Combine(temp, $"bench_read_cbl_{id}");
        _duckPath         = Path.Combine(temp, $"bench_read_duck_{id}.db");

        Couchbase.Lite.Logging.LogSinks.Console = null;

        foreach (var p in new[] { _docDbPath, _sqlitePath, _litePath, _duckPath })
            if (File.Exists(p)) File.Delete(p);
        if (Directory.Exists(_cblDir)) Directory.Delete(_cblDir, true);

        var orders = Enumerable.Range(0, DocCount)
                               .Select(BenchmarkDataFactory.CreateOrder)
                               .ToArray();

        // 1. BLite
        _ctx = new TestDbContext(_docDbPath);
        _ctx.CustomerOrders.InsertBulk(orders);

        // 2. BLite Server (multi-file)
        InsertBenchmarks.DeleteServerDb(_docDbServerPath);
        _serverCtx = new TestDbContext(_docDbServerPath, PageFileConfig.Server(_docDbServerPath));
        _serverCtx.CustomerOrders.InsertBulk(orders);

        // 3. SQLite + JSON
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

        // 4. CouchbaseLite
        Directory.CreateDirectory(_cblDir);
        _cblDb  = new Database("bench", new DatabaseConfiguration { Directory = _cblDir });
        _cblCol = _cblDb.GetDefaultCollection();
        _cblDb.InBatch(() =>
        {
            foreach (var o in orders)
            {
                var doc = new MutableDocument(o.Id);
                doc.SetJSON(System.Text.Json.JsonSerializer.Serialize(o));
                _cblCol.Save(doc);
            }
        });

        // 5. DuckDB
        using var duck = new DuckDBConnection($"Data Source={_duckPath}");
        duck.Open();
        duck.Execute("CREATE TABLE Orders (Id VARCHAR PRIMARY KEY, Data VARCHAR NOT NULL)");
        using var duckTxn = duck.BeginTransaction();
        foreach (var o in orders)
            duck.Execute("INSERT INTO Orders (Id, Data) VALUES ($Id, $Data)",
                new { o.Id, Data = System.Text.Json.JsonSerializer.Serialize(o) }, transaction: duckTxn);
        duckTxn.Commit();

        // Middle document as lookup target
        _targetId = orders[DocCount / 2].Id;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _ctx?.Dispose();
        _liteDb?.Dispose();
        _serverCtx?.Dispose();
        InsertBenchmarks.DeleteServerDb(_docDbServerPath);
        _cblCol = null!;
        _cblDb?.Dispose();
        SqliteConnection.ClearAllPools();
        System.Threading.Thread.Sleep(200);
        foreach (var p in new[] { _docDbPath, _sqlitePath, _litePath, _duckPath })
            if (File.Exists(p)) File.Delete(p);
        for (int i = 0; i < 5 && Directory.Exists(_cblDir); i++)
        {
            try   { Directory.Delete(_cblDir, true); }
            catch { System.Threading.Thread.Sleep(100 * (i + 1)); }
        }
    }

    // ──── FindById (primary key lookup) ──────────────────────────────

    [Benchmark(Baseline = true, Description = "BLite – FindById")]
    [BenchmarkCategory("FindById")]
    public CustomerOrder? BLite_FindById() => _ctx.CustomerOrders.FindById(_targetId);

    [Benchmark(Description = "BLite Server – FindById")]
    [BenchmarkCategory("FindById")]
    public CustomerOrder? BLiteServer_FindById() => _serverCtx.CustomerOrders.FindById(_targetId);

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

    [Benchmark(Description = "BLite Server – Scan by Status")]
    [BenchmarkCategory("Scan")]
    public List<CustomerOrder> BLiteServer_Scan()
        => _serverCtx.CustomerOrders.Find(x => x.Status == ScanStatus).ToList();

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

    // ──── CouchbaseLite ─────────────────────────────────────────────

    [Benchmark(Description = "CouchbaseLite – FindById")]
    [BenchmarkCategory("FindById")]
    public CustomerOrder? CBL_FindById()
    {
        var doc = _cblCol.GetDocument(_targetId);
        return doc is null ? null : System.Text.Json.JsonSerializer.Deserialize<CustomerOrder>(doc.ToJSON()!);
    }

    [Benchmark(Description = "CouchbaseLite – Scan by Status")]
    [BenchmarkCategory("Scan")]
    public List<CustomerOrder> CBL_Scan()
    {
        using var query  = _cblDb.CreateQuery(
            $"SELECT * FROM _ WHERE Status = '{ScanStatus}'");
        using var result = query.Execute();
        return result.AllResults()
                     .Select(r => System.Text.Json.JsonSerializer.Deserialize<CustomerOrder>(
                                      r.GetDictionary(0)!.ToJSON()!)!)
                     .ToList();
    }

    // ──── DuckDB ─────────────────────────────────────────────────────

    [Benchmark(Description = "DuckDB – FindById")]
    [BenchmarkCategory("FindById")]
    public CustomerOrder DuckDB_FindById()
    {
        using var conn = new DuckDBConnection($"Data Source={_duckPath}");
        conn.Open();
        var json = conn.QueryFirstOrDefault<string>(
            "SELECT Data FROM Orders WHERE Id = $Id", new { Id = _targetId });
        return json is null ? throw new InvalidOperationException()
                           : System.Text.Json.JsonSerializer.Deserialize<CustomerOrder>(json)!;
    }

    [Benchmark(Description = "DuckDB – Scan by Status")]
    [BenchmarkCategory("Scan")]
    public List<CustomerOrder> DuckDB_Scan()
    {
        using var conn = new DuckDBConnection($"Data Source={_duckPath}");
        conn.Open();
        // json_extract_string uses DuckDB's native JSON functions for the filter
        return conn.Query<string>(
                "SELECT Data FROM Orders WHERE json_extract_string(Data, '$.Status') = $Status",
                new { Status = ScanStatus })
                   .Select(json => System.Text.Json.JsonSerializer.Deserialize<CustomerOrder>(json)!)
                   .ToList();
    }
}
