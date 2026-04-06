using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BLite.Core.Query;
using BLite.Shared;
using BLite.Tests;
using Dapper;
using DuckDB.NET.Data;
using LiteDB;
using Microsoft.Data.Sqlite;
using System.IO;
using STJ = System.Text.Json;

namespace BLite.Benchmark;

/// <summary>
/// Benchmarks for <c>IQueryable.CountAsync()</c> and <c>IQueryable.CountAsync(predicate)</c>.
/// Validates the engine-native key-only scan path introduced to replace full document
/// materialisation, and measures three distinct code paths:
/// <list type="number">
///   <item><b>CountAll</b>       — no predicate; primary B-Tree key scan, zero data-page reads.</item>
///   <item><b>CountIndexed</b>   — predicate on an indexed field (<c>Total &gt; threshold</c>);
///                                  key-only secondary-index leaf scan, zero data-page reads.</item>
///   <item><b>CountNonIndexed</b>— predicate on a non-indexed field (<c>Currency == "EUR"</c>);
///                                  streaming BSON scan, no <c>List&lt;T&gt;</c> accumulation.</item>
/// </list>
/// All three BLite paths are compared with SQLite, LiteDB, and DuckDB equivalents over
/// 10 000 <see cref="CustomerOrder"/> documents.
/// </summary>
[SimpleJob(launchCount: 2, warmupCount: 5, iterationCount: 10, id: "Count")]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[HtmlExporter]
[JsonExporterAttribute.Full]
public class CountBenchmarks
{
    private const int DocCount        = 10_000;
    // Total = (100 + i*1.5) * 1.22 → top ~25 % means Total > ~4 000
    private const decimal RangeThreshold = 4_000m;

    private string _docDbPath     = null!;
    private string _sqlitePath    = null!;
    private string _sqliteConnStr = null!;
    private string _litePath      = null!;
    private string _duckPath      = null!;

    private TestDbContext _ctx    = null!;
    private LiteDatabase  _liteDb = null!;

    // ── setup / cleanup ──────────────────────────────────────────────

    [GlobalSetup]
    public async Task Setup()
    {
        var temp = AppContext.BaseDirectory;
        var id   = Guid.NewGuid().ToString("N");
        _docDbPath     = Path.Combine(temp, $"count_docdb_{id}.db");
        _sqlitePath    = Path.Combine(temp, $"count_sqlite_{id}.db");
        _sqliteConnStr = $"Data Source={_sqlitePath}";
        _litePath      = Path.Combine(temp, $"count_lite_{id}.db");
        _duckPath      = Path.Combine(temp, $"count_duck_{id}.db");

        foreach (var p in new[] { _docDbPath, _sqlitePath, _litePath, _duckPath })
            if (File.Exists(p)) File.Delete(p);

        var orders = Enumerable.Range(0, DocCount)
                               .Select(BenchmarkDataFactory.CreateOrder)
                               .ToArray();

        // 1. BLite — with secondary indexes on Total and Status so the
        //    CountIndexed path can use the key-only index leaf scan.
        _ctx = new TestDbContext(_docDbPath);
        await _ctx.CustomerOrders.InsertBulkAsync(orders);
        await _ctx.CustomerOrders.EnsureIndexAsync(o => o.Total);
        await _ctx.CustomerOrders.EnsureIndexAsync(o => o.Status);

        // 2. SQLite + JSON (also store Total and Currency as native columns)
        using var conn = new SqliteConnection(_sqliteConnStr);
        conn.Open();
        conn.Execute("""
            CREATE TABLE Orders (
                Id       TEXT    PRIMARY KEY,
                Total    REAL    NOT NULL,
                Currency TEXT    NOT NULL,
                Data     TEXT    NOT NULL
            )
            """);
        using var txn = conn.BeginTransaction();
        foreach (var o in orders)
            conn.Execute(
                "INSERT INTO Orders (Id, Total, Currency, Data) VALUES (@Id, @Total, @Currency, @Data)",
                new { o.Id, Total = (double)o.Total, o.Currency,
                      Data = STJ.JsonSerializer.Serialize(o) },
                transaction: txn);
        txn.Commit();
        conn.Execute("CREATE INDEX idx_total    ON Orders(Total)");
        conn.Execute("CREATE INDEX idx_currency ON Orders(Currency)");

        // 3. LiteDB
        _liteDb = new LiteDatabase($"Filename={_litePath};Connection=direct");
        var col = _liteDb.GetCollection<CustomerOrder>("orders");
        col.EnsureIndex(x => x.Total);
        col.InsertBulk(orders);

        // 4. DuckDB
        using var duck = new DuckDBConnection($"Data Source={_duckPath}");
        duck.Open();
        duck.Execute("""
            CREATE TABLE Orders (
                Id       VARCHAR PRIMARY KEY,
                Total    DOUBLE  NOT NULL,
                Currency VARCHAR NOT NULL,
                Data     VARCHAR NOT NULL
            )
            """);
        using var duckTxn = duck.BeginTransaction();
        foreach (var o in orders)
            duck.Execute(
                "INSERT INTO Orders (Id, Total, Currency, Data) VALUES ($Id, $Total, $Currency, $Data)",
                new { o.Id, Total = (double)o.Total, o.Currency,
                      Data = STJ.JsonSerializer.Serialize(o) },
                transaction: duckTxn);
        duckTxn.Commit();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _ctx?.Dispose();
        _liteDb?.Dispose();
        SqliteConnection.ClearAllPools();
        System.Threading.Thread.Sleep(200);
        foreach (var p in new[] { _docDbPath, _sqlitePath, _litePath, _duckPath })
            if (File.Exists(p)) File.Delete(p);
    }

    // ════════════════════════════════════════════════════════════════
    // 1. COUNT ALL — no predicate
    //    BLite: primary B-Tree key scan (zero data-page reads)
    // ════════════════════════════════════════════════════════════════

    [Benchmark(Baseline = true, Description = "BLite – CountAsync()")]
    [BenchmarkCategory("CountAll")]
    public async Task<int> BLite_CountAll()
        => await _ctx.CustomerOrders.AsQueryable().CountAsync();

    [Benchmark(Description = "LiteDB – Count()")]
    [BenchmarkCategory("CountAll")]
    public int LiteDb_CountAll()
        => _liteDb.GetCollection<CustomerOrder>("orders").Count();

    [Benchmark(Description = "SQLite – COUNT(*)")]
    [BenchmarkCategory("CountAll")]
    public int Sqlite_CountAll()
    {
        using var conn = new SqliteConnection(_sqliteConnStr);
        conn.Open();
        return conn.QueryFirst<int>("SELECT COUNT(*) FROM Orders");
    }

    [Benchmark(Description = "DuckDB – COUNT(*)")]
    [BenchmarkCategory("CountAll")]
    public int DuckDb_CountAll()
    {
        using var conn = new DuckDBConnection($"Data Source={_duckPath}");
        conn.Open();
        return conn.QueryFirst<int>("SELECT COUNT(*) FROM Orders");
    }

    // ════════════════════════════════════════════════════════════════
    // 2. COUNT INDEXED — predicate on an indexed field
    //    BLite: secondary-index key-only leaf scan, zero data-page reads
    // ════════════════════════════════════════════════════════════════

    [Benchmark(Baseline = true, Description = "BLite – CountAsync(o => o.Total > threshold)")]
    [BenchmarkCategory("CountIndexed")]
    public async Task<int> BLite_CountIndexed()
        => await _ctx.CustomerOrders.AsQueryable()
               .CountAsync(o => o.Total > RangeThreshold);

    [Benchmark(Description = "LiteDB – Count(predicate on Total)")]
    [BenchmarkCategory("CountIndexed")]
    public int LiteDb_CountIndexed()
        => _liteDb.GetCollection<CustomerOrder>("orders")
                  .Count(o => o.Total > RangeThreshold);

    [Benchmark(Description = "SQLite – COUNT(*) WHERE Total > threshold")]
    [BenchmarkCategory("CountIndexed")]
    public int Sqlite_CountIndexed()
    {
        using var conn = new SqliteConnection(_sqliteConnStr);
        conn.Open();
        return conn.QueryFirst<int>(
            "SELECT COUNT(*) FROM Orders WHERE Total > @t",
            new { t = (double)RangeThreshold });
    }

    [Benchmark(Description = "DuckDB – COUNT(*) WHERE Total > threshold")]
    [BenchmarkCategory("CountIndexed")]
    public int DuckDb_CountIndexed()
    {
        using var conn = new DuckDBConnection($"Data Source={_duckPath}");
        conn.Open();
        return conn.QueryFirst<int>(
            "SELECT COUNT(*) FROM Orders WHERE Total > $t",
            new { t = (double)RangeThreshold });
    }

    // ════════════════════════════════════════════════════════════════
    // 3. COUNT NON-INDEXED — predicate on a non-indexed field
    //    BLite: streaming BSON scan, no List<T> accumulation
    //    (Currency == "EUR" matches all 10 000 records — worst-case scan)
    // ════════════════════════════════════════════════════════════════

    [Benchmark(Baseline = true, Description = "BLite – CountAsync(o => o.Currency == \"EUR\")")]
    [BenchmarkCategory("CountNonIndexed")]
    public async Task<int> BLite_CountNonIndexed()
        => await _ctx.CustomerOrders.AsQueryable()
               .CountAsync(o => o.Currency == "EUR");

    [Benchmark(Description = "LiteDB – Count(predicate on Currency)")]
    [BenchmarkCategory("CountNonIndexed")]
    public int LiteDb_CountNonIndexed()
        => _liteDb.GetCollection<CustomerOrder>("orders")
                  .Count(o => o.Currency == "EUR");

    [Benchmark(Description = "SQLite – COUNT(*) WHERE Currency = 'EUR'")]
    [BenchmarkCategory("CountNonIndexed")]
    public int Sqlite_CountNonIndexed()
    {
        using var conn = new SqliteConnection(_sqliteConnStr);
        conn.Open();
        return conn.QueryFirst<int>(
            "SELECT COUNT(*) FROM Orders WHERE Currency = @c",
            new { c = "EUR" });
    }

    [Benchmark(Description = "DuckDB – COUNT(*) WHERE Currency = 'EUR'")]
    [BenchmarkCategory("CountNonIndexed")]
    public int DuckDb_CountNonIndexed()
    {
        using var conn = new DuckDBConnection($"Data Source={_duckPath}");
        conn.Open();
        return conn.QueryFirst<int>(
            "SELECT COUNT(*) FROM Orders WHERE Currency = $c",
            new { c = "EUR" });
    }
}
