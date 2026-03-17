using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BLite.Shared;
using BLite.Tests;
using Couchbase.Lite;
using Couchbase.Lite.Query;
using Dapper;
using DuckDB.NET.Data;
using LiteDB;
using Microsoft.Data.Sqlite;
using System.IO;
using STJ = System.Text.Json;

namespace BLite.Benchmark;

/// <summary>
/// OLAP benchmark: analytical queries over 10 000 CustomerOrder documents.
/// Operations:
///   1. Aggregate   — SUM(Total), AVG(Total), COUNT(*)
///   2. GroupBy     — COUNT + SUM(Total) per Status (4 groups)
///   3. RangeFilter — all orders with Total > threshold (~top 25 %)
///   4. TopN        — top 10 orders by Total DESC
/// Goal: compare BLite (OLTP) vs OLAP-oriented engines on analytical workloads.
/// </summary>
[SimpleJob]
[InProcess]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[HtmlExporter]
[JsonExporterAttribute.Full]
public class OlapBenchmarks
{
    private const int DocCount       = 10_000;
    // Total = (100 + i*1.5) * 1.22  →  top ~25% means Total > ~4 000
    private const decimal RangeThreshold = 4_000m;
    private const int TopN = 10;

    private string _docDbPath      = null!;
    private string _sqlitePath     = null!;
    private string _sqliteConnStr  = null!;
    private string _litePath       = null!;
    private string _cblDir         = null!;
    private string _duckPath       = null!;

    private TestDbContext             _ctx    = null!;
    private LiteDatabase              _liteDb = null!;
    private Database                  _cblDb  = null!;
    private Couchbase.Lite.Collection _cblCol = null!;

    // ── helpers ──────────────────────────────────────────────────────

    public record AggResult(decimal Sum, decimal Avg, int Count);
    public record GroupRow(string Status, int Count, decimal SumTotal);

    // ── setup / cleanup ──────────────────────────────────────────────

    [GlobalSetup]
    public void Setup()
    {
        var temp = AppContext.BaseDirectory;
        var id   = Guid.NewGuid().ToString("N");
        _docDbPath     = Path.Combine(temp, $"olap_docdb_{id}.db");
        _sqlitePath    = Path.Combine(temp, $"olap_sqlite_{id}.db");
        _sqliteConnStr = $"Data Source={_sqlitePath}";
        _litePath      = Path.Combine(temp, $"olap_lite_{id}.db");
        _cblDir        = Path.Combine(temp, $"olap_cbl_{id}");
        _duckPath      = Path.Combine(temp, $"olap_duck_{id}.db");

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
        // Create secondary indexes so optimized LINQ paths can exploit them
        _ctx.CustomerOrders.EnsureIndex(o => o.Total);
        _ctx.CustomerOrders.EnsureIndex(o => o.Status);

        // 2. SQLite + JSON  (also store Total as real column for OLAP queries)
        using var conn = new SqliteConnection(_sqliteConnStr);
        conn.Open();
        conn.Execute("""
            CREATE TABLE Orders (
                Id     TEXT    PRIMARY KEY,
                Total  REAL    NOT NULL,
                Status TEXT    NOT NULL,
                Data   TEXT    NOT NULL
            )
            """);
        using var txn = conn.BeginTransaction();
        foreach (var o in orders)
            conn.Execute(
                "INSERT INTO Orders (Id, Total, Status, Data) VALUES (@Id, @Total, @Status, @Data)",
                new { o.Id, Total = (double)o.Total, Status = o.Status,
                      Data = STJ.JsonSerializer.Serialize(o) },
                transaction: txn);
        txn.Commit();
        conn.Execute("CREATE INDEX idx_total  ON Orders(Total)");
        conn.Execute("CREATE INDEX idx_status ON Orders(Status)");

        // 3. LiteDB
        _liteDb = new LiteDatabase($"Filename={_litePath};Connection=direct");
        var col = _liteDb.GetCollection<CustomerOrder>("orders");
        col.EnsureIndex(x => x.Total);
        col.EnsureIndex(x => x.Status);
        col.InsertBulk(orders);

        // 4. CouchbaseLite
        Directory.CreateDirectory(_cblDir);
        _cblDb  = new Database("olap", new DatabaseConfiguration { Directory = _cblDir });
        _cblCol = _cblDb.GetDefaultCollection();
        _cblDb.InBatch(() =>
        {
            foreach (var o in orders)
            {
                var doc = new MutableDocument(o.Id);
                doc.SetJSON(STJ.JsonSerializer.Serialize(o));
                _cblCol.Save(doc);
            }
        });
        _cblCol.CreateIndex("idx_total",
            IndexBuilder.ValueIndex(ValueIndexItem.Expression(Expression.Property("Total"))));
        _cblCol.CreateIndex("idx_status",
            IndexBuilder.ValueIndex(ValueIndexItem.Expression(Expression.Property("Status"))));

        // 5. DuckDB
        using var duck = new DuckDBConnection($"Data Source={_duckPath}");
        duck.Open();
        duck.Execute("""
            CREATE TABLE Orders (
                Id     VARCHAR PRIMARY KEY,
                Total  DOUBLE  NOT NULL,
                Status VARCHAR NOT NULL,
                Data   VARCHAR NOT NULL
            )
            """);
        using var duckTxn = duck.BeginTransaction();
        foreach (var o in orders)
            duck.Execute(
                "INSERT INTO Orders (Id, Total, Status, Data) VALUES ($Id, $Total, $Status, $Data)",
                new { o.Id, Total = (double)o.Total, Status = o.Status,
                      Data = STJ.JsonSerializer.Serialize(o) },
                transaction: duckTxn);
        duckTxn.Commit();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _ctx?.Dispose();
        _liteDb?.Dispose();
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

    // ════════════════════════════════════════════════════════════════
    // 1. AGGREGATE  — SUM, AVG, COUNT
    // ════════════════════════════════════════════════════════════════

    [Benchmark(Baseline = true, Description = "BLite – Aggregate SUM/AVG/COUNT")]
    [BenchmarkCategory("Aggregate")]
    public AggResult BLite_Aggregate()
    {
        // Sum and Average use the BSON field-projection scan fast path:
        // only the Total bytes are read from each page — T is never instantiated.
        var q = _ctx.CustomerOrders.AsQueryable();
        decimal sum   = q.Sum(o => o.Total);
        decimal avg   = q.Average(o => o.Total);
        int     count = _ctx.CustomerOrders.Count();   // primary BTree key scan
        return new(sum, avg, count);
    }

    [Benchmark(Description = "LiteDB – Aggregate SUM/AVG/COUNT")]
    [BenchmarkCategory("Aggregate")]
    public AggResult LiteDb_Aggregate()
    {
        var col = _liteDb.GetCollection<CustomerOrder>("orders");
        var all = col.FindAll().ToList();
        return new(all.Sum(o => o.Total), all.Average(o => o.Total), all.Count);
    }

    [Benchmark(Description = "SQLite+JSON – Aggregate SUM/AVG/COUNT")]
    [BenchmarkCategory("Aggregate")]
    public AggResult Sqlite_Aggregate()
    {
        using var conn = new SqliteConnection(_sqliteConnStr);
        conn.Open();
        var row = conn.QueryFirst<(double Sum, double Avg, int Count)>(
            "SELECT SUM(Total), AVG(Total), COUNT(*) FROM Orders");
        return new((decimal)row.Sum, (decimal)row.Avg, row.Count);
    }

    [Benchmark(Description = "CouchbaseLite – Aggregate SUM/AVG/COUNT")]
    [BenchmarkCategory("Aggregate")]
    public AggResult CBL_Aggregate()
    {
        using var q = _cblDb.CreateQuery(
            "SELECT SUM(Total), AVG(Total), COUNT(*) FROM _");
        using var rs = q.Execute();
        var r = rs.AllResults().First();
        return new((decimal)r.GetDouble(0),
                   (decimal)r.GetDouble(1),
                   r.GetInt(2));
    }

    [Benchmark(Description = "DuckDB – Aggregate SUM/AVG/COUNT")]
    [BenchmarkCategory("Aggregate")]
    public AggResult DuckDB_Aggregate()
    {
        using var conn = new DuckDBConnection($"Data Source={_duckPath}");
        conn.Open();
        var row = conn.QueryFirst<(double Sum, double Avg, long Count)>(
            "SELECT SUM(Total), AVG(Total), COUNT(*) FROM Orders");
        return new((decimal)row.Sum, (decimal)row.Avg, (int)row.Count);
    }

    // ════════════════════════════════════════════════════════════════
    // 2. GROUP BY Status — COUNT + SUM(Total) per group
    // ════════════════════════════════════════════════════════════════

    [Benchmark(Baseline = true, Description = "BLite – GroupBy Status")]
    [BenchmarkCategory("GroupBy")]
    public List<GroupRow> BLite_GroupBy()
        // Single-pass BSON two-field scan: only Status (string) and Total (decimal)
        // are read per document — no full CustomerOrder instantiation.
        // GroupBy/Count/Sum then run on small (string, decimal) value tuples in memory.
        => _ctx.CustomerOrders
               .ScanPairs(o => o.Status, o => o.Total)
               .GroupBy(p => p.Key)
               .Select(g => new GroupRow(g.Key, g.Count(), g.Sum(p => p.Value)))
               .ToList();

    [Benchmark(Description = "LiteDB – GroupBy Status")]
    [BenchmarkCategory("GroupBy")]
    public List<GroupRow> LiteDb_GroupBy()
        => _liteDb.GetCollection<CustomerOrder>("orders")
                  .FindAll()
                  .GroupBy(o => o.Status)
                  .Select(g => new GroupRow(g.Key, g.Count(), g.Sum(o => o.Total)))
                  .ToList();

    [Benchmark(Description = "SQLite+JSON – GroupBy Status")]
    [BenchmarkCategory("GroupBy")]
    public List<GroupRow> Sqlite_GroupBy()
    {
        using var conn = new SqliteConnection(_sqliteConnStr);
        conn.Open();
        return conn.Query<(string Status, int Count, double SumTotal)>(
                "SELECT Status, COUNT(*), SUM(Total) FROM Orders GROUP BY Status")
                   .Select(r => new GroupRow(r.Status, r.Count, (decimal)r.SumTotal))
                   .ToList();
    }

    [Benchmark(Description = "CouchbaseLite – GroupBy Status")]
    [BenchmarkCategory("GroupBy")]
    public List<GroupRow> CBL_GroupBy()
    {
        using var q = _cblDb.CreateQuery(
            "SELECT Status, COUNT(*), SUM(Total) FROM _ GROUP BY Status");
        using var rs = q.Execute();
        return rs.AllResults()
                 .Select(r => new GroupRow(r.GetString(0)!, r.GetInt(1), (decimal)r.GetDouble(2)))
                 .ToList();
    }

    [Benchmark(Description = "DuckDB – GroupBy Status")]
    [BenchmarkCategory("GroupBy")]
    public List<GroupRow> DuckDB_GroupBy()
    {
        using var conn = new DuckDBConnection($"Data Source={_duckPath}");
        conn.Open();
        return conn.Query<(string Status, long Count, double SumTotal)>(
                "SELECT Status, COUNT(*), SUM(Total) FROM Orders GROUP BY Status")
                   .Select(r => new GroupRow(r.Status, (int)r.Count, (decimal)r.SumTotal))
                   .ToList();
    }

    // ════════════════════════════════════════════════════════════════
    // 3. RANGE FILTER — Total > threshold (deserialise matching docs)
    // ════════════════════════════════════════════════════════════════

    [Benchmark(Baseline = true, Description = "BLite – Range Total > threshold")]
    [BenchmarkCategory("RangeFilter")]
    public List<CustomerOrder> BLite_Range()
        // AsQueryable() + WHERE lets IndexOptimizer use the Total BTree index for a
        // range scan, returning only the matching ~25 % of documents.
        => _ctx.CustomerOrders.AsQueryable()
               .Where(o => o.Total > RangeThreshold)
               .ToList();

    [Benchmark(Description = "LiteDB – Range Total > threshold")]
    [BenchmarkCategory("RangeFilter")]
    public List<CustomerOrder> LiteDb_Range()
        => _liteDb.GetCollection<CustomerOrder>("orders")
                  .Find(o => o.Total > RangeThreshold).ToList();

    [Benchmark(Description = "SQLite+JSON – Range Total > threshold")]
    [BenchmarkCategory("RangeFilter")]
    public List<CustomerOrder> Sqlite_Range()
    {
        using var conn = new SqliteConnection(_sqliteConnStr);
        conn.Open();
        return conn.Query<string>(
                "SELECT Data FROM Orders WHERE Total > @t", new { t = (double)RangeThreshold })
                   .Select(json => STJ.JsonSerializer.Deserialize<CustomerOrder>(json)!)
                   .ToList();
    }

    [Benchmark(Description = "CouchbaseLite – Range Total > threshold")]
    [BenchmarkCategory("RangeFilter")]
    public List<CustomerOrder> CBL_Range()
    {
        using var q = _cblDb.CreateQuery(
            $"SELECT * FROM _ WHERE Total > {(double)RangeThreshold}");
        using var rs = q.Execute();
        return rs.AllResults()
                 .Select(r => STJ.JsonSerializer.Deserialize<CustomerOrder>(r.GetDictionary(0)!.ToJSON()!)!)
                 .ToList();
    }

    [Benchmark(Description = "DuckDB – Range Total > threshold")]
    [BenchmarkCategory("RangeFilter")]
    public List<CustomerOrder> DuckDB_Range()
    {
        using var conn = new DuckDBConnection($"Data Source={_duckPath}");
        conn.Open();
        return conn.Query<string>(
                "SELECT Data FROM Orders WHERE Total > $t", new { t = (double)RangeThreshold })
                   .Select(json => STJ.JsonSerializer.Deserialize<CustomerOrder>(json)!)
                   .ToList();
    }

    // ════════════════════════════════════════════════════════════════
    // 4. TOP-N — top 10 orders by Total DESC
    // ════════════════════════════════════════════════════════════════

    [Benchmark(Baseline = true, Description = "BLite – Top-10 by Total")]
    [BenchmarkCategory("TopN")]
    public List<CustomerOrder> BLite_TopN()
        // TryOptimizeOrderBy detects the indexed Total field and performs a BTree
        // backward scan, fetching only the top N documents without sorting in RAM.
        => _ctx.CustomerOrders.AsQueryable()
               .OrderByDescending(o => o.Total)
               .Take(TopN)
               .ToList();

    [Benchmark(Description = "LiteDB – Top-10 by Total")]
    [BenchmarkCategory("TopN")]
    public List<CustomerOrder> LiteDb_TopN()
        => _liteDb.GetCollection<CustomerOrder>("orders")
                  .Find(LiteDB.Query.All(nameof(CustomerOrder.Total), LiteDB.Query.Descending), limit: TopN)
                  .ToList();

    [Benchmark(Description = "SQLite+JSON – Top-10 by Total")]
    [BenchmarkCategory("TopN")]
    public List<CustomerOrder> Sqlite_TopN()
    {
        using var conn = new SqliteConnection(_sqliteConnStr);
        conn.Open();
        return conn.Query<string>(
                $"SELECT Data FROM Orders ORDER BY Total DESC LIMIT {TopN}")
                   .Select(json => STJ.JsonSerializer.Deserialize<CustomerOrder>(json)!)
                   .ToList();
    }

    [Benchmark(Description = "CouchbaseLite – Top-10 by Total")]
    [BenchmarkCategory("TopN")]
    public List<CustomerOrder> CBL_TopN()
    {
        using var q = _cblDb.CreateQuery(
            $"SELECT * FROM _ ORDER BY Total DESC LIMIT {TopN}");
        using var rs = q.Execute();
        return rs.AllResults()
                 .Select(r => STJ.JsonSerializer.Deserialize<CustomerOrder>(r.GetDictionary(0)!.ToJSON()!)!)
                 .ToList();
    }

    [Benchmark(Description = "DuckDB – Top-10 by Total")]
    [BenchmarkCategory("TopN")]
    public List<CustomerOrder> DuckDB_TopN()
    {
        using var conn = new DuckDBConnection($"Data Source={_duckPath}");
        conn.Open();
        return conn.Query<string>(
                $"SELECT Data FROM Orders ORDER BY Total DESC LIMIT {TopN}")
                   .Select(json => STJ.JsonSerializer.Deserialize<CustomerOrder>(json)!)
                   .ToList();
    }
}
