using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BLite.Bson;
using BLite.Core;
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
using System.Threading;

namespace BLite.Benchmark;

[InProcess]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[HtmlExporter]
[JsonExporterAttribute.Full]
public class InsertBenchmarks
{
    private const int BatchSize = 1000;

    private string _docDbPath       = "";
    private string _docDbServerPath  = "";
    private string _sqlitePath       = "";
    private string _sqliteConnString = "";
    private string _litePath         = "";
    private string _cblDir           = "";
    private string _duckPath         = "";
    private string _bsonEnginePath   = "";

    private TestDbContext?          _ctx       = null;
    private TestDbContext?          _serverCtx = null;
    private LiteDatabase?           _liteDb = null;
    private Database?               _cblDb  = null;
    private Couchbase.Lite.Collection? _cblCol = null;
    private BLiteEngine?            _bsonEngine = null;
    private DynamicCollection?      _bsonCol    = null;

    private CustomerOrder[]  _batchData   = Array.Empty<CustomerOrder>();
    private CustomerOrder?   _singleOrder = null;
    private BLite.Bson.BsonDocument?   _bsonSingleDoc  = null;
    private BLite.Bson.BsonDocument[]  _bsonBatchDocs  = Array.Empty<BLite.Bson.BsonDocument>();

    [GlobalSetup]
    public void Setup()
    {
        var temp = AppContext.BaseDirectory;
        var id   = Guid.NewGuid().ToString("N");
        _docDbPath         = Path.Combine(temp, $"bench_docdb_{id}.db");
        _docDbServerPath   = Path.Combine(temp, $"bench_docdb_server_{id}.db");
        _sqlitePath        = Path.Combine(temp, $"bench_sqlite_{id}.db");
        _sqliteConnString  = $"Data Source={_sqlitePath}";
        _litePath          = Path.Combine(temp, $"bench_lite_{id}.db");
        _cblDir            = Path.Combine(temp, $"bench_cbl_{id}");

        Couchbase.Lite.Logging.LogSinks.Console = null;

        _singleOrder = BenchmarkDataFactory.CreateOrder(0);
        _batchData   = Enumerable.Range(0, BatchSize)
                                 .Select(BenchmarkDataFactory.CreateOrder)
                                 .ToArray();
        _duckPath        = Path.Combine(temp, $"bench_duck_{id}.db");
        _bsonEnginePath  = Path.Combine(temp, $"bench_bson_{id}.db");
    }

    [IterationSetup]
    public void IterationSetup()
    {
        if (File.Exists(_docDbPath)) File.Delete(_docDbPath);
        _ctx = new TestDbContext(_docDbPath);

        DeleteServerDb(_docDbServerPath);
        _serverCtx = new TestDbContext(_docDbServerPath, PageFileConfig.Server(_docDbServerPath));

        if (File.Exists(_sqlitePath)) File.Delete(_sqlitePath);
        using var conn = new SqliteConnection(_sqliteConnString);
        conn.Open();
        conn.Execute("CREATE TABLE Orders (Id TEXT PRIMARY KEY, Data TEXT NOT NULL)");

        if (File.Exists(_litePath)) File.Delete(_litePath);
        _liteDb = new LiteDatabase($"Filename={_litePath};Connection=direct");

        if (Directory.Exists(_cblDir)) Directory.Delete(_cblDir, true);
        Directory.CreateDirectory(_cblDir);
        _cblDb  = new Database("bench", new DatabaseConfiguration { Directory = _cblDir });
        _cblCol = _cblDb.GetDefaultCollection();

        if (File.Exists(_duckPath)) File.Delete(_duckPath);
        using var duck = new DuckDBConnection($"Data Source={_duckPath}");
        duck.Open();
        duck.Execute("CREATE TABLE Orders (Id VARCHAR PRIMARY KEY, Data VARCHAR NOT NULL)");

        if (File.Exists(_bsonEnginePath)) File.Delete(_bsonEnginePath);
        _bsonEngine = new BLiteEngine(_bsonEnginePath);
        _bsonCol    = _bsonEngine.GetOrCreateCollection("orders");
        _bsonSingleDoc  = BenchmarkDataFactory.CreateBsonDocument(0, _bsonEngine);
        _bsonBatchDocs  = Enumerable.Range(0, BatchSize)
                                    .Select(i => BenchmarkDataFactory.CreateBsonDocument(i, _bsonEngine))
                                    .ToArray();
    }

    [IterationCleanup]
    public void Cleanup()
    {
        try
        {
            _ctx?.Dispose();
            _serverCtx?.Dispose();
            _bsonEngine?.Dispose();
            _bsonEngine = null;
            _bsonCol    = null;
            DeleteServerDb(_docDbServerPath);
            _liteDb?.Dispose();
            _cblCol = null!;
            _cblDb?.Dispose();
            SqliteConnection.ClearAllPools();
            System.Threading.Thread.Sleep(200);
            if (File.Exists(_docDbPath)) File.Delete(_docDbPath);
            if (File.Exists(_sqlitePath)) File.Delete(_sqlitePath);
            if (File.Exists(_litePath))   File.Delete(_litePath);
            if (File.Exists(_duckPath))        File.Delete(_duckPath);
            if (File.Exists(_bsonEnginePath))  File.Delete(_bsonEnginePath);
            for (int i = 0; i < 5 && Directory.Exists(_cblDir); i++)
            {
                try   { Directory.Delete(_cblDir, true); }
                catch { System.Threading.Thread.Sleep(100 * (i + 1)); }
            }
        }
        catch (Exception ex) { Console.WriteLine($"Cleanup warning: {ex.Message}"); }
    }

    // ──── Single Insert ──────────────────────────────────────────────

    [Benchmark(Baseline = true, Description = "BLite – Single Insert")]
    [BenchmarkCategory("Insert_Single")]
    public async Task BLite_Insert_Single() => await _ctx!.CustomerOrders.InsertAsync(_singleOrder!);

    [Benchmark(Description = "BLite BSON – Single Insert")]
    [BenchmarkCategory("Insert_Single")]
    public async Task BLiteBson_Insert_Single() => await _bsonCol!.InsertAsync(_bsonSingleDoc!);

    [Benchmark(Description = "BLite Server – Single Insert")]
    [BenchmarkCategory("Insert_Single")]
    public async Task BLiteServer_Insert_Single() => await _serverCtx!.CustomerOrders.InsertAsync(_singleOrder!);

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
    public async Task BLite_Insert_Batch() => await _ctx!.CustomerOrders.InsertBulkAsync(_batchData);

    [Benchmark(Description = "BLite BSON – Batch Insert (1000)")]
    [BenchmarkCategory("Insert_Batch")]
    public async Task BLiteBson_Insert_Batch() => await _bsonCol!.InsertBulkAsync(_bsonBatchDocs);

    [Benchmark(Description = "BLite Server – Batch Insert (1000)")]
    [BenchmarkCategory("Insert_Batch")]
    public async Task BLiteServer_Insert_Batch() => await _serverCtx!.CustomerOrders.InsertBulkAsync(_batchData);

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

    // ──── CouchbaseLite ─────────────────────────────────────────────

    [Benchmark(Description = "CouchbaseLite – Single Insert")]
    [BenchmarkCategory("Insert_Single")]
    public void CBL_Insert_Single()
    {
        var doc = new MutableDocument(_singleOrder!.Id);
        doc.SetJSON(System.Text.Json.JsonSerializer.Serialize(_singleOrder));
        _cblCol!.Save(doc);
    }

    [Benchmark(Description = "CouchbaseLite – Batch Insert (1000)")]
    [BenchmarkCategory("Insert_Batch")]
    public void CBL_Insert_Batch()
    {
        _cblDb!.InBatch(() =>
        {
            foreach (var o in _batchData)
            {
                var doc = new MutableDocument(o.Id);
                doc.SetJSON(System.Text.Json.JsonSerializer.Serialize(o));
                _cblCol!.Save(doc);
            }
        });
    }

    // ──── DuckDB ─────────────────────────────────────────────────────

    [Benchmark(Description = "DuckDB – Single Insert")]
    [BenchmarkCategory("Insert_Single")]
    public void DuckDB_Insert_Single()
    {
        using var conn = new DuckDBConnection($"Data Source={_duckPath}");
        conn.Open();
        conn.Execute("INSERT INTO Orders (Id, Data) VALUES ($Id, $Data)",
            new { _singleOrder!.Id, Data = System.Text.Json.JsonSerializer.Serialize(_singleOrder) });
    }

    [Benchmark(Description = "DuckDB – Batch Insert (1000, 1 Txn)")]
    [BenchmarkCategory("Insert_Batch")]
    public void DuckDB_Insert_Batch()
    {
        using var conn = new DuckDBConnection($"Data Source={_duckPath}");
        conn.Open();
        using var txn = conn.BeginTransaction();
        foreach (var o in _batchData)
            conn.Execute("INSERT INTO Orders (Id, Data) VALUES ($Id, $Data)",
                new { o.Id, Data = System.Text.Json.JsonSerializer.Serialize(o) }, transaction: txn);
        txn.Commit();
    }

    // ──── helpers ─────────────────────────────────────────────────────

    internal static void DeleteServerDb(string dbPath)
    {
        if (File.Exists(dbPath)) File.Delete(dbPath);
        var dir  = Path.GetDirectoryName(dbPath) ?? ".";
        var name = Path.GetFileNameWithoutExtension(dbPath);
        var idx  = Path.ChangeExtension(dbPath, ".idx");
        if (File.Exists(idx)) File.Delete(idx);
        var walFile = Path.Combine(dir, "wal", name + ".wal");
        if (File.Exists(walFile)) File.Delete(walFile);
        var collDir = Path.Combine(dir, "collections", name);
        if (Directory.Exists(collDir))
        {
            // On Windows, file handles inside the directory may still be open briefly
            // after StorageEngine.Dispose() returns. Retry with back-off.
            for (int attempt = 0; attempt < 5; attempt++)
            {
                try { Directory.Delete(collDir, true); break; }
                catch (IOException) when (attempt < 4) { Thread.Sleep(50 << attempt); }
            }
        }
    }
}
