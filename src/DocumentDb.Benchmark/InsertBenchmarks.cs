using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using Dapper;
using DocumentDb.Bson;
using DocumentDb.Core;
using DocumentDb.Core.Collections;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Transactions;
using Microsoft.Data.Sqlite;
using System.IO;
using System.Security.Cryptography;
using System.Text.Json;

namespace DocumentDb.Benchmark;


[InProcess]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[HtmlExporter]
[JsonExporterAttribute.Full]
public class InsertBenchmarks
{
    private const int BatchSize = 1000;
    
    // Paths
    private string _docDbPath = "";
    private string _docDbWalPath = "";
    private string _sqlitePath = "";
    private string _sqliteConnString = "";

    // DocumentDb
    private TransactionManager? _txnMgr = null;
    private StorageEngine? _storage = null;
    private DocumentCollection<Person>? _collection = null;

    // Data
    private Person[] _batchData = Array.Empty<Person>();
    private Person? _singlePerson = null;

    [GlobalSetup]
    public void Setup()
    {
        var temp = AppContext.BaseDirectory;
        var id = Guid.NewGuid().ToString("N");
        _docDbPath = Path.Combine(temp, $"bench_docdb_{id}.db");
        _docDbWalPath = Path.Combine(temp, $"bench_docdb_{id}.wal");
        _sqlitePath = Path.Combine(temp, $"bench_sqlite_{id}.db");
        _sqliteConnString = $"Data Source={_sqlitePath}";

        // Prepare Data
        _singlePerson = CreatePerson(0);
        _batchData = new Person[BatchSize];
        for (int i = 0; i < BatchSize; i++)
        {
            _batchData[i] = CreatePerson(i);
        }
    }

    private Person CreatePerson(int i)
    {
        var p = new Person
        {
            Id = ObjectId.NewObjectId(),
            FirstName = $"First_{i}",
            LastName = $"Last_{i}",
            Age = 20 + (i % 50),
            Bio = null, // Removed large payload to focus on structure
            CreatedAt = DateTime.UtcNow,
            Balance = 1000.50m * (i + 1),
            HomeAddress = new Address 
            {
                Street = $"{i} Main St",
                City = "Tech City",
                ZipCode = "12345"
            }
        };

        // Add 10 work history items to stress structure traversal
        for(int j=0; j<10; j++)
        {
            p.EmploymentHistory.Add(new WorkHistory
            {
                CompanyName = $"TechCorp_{i}_{j}",
                Title = "Developer",
                DurationYears = j,
                Tags = new List<string> { "C#", "BSON", "Performance", "Database", "Complex" }
            });
        }

        return p;
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _storage = new StorageEngine(_docDbWalPath, PageFileConfig.Default);
        _txnMgr = new TransactionManager(_storage);
        _collection = new DocumentCollection<Person>(new PersonMapper(), _storage, _txnMgr);

        // 2. Reset SQLite
        if (File.Exists(_sqlitePath)) File.Delete(_sqlitePath);
        using var conn = new SqliteConnection(_sqliteConnString);
        conn.Open();
        conn.Execute("CREATE TABLE Documents (Id TEXT PRIMARY KEY, Payload TEXT)");
    }

    [IterationCleanup]
    public void Cleanup()
    {
        try
        {
            _storage?.Dispose();
            _txnMgr?.Dispose();
            
            SqliteConnection.ClearAllPools();
            
            // Small delay to ensure file handles are released
            System.Threading.Thread.Sleep(100);
            
            if (File.Exists(_docDbPath)) File.Delete(_docDbPath);
            if (File.Exists(_docDbWalPath)) File.Delete(_docDbWalPath);
            if (File.Exists(_sqlitePath)) File.Delete(_sqlitePath);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Cleanup warning: {ex.Message}");
        }
    }

    // --- Benchmarks ---

    [Benchmark(Baseline = true, Description = "SQLite Single Insert (AutoCommit)")]
    [BenchmarkCategory("Insert_Single")]
    public void Sqlite_Insert_Single()
    {
        using var conn = new SqliteConnection(_sqliteConnString);
        conn.Open();
        
        var json = JsonSerializer.Serialize(_singlePerson);
        conn.Execute("INSERT INTO Documents (Id, Payload) VALUES (@Id, @Payload)", 
            new { Id = _singlePerson?.Id.ToString(), Payload = json });
    }

    [Benchmark(Description = "SQLite Batch Insert (1000 items, Forced Checkpoint)")]
    [BenchmarkCategory("Insert_Batch")]
    public void Sqlite_Insert_Batch_ForcedCheckpoint()
    {
        using var conn = new SqliteConnection(_sqliteConnString);
        conn.Open();
        
        // Ensure WAL mode
        conn.Execute("PRAGMA journal_mode=WAL;");
        
        using (var txn = conn.BeginTransaction())
        {
            foreach (var p in _batchData)
            {
                var json = JsonSerializer.Serialize(p);
                conn.Execute("INSERT INTO Documents (Id, Payload) VALUES (@Id, @Payload)", 
                    new { Id = p.Id.ToString(), Payload = json }, txn);
            }
            txn.Commit();
        }
        
        // Force checkpoint to flush WAL to main DB (simulate DocumentDb's immediate write)
        conn.Execute("PRAGMA wal_checkpoint(FULL);");
    }

    [Benchmark(Description = "DocumentDb Single Insert")]
    [BenchmarkCategory("Insert_Single")]
    public void DocumentDb_Insert_Single()
    {
        _collection?.Insert(_singlePerson!);
    }

    [Benchmark(Description = "SQLite Batch Insert (1000 items, 1 Txn)")]
    [BenchmarkCategory("Insert_Batch")]
    public void Sqlite_Insert_Batch()
    {
        using var conn = new SqliteConnection(_sqliteConnString);
        conn.Open();
        using var txn = conn.BeginTransaction();
        
        foreach (var p in _batchData)
        {
            var json = JsonSerializer.Serialize(p);
            conn.Execute("INSERT INTO Documents (Id, Payload) VALUES (@Id, @Payload)", 
                new { Id = p.Id.ToString(), Payload = json }, transaction: txn);
        }
        
        txn.Commit();
    }

    [Benchmark(Description = "DocumentDb Batch Insert (1000 items, 1 Txn)")]
    [BenchmarkCategory("Insert_Batch")]
    public void DocumentDb_Insert_Batch()
    {
        // Now uses transaction API via InsertBulk
        _collection?.InsertBulk(_batchData);
    }
}
