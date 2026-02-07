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

[SimpleJob]
[InProcess]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class InsertBenchmarks
{
    private const int BatchSize = 1000;
    
    // Paths
    private string _docDbPath = "";
    private string _docDbWalPath = "";
    private string _sqlitePath = "";
    private string _sqliteConnString = "";

    // DocumentDb
    private PageFile? _pageFile = null;
    private TransactionManager? _txnMgr = null;
    private DocumentCollection<Person>? _collection = null;
    private UserMapper? _mapper = null;

    // Data
    private Person[] _batchData = Array.Empty<Person>();
    private Person? _singlePerson = null;

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
        return new Person
        {
            Id = ObjectId.NewObjectId(),
            FirstName = $"First_{i}",
            LastName = $"Last_{i}",
            Age = 20 + (i % 50),
            Bio = new string('A', 500), // 500 chars payload
            CreatedAt = DateTime.UtcNow
        };
    }

    [IterationSetup]
    public void IterationSetup()
    {
        // Cleanup and Re-Initialize for fairness per iteration/invocation?
        // Actually, for Insert benchmarks we want to append.
        // But for Batch insert we might fill up disk.
        // Let's reset DBs every iteration to keep it clean.
        
        // 1. Reset DocumentDb
        // Due to unique GUIDs, files should not exist. Skipping delete to avoid phantom IOExceptions.
        // if (File.Exists(_docDbPath)) File.Delete(_docDbPath);
        // if (File.Exists(_docDbWalPath)) File.Delete(_docDbWalPath);
        
        _pageFile = new PageFile(_docDbPath, PageFileConfig.Default);
        _pageFile.Open();
        _txnMgr = new TransactionManager(_docDbWalPath, _pageFile);
        _mapper = new UserMapper(); // We need a generic mapper, using UserMapper layout for now or adapting
        // Wait, UserMapper is for User class. Person is compatible?
        // UserMapper maps "Name", "Age". Person has "FirstName", "LastName".
        // We need a PersonMapper.
        _collection = new DocumentCollection<Person>(new PersonMapper(), _pageFile, _txnMgr);

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
            _pageFile?.Dispose();
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
