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
using System.Text.Json;

namespace DocumentDb.Benchmark;

[SimpleJob]
[InProcess]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class ReadBenchmarks
{
    private const int DocCount = 1000;
    
    // Paths
    private string _docDbPath = null!;
    private string _docDbWalPath = null!;
    private string _sqlitePath = null!;
    private string _sqliteConnString = null!;

    // DocumentDb
    private PageFile _pageFile = null!;
    private TransactionManager _txnMgr = null!;
    private DocumentCollection<Person> _collection = null!;
    // _mapper removed (unused)

    // Data
    private ObjectId[] _ids = null!;
    private ObjectId _targetId; // Middle item

    [GlobalSetup]
    public void Setup()
    {
        var temp = AppContext.BaseDirectory;
        var id = Guid.NewGuid().ToString("N");
        _docDbPath = Path.Combine(temp, $"bench_read_docdb_{id}.db");
        _docDbWalPath = Path.Combine(temp, $"bench_read_docdb_{id}.wal");
        _sqlitePath = Path.Combine(temp, $"bench_read_sqlite_{id}.db");
        _sqliteConnString = $"Data Source={_sqlitePath}";

        // Cleanup
        if (File.Exists(_docDbPath)) File.Delete(_docDbPath);
        if (File.Exists(_docDbWalPath)) File.Delete(_docDbWalPath);
        if (File.Exists(_sqlitePath)) File.Delete(_sqlitePath);

        // 1. Setup DocumentDb & Insert Data
        _pageFile = new PageFile(_docDbPath, PageFileConfig.Default);
        _pageFile.Open();
        _txnMgr = new TransactionManager(_docDbWalPath, _pageFile);
        _collection = new DocumentCollection<Person>(new PersonMapper(), _pageFile, _txnMgr);

        _ids = new ObjectId[DocCount];
        for (int i = 0; i < DocCount; i++)
        {
             var p = CreatePerson(i);
             _ids[i] = _collection.Insert(p);
        }
        
        // 2. Setup SQLite & Insert Data
        using (var conn = new SqliteConnection(_sqliteConnString))
        {
            conn.Open();
            conn.Execute("CREATE TABLE Documents (Id TEXT PRIMARY KEY, Payload TEXT)");
            using var txn = conn.BeginTransaction();
            for (int i = 0; i < DocCount; i++)
            {
                var p = CreatePerson(i);
                p.Id = _ids[i]; // Sync IDs
                var json = JsonSerializer.Serialize(p);
                conn.Execute("INSERT INTO Documents (Id, Payload) VALUES (@Id, @Payload)", 
                    new { Id = p.Id.ToString(), Payload = json }, transaction: txn);
            }
            txn.Commit();
        }

        // Target
        _targetId = _ids[DocCount / 2];
    }
    
    [GlobalCleanup]
    public void Cleanup()
    {
        _pageFile?.Dispose();
        SqliteConnection.ClearAllPools();
        _txnMgr?.Dispose();
        
        if (File.Exists(_docDbPath)) File.Delete(_docDbPath);
        if (File.Exists(_docDbWalPath)) File.Delete(_docDbWalPath);
        if (File.Exists(_sqlitePath)) File.Delete(_sqlitePath);
    }

    private Person CreatePerson(int i)
    {
        return new Person
        {
            // Id set by Insert or manually
            FirstName =($"First_{i}"),
            LastName = ($"Last_{i}"),
            Age = 20 + (i % 50),
            Bio = new string('A', 500),
            CreatedAt = DateTime.UtcNow
        };
    }

    [Benchmark(Baseline = true, Description = "SQLite FindById (Deserialize)")]
    [BenchmarkCategory("Read_Single")]
    public Person Sqlite_FindById()
    {
        using var conn = new SqliteConnection(_sqliteConnString);
        conn.Open();
        
        var json = conn.QueryFirstOrDefault<string>(
            "SELECT Payload FROM Documents WHERE Id = @Id", 
            new { Id = _targetId.ToString() });
            
        if (string.IsNullOrEmpty(json)) 
            throw new InvalidOperationException($"Document not found: {_targetId}");
            
        return JsonSerializer.Deserialize<Person>(json) ?? throw new InvalidOperationException("Deserialization returned null");
    }

    [Benchmark(Description = "DocumentDb FindById")]
    [BenchmarkCategory("Read_Single")]
    public Person? DocumentDb_FindById()
    {
        return _collection.FindById(_targetId);
    }
}
