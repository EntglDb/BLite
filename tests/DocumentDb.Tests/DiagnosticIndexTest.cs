using Xunit;
using Xunit.Abstractions;
using DocumentDb.Bson;
using DocumentDb.Core.Collections;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Transactions;

namespace DocumentDb.Tests;

/// <summary>
/// Diagnostic test to debug transaction visibility issues
/// </summary>
public class DiagnosticIndexTest : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;
    private readonly StorageEngine _storage;
    private readonly TransactionManager _txnManager;
    private readonly ITestOutputHelper _output;

    public DiagnosticIndexTest(ITestOutputHelper output)
    {
        _output = output;
        var id = Guid.NewGuid().ToString("N");
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_diag_{id}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"test_diag_{id}.wal");
        
        var _pageFile = new PageFile(_dbPath, PageFileConfig.Default);
        _pageFile.Open();
        var _wal = new WriteAheadLog(_walPath);
        _storage = new StorageEngine(_pageFile, _wal);
        _txnManager = new TransactionManager(_storage);
    }

    public void Dispose()
    {
        _txnManager?.Dispose();
        _storage?.Dispose();
        try { File.Delete(_dbPath); } catch { }
        try { File.Delete(_walPath); } catch { }
    }

    [Fact]
    public void Debug_InsertAndSeekInSameTransaction()
    {
        // Arrange
        var mapper = new SimplePersonMapper();
        var collection = new DocumentCollection<SimplePerson>(mapper, _storage, _txnManager);
        
        _output.WriteLine("Creating index on Age...");
        var ageIndex = collection.CreateIndex(p => p.Age);
        _output.WriteLine($"Index created. ActiveTransactionCount: {_storage.ActiveTransactionCount}");

        // Act - Insert WITH explicit transaction
        using (var txn = collection.BeginTransaction())
        {
            _output.WriteLine($"Transaction started. TxnId: {txn.TransactionId}");
            _output.WriteLine($"ActiveTransactionCount: {_storage.ActiveTransactionCount}");
            
            var person = new SimplePerson { FirstName = "Alice", Age = 25 };
            _output.WriteLine($"Inserting person with Age={person.Age}...");
            
            var id = collection.Insert(person, txn);
            _output.WriteLine($"Person inserted. Id: {id}");
            _output.WriteLine($"ActiveTransactionCount after insert: {_storage.ActiveTransactionCount}");
            
            // Try to find immediately
            _output.WriteLine($"Seeking Age=25 within transaction (TxnId={txn.TransactionId})...");
            var result = ageIndex.Seek(25, txn);
            
            _output.WriteLine($"Seek result: {result?.ToString() ?? "NULL"}");
            _output.WriteLine($"Expected: {id}");
            
            // Also try Range query
            _output.WriteLine("Trying Range query...");
            var rangeResults = ageIndex.Range(25, 25, txn).ToList();
            _output.WriteLine($"Range results count: {rangeResults.Count}");
            foreach (var rangeId in rangeResults)
            {
                _output.WriteLine($"  - Found: {rangeId}");
            }
            
            Assert.NotNull(result);
            Assert.Equal(id, result);

            txn.Commit();
        }

        using (var readTxn = collection.BeginTransaction())
        {
            // After commit, should still be visible
            _output.WriteLine("After commit, seeking Age=25 without transaction...");
            var resultAfterCommit = ageIndex.Seek(25, readTxn);
            _output.WriteLine($"Result after commit: {resultAfterCommit?.ToString() ?? "NULL"}");
            Assert.NotNull(resultAfterCommit);
        }
    }
}
