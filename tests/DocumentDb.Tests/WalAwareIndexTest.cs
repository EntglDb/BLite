using Xunit;
using DocumentDb.Bson;
using DocumentDb.Core.Collections;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Transactions;

namespace DocumentDb.Tests;

/// <summary>
/// Tests to verify WAL-aware index reads
/// </summary>
public class WalAwareIndexTest : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;
    private readonly PageFile _pageFile;
    private WriteAheadLog _wal;
    private readonly StorageEngine _storage;
    private readonly TransactionManager _txnManager;

    public WalAwareIndexTest()
    {
        var id = Guid.NewGuid().ToString("N");
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_wal_idx_{id}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"test_wal_idx_{id}.wal");
        
        _pageFile = new PageFile(_dbPath, PageFileConfig.Default);
        _pageFile.Open();
        _wal = new WriteAheadLog(_walPath);
        _storage = new StorageEngine(_pageFile, _wal);
        _txnManager = new TransactionManager(_storage);
    }

    public void Dispose()
    {
        _txnManager?.Dispose();
        _pageFile?.Dispose();
        try { File.Delete(_dbPath); } catch { }
        try { File.Delete(_walPath); } catch { }
    }

    [Fact]
    public void Insert_WithInternalTransaction_ShouldBeImmediatelyVisible()
    {
        // Arrange
        var mapper = new SimplePersonMapper();
        var collection = new DocumentCollection<SimplePerson>(mapper, _pageFile, _wal, _txnManager);
        var ageIndex = collection.CreateIndex(p => p.Age);

        // Act - Insert WITHOUT explicit transaction (uses internal auto-commit transaction)
        var person = new SimplePerson { FirstName = "Alice", Age = 25 };
        var id = collection.Insert(person);  // This creates, commits, and destroys internal transaction

        // Assert - Data should be visible immediately after insert
        // because internal transaction was committed and wrote to PageFile
        var result = ageIndex.Seek(25);
        
        Assert.NotNull(result);
        Assert.Equal(id, result);
    }

    [Fact]
    public void MultipleInserts_WithInternalTransactions_AllShouldBeVisible()
    {
        // Arrange
        var mapper = new SimplePersonMapper();
        var collection = new DocumentCollection<SimplePerson>(mapper, _pageFile, _wal, _txnManager);
        var ageIndex = collection.CreateIndex(p => p.Age);

        // Act - Insert 3 people, 2 with same age
        var person1 = new SimplePerson { FirstName = "Alice", Age = 25 };
        var person2 = new SimplePerson { FirstName = "Bob", Age = 30 };
        var person3 = new SimplePerson { FirstName = "Charlie", Age = 25 };

        collection.Insert(person1);
        collection.Insert(person2);
        collection.Insert(person3);

        // Assert - All should be visible
        var age25Results = ageIndex.Range(25, 25).ToList();
        
        Assert.Equal(2, age25Results.Count);
        Assert.Contains(person1.Id, age25Results);
        Assert.Contains(person3.Id, age25Results);
    }

    [Fact]
    public void Insert_WithExplicitTransaction_ShouldBeVisibleWithinTransaction()
    {
        // Arrange
        var mapper = new SimplePersonMapper();
        var collection = new DocumentCollection<SimplePerson>(mapper, _pageFile, _wal, _txnManager);
        var ageIndex = collection.CreateIndex(p => p.Age);

        // Act - Insert WITH explicit transaction
        using (var txn = collection.BeginTransaction())
        {
            var person = new SimplePerson { FirstName = "Alice", Age = 25 };
            var id = collection.Insert(person, txn);

            // Assert - Should be visible within same transaction (WAL-aware read)
            var result = ageIndex.Seek(25, txn);
            Assert.NotNull(result);
            Assert.Equal(id, result);

            txn.Commit();
        }

        // After commit, should still be visible
        var resultAfterCommit = ageIndex.Seek(25);
        Assert.NotNull(resultAfterCommit);
    }
}
