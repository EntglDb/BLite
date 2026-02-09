using DocumentDb.Bson;
using DocumentDb.Core;
using DocumentDb.Core.Indexing;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Transactions;
using Xunit;

namespace DocumentDb.Tests;

public class TempIndexTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly StorageEngine _storageEngine;
    private readonly TransactionManager _transactionManager;

    public TempIndexTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"test_temp_index_{Guid.NewGuid()}.db");
        var _pageFile = new PageFile(_testDbPath, PageFileConfig.Default);
        _pageFile.Open();
        var _wal = new WriteAheadLog(Path.Combine(Path.GetTempPath(), $"test_temp_index_{Guid.NewGuid()}.wal"));
        _storageEngine = new StorageEngine(_pageFile, _wal);
        _transactionManager = new TransactionManager(_storageEngine);
    }

    public void Dispose()
    {
        _storageEngine.Dispose();
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);
    }

    [Fact]
    public void BTreeIndex_SingleInsert_ShouldWork()
    {
        var options = IndexOptions.CreateBTree("testField");
        var btree = new BTreeIndex(_storageEngine, options);
        var key = new IndexKey(100);
        var docId = ObjectId.NewObjectId();
        using var transaction = _transactionManager.BeginTransaction(IsolationLevel.Serializable);
        btree.Insert(key, docId, transaction.TransactionId);
        Assert.True(btree.TryFind(key, out var foundId, transaction.TransactionId));
        Assert.Equal(docId, foundId);
    }
}
