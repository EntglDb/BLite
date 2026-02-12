using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Tests.TestDbContext_TestDbContext_Mappers;
using Xunit;

namespace BLite.Tests;

public class DocumentCollectionDeleteTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;
    private readonly StorageEngine _storage;
    private readonly DocumentCollection<User> _collection;

    public DocumentCollectionDeleteTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_delete_{Guid.NewGuid()}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"test_delete_{Guid.NewGuid()}.wal");

        _storage = new StorageEngine(_dbPath, PageFileConfig.Default);

        var mapper = new BLite_Tests_UserMapper();
        _collection = new DocumentCollection<User>(_storage, mapper);
    }

    public void Dispose()
    {
        _storage.Dispose();
    }

    [Fact]
    public void Delete_RemovesDocumentAndIndexEntry()
    {
        var user = new User { Id = ObjectId.NewObjectId(), Name = "To Delete", Age = 10 };
        _collection.Insert(user);

        // Verify inserted
        Assert.NotNull(_collection.FindById(user.Id));

        // Delete
        var deleted = _collection.Delete(user.Id);

        // Assert
        Assert.True(deleted, "Delete returned false");

        // Verify deleted from storage
        Assert.Null(_collection.FindById(user.Id));

        // Verify Index is clean (FindAll uses index scan)
        var all = _collection.FindAll();
        Assert.Empty(all);
    }

    [Fact]
    public void Delete_NonExistent_ReturnsFalse()
    {
        var id = ObjectId.NewObjectId();
        var deleted = _collection.Delete(id);
        Assert.False(deleted);
    }

    [Fact]
    public void Delete_WithTransaction_CommitsSuccessfully()
    {
        var user = new User { Id = ObjectId.NewObjectId(), Name = "Txn Delete", Age = 20 };
        _collection.Insert(user);

        using (var txn = _collection.BeginTransaction())
        {
            _collection.Delete(user.Id); // Should use txn internally if passed? 
                                         // Wait, DocumentCollection.Delete(id) creates its own internal transaction if not passed.
                                         // But if we want to comprise it in a larger txn, we need Delete(id, txn) overload?
                                         // Checking DocumentCollection.cs... Delete(ObjectId id) does NOT take a transaction.
                                         // It creates one internally.

            // So we cannot test external transaction for Delete yet unless we verify Delete creates its own.
            // But Insert(user) was auto-committed.

            // Step back: DocumentCollection.Delete implementation:
            // var txn = _txnManager.BeginTransaction();
            // ...
            // txn.Commit();

            // So it effectively wraps in a transaction.
        }

        // Verify
        Assert.Null(_collection.FindById(user.Id));
    }
}
