using DocumentDb.Core.Storage;
using DocumentDb.Core.Collections;
using DocumentDb.Bson;
using DocumentDb.Core.Transactions;
using Xunit;

namespace DocumentDb.Tests;

public class BulkOperationsTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;
    private readonly PageFile _pageFile;
    private readonly TransactionManager _txnManager;
    private readonly DocumentCollection<User> _collection;

    public BulkOperationsTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_bulk_{Guid.NewGuid()}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"test_bulk_{Guid.NewGuid()}.wal");
        
        _pageFile = new PageFile(_dbPath, PageFileConfig.Default);
        _pageFile.Open();
        
        _txnManager = new TransactionManager(_walPath, _pageFile);
        
        var mapper = new UserMapper();
        _collection = new DocumentCollection<User>(mapper, _pageFile, _txnManager);
    }

    public void Dispose()
    {
        _pageFile.Dispose();
        _txnManager.Dispose();
        
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);
    }

    [Fact]
    public void UpdateBulk_UpdatesMultipleDocuments()
    {
        // Arrange: Insert 100 users
        var users = new List<User>();
        for (int i = 0; i < 100; i++)
        {
            users.Add(new User { Id = ObjectId.NewObjectId(), Name = $"User {i}", Age = 20 });
        }
        _collection.InsertBulk(users);

        // Modify users
        foreach (var u in users)
        {
            u.Age = 30; // In-place update (int is same size)
            if (u.Name.EndsWith("0")) u.Name += "_Modified_Longer"; // Force move update
        }

        // Act
        var updatedCount = _collection.UpdateBulk(users);

        // Assert
        Assert.Equal(100, updatedCount);
        
        // Verify changes
        foreach (var u in users)
        {
            var stored = _collection.FindById(u.Id);
            Assert.NotNull(stored);
            Assert.Equal(30, stored.Age);
            Assert.Equal(u.Name, stored.Name);
        }
    }

    [Fact]
    public void DeleteBulk_RemovesMultipleDocuments()
    {
        // Arrange: Insert 100 users
        var users = new List<User>();
        for (int i = 0; i < 100; i++)
        {
            users.Add(new User { Id = ObjectId.NewObjectId(), Name = $"User {i}", Age = 20 });
        }
        _collection.InsertBulk(users);

        var idsToDelete = users.Take(50).Select(u => u.Id).ToList();

        // Act
        var deletedCount = _collection.DeleteBulk(idsToDelete);

        // Assert
        Assert.Equal(50, deletedCount);
        
        // Verify deleted
        foreach (var id in idsToDelete)
        {
            Assert.Null(_collection.FindById(id));
        }

        // Verify remaining
        var remaining = users.Skip(50).ToList();
        foreach (var u in remaining)
        {
            Assert.NotNull(_collection.FindById(u.Id));
        }
        
        // Verify count
        // Note: Count() is not fully implemented efficiently yet (iterates everything), but FindAll().Count() works
        Assert.Equal(50, _collection.FindAll().Count());
    }

    [Fact]
    public void DeleteBulk_WithTransaction_Rollworks()
    {
        // Arrange
        var user = new User { Id = ObjectId.NewObjectId(), Name = "Txn User", Age = 20 };
        _collection.Insert(user);

        using (var txn = _collection.BeginTransaction())
        {
            _collection.DeleteBulk(new[] { user.Id }, txn);
            txn.Rollback();
        }

        // Assert: Should still exist
        Assert.NotNull(_collection.FindById(user.Id));
    }
}
