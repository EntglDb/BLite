using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Shared;
using BLite.Tests.TestDbContext_TestDbContext_Mappers;
using Xunit;

namespace BLite.Tests;

public class DocumentCollectionDeleteTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;
    private readonly TestDbContext _dbContext;

    public DocumentCollectionDeleteTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_delete_{Guid.NewGuid()}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"test_delete_{Guid.NewGuid()}.wal");

        _dbContext = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _dbContext.Dispose();
    }

    [Fact]
    public void Delete_RemovesDocumentAndIndexEntry()
    {
        var user = new User { Id = ObjectId.NewObjectId(), Name = "To Delete", Age = 10 };
        _dbContext.Users.Insert(user);
        _dbContext.SaveChanges();

        // Verify inserted
        Assert.NotNull(_dbContext.Users.FindById(user.Id));

        // Delete
        var deleted = _dbContext.Users.Delete(user.Id);
        _dbContext.SaveChanges();

        // Assert
        Assert.True(deleted, "Delete returned false");

        // Verify deleted from storage
        Assert.Null(_dbContext.Users.FindById(user.Id));

        // Verify Index is clean (FindAll uses index scan)
        var all = _dbContext.Users.FindAll();
        Assert.Empty(all);
    }

    [Fact]
    public void Delete_NonExistent_ReturnsFalse()
    {
        var id = ObjectId.NewObjectId();
        var deleted = _dbContext.Users.Delete(id);
        _dbContext.SaveChanges();
        Assert.False(deleted);
    }

    [Fact]
    public void Delete_WithTransaction_CommitsSuccessfully()
    {
        var user = new User { Id = ObjectId.NewObjectId(), Name = "Txn Delete", Age = 20 };
        _dbContext.Users.Insert(user);
        _dbContext.SaveChanges();

        using (var txn = _dbContext.BeginTransaction())
        {
            _dbContext.Users.Delete(user.Id);
            _dbContext.SaveChanges();
        }

        // Verify
        Assert.Null(_dbContext.Users.FindById(user.Id));
    }
}
