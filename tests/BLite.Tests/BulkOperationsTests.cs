using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Shared;
using BLite.Tests.TestDbContext_TestDbContext_Mappers;
using Xunit;
using static BLite.Tests.SchemaTests;

namespace BLite.Tests;

public class BulkOperationsTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;
    private readonly TestDbContext _dbContext;

    public BulkOperationsTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_bulk_{Guid.NewGuid()}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"test_bulk_{Guid.NewGuid()}.wal");
        
        _dbContext = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _dbContext.Dispose();
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
        _dbContext.Users.InsertBulk(users);
        _dbContext.SaveChanges();

        // Modify users
        foreach (var u in users)
        {
            u.Age = 30; // In-place update (int is same size)
            if (u.Name.EndsWith("0")) u.Name += "_Modified_Longer"; // Force move update
        }

        // Act
        var updatedCount = _dbContext.Users.UpdateBulk(users);
        _dbContext.SaveChanges();

        // Assert
        Assert.Equal(100, updatedCount);
        
        // Verify changes
        foreach (var u in users)
        {
            var stored = _dbContext.Users.FindById(u.Id);
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
        _dbContext.Users.InsertBulk(users);
        _dbContext.SaveChanges();

        var idsToDelete = users.Take(50).Select(u => u.Id).ToList();

        // Act
        var deletedCount = _dbContext.Users.DeleteBulk(idsToDelete);
        _dbContext.SaveChanges();

        // Assert
        Assert.Equal(50, deletedCount);
        
        // Verify deleted
        foreach (var id in idsToDelete)
        {
            Assert.Null(_dbContext.Users.FindById(id));
        }

        // Verify remaining
        var remaining = users.Skip(50).ToList();
        foreach (var u in remaining)
        {
            Assert.NotNull(_dbContext.Users.FindById(u.Id));
        }
        
        // Verify count
        // Note: Count() is not fully implemented efficiently yet (iterates everything), but FindAll().Count() works
        Assert.Equal(50, _dbContext.Users.FindAll().Count());
    }

    [Fact]
    public void DeleteBulk_WithTransaction_Rollworks()
    {
        // Arrange
        var user = new User { Id = ObjectId.NewObjectId(), Name = "Txn User", Age = 20 };
        _dbContext.Users.Insert(user);
        _dbContext.SaveChanges();

        Assert.NotNull(_dbContext.Users.FindById(user.Id));

        using (var txn = _dbContext.BeginTransaction())
        {
            _dbContext.Users.DeleteBulk(new[] { user.Id });
            txn.Rollback();
        }

        // Assert: Should still exist
        Assert.NotNull(_dbContext.Users.FindById(user.Id));
    }
}
