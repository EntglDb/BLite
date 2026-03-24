using BLite.Bson;
using BLite.Shared;

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
    public async Task UpdateBulk_UpdatesMultipleDocuments()
    {
        // Arrange: Insert 100 users
        var users = new List<User>();
        for (int i = 0; i < 100; i++)
        {
            users.Add(new User { Id = ObjectId.NewObjectId(), Name = $"User {i}", Age = 20 });
        }
        await _dbContext.Users.InsertBulkAsync(users);
        await _dbContext.SaveChangesAsync();

        // Modify users
        foreach (var u in users)
        {
            u.Age = 30; // In-place update (int is same size)
            if (u.Name.EndsWith("0")) u.Name += "_Modified_Longer"; // Force move update
        }

        // Act
        var updatedCount = await _dbContext.Users.UpdateBulkAsync(users);
        await _dbContext.SaveChangesAsync();

        // Assert
        Assert.Equal(100, updatedCount);
        
        // Verify changes
        foreach (var u in users)
        {
            var stored = await _dbContext.Users.FindByIdAsync(u.Id);
            Assert.NotNull(stored);
            Assert.Equal(30, stored.Age);
            Assert.Equal(u.Name, stored.Name);
        }
    }

    [Fact]
    public async Task DeleteBulk_RemovesMultipleDocuments()
    {
        // Arrange: Insert 100 users
        var users = new List<User>();
        for (int i = 0; i < 100; i++)
        {
            users.Add(new User { Id = ObjectId.NewObjectId(), Name = $"User {i}", Age = 20 });
        }
        await _dbContext.Users.InsertBulkAsync(users);
        await _dbContext.SaveChangesAsync();

        var idsToDelete = users.Take(50).Select(u => u.Id).ToList();

        // Act
        var deletedCount = await _dbContext.Users.DeleteBulkAsync(idsToDelete);
        await _dbContext.SaveChangesAsync();

        // Assert
        Assert.Equal(50, deletedCount);
        
        // Verify deleted
        foreach (var id in idsToDelete)
        {
            Assert.Null(await _dbContext.Users.FindByIdAsync(id));
        }

        // Verify remaining
        var remaining = users.Skip(50).ToList();
        foreach (var u in remaining)
        {
            Assert.NotNull(await _dbContext.Users.FindByIdAsync(u.Id));
        }
        
        // Verify count
        // Note: Count() is not fully implemented efficiently yet (iterates everything), but FindAll().Count() works
        Assert.Equal(50, (await _dbContext.Users.FindAllAsync().ToListAsync()).Count);
    }

    [Fact]
    public async Task DeleteBulk_WithTransaction_Rollworks()
    {
        // Arrange
        var user = new User { Id = ObjectId.NewObjectId(), Name = "Txn User", Age = 20 };
        await _dbContext.Users.InsertAsync(user);
        await _dbContext.SaveChangesAsync();

        Assert.NotNull(await _dbContext.Users.FindByIdAsync(user.Id));

        using (var txn = _dbContext.BeginTransaction())
        {
            await _dbContext.Users.DeleteBulkAsync(new[] { user.Id });
            await txn.RollbackAsync();
        }

        // Assert: Should still exist
        Assert.NotNull(await _dbContext.Users.FindByIdAsync(user.Id));
    }
}
