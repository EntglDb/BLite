using BLite.Bson;
using BLite.Shared;

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
    public async Task Delete_RemovesDocumentAndIndexEntry()
    {
        var user = new User { Id = ObjectId.NewObjectId(), Name = "To Delete", Age = 10 };
        await _dbContext.Users.InsertAsync(user);
        await _dbContext.SaveChangesAsync();
        // Verify inserted
        Assert.NotNull(await _dbContext.Users.FindByIdAsync(user.Id));

        // Delete
        var deleted = await _dbContext.Users.DeleteAsync(user.Id);
        await _dbContext.SaveChangesAsync();

        // Assert
        Assert.True(deleted, "Delete returned false");

        // Verify deleted from storage
        Assert.Null(await _dbContext.Users.FindByIdAsync(user.Id));

        // Verify Index is clean (FindAll uses index scan)
        var all = await _dbContext.Users.FindAllAsync().ToListAsync();
        Assert.Empty(all);
    }

    [Fact]
    public async Task Delete_NonExistent_ReturnsFalse()
    {
        var id = ObjectId.NewObjectId();
        var deleted = await _dbContext.Users.DeleteAsync(id);
        await _dbContext.SaveChangesAsync();
        Assert.False(deleted);
    }

    [Fact]
    public async Task Delete_WithTransaction_CommitsSuccessfully()
    {
        var user = new User { Id = ObjectId.NewObjectId(), Name = "Txn Delete", Age = 20 };
        await _dbContext.Users.InsertAsync(user);
        await _dbContext.SaveChangesAsync();
        using (var txn = _dbContext.BeginTransaction())
        {
            await _dbContext.Users.DeleteAsync(user.Id);
            await _dbContext.SaveChangesAsync();
        }

        // Verify
        Assert.Null(await _dbContext.Users.FindByIdAsync(user.Id));
    }
}
