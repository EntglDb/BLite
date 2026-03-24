using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

public class DocumentCollectionTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;
    private readonly TestDbContext _db;

    public DocumentCollectionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_collection_{Guid.NewGuid()}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"test_collection_{Guid.NewGuid()}.wal");
        
        _db = new TestDbContext(_dbPath);
    }

    [Fact]
    public async Task Insert_And_FindById_Works()
    {
        // Arrange
        var user = new User { Name = "Alice", Age = 30 };
        
        // Act
        var id = await _db.Users.InsertAsync(user);
        await _db.SaveChangesAsync();
        var found = await _db.Users.FindByIdAsync(id);
        
        // Assert
        Assert.NotNull(found);
        Assert.Equal(id, found.Id);
        Assert.Equal("Alice", found.Name);
        Assert.Equal(30, found.Age);
    }

    [Fact]
    public async Task Insert_With_Duplicate_Id_Throws()
    {
        var id = ObjectId.NewObjectId();

        await _db.Users.InsertAsync(new User { Id = id, Name = "Alice", Age = 30 });
        await _db.SaveChangesAsync();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _db.Users.InsertAsync(new User { Id = id, Name = "Bob", Age = 31 }));
        Assert.Contains("Duplicate key violation", ex.Message);
        Assert.Equal(1, await _db.Users.CountAsync());
    }

    [Fact]
    public async Task FindById_Returns_Null_When_Not_Found()
    {
        // Act
        var found = await _db.Users.FindByIdAsync(ObjectId.NewObjectId());
        
        // Assert
        Assert.Null(found);
    }

    [Fact]
    public async Task FindAll_Returns_All_Entities()
    {
        // Arrange
        await _db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
        await _db.Users.InsertAsync(new User { Name = "Bob", Age = 25 });
        await _db.Users.InsertAsync(new User { Name = "Charlie", Age = 35 });
        await _db.SaveChangesAsync();

        // Act
        var all = (await _db.Users.FindAllAsync().ToListAsync());
        
        // Assert
        Assert.Equal(3, all.Count);
        Assert.Contains(all, u => u.Name == "Alice");
        Assert.Contains(all, u => u.Name == "Bob");
        Assert.Contains(all, u => u.Name == "Charlie");
    }

    [Fact]
    public async Task Update_Modifies_Entity()
    {
        // Arrange
        var user = new User { Name = "Alice", Age = 30 };
        var id = await _db.Users.InsertAsync(user);
        await _db.SaveChangesAsync();

        // Act
        user.Age = 31;
        var updated = await _db.Users.UpdateAsync(user);
        await _db.SaveChangesAsync();

        // Assert
        Assert.True(updated);
        
        var found = await _db.Users.FindByIdAsync(id);
        Assert.NotNull(found);
        Assert.Equal(31, found.Age);
    }

    [Fact]
    public async Task Update_Returns_False_When_Not_Found()
    {
        // Arrange
        var user = new User { Id = ObjectId.NewObjectId(), Name = "Ghost", Age = 99 };
        
        // Act
        var updated = await _db.Users.UpdateAsync(user);
        await _db.SaveChangesAsync();
        
        // Assert
        Assert.False(updated);
    }

    [Fact]
    public async Task Delete_Removes_Entity()
    {
        // Arrange
        var user = new User { Name = "Alice", Age = 30 };
        var id = await _db.Users.InsertAsync(user);
        await _db.SaveChangesAsync();
        
        // Act
        var deleted = await _db.Users.DeleteAsync(id);
        await _db.SaveChangesAsync();
        // Assert
        Assert.True(deleted);
        Assert.Null(await _db.Users.FindByIdAsync(id));
    }

    [Fact]
    public async Task Delete_Returns_False_When_Not_Found()
    {
        // Act
        var deleted = await _db.Users.DeleteAsync(ObjectId.NewObjectId());
        await _db.SaveChangesAsync();
        // Assert
        Assert.False(deleted);
    }

    [Fact]
    public async Task Count_Returns_Correct_Count()
    {
        // Arrange
        await _db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
        await _db.Users.InsertAsync(new User { Name = "Bob", Age = 25 });
        await _db.SaveChangesAsync();

        // Act
        var count = await _db.Users.CountAsync();
        
        // Assert
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Find_With_Predicate_Filters_Correctly()
    {
        // Arrange
        await _db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
        await _db.Users.InsertAsync(new User { Name = "Bob", Age = 25 });
        await _db.Users.InsertAsync(new User { Name = "Charlie", Age = 35 });
        await _db.SaveChangesAsync();

        // Act
        var over30 = (await _db.Users.FindAsync(u => u.Age > 30).ToListAsync());
        
        // Assert
        Assert.Single(over30);
        Assert.Equal("Charlie", over30[0].Name);
    }

    [Fact]
    public async Task InsertBulk_Inserts_Multiple_Entities()
    {
        // Arrange
        var users = new[]
        {
            new User { Name = "User1", Age = 20 },
            new User { Name = "User2", Age = 21 },
            new User { Name = "User3", Age = 22 }
        };
        
        // Act
        var count = await _db.Users.InsertBulkAsync(users);
        await _db.SaveChangesAsync();

        // Assert
        Assert.Equal(3, count.Count);
        Assert.Equal(3, await _db.Users.CountAsync());
    }

    [Fact]
    public async Task Insert_With_SpecifiedId_RetainsId()
    {
        // Arrange
        var id = ObjectId.NewObjectId();
        var user = new User { Id = id, Name = "SpecifiedID", Age = 40 };

        // Act
        var insertedId = await _db.Users.InsertAsync(user);
        await _db.SaveChangesAsync();

        // Assert
        Assert.Equal(id, insertedId);
        
        var found = await _db.Users.FindByIdAsync(id);
        Assert.NotNull(found);
        Assert.Equal(id, found.Id);
        Assert.Equal("SpecifiedID", found.Name);
    }

    public void Dispose()
    {
        _db?.Dispose();
    }
}
