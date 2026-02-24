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
    public void Insert_And_FindById_Works()
    {
        // Arrange
        var user = new User { Name = "Alice", Age = 30 };
        
        // Act
        var id = _db.Users.Insert(user);
        _db.SaveChanges();
        var found = _db.Users.FindById(id);
        
        // Assert
        Assert.NotNull(found);
        Assert.Equal(id, found.Id);
        Assert.Equal("Alice", found.Name);
        Assert.Equal(30, found.Age);
    }

    [Fact]
    public void FindById_Returns_Null_When_Not_Found()
    {
        // Act
        var found = _db.Users.FindById(ObjectId.NewObjectId());
        
        // Assert
        Assert.Null(found);
    }

    [Fact]
    public void FindAll_Returns_All_Entities()
    {
        // Arrange
        _db.Users.Insert(new User { Name = "Alice", Age = 30 });
        _db.Users.Insert(new User { Name = "Bob", Age = 25 });
        _db.Users.Insert(new User { Name = "Charlie", Age = 35 });
        _db.SaveChanges();

        // Act
        var all = _db.Users.FindAll().ToList();
        
        // Assert
        Assert.Equal(3, all.Count);
        Assert.Contains(all, u => u.Name == "Alice");
        Assert.Contains(all, u => u.Name == "Bob");
        Assert.Contains(all, u => u.Name == "Charlie");
    }

    [Fact]
    public void Update_Modifies_Entity()
    {
        // Arrange
        var user = new User { Name = "Alice", Age = 30 };
        var id = _db.Users.Insert(user);
        _db.SaveChanges();

        // Act
        user.Age = 31;
        var updated = _db.Users.Update(user);
        _db.SaveChanges();

        // Assert
        Assert.True(updated);
        
        var found = _db.Users.FindById(id);
        Assert.NotNull(found);
        Assert.Equal(31, found.Age);
    }

    [Fact]
    public void Update_Returns_False_When_Not_Found()
    {
        // Arrange
        var user = new User { Id = ObjectId.NewObjectId(), Name = "Ghost", Age = 99 };
        
        // Act
        var updated = _db.Users.Update(user);
        _db.SaveChanges();
        
        // Assert
        Assert.False(updated);
    }

    [Fact]
    public void Delete_Removes_Entity()
    {
        // Arrange
        var user = new User { Name = "Alice", Age = 30 };
        var id = _db.Users.Insert(user);
        _db.SaveChanges();
        
        // Act
        var deleted = _db.Users.Delete(id);
        _db.SaveChanges();

        // Assert
        Assert.True(deleted);
        Assert.Null(_db.Users.FindById(id));
    }

    [Fact]
    public void Delete_Returns_False_When_Not_Found()
    {
        // Act
        var deleted = _db.Users.Delete(ObjectId.NewObjectId());
        _db.SaveChanges();

        // Assert
        Assert.False(deleted);
    }

    [Fact]
    public void Count_Returns_Correct_Count()
    {
        // Arrange
        _db.Users.Insert(new User { Name = "Alice", Age = 30 });
        _db.Users.Insert(new User { Name = "Bob", Age = 25 });
        _db.SaveChanges();

        // Act
        var count = _db.Users.Count();
        
        // Assert
        Assert.Equal(2, count);
    }

    [Fact]
    public void Find_With_Predicate_Filters_Correctly()
    {
        // Arrange
        _db.Users.Insert(new User { Name = "Alice", Age = 30 });
        _db.Users.Insert(new User { Name = "Bob", Age = 25 });
        _db.Users.Insert(new User { Name = "Charlie", Age = 35 });
        _db.SaveChanges();

        // Act
        var over30 = _db.Users.Find(u => u.Age > 30).ToList();
        
        // Assert
        Assert.Single(over30);
        Assert.Equal("Charlie", over30[0].Name);
    }

    [Fact]
    public void InsertBulk_Inserts_Multiple_Entities()
    {
        // Arrange
        var users = new[]
        {
            new User { Name = "User1", Age = 20 },
            new User { Name = "User2", Age = 21 },
            new User { Name = "User3", Age = 22 }
        };
        
        // Act
        var count = _db.Users.InsertBulk(users);
        _db.SaveChanges();

        // Assert
        Assert.Equal(3, count.Count);
        Assert.Equal(3, _db.Users.Count());
    }

    [Fact]
    public void Insert_With_SpecifiedId_RetainsId()
    {
        // Arrange
        var id = ObjectId.NewObjectId();
        var user = new User { Id = id, Name = "SpecifiedID", Age = 40 };

        // Act
        var insertedId = _db.Users.Insert(user);
        _db.SaveChanges();

        // Assert
        Assert.Equal(id, insertedId);
        
        var found = _db.Users.FindById(id);
        Assert.NotNull(found);
        Assert.Equal(id, found.Id);
        Assert.Equal("SpecifiedID", found.Name);
    }

    public void Dispose()
    {
        _db?.Dispose();
    }
}
