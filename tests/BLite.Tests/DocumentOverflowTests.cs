using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Shared;
using BLite.Tests.TestDbContext_TestDbContext_Mappers;
using Xunit;

namespace BLite.Tests;

public class DocumentOverflowTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public DocumentOverflowTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_overflow_{Guid.NewGuid()}.db");
        // Use default PageSize (16KB)
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Fact]
    public void Insert_MediumDoc_64KB_ShouldSucceed()
    {
        // 20KB - Fits in 64KB buffer (First attempt)
        // But triggers overflow pages in storage (20KB > 16KB PageSize)
        var largeString = new string('A', 20 * 1024);
        var user = new User 
        { 
            Id = ObjectId.NewObjectId(), 
            Name = largeString,
            Age = 10
        };

        var id = _db.Users.Insert(user);
        _db.SaveChanges();
        var retrieved = _db.Users.FindById(id);
        
        Assert.NotNull(retrieved);
        Assert.Equal(largeString, retrieved.Name);
    }

    [Fact]
    public void Insert_LargeDoc_100KB_ShouldSucceed()
    {
        // 100KB - Fails 64KB buffer, Retries with 2MB
        var largeString = new string('B', 100 * 1024);
        var user = new User 
        { 
            Id = ObjectId.NewObjectId(), 
            Name = largeString,
            Age = 20
        };

        var id = _db.Users.Insert(user);
        _db.SaveChanges();
        var retrieved = _db.Users.FindById(id);
        
        Assert.NotNull(retrieved);
        Assert.Equal(largeString, retrieved.Name);
    }

    [Fact]
    public void Insert_HugeDoc_3MB_ShouldSucceed()
    {
        // 3MB - Fails 64KB, Fails 2MB, Retries with 16MB
        var largeString = new string('C', 3 * 1024 * 1024);
        var user = new User 
        { 
            Id = ObjectId.NewObjectId(), 
            Name = largeString,
            Age = 30
        };

        var id = _db.Users.Insert(user);
        _db.SaveChanges();
        var retrieved = _db.Users.FindById(id);
        
        Assert.NotNull(retrieved);
        Assert.Equal(largeString.Length, retrieved.Name.Length);
        // Checking full string might be slow, length check + substring check is faster
        Assert.Equal(largeString.Substring(0, 100), retrieved.Name.Substring(0, 100));
        Assert.Equal(largeString.Substring(largeString.Length - 100), retrieved.Name.Substring(retrieved.Name.Length - 100));
    }

    [Fact]
    public void Update_SmallToHuge_ShouldSucceed()
    {
        // Insert Small
        var user = new User { Id = ObjectId.NewObjectId(), Name = "Small", Age = 1 };
        var id = _db.Users.Insert(user);
        _db.SaveChanges();

        // Update to Huge (3MB)
        var hugeString = new string('U', 3 * 1024 * 1024);
        user.Name = hugeString;
        
        var updated = _db.Users.Update(user);
        _db.SaveChanges();
        Assert.True(updated);

        var retrieved = _db.Users.FindById(id);
        Assert.NotNull(retrieved);
        Assert.Equal(hugeString.Length, retrieved.Name.Length);
    }

    [Fact]
    public void InsertBulk_MixedSizes_ShouldSucceed()
    {
        var users = new List<User>
        {
            new User { Id = ObjectId.NewObjectId(), Name = "Small 1", Age = 1 },
            new User { Id = ObjectId.NewObjectId(), Name = new string('M', 100 * 1024), Age = 2 }, // 100KB
            new User { Id = ObjectId.NewObjectId(), Name = "Small 2", Age = 3 },
            new User { Id = ObjectId.NewObjectId(), Name = new string('H', 3 * 1024 * 1024), Age = 4 } // 3MB
        };

        var ids = _db.Users.InsertBulk(users);
        Assert.Equal(4, ids.Count);

        foreach (var u in users)
        {
            var r = _db.Users.FindById(u.Id);
            Assert.NotNull(r);
            Assert.Equal(u.Name.Length, r.Name.Length);
        }
    }
}
