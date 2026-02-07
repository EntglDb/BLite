using DocumentDb.Bson;

namespace DocumentDb.Tests;

public class DbContextTests : IDisposable
{
    private readonly string _dbPath;
    
    public DbContextTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_dbcontext_{Guid.NewGuid()}.db");
    }

    [Fact]
    public void DbContext_BasicLifecycle_Works()
    {
        using var db = new TestDbContext(_dbPath);
        
        var user = new User { Name = "Alice", Age = 30 };
        var id = db.Users.Insert(user);
        
        var found = db.Users.FindById(id);
        Assert.NotNull(found);
        Assert.Equal("Alice", found.Name);
        Assert.Equal(30, found.Age);
    }

    [Fact]
    public void DbContext_MultipleOperations_Work()
    {
        using var db = new TestDbContext(_dbPath);
        
        // Insert
        var alice = new User { Name = "Alice", Age = 30 };
        var bob = new User { Name = "Bob", Age = 25 };
        
        var id1 = db.Users.Insert(alice);
        var id2 = db.Users.Insert(bob);
        
        // FindAll
        var all = db.Users.FindAll().ToList();
        Assert.Equal(2, all.Count);
        
        // Update
        alice.Age = 31;
        Assert.True(db.Users.Update(alice));
        
        var updated = db.Users.FindById(id1);
        Assert.Equal(31, updated!.Age);
        
        // Delete
        Assert.True(db.Users.Delete(id2));
        Assert.Equal(1, db.Users.Count());
    }

    [Fact(Skip = "TODO: Requires persistent _idToPageMap - currently in-memory only")]
    public void DbContext_Dispose_ReleasesResources()
    {
        // First context - use using to ensure proper disposal
        using (var db = new TestDbContext(_dbPath))
        {
            db.Users.Insert(new User { Name = "Test", Age = 20 });
        } // Dispose called here, commits transaction
        
        // Should be able to open again and see persisted data
        // LIMITATION: _idToPageMap is currently in-memory, so data is lost on close
        using var db2 = new TestDbContext(_dbPath);
        Assert.Equal(1, db2.Users.Count());
    }

    [Fact]
    public void DbContext_AutoDerivesWalPath()
    {
        using var db = new TestDbContext(_dbPath);
        db.Users.Insert(new User { Name = "Test", Age = 20 });
        
        var walPath = Path.ChangeExtension(_dbPath, ".wal");
        Assert.True(File.Exists(walPath));
    }

    public void Dispose()
    {
        try
        {
            if (File.Exists(_dbPath)) File.Delete(_dbPath);
            
            var walPath = Path.ChangeExtension(_dbPath, ".wal");
            if (File.Exists(walPath)) File.Delete(walPath);
        }
        catch
        {
            // Ignore cleanup errors
        }
    }
}
