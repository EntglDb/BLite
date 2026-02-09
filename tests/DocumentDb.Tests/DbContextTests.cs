using DocumentDb.Bson;
using System.Security.Cryptography;

namespace DocumentDb.Tests;

public class DbContextTests : IDisposable
{
    private string _dbPath;
    
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

    [Fact]
    public void DbContext_Dispose_ReleasesResources()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_dbcontext_reopen.db");
        var totalUsers = 0;
        // First context - insert and dispose (auto-checkpoint)
        using (var db = new TestDbContext(_dbPath))
        {
            db.Users.Insert(new User { Name = "Test", Age = 20 });
            var beforeCheckpointTotalUsers = db.Users.FindAll().Count();
            db._storage.Checkpoint(); // Force checkpoint to ensure data is persisted to main file
            totalUsers = db.Users.FindAll().Count();
            var countedUsers = db.Users.Count();
            Assert.Equal(beforeCheckpointTotalUsers, totalUsers);
        } // Dispose → Commit → Checkpoint → Write to PageFile
        
        // Should be able to open again and see persisted data
        using var db2 = new TestDbContext(_dbPath);
        
        Assert.Equal(1, totalUsers);
        Assert.Equal(totalUsers, db2.Users.FindAll().Count());
        Assert.Equal(totalUsers, db2.Users.Count());
    }
    private static string ComputeFileHash(string path)
    {
        using var stream = File.OpenRead(path);
        using var sha256 = SHA256.Create();
        return Convert.ToHexString(sha256.ComputeHash(stream));
    }

    [Fact]
    public void DatabaseFile_SizeAndContent_ChangeAfterInsert()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_dbfile_{Guid.NewGuid()}.db");

        // 1. Crea e chiudi database vuoto
        using (var db = new TestDbContext(dbPath))
        {
            db.Users.Insert(new User { Name = "Pippo", Age = 42 });
        }
        var initialSize = new FileInfo(dbPath).Length;
        var initialHash = ComputeFileHash(dbPath);

        // 2. Riapri, inserisci, chiudi
        using (var db = new TestDbContext(dbPath))
        {
            db.Users.Insert(new User { Name = "Test", Age = 42 });
            db._storage.Checkpoint(); // Forza persistenza
        }
        var afterInsertSize = new FileInfo(dbPath).Length;
        var afterInsertHash = ComputeFileHash(dbPath);

        // 3. Verifica che dimensione e hash siano cambiati
        Assert.NotEqual(initialSize, afterInsertSize);
        Assert.NotEqual(initialHash, afterInsertHash);
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
