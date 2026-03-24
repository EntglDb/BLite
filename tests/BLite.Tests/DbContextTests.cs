using BLite.Shared;
using System.Security.Cryptography;

namespace BLite.Tests;

public class DbContextTests : IDisposable
{
    private string _dbPath;
    
    public DbContextTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_dbcontext_{Guid.NewGuid()}.db");
    }

    [Fact]
    public async Task DbContext_BasicLifecycle_Works()
    {
        using var db = new TestDbContext(_dbPath);
        
        var user = new User { Name = "Alice", Age = 30 };
        var id = await db.Users.InsertAsync(user);
        
        var found = await db.Users.FindByIdAsync(id);
        Assert.NotNull(found);
        Assert.Equal("Alice", found.Name);
        Assert.Equal(30, found.Age);
    }

    [Fact]
    public async Task DbContext_MultipleOperations_Work()
    {
        using var db = new TestDbContext(_dbPath);
        
        // Insert
        var alice = new User { Name = "Alice", Age = 30 };
        var bob = new User { Name = "Bob", Age = 25 };
        
        var id1 = await db.Users.InsertAsync(alice);
        var id2 = await db.Users.InsertAsync(bob);
        await db.SaveChangesAsync();
        
        // FindAll
        var all = (await db.Users.FindAllAsync().ToListAsync());
        Assert.Equal(2, all.Count);
        
        // UpdateAsync
        alice.Age = 31;
        Assert.True(await db.Users.UpdateAsync(alice));
        
        var updated = await db.Users.FindByIdAsync(id1);
        Assert.Equal(31, updated!.Age);
        
        // Delete
        Assert.True(await db.Users.DeleteAsync(id2));
        Assert.Equal(1, await db.Users.CountAsync());
    }

    [Fact]
    public async Task DbContext_Dispose_ReleasesResources()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_dbcontext_reopen_{Guid.NewGuid()}.db");
        var totalUsers = 0;
        // First context - insert and dispose (auto-checkpoint)
        using (var db = new TestDbContext(_dbPath))
        {
            await db.Users.InsertAsync(new User { Name = "Test", Age = 20 });
            await db.SaveChangesAsync(); // Explicitly save changes to ensure data is in WAL
            var beforeCheckpointTotalUsers = (await db.Users.FindAllAsync().ToListAsync()).Count;
            db.ForceCheckpoint(); // Force checkpoint to ensure data is persisted to main file
            totalUsers = (await db.Users.FindAllAsync().ToListAsync()).Count;
            var countedUsers = (await db.Users.CountAsync());
            Assert.Equal(beforeCheckpointTotalUsers, totalUsers);
        } // Dispose → Commit → ForceCheckpoint → Write to PageFile
        
        // Should be able to open again and see persisted data
        using var db2 = new TestDbContext(_dbPath);
        
        Assert.Equal(1, totalUsers);
        Assert.Equal(totalUsers, (await db2.Users.FindAllAsync().ToListAsync()).Count);
        Assert.Equal(totalUsers, await db2.Users.CountAsync());
    }
    private static string ComputeFileHash(string path)
    {
        using var stream = File.OpenRead(path);
        using var sha256 = SHA256.Create();
        return Convert.ToHexString(sha256.ComputeHash(stream));
    }

    [Fact]
    public async Task DatabaseFile_SizeAndContent_ChangeAfterInsert()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_dbfile_{Guid.NewGuid()}.db");

        // 1. Crea e chiudi database vuoto
        using (var db = new TestDbContext(dbPath))
        {
            await db.Users.InsertAsync(new User { Name = "Pippo", Age = 42 });
        }
        var initialSize = new FileInfo(dbPath).Length;
        var initialHash = ComputeFileHash(dbPath);

        // 2. Riapri, inserisci, chiudi
        using (var db = new TestDbContext(dbPath))
        {
            await db.Users.InsertAsync(new User { Name = "Test", Age = 42 });
            db.ForceCheckpoint(); // Forza persistenza
        }
        var afterInsertSize = new FileInfo(dbPath).Length;
        var afterInsertHash = ComputeFileHash(dbPath);

        // 3. Verifica che dimensione e hash siano cambiati
        Assert.NotEqual(initialSize, afterInsertSize);
        Assert.NotEqual(initialHash, afterInsertHash);
    }

    [Fact]
    public async Task DbContext_AutoDerivesWalPath()
    {
        using var db = new TestDbContext(_dbPath);
        await db.Users.InsertAsync(new User { Name = "Test", Age = 20 });
        
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
