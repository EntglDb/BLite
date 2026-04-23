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

    /// <summary>
    /// Regression scenario: two separate DocumentCollection instances that share the same
    /// physical "users" collection (same StorageEngine, same on-disk pages) are created
    /// after a page is partially filled.  Each rebuilds its own in-memory FreeSpaceIndex
    /// from disk; both therefore see page P as having ~6 120 B free (stale after process 1
    /// writes).
    ///
    /// Page = 16 384 B, header = 24 B, SlotEntry = 8 B, BSON overhead ≈ 45 B per User:
    ///   pre-fill  : name = 10 187 chars → doc = 10 232 B → needs 10 240 B → leaves 6 120 B free
    ///   insert doc: name =  3 500 chars → doc =  3 545 B → needs  3 553 B
    ///   after col1 writes: 6 120 − 3 553 = 2 567 B remain — too little for col2 (3 553 > 2 567)
    ///
    /// DESIRED  : both inserts succeed (engine handles the stale FSI gracefully by retrying on
    ///            a newly allocated page).
    /// CURRENT  : the second InsertAsync throws InvalidOperationException("Not enough space …").
    /// </summary>
    [Fact]
    public async Task TwoCollectionInstances_StaleFSI_SecondInsertShouldSucceed()
    {
        const int preFillNameLen = 10000;
        const int insertNameLen  =  400;

        var dbPath = Path.Combine(Path.GetTempPath(), $"test_fsi_stale_{Guid.NewGuid():N}.db");
        try
        {
            using var db = new TestDbContext(dbPath);

            // ── Step 1: fill the "users" data page to ≈10 KB ──────────────────────
            await db.Users.InsertAsync(new User { Name = new string('X', preFillNameLen), Age = 0 });
            await db.SaveChangesAsync();

            // ── Step 3 (process 1): insert 4 KB document via col1 ─────────────────
            // col1 FSI: P = 6120 ≥ 3553 → uses page P → commits.
            // col1 FSI updated: P = 2567.  col2 FSI is STILL stale: P = 6120.
            var col1 = db.Users;
            List<User> docs1 = new List<User>();

            for (int i = 0; i < 15; i++)
            {
                docs1.Add(new User { Name = new string('A', insertNameLen), Age = i });
            }

            var tasks = new List<Task>();

            var col2 = db.ComplexUsers;
            List<ComplexUser> docs2 = new List<ComplexUser>();
            for (int i = 0; i < 15; i++)
            {
                docs2.Add(new ComplexUser { Name = new string('B', insertNameLen) });
            }

            tasks.Add(Task.Run(async () =>
            {
                await Task.Delay(1);
                var id1 = await col1.InsertBulkAsync(docs1);
                return id1;
            }));

            foreach(var doc in docs2)
            {
                tasks.Add(Task.Run(async () =>
                {
                    var id2 = await col2.InsertAsync(doc);
                    return id2;
                }));
            }

            await Task.WhenAll(tasks).ContinueWith(t =>
            {
                Assert.False(t.IsFaulted, $"Second insert failed with exception: {t.Exception}");
            });
            
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            var wal = Path.ChangeExtension(dbPath, ".wal");
            if (File.Exists(wal)) File.Delete(wal);
        }
    }

    public void Dispose()
    {
        _db?.Dispose();
    }
}
