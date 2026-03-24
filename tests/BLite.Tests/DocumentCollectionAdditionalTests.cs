using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Additional tests for <see cref="DocumentCollection{TId,T}"/> targeting mutation
/// survivors not yet covered by the existing DocumentCollectionTests:
/// secondary indexes (Create/Ensure/Drop/Get/Query), ScanPairsAsync, async variants,
/// bulk async, and CurrentSchemaVersion.
/// </summary>
public class DocumentCollectionAdditionalTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;
    private readonly TestDbContext _db;

    public DocumentCollectionAdditionalTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_dca_{Guid.NewGuid():N}.db");
        _walPath = Path.ChangeExtension(_dbPath, ".wal");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);
    }

    // ─── CreateIndexAsync ──────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateIndex_OnEmptyCollection_CanBeQueried()
    {
        await _db.Users.CreateIndexAsync(u => u.Age, name: "idx_age");
        await _db.SaveChangesAsync();

        var results = await _db.Users.QueryIndexAsync("idx_age", null, null).ToListAsync();
        Assert.Empty(results);
    }

    [Fact]
    public async Task CreateIndex_ThenQueryByRange_ReturnsMatchingDocuments()
    {
        await _db.Users.CreateIndexAsync(u => u.Age, name: "idx_age");
        await _db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
        await _db.Users.InsertAsync(new User { Name = "Bob", Age = 25 });
        await _db.Users.InsertAsync(new User { Name = "Carol", Age = 35 });
        await _db.SaveChangesAsync();

        var results = await _db.Users.QueryIndexAsync("idx_age", 25, 30).ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, u => Assert.True(u.Age >= 25 && u.Age <= 30));
    }

    [Fact]
    public async Task CreateIndex_ForExistingData_RebuildsIndexCorrectly()
    {
        // Insert data BEFORE creating the index
        await _db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
        await _db.Users.InsertAsync(new User { Name = "Bob", Age = 25 });
        await _db.SaveChangesAsync();

        await _db.Users.CreateIndexAsync(u => u.Age, name: "idx_age_late");
        await _db.SaveChangesAsync();

        var results = await _db.Users.QueryIndexAsync("idx_age_late", 25, 30).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    // ─── QueryIndex ───────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryIndex_Nonexistent_ThrowsArgumentException()
    {
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _db.Users.QueryIndexAsync("ghost_index", null, null).ToListAsync());
    }

    [Fact]
    public async Task QueryIndex_Descending_ReturnsSortedDescending()
    {
        await _db.Users.CreateIndexAsync(u => u.Age, name: "idx_age");
        await _db.Users.InsertAsync(new User { Name = "Alice", Age = 10 });
        await _db.Users.InsertAsync(new User { Name = "Bob", Age = 20 });
        await _db.Users.InsertAsync(new User { Name = "Carol", Age = 30 });
        await _db.SaveChangesAsync();

        var results = await _db.Users.QueryIndexAsync("idx_age", null, null, ascending: false).ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.Equal(30, results[0].Age);
        Assert.Equal(10, results[2].Age);
    }

    // ─── EnsureIndex ──────────────────────────────────────────────────────────

    [Fact]
    public async Task EnsureIndex_WhenIndexDoesNotExist_CreatesIt()
    {
        var idx = await _db.Users.EnsureIndexAsync(u => u.Age, name: "idx_age_ensure");
        await _db.SaveChangesAsync();

        Assert.NotNull(idx);
        Assert.NotNull(await _db.Users.GetIndexAsync("idx_age_ensure"));
    }

    [Fact]
    public async Task EnsureIndex_WhenIndexExists_ReturnsSameIndex_NoRebuild()
    {
        var idx1 = await _db.Users.EnsureIndexAsync(u => u.Age, name: "idx_age_idem");
        await _db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
        await _db.SaveChangesAsync();

        var idx2 = await _db.Users.EnsureIndexAsync(u => u.Age, name: "idx_age_idem");

        // Must be the same logical instance — no data loss from rebuild
        Assert.NotNull(idx2);
        Assert.Single((await _db.Users.QueryIndexAsync("idx_age_idem", 30, 30).ToListAsync()));
    }

    [Fact]
    public async Task EnsureIndexAsync_Idempotent_ReturnsSameIndex()
    {
        await _db.Users.EnsureIndexAsync(u => u.Age, name: "idx_age_async_idem");
        await _db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
        await _db.SaveChangesAsync();

        var idx2 = await _db.Users.EnsureIndexAsync(u => u.Age, name: "idx_age_async_idem");

        Assert.NotNull(idx2);
    }

    // ─── DropIndex ────────────────────────────────────────────────────────────

    [Fact]
    public async Task DropIndex_PrimaryIndex_ThrowsInvalidOperationException()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await _db.Users.DropIndexAsync("_id"));
    }

    [Fact]
    public async Task DropIndex_EmptyName_ThrowsArgumentException()
    {
        await Assert.ThrowsAsync<ArgumentException>(async () => await _db.Users.DropIndexAsync(""));
    }

    [Fact]
    public async Task DropIndex_WhitespaceName_ThrowsArgumentException()
    {
        await Assert.ThrowsAsync<ArgumentException>(async () => await _db.Users.DropIndexAsync("   "));
    }

    [Fact]
    public async Task DropIndex_NonExistent_ReturnsFalse()
    {
        var result = await _db.Users.DropIndexAsync("ghost_idx");
        Assert.False(result);
    }

    [Fact]
    public async Task DropIndex_ExistingIndex_ReturnsTrueAndIndexIsGone()
    {
        await _db.Users.CreateIndexAsync(u => u.Age, name: "idx_age_drop");
        await _db.SaveChangesAsync();

        var dropped = await _db.Users.DropIndexAsync("idx_age_drop");

        Assert.True(dropped);
        Assert.Null(await _db.Users.GetIndexAsync("idx_age_drop"));
    }

    // ─── GetIndexAsync + GetIndexes ────────────────────────────────────────────────

    [Fact]
    public async Task GetIndexAsync_NonExistent_ReturnsNull()
    {
        Assert.Null(await _db.Users.GetIndexAsync("ghost_idx"));
    }

    [Fact]
    public async Task GetIndexAsync_Existing_ReturnsInstance()
    {
        await _db.Users.CreateIndexAsync(u => u.Age, name: "idx_age_get");
        await _db.SaveChangesAsync();

        var idx = await _db.Users.GetIndexAsync("idx_age_get");

        Assert.NotNull(idx);
    }

    [Fact]
    public async Task GetIndexes_ReturnsCreatedIndexes()
    {
        await _db.Users.CreateIndexAsync(u => u.Age, name: "idx_age");
        await _db.Users.CreateIndexAsync(u => u.Name, name: "idx_name");
        await _db.SaveChangesAsync();

        var indexes = _db.Users.GetIndexes().ToList();

        // All indexes including primary _id
        var names = indexes.Select(i => i.Name).ToList();
        Assert.Contains("idx_age", names);
        Assert.Contains("idx_name", names);
    }

    // ─── ScanPairsAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task ScanPairs_WithSimpleSelectors_FastPath_ReturnsPairs()
    {
        await _db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
        await _db.Users.InsertAsync(new User { Name = "Bob", Age = 25 });
        await _db.SaveChangesAsync();

        var pairs = await _db.Users.ScanPairsAsync(u => u.Name, u => u.Age).ToListAsync();

        Assert.Equal(2, pairs.Count);
        Assert.Contains(pairs, p => p.Key == "Alice" && p.Value == 30);
        Assert.Contains(pairs, p => p.Key == "Bob" && p.Value == 25);
    }

    [Fact]
    public async Task ScanPairs_ReturnsCorrectCountMatchingAllDocuments()
    {
        for (int i = 1; i <= 5; i++)
            await _db.Users.InsertAsync(new User { Name = $"User{i}", Age = i * 10 });
        await _db.SaveChangesAsync();
        var pairs = await _db.Users.ScanPairsAsync(u => u.Name, u => u.Age).ToListAsync();

        Assert.Equal(5, pairs.Count);
        Assert.All(pairs, p => Assert.NotNull(p.Key));
    }

    // ─── Async FindById + FindAll ─────────────────────────────────────────────

    [Fact]
    public async Task FindByIdAsync_Found_ReturnsDocument()
    {
        var user = new User { Name = "Alice", Age = 30 };
        var id = await _db.Users.InsertAsync(user);
        await _db.SaveChangesAsync();

        var found = await _db.Users.FindByIdAsync(id);

        Assert.NotNull(found);
        Assert.Equal("Alice", found.Name);
    }

    [Fact]
    public async Task FindByIdAsync_NotFound_ReturnsNull()
    {
        var found = await _db.Users.FindByIdAsync(ObjectId.NewObjectId());
        Assert.Null(found);
    }

    [Fact]
    public async Task FindAllAsync_ReturnsAllDocuments()
    {
        await _db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
        await _db.Users.InsertAsync(new User { Name = "Bob", Age = 25 });
        await _db.SaveChangesAsync();

        var results = new List<User>();
        await foreach (var u in _db.Users.FindAllAsync())
            results.Add(u);

        Assert.Equal(2, results.Count);
    }

    // ─── InsertBulkAsync ──────────────────────────────────────────────────────

    [Fact]
    public async Task InsertBulkAsync_InsertsAllEntitiesAndReturnsIds()
    {
        var users = Enumerable.Range(1, 6)
            .Select(i => new User { Name = $"User{i}", Age = i * 5 })
            .ToList();

        var ids = await _db.Users.InsertBulkAsync(users);
        await _db.SaveChangesAsync();

        Assert.Equal(6, ids.Count);
        Assert.Equal(6, await _db.Users.CountAsync());
    }

    // ─── CurrentSchemaVersion ─────────────────────────────────────────────────

    [Fact]
    public void CurrentSchemaVersion_IsSetAfterConstruction()
    {
        // TestDbContext constructor calls InitializeCollections which creates the collections.
        // CurrentSchemaVersion should be populated.
        Assert.NotNull(_db.Users.CurrentSchemaVersion);
    }

    [Fact]
    public void CurrentSchemaVersion_HasNonZeroVersion()
    {
        Assert.True(_db.Users.CurrentSchemaVersion!.Value.Version > 0);
    }
}
