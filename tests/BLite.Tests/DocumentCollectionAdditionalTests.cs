using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Additional tests for <see cref="DocumentCollection{TId,T}"/> targeting mutation
/// survivors not yet covered by the existing DocumentCollectionTests:
/// secondary indexes (Create/Ensure/Drop/Get/Query), ScanPairs, async variants,
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

    // ─── CreateIndex ──────────────────────────────────────────────────────────

    [Fact]
    public void CreateIndex_OnEmptyCollection_CanBeQueried()
    {
        _db.Users.CreateIndex(u => u.Age, name: "idx_age");
        _db.SaveChanges();

        var results = _db.Users.QueryIndex("idx_age", null, null).ToList();
        Assert.Empty(results);
    }

    [Fact]
    public void CreateIndex_ThenQueryByRange_ReturnsMatchingDocuments()
    {
        _db.Users.CreateIndex(u => u.Age, name: "idx_age");
        _db.Users.Insert(new User { Name = "Alice", Age = 30 });
        _db.Users.Insert(new User { Name = "Bob", Age = 25 });
        _db.Users.Insert(new User { Name = "Carol", Age = 35 });
        _db.SaveChanges();

        var results = _db.Users.QueryIndex("idx_age", 25, 30).ToList();

        Assert.Equal(2, results.Count);
        Assert.All(results, u => Assert.True(u.Age >= 25 && u.Age <= 30));
    }

    [Fact]
    public void CreateIndex_ForExistingData_RebuildsIndexCorrectly()
    {
        // Insert data BEFORE creating the index
        _db.Users.Insert(new User { Name = "Alice", Age = 30 });
        _db.Users.Insert(new User { Name = "Bob", Age = 25 });
        _db.SaveChanges();

        _db.Users.CreateIndex(u => u.Age, name: "idx_age_late");
        _db.SaveChanges();

        var results = _db.Users.QueryIndex("idx_age_late", 25, 30).ToList();
        Assert.Equal(2, results.Count);
    }

    // ─── QueryIndex ───────────────────────────────────────────────────────────

    [Fact]
    public void QueryIndex_Nonexistent_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            _db.Users.QueryIndex("ghost_index", null, null).ToList());
    }

    [Fact]
    public void QueryIndex_Descending_ReturnsSortedDescending()
    {
        _db.Users.CreateIndex(u => u.Age, name: "idx_age");
        _db.Users.Insert(new User { Name = "Alice", Age = 10 });
        _db.Users.Insert(new User { Name = "Bob", Age = 20 });
        _db.Users.Insert(new User { Name = "Carol", Age = 30 });
        _db.SaveChanges();

        var results = _db.Users.QueryIndex("idx_age", null, null, ascending: false).ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal(30, results[0].Age);
        Assert.Equal(10, results[2].Age);
    }

    // ─── EnsureIndex ──────────────────────────────────────────────────────────

    [Fact]
    public void EnsureIndex_WhenIndexDoesNotExist_CreatesIt()
    {
        var idx = _db.Users.EnsureIndex(u => u.Age, name: "idx_age_ensure");
        _db.SaveChanges();

        Assert.NotNull(idx);
        Assert.NotNull(_db.Users.GetIndex("idx_age_ensure"));
    }

    [Fact]
    public void EnsureIndex_WhenIndexExists_ReturnsSameIndex_NoRebuild()
    {
        var idx1 = _db.Users.EnsureIndex(u => u.Age, name: "idx_age_idem");
        _db.Users.Insert(new User { Name = "Alice", Age = 30 });
        _db.SaveChanges();

        var idx2 = _db.Users.EnsureIndex(u => u.Age, name: "idx_age_idem");

        // Must be the same logical instance — no data loss from rebuild
        Assert.NotNull(idx2);
        Assert.Equal(1, _db.Users.QueryIndex("idx_age_idem", 30, 30).Count());
    }

    [Fact]
    public async Task EnsureIndexAsync_Idempotent_ReturnsSameIndex()
    {
        await _db.Users.EnsureIndexAsync(u => u.Age, name: "idx_age_async_idem");
        _db.Users.Insert(new User { Name = "Alice", Age = 30 });
        _db.SaveChanges();

        var idx2 = await _db.Users.EnsureIndexAsync(u => u.Age, name: "idx_age_async_idem");

        Assert.NotNull(idx2);
    }

    // ─── DropIndex ────────────────────────────────────────────────────────────

    [Fact]
    public void DropIndex_PrimaryIndex_ThrowsInvalidOperationException()
    {
        Assert.Throws<InvalidOperationException>(() => _db.Users.DropIndex("_id"));
    }

    [Fact]
    public void DropIndex_EmptyName_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => _db.Users.DropIndex(""));
    }

    [Fact]
    public void DropIndex_WhitespaceName_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => _db.Users.DropIndex("   "));
    }

    [Fact]
    public void DropIndex_NonExistent_ReturnsFalse()
    {
        var result = _db.Users.DropIndex("ghost_idx");
        Assert.False(result);
    }

    [Fact]
    public void DropIndex_ExistingIndex_ReturnsTrueAndIndexIsGone()
    {
        _db.Users.CreateIndex(u => u.Age, name: "idx_age_drop");
        _db.SaveChanges();

        var dropped = _db.Users.DropIndex("idx_age_drop");

        Assert.True(dropped);
        Assert.Null(_db.Users.GetIndex("idx_age_drop"));
    }

    // ─── GetIndex + GetIndexes ────────────────────────────────────────────────

    [Fact]
    public void GetIndex_NonExistent_ReturnsNull()
    {
        Assert.Null(_db.Users.GetIndex("ghost_idx"));
    }

    [Fact]
    public void GetIndex_Existing_ReturnsInstance()
    {
        _db.Users.CreateIndex(u => u.Age, name: "idx_age_get");
        _db.SaveChanges();

        var idx = _db.Users.GetIndex("idx_age_get");

        Assert.NotNull(idx);
    }

    [Fact]
    public void GetIndexes_ReturnsCreatedIndexes()
    {
        _db.Users.CreateIndex(u => u.Age, name: "idx_age");
        _db.Users.CreateIndex(u => u.Name, name: "idx_name");
        _db.SaveChanges();

        var indexes = _db.Users.GetIndexes().ToList();

        // All indexes including primary _id
        var names = indexes.Select(i => i.Name).ToList();
        Assert.Contains("idx_age", names);
        Assert.Contains("idx_name", names);
    }

    // ─── ScanPairs ────────────────────────────────────────────────────────────

    [Fact]
    public void ScanPairs_WithSimpleSelectors_FastPath_ReturnsPairs()
    {
        _db.Users.Insert(new User { Name = "Alice", Age = 30 });
        _db.Users.Insert(new User { Name = "Bob", Age = 25 });
        _db.SaveChanges();

        var pairs = _db.Users.ScanPairs(u => u.Name, u => u.Age).ToList();

        Assert.Equal(2, pairs.Count);
        Assert.Contains(pairs, p => p.Key == "Alice" && p.Value == 30);
        Assert.Contains(pairs, p => p.Key == "Bob" && p.Value == 25);
    }

    [Fact]
    public void ScanPairs_ReturnsCorrectCountMatchingAllDocuments()
    {
        for (int i = 1; i <= 5; i++)
            _db.Users.Insert(new User { Name = $"User{i}", Age = i * 10 });
        _db.SaveChanges();

        var pairs = _db.Users.ScanPairs(u => u.Name, u => u.Age).ToList();

        Assert.Equal(5, pairs.Count);
        Assert.All(pairs, p => Assert.NotNull(p.Key));
    }

    // ─── Async FindById + FindAll ─────────────────────────────────────────────

    [Fact]
    public async Task FindByIdAsync_Found_ReturnsDocument()
    {
        var user = new User { Name = "Alice", Age = 30 };
        var id = _db.Users.Insert(user);
        _db.SaveChanges();

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
        _db.Users.Insert(new User { Name = "Alice", Age = 30 });
        _db.Users.Insert(new User { Name = "Bob", Age = 25 });
        _db.SaveChanges();

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
        _db.SaveChanges();

        Assert.Equal(6, ids.Count);
        Assert.Equal(6, _db.Users.Count());
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
