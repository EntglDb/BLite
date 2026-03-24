using BLite.Bson;
using BLite.Core.Indexing;
using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Tests for creating and querying secondary indexes on Enum-typed properties.
///
/// Bug: CollectionSecondaryIndex.ConvertToIndexKey received a boxed enum value
/// (typed as the enum type, NOT the underlying int/long) which fell through all
/// typed patterns and ended up stored as a UTF-8 string (e.g. "Admin", "Guest").
/// This broke range queries because string ordering != numeric ordering.
///
/// Fix: an extra pattern `_ when value.GetType().IsEnum => new IndexKey(Convert.ToInt64(value))`
/// ensures enum values are always stored as their numeric equivalent.
/// </summary>
public class EnumIndexTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public EnumIndexTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_enum_index_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private EnumEntity MakeEntity(UserRole role, string label) => new EnumEntity
    {
        Role = role,
        Status = null,
        Priority = Priority.Normal,
        LastAction = AuditAction.None,
        Permissions = Permissions.None,
        Label = label
    };

    // ------------------------------------------------------------------
    // Creation / EnsureIndex
    // ------------------------------------------------------------------

    [Fact]
    public async Task CreateIndex_On_Enum_Property_Does_Not_Throw()
    {
        // Should not throw any exception
        var idx = await _db.EnumEntities.EnsureIndexAsync(e => e.Role, "idx_role_create");
        Assert.NotNull(idx);
    }

    // ------------------------------------------------------------------
    // Seek (exact match)
    // ------------------------------------------------------------------

    [Fact]
    public async Task Seek_By_Enum_Finds_Inserted_Document()
    {
        var idx = (CollectionSecondaryIndex<ObjectId, EnumEntity>)await _db.EnumEntities.EnsureIndexAsync(e => e.Role, "idx_role_seek");

        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Guest, "guest-doc"));
        var adminId = await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Admin, "admin-doc"));
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.User, "user-doc"));
        await _db.SaveChangesAsync();

        var location = idx.Seek(UserRole.Admin);
        Assert.True(location.HasValue, "Seek by enum value should find the document");
    }

    [Fact]
    public async Task Seek_By_Enum_Returns_Null_For_Missing_Value()
    {
        var idx = (CollectionSecondaryIndex<ObjectId, EnumEntity>)await _db.EnumEntities.EnsureIndexAsync(e => e.Role, "idx_role_seek_miss");

        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Guest, "guest-doc"));
        await _db.SaveChangesAsync();

        // Admin was never inserted
        var location = idx.Seek(UserRole.Admin);
        Assert.False(location.HasValue, "Seek should return null for a value not present in the index");
    }

    // ------------------------------------------------------------------
    // Range (numeric ordering must hold)
    // ------------------------------------------------------------------

    /// <summary>
    /// This is the definitive regression test.
    ///
    /// UserRole values: Guest=0, User=1, Moderator=2, Admin=3.
    ///
    /// With the BUG (string keys):
    ///   Alphabetic order: "Admin" < "Guest" < "Moderator" < "User"
    ///   Range(User, Admin) → min="User" > max="Admin" → 0 results (wrong).
    ///
    /// With the FIX (long keys):
    ///   Numeric order: 0 < 1 < 2 < 3
    ///   Range(User=1, Admin=3) → finds User, Moderator, Admin → 3 results (correct).
    /// </summary>
    [Fact]
    public async Task Range_On_Enum_Index_Uses_Numeric_Ordering()
    {
        var idx = (CollectionSecondaryIndex<ObjectId, EnumEntity>)await _db.EnumEntities.EnsureIndexAsync(e => e.Role, "idx_role_range");

        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Guest,     "guest"));     // 0
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.User,      "user"));      // 1
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Moderator, "moderator")); // 2
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Admin,     "admin"));     // 3
        await _db.SaveChangesAsync();

        // Range [User=1 .. Admin=3] must return 3 documents (User, Moderator, Admin)
        var locations = idx.Range(UserRole.User, UserRole.Admin).ToList();

        Assert.Equal(3, locations.Count);
    }

    [Fact]
    public async Task Range_Single_Value_Returns_Exactly_One_Doc()
    {
        var idx = (CollectionSecondaryIndex<ObjectId, EnumEntity>)await _db.EnumEntities.EnsureIndexAsync(e => e.Role, "idx_role_range_single");

        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Guest, "g1"));
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.User,  "u1"));
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.User,  "u2")); // second User
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Admin, "a1"));
        await _db.SaveChangesAsync();

        var locations = idx.Range(UserRole.User, UserRole.User).ToList();
        Assert.Equal(2, locations.Count); // both User documents
    }

    // ------------------------------------------------------------------
    // byte-underlying enum (Priority)
    // ------------------------------------------------------------------

    [Fact]
    public async Task CreateIndex_On_Byte_Enum_And_Seek_Works()
    {
        var idx = (CollectionSecondaryIndex<ObjectId, EnumEntity>) await _db.EnumEntities.EnsureIndexAsync(e => e.Priority, "idx_priority");

        await _db.EnumEntities.InsertAsync(new EnumEntity { Priority = Priority.Low,      Label = "low" });
        await _db.EnumEntities.InsertAsync(new EnumEntity { Priority = Priority.High,     Label = "high" });
        await _db.EnumEntities.InsertAsync(new EnumEntity { Priority = Priority.Critical, Label = "critical" });
        await _db.SaveChangesAsync();

        var loc = idx.Seek(Priority.High);
        Assert.True(loc.HasValue);
    }

    [Fact]
    public async Task Range_On_Byte_Enum_Uses_Numeric_Ordering()
    {
        var idx = (CollectionSecondaryIndex<ObjectId, EnumEntity>) await _db.EnumEntities.EnsureIndexAsync(e => e.Priority, "idx_priority_range");

        // Priority: Low=0, Normal=1, High=2, Critical=3
        await _db.EnumEntities.InsertAsync(new EnumEntity { Priority = Priority.Low,      Label = "a" });
        await _db.EnumEntities.InsertAsync(new EnumEntity { Priority = Priority.Normal,   Label = "b" });
        await _db.EnumEntities.InsertAsync(new EnumEntity { Priority = Priority.High,     Label = "c" });
        await _db.EnumEntities.InsertAsync(new EnumEntity { Priority = Priority.Critical, Label = "d" });
        await _db.SaveChangesAsync();
        // Range [Normal=1 .. High=2] → 2 docs
        var locs = idx.Range(Priority.Normal, Priority.High).ToList();
        Assert.Equal(2, locs.Count);
    }

    // ------------------------------------------------------------------
    // long-underlying enum (AuditAction)
    // ------------------------------------------------------------------

    [Fact]
    public async Task CreateIndex_On_Long_Enum_And_Range_Works()
    {
        var idx = (CollectionSecondaryIndex<ObjectId, EnumEntity>) await _db.EnumEntities.EnsureIndexAsync(e => e.LastAction, "idx_last_action");

        // AuditAction: None=0, Created=1, Updated=2, Deleted=3, Archived=100
        await _db.EnumEntities.InsertAsync(new EnumEntity { LastAction = AuditAction.None,     Label = "n" });
        await _db.EnumEntities.InsertAsync(new EnumEntity { LastAction = AuditAction.Created,  Label = "c" });
        await _db.EnumEntities.InsertAsync(new EnumEntity { LastAction = AuditAction.Updated,  Label = "u" });
        await _db.EnumEntities.InsertAsync(new EnumEntity { LastAction = AuditAction.Deleted,  Label = "d" });
        await _db.EnumEntities.InsertAsync(new EnumEntity { LastAction = AuditAction.Archived, Label = "a" });
        await _db.SaveChangesAsync();

        // Range [Created=1 .. Deleted=3] → 3 docs
        var locs = idx.Range(AuditAction.Created, AuditAction.Deleted).ToList();
        Assert.Equal(3, locs.Count);

        // Archived=100 must be outside that range
        var archivedLoc = idx.Seek(AuditAction.Archived);
        Assert.True(archivedLoc.HasValue);
        Assert.DoesNotContain(locs, l => l.PageId == archivedLoc!.Value.PageId && l.SlotIndex == archivedLoc.Value.SlotIndex);
    }

    // ------------------------------------------------------------------
    // EnsureIndex is idempotent
    // ------------------------------------------------------------------

    [Fact]
    public async Task EnsureIndex_Called_Twice_Does_Not_Throw()
    {
        await _db.EnumEntities.EnsureIndexAsync(e => e.Role, "idx_role_idempotent");
        var ex = await Record.ExceptionAsync(() => _db.EnumEntities.EnsureIndexAsync(e => e.Role, "idx_role_idempotent"));
        Assert.Null(ex);
    }

    // ------------------------------------------------------------------
    // Query – full document retrieval via index
    // ------------------------------------------------------------------

    /// <summary>
    /// QueryIndex with equal min/max returns the single matching document
    /// and its deserialized Role property is correct.
    /// </summary>
    [Fact]
    public async Task QueryIndex_Exact_Enum_Returns_Document_With_Correct_Role()
    {
        await _db.EnumEntities.EnsureIndexAsync(e => e.Role, "idx_role_query_exact");

        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Guest,     "guest"));
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Admin,     "admin"));
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Moderator, "moderator"));
        await _db.SaveChangesAsync();

        var results = await _db.EnumEntities
            .QueryIndexAsync("idx_role_query_exact", UserRole.Admin, UserRole.Admin)
            .ToListAsync();
        Assert.Single(results);
        Assert.Equal(UserRole.Admin, results[0].Role);
        Assert.Equal("admin", results[0].Label);
    }

    /// <summary>
    /// QueryIndex with a range returns all documents whose Role falls in
    /// [User=1 .. Admin=3], verifying both count and the actual Role values.
    /// </summary>
    [Fact]
    public async Task QueryIndex_Range_Enum_Returns_Documents_With_Correct_Roles()
    {
        await _db.EnumEntities.EnsureIndexAsync(e => e.Role, "idx_role_query_range");

        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Guest,     "guest"));     // 0 – outside range
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.User,      "user"));      // 1
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Moderator, "moderator")); // 2
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Admin,     "admin"));     // 3
        await _db.SaveChangesAsync();

        var results = await _db.EnumEntities
            .QueryIndexAsync("idx_role_query_range", UserRole.User, UserRole.Admin)
            .ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.All(results, e => Assert.True(e.Role >= UserRole.User && e.Role <= UserRole.Admin));
        Assert.DoesNotContain(results, e => e.Role == UserRole.Guest);
    }

    /// <summary>
    /// AsQueryable().Where() with an equality predicate on an indexed enum
    /// property returns only the matching documents with the correct Role.
    /// </summary>
    [Fact]
    public async Task AsQueryable_Where_On_Indexed_Enum_Returns_Correct_Documents()
    {
        await _db.EnumEntities.EnsureIndexAsync(e => e.Role, "idx_role_linq");

        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Guest,     "g1"));
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.User,      "u1"));
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Moderator, "mod1"));
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Admin,     "a1"));
        await _db.EnumEntities.InsertAsync(MakeEntity(UserRole.Admin,     "a2")); // second Admin
        await _db.SaveChangesAsync();
        var results = await _db.EnumEntities
            .AsQueryable()
            .Where(e => e.Role == UserRole.Admin)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, e => Assert.Equal(UserRole.Admin, e.Role));
    }
}
