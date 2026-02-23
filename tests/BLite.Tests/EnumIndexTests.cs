using BLite.Bson;
using BLite.Core.Indexing;
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
    public void CreateIndex_On_Enum_Property_Does_Not_Throw()
    {
        // Should not throw any exception
        var idx = _db.EnumEntities.EnsureIndex(e => e.Role, "idx_role_create");
        Assert.NotNull(idx);
    }

    // ------------------------------------------------------------------
    // Seek (exact match)
    // ------------------------------------------------------------------

    [Fact]
    public void Seek_By_Enum_Finds_Inserted_Document()
    {
        var idx = _db.EnumEntities.EnsureIndex(e => e.Role, "idx_role_seek");

        _db.EnumEntities.Insert(MakeEntity(UserRole.Guest, "guest-doc"));
        var adminId = _db.EnumEntities.Insert(MakeEntity(UserRole.Admin, "admin-doc"));
        _db.EnumEntities.Insert(MakeEntity(UserRole.User, "user-doc"));
        _db.SaveChanges();

        var location = idx.Seek(UserRole.Admin);
        Assert.True(location.HasValue, "Seek by enum value should find the document");
    }

    [Fact]
    public void Seek_By_Enum_Returns_Null_For_Missing_Value()
    {
        var idx = _db.EnumEntities.EnsureIndex(e => e.Role, "idx_role_seek_miss");

        _db.EnumEntities.Insert(MakeEntity(UserRole.Guest, "guest-doc"));
        _db.SaveChanges();

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
    public void Range_On_Enum_Index_Uses_Numeric_Ordering()
    {
        var idx = _db.EnumEntities.EnsureIndex(e => e.Role, "idx_role_range");

        _db.EnumEntities.Insert(MakeEntity(UserRole.Guest,     "guest"));     // 0
        _db.EnumEntities.Insert(MakeEntity(UserRole.User,      "user"));      // 1
        _db.EnumEntities.Insert(MakeEntity(UserRole.Moderator, "moderator")); // 2
        _db.EnumEntities.Insert(MakeEntity(UserRole.Admin,     "admin"));     // 3
        _db.SaveChanges();

        // Range [User=1 .. Admin=3] must return 3 documents (User, Moderator, Admin)
        var locations = idx.Range(UserRole.User, UserRole.Admin).ToList();

        Assert.Equal(3, locations.Count);
    }

    [Fact]
    public void Range_Single_Value_Returns_Exactly_One_Doc()
    {
        var idx = _db.EnumEntities.EnsureIndex(e => e.Role, "idx_role_range_single");

        _db.EnumEntities.Insert(MakeEntity(UserRole.Guest, "g1"));
        _db.EnumEntities.Insert(MakeEntity(UserRole.User,  "u1"));
        _db.EnumEntities.Insert(MakeEntity(UserRole.User,  "u2")); // second User
        _db.EnumEntities.Insert(MakeEntity(UserRole.Admin, "a1"));
        _db.SaveChanges();

        var locations = idx.Range(UserRole.User, UserRole.User).ToList();
        Assert.Equal(2, locations.Count); // both User documents
    }

    // ------------------------------------------------------------------
    // byte-underlying enum (Priority)
    // ------------------------------------------------------------------

    [Fact]
    public void CreateIndex_On_Byte_Enum_And_Seek_Works()
    {
        var idx = _db.EnumEntities.EnsureIndex(e => e.Priority, "idx_priority");

        _db.EnumEntities.Insert(new EnumEntity { Priority = Priority.Low,      Label = "low" });
        _db.EnumEntities.Insert(new EnumEntity { Priority = Priority.High,     Label = "high" });
        _db.EnumEntities.Insert(new EnumEntity { Priority = Priority.Critical, Label = "critical" });
        _db.SaveChanges();

        var loc = idx.Seek(Priority.High);
        Assert.True(loc.HasValue);
    }

    [Fact]
    public void Range_On_Byte_Enum_Uses_Numeric_Ordering()
    {
        var idx = _db.EnumEntities.EnsureIndex(e => e.Priority, "idx_priority_range");

        // Priority: Low=0, Normal=1, High=2, Critical=3
        _db.EnumEntities.Insert(new EnumEntity { Priority = Priority.Low,      Label = "a" });
        _db.EnumEntities.Insert(new EnumEntity { Priority = Priority.Normal,   Label = "b" });
        _db.EnumEntities.Insert(new EnumEntity { Priority = Priority.High,     Label = "c" });
        _db.EnumEntities.Insert(new EnumEntity { Priority = Priority.Critical, Label = "d" });
        _db.SaveChanges();

        // Range [Normal=1 .. High=2] → 2 docs
        var locs = idx.Range(Priority.Normal, Priority.High).ToList();
        Assert.Equal(2, locs.Count);
    }

    // ------------------------------------------------------------------
    // long-underlying enum (AuditAction)
    // ------------------------------------------------------------------

    [Fact]
    public void CreateIndex_On_Long_Enum_And_Range_Works()
    {
        var idx = _db.EnumEntities.EnsureIndex(e => e.LastAction, "idx_last_action");

        // AuditAction: None=0, Created=1, Updated=2, Deleted=3, Archived=100
        _db.EnumEntities.Insert(new EnumEntity { LastAction = AuditAction.None,     Label = "n" });
        _db.EnumEntities.Insert(new EnumEntity { LastAction = AuditAction.Created,  Label = "c" });
        _db.EnumEntities.Insert(new EnumEntity { LastAction = AuditAction.Updated,  Label = "u" });
        _db.EnumEntities.Insert(new EnumEntity { LastAction = AuditAction.Deleted,  Label = "d" });
        _db.EnumEntities.Insert(new EnumEntity { LastAction = AuditAction.Archived, Label = "a" });
        _db.SaveChanges();

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
    public void EnsureIndex_Called_Twice_Does_Not_Throw()
    {
        _db.EnumEntities.EnsureIndex(e => e.Role, "idx_role_idempotent");
        var ex = Record.Exception(() => _db.EnumEntities.EnsureIndex(e => e.Role, "idx_role_idempotent"));
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
    public void QueryIndex_Exact_Enum_Returns_Document_With_Correct_Role()
    {
        _db.EnumEntities.EnsureIndex(e => e.Role, "idx_role_query_exact");

        _db.EnumEntities.Insert(MakeEntity(UserRole.Guest,     "guest"));
        _db.EnumEntities.Insert(MakeEntity(UserRole.Admin,     "admin"));
        _db.EnumEntities.Insert(MakeEntity(UserRole.Moderator, "moderator"));
        _db.SaveChanges();

        var results = _db.EnumEntities
            .QueryIndex("idx_role_query_exact", UserRole.Admin, UserRole.Admin)
            .ToList();

        Assert.Single(results);
        Assert.Equal(UserRole.Admin, results[0].Role);
        Assert.Equal("admin", results[0].Label);
    }

    /// <summary>
    /// QueryIndex with a range returns all documents whose Role falls in
    /// [User=1 .. Admin=3], verifying both count and the actual Role values.
    /// </summary>
    [Fact]
    public void QueryIndex_Range_Enum_Returns_Documents_With_Correct_Roles()
    {
        _db.EnumEntities.EnsureIndex(e => e.Role, "idx_role_query_range");

        _db.EnumEntities.Insert(MakeEntity(UserRole.Guest,     "guest"));     // 0 – outside range
        _db.EnumEntities.Insert(MakeEntity(UserRole.User,      "user"));      // 1
        _db.EnumEntities.Insert(MakeEntity(UserRole.Moderator, "moderator")); // 2
        _db.EnumEntities.Insert(MakeEntity(UserRole.Admin,     "admin"));     // 3
        _db.SaveChanges();

        var results = _db.EnumEntities
            .QueryIndex("idx_role_query_range", UserRole.User, UserRole.Admin)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.All(results, e => Assert.True(e.Role >= UserRole.User && e.Role <= UserRole.Admin));
        Assert.DoesNotContain(results, e => e.Role == UserRole.Guest);
    }

    /// <summary>
    /// AsQueryable().Where() with an equality predicate on an indexed enum
    /// property returns only the matching documents with the correct Role.
    /// </summary>
    [Fact]
    public void AsQueryable_Where_On_Indexed_Enum_Returns_Correct_Documents()
    {
        _db.EnumEntities.EnsureIndex(e => e.Role, "idx_role_linq");

        _db.EnumEntities.Insert(MakeEntity(UserRole.Guest,     "g1"));
        _db.EnumEntities.Insert(MakeEntity(UserRole.User,      "u1"));
        _db.EnumEntities.Insert(MakeEntity(UserRole.Moderator, "mod1"));
        _db.EnumEntities.Insert(MakeEntity(UserRole.Admin,     "a1"));
        _db.EnumEntities.Insert(MakeEntity(UserRole.Admin,     "a2")); // second Admin
        _db.SaveChanges();

        var results = _db.EnumEntities
            .AsQueryable()
            .Where(e => e.Role == UserRole.Admin)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.All(results, e => Assert.Equal(UserRole.Admin, e.Role));
    }
}
