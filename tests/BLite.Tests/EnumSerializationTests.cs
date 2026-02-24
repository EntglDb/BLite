using BLite.Core;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Tests for enum serialization / deserialization via the Source Generator.
/// Covers: default (int) enums, byte/long underlying types, nullable enums,
/// [Flags] enums, enum collections (List&lt;T&gt; and arrays), and cross-path
/// compatibility (typed DbContext → dynamic BLiteEngine).
/// </summary>
public class EnumSerializationTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public EnumSerializationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_enum_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var walPath = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(walPath)) File.Delete(walPath);
    }

    // ------------------------------------------------------------------
    // Basic round-trip
    // ------------------------------------------------------------------

    [Fact]
    public void Insert_And_FindById_Preserves_Enum_Values()
    {
        // Arrange
        var entity = new EnumEntity
        {
            Role = UserRole.Admin,
            Status = OrderStatus.Shipped,
            Priority = Priority.High,
            FallbackPriority = Priority.Low,
            LastAction = AuditAction.Updated,
            Permissions = Permissions.Read | Permissions.Write,
            AssignableRoles = new List<UserRole> { UserRole.User, UserRole.Moderator },
            StatusHistory = new[] { OrderStatus.Pending, OrderStatus.Processing, OrderStatus.Shipped },
            Label = "full-test"
        };

        // Act
        var id = _db.EnumEntities.Insert(entity);
        _db.SaveChanges();
        var found = _db.EnumEntities.FindById(id);

        // Assert
        Assert.NotNull(found);
        Assert.Equal(UserRole.Admin, found.Role);
        Assert.Equal(OrderStatus.Shipped, found.Status);
        Assert.Equal(Priority.High, found.Priority);
        Assert.Equal(Priority.Low, found.FallbackPriority);
        Assert.Equal(AuditAction.Updated, found.LastAction);
        Assert.Equal(Permissions.Read | Permissions.Write, found.Permissions);
        Assert.Equal("full-test", found.Label);
    }

    // ------------------------------------------------------------------
    // Nullable enum ─ null round-trip
    // ------------------------------------------------------------------

    [Fact]
    public void Nullable_Enum_Null_RoundTrips()
    {
        var entity = new EnumEntity
        {
            Role = UserRole.Guest,
            Status = null,               // explicit null
            FallbackPriority = null,      // explicit null
            Priority = Priority.Normal,
            LastAction = AuditAction.None,
            Permissions = Permissions.None,
            Label = "nullable-null"
        };

        var id = _db.EnumEntities.Insert(entity);
        _db.SaveChanges();
        var found = _db.EnumEntities.FindById(id);

        Assert.NotNull(found);
        Assert.Null(found.Status);
        Assert.Null(found.FallbackPriority);
    }

    [Fact]
    public void Nullable_Enum_With_Value_RoundTrips()
    {
        var entity = new EnumEntity
        {
            Role = UserRole.User,
            Status = OrderStatus.Cancelled,
            FallbackPriority = Priority.Critical,
            Priority = Priority.Normal,
            LastAction = AuditAction.Deleted,
            Permissions = Permissions.Admin,
            Label = "nullable-set"
        };

        var id = _db.EnumEntities.Insert(entity);
        _db.SaveChanges();
        var found = _db.EnumEntities.FindById(id);

        Assert.NotNull(found);
        Assert.Equal(OrderStatus.Cancelled, found.Status);
        Assert.Equal(Priority.Critical, found.FallbackPriority);
    }

    // ------------------------------------------------------------------
    // Different underlying types
    // ------------------------------------------------------------------

    [Fact]
    public void Byte_Underlying_Enum_RoundTrips()
    {
        var entity = new EnumEntity
        {
            Role = UserRole.Guest,
            Priority = Priority.Critical,   // byte-based
            LastAction = AuditAction.None,
            Permissions = Permissions.None,
            Label = "byte-enum"
        };

        var id = _db.EnumEntities.Insert(entity);
        _db.SaveChanges();
        var found = _db.EnumEntities.FindById(id);

        Assert.NotNull(found);
        Assert.Equal(Priority.Critical, found.Priority);
    }

    [Fact]
    public void Long_Underlying_Enum_RoundTrips()
    {
        var entity = new EnumEntity
        {
            Role = UserRole.Guest,
            Priority = Priority.Normal,
            LastAction = AuditAction.Archived,   // long-based, value = 100L
            Permissions = Permissions.None,
            Label = "long-enum"
        };

        var id = _db.EnumEntities.Insert(entity);
        _db.SaveChanges();
        var found = _db.EnumEntities.FindById(id);

        Assert.NotNull(found);
        Assert.Equal(AuditAction.Archived, found.LastAction);
    }

    // ------------------------------------------------------------------
    // [Flags] enum
    // ------------------------------------------------------------------

    [Fact]
    public void Flags_Enum_Preserves_Combined_Value()
    {
        var entity = new EnumEntity
        {
            Role = UserRole.Guest,
            Priority = Priority.Normal,
            LastAction = AuditAction.None,
            Permissions = Permissions.All,   // Read | Write | Execute | Admin = 15
            Label = "flags"
        };

        var id = _db.EnumEntities.Insert(entity);
        _db.SaveChanges();
        var found = _db.EnumEntities.FindById(id);

        Assert.NotNull(found);
        Assert.Equal(Permissions.All, found.Permissions);
        Assert.True(found.Permissions.HasFlag(Permissions.Read));
        Assert.True(found.Permissions.HasFlag(Permissions.Write));
        Assert.True(found.Permissions.HasFlag(Permissions.Execute));
        Assert.True(found.Permissions.HasFlag(Permissions.Admin));
    }

    [Fact]
    public void Flags_Enum_Custom_Combination_RoundTrips()
    {
        var combo = Permissions.Read | Permissions.Execute;

        var entity = new EnumEntity
        {
            Role = UserRole.Guest,
            Priority = Priority.Normal,
            LastAction = AuditAction.None,
            Permissions = combo,
            Label = "flags-custom"
        };

        var id = _db.EnumEntities.Insert(entity);
        _db.SaveChanges();
        var found = _db.EnumEntities.FindById(id);

        Assert.NotNull(found);
        Assert.Equal(combo, found.Permissions);
        Assert.True(found.Permissions.HasFlag(Permissions.Read));
        Assert.False(found.Permissions.HasFlag(Permissions.Write));
        Assert.True(found.Permissions.HasFlag(Permissions.Execute));
    }

    // ------------------------------------------------------------------
    // Enum collections (List<TEnum>, TEnum[])
    // ------------------------------------------------------------------

    [Fact]
    public void List_Of_Enums_RoundTrips()
    {
        var roles = new List<UserRole> { UserRole.Guest, UserRole.User, UserRole.Admin };

        var entity = new EnumEntity
        {
            Role = UserRole.Guest,
            Priority = Priority.Normal,
            LastAction = AuditAction.None,
            Permissions = Permissions.None,
            AssignableRoles = roles,
            Label = "list-enum"
        };

        var id = _db.EnumEntities.Insert(entity);
        _db.SaveChanges();
        var found = _db.EnumEntities.FindById(id);

        Assert.NotNull(found);
        Assert.Equal(3, found.AssignableRoles.Count);
        Assert.Equal(UserRole.Guest, found.AssignableRoles[0]);
        Assert.Equal(UserRole.User, found.AssignableRoles[1]);
        Assert.Equal(UserRole.Admin, found.AssignableRoles[2]);
    }

    [Fact]
    public void Array_Of_Enums_RoundTrips()
    {
        var history = new[]
        {
            OrderStatus.Pending,
            OrderStatus.Processing,
            OrderStatus.Shipped,
            OrderStatus.Delivered
        };

        var entity = new EnumEntity
        {
            Role = UserRole.Guest,
            Priority = Priority.Normal,
            LastAction = AuditAction.None,
            Permissions = Permissions.None,
            StatusHistory = history,
            Label = "array-enum"
        };

        var id = _db.EnumEntities.Insert(entity);
        _db.SaveChanges();
        var found = _db.EnumEntities.FindById(id);

        Assert.NotNull(found);
        Assert.Equal(4, found.StatusHistory.Length);
        Assert.Equal(OrderStatus.Pending, found.StatusHistory[0]);
        Assert.Equal(OrderStatus.Processing, found.StatusHistory[1]);
        Assert.Equal(OrderStatus.Shipped, found.StatusHistory[2]);
        Assert.Equal(OrderStatus.Delivered, found.StatusHistory[3]);
    }

    [Fact]
    public void Empty_Enum_Collection_RoundTrips()
    {
        var entity = new EnumEntity
        {
            Role = UserRole.Guest,
            Priority = Priority.Normal,
            LastAction = AuditAction.None,
            Permissions = Permissions.None,
            AssignableRoles = new List<UserRole>(),
            StatusHistory = Array.Empty<OrderStatus>(),
            Label = "empty-collections"
        };

        var id = _db.EnumEntities.Insert(entity);
        _db.SaveChanges();
        var found = _db.EnumEntities.FindById(id);

        Assert.NotNull(found);
        Assert.Empty(found.AssignableRoles);
        Assert.Empty(found.StatusHistory);
    }

    // ------------------------------------------------------------------
    // Update enum values
    // ------------------------------------------------------------------

    [Fact]
    public void Update_Enum_Values_Persists()
    {
        var entity = new EnumEntity
        {
            Role = UserRole.Guest,
            Status = OrderStatus.Pending,
            Priority = Priority.Low,
            LastAction = AuditAction.Created,
            Permissions = Permissions.Read,
            Label = "update-test"
        };

        var id = _db.EnumEntities.Insert(entity);
        _db.SaveChanges();

        // Mutate every enum field
        entity.Role = UserRole.Moderator;
        entity.Status = OrderStatus.Delivered;
        entity.Priority = Priority.Critical;
        entity.FallbackPriority = Priority.High;
        entity.LastAction = AuditAction.Archived;
        entity.Permissions = Permissions.All;
        entity.AssignableRoles = new List<UserRole> { UserRole.Admin };
        entity.StatusHistory = new[] { OrderStatus.Cancelled };

        _db.EnumEntities.Update(entity);
        _db.SaveChanges();

        var found = _db.EnumEntities.FindById(id);

        Assert.NotNull(found);
        Assert.Equal(UserRole.Moderator, found.Role);
        Assert.Equal(OrderStatus.Delivered, found.Status);
        Assert.Equal(Priority.Critical, found.Priority);
        Assert.Equal(Priority.High, found.FallbackPriority);
        Assert.Equal(AuditAction.Archived, found.LastAction);
        Assert.Equal(Permissions.All, found.Permissions);
        Assert.Single(found.AssignableRoles);
        Assert.Equal(UserRole.Admin, found.AssignableRoles[0]);
        Assert.Single(found.StatusHistory);
        Assert.Equal(OrderStatus.Cancelled, found.StatusHistory[0]);
    }

    // ------------------------------------------------------------------
    // Negative / edge-case enum values
    // ------------------------------------------------------------------

    [Fact]
    public void Negative_Enum_Value_RoundTrips()
    {
        var entity = new EnumEntity
        {
            Role = UserRole.Guest,
            Status = OrderStatus.Cancelled,   // -1
            Priority = Priority.Normal,
            LastAction = AuditAction.None,
            Permissions = Permissions.None,
            Label = "negative"
        };

        var id = _db.EnumEntities.Insert(entity);
        _db.SaveChanges();
        var found = _db.EnumEntities.FindById(id);

        Assert.NotNull(found);
        Assert.Equal(OrderStatus.Cancelled, found.Status);
        Assert.Equal(-1, (int)found.Status!);
    }

    // ------------------------------------------------------------------
    // Cross-path: typed DbContext → dynamic BLiteEngine
    // ------------------------------------------------------------------

    [Fact]
    public void CrossPath_Enum_Written_By_DbContext_ReadBack_By_Engine()
    {
        // Write with typed path
        var entity = new EnumEntity
        {
            Role = UserRole.Admin,
            Status = OrderStatus.Shipped,
            Priority = Priority.High,
            LastAction = AuditAction.Updated,
            Permissions = Permissions.Read | Permissions.Write,
            AssignableRoles = new List<UserRole> { UserRole.User, UserRole.Moderator },
            StatusHistory = new[] { OrderStatus.Pending, OrderStatus.Shipped },
            Label = "cross-path"
        };
        var id = _db.EnumEntities.Insert(entity);
        _db.SaveChanges();
        _db.ForceCheckpoint();
        _db.Dispose();

        // Read with dynamic path
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("enum_entities");
        var docs = col.FindAll().ToList();

        Assert.Single(docs);
        var doc = docs[0];

        // Enum values should appear as their underlying integer
        Assert.True(doc.TryGetInt32("role", out var role));
        Assert.Equal((int)UserRole.Admin, role);

        Assert.True(doc.TryGetInt32("status", out var status));
        Assert.Equal((int)OrderStatus.Shipped, status);

        Assert.True(doc.TryGetInt32("priority", out var priority));
        Assert.Equal((int)Priority.High, priority);

        Assert.True(doc.TryGetValue("lastaction", out var lastActionVal));
        Assert.Equal((long)AuditAction.Updated, lastActionVal.AsInt64);

        Assert.True(doc.TryGetInt32("permissions", out var perms));
        Assert.Equal((int)(Permissions.Read | Permissions.Write), perms);

        Assert.True(doc.TryGetString("label", out var label));
        Assert.Equal("cross-path", label);
    }

    // ------------------------------------------------------------------
    // FindAll / multiple entities
    // ------------------------------------------------------------------

    [Fact]
    public void FindAll_Returns_Multiple_Enum_Entities()
    {
        _db.EnumEntities.Insert(new EnumEntity
        {
            Role = UserRole.Guest,
            Priority = Priority.Low,
            LastAction = AuditAction.None,
            Permissions = Permissions.None,
            Label = "first"
        });
        _db.EnumEntities.Insert(new EnumEntity
        {
            Role = UserRole.Admin,
            Priority = Priority.Critical,
            LastAction = AuditAction.Archived,
            Permissions = Permissions.All,
            Label = "second"
        });
        _db.SaveChanges();

        var all = _db.EnumEntities.FindAll().ToList();

        Assert.Equal(2, all.Count);
        Assert.Contains(all, e => e.Role == UserRole.Guest && e.Label == "first");
        Assert.Contains(all, e => e.Role == UserRole.Admin && e.Label == "second");
    }

    // ------------------------------------------------------------------
    // LINQ Where on enum property
    // ------------------------------------------------------------------

    [Fact]
    public void Linq_Where_On_Enum_Property()
    {
        _db.EnumEntities.Insert(new EnumEntity
        {
            Role = UserRole.Guest,
            Priority = Priority.Normal,
            LastAction = AuditAction.None,
            Permissions = Permissions.None,
            Label = "guest"
        });
        _db.EnumEntities.Insert(new EnumEntity
        {
            Role = UserRole.Admin,
            Priority = Priority.High,
            LastAction = AuditAction.Created,
            Permissions = Permissions.All,
            Label = "admin"
        });
        _db.SaveChanges();

        var admins = _db.EnumEntities
            .AsQueryable()
            .Where(e => e.Role == UserRole.Admin)
            .ToList();

        Assert.Single(admins);
        Assert.Equal("admin", admins[0].Label);
    }
}
