using System.Linq.Expressions;
using BLite.Bson;
using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Tests for <see cref="BsonExpressionEvaluator"/>.
///
/// TryCompile returns null/non-null is verified at the unit level (InternalsVisibleTo).
/// Predicate correctness over int and string comparisons is validated via LINQ queries
/// on a real DocumentCollection (which routes through BTreeQueryProvider → BsonExpressionEvaluator).
/// </summary>
public class BsonExpressionEvaluatorTests : IDisposable
{
    // Local entity with bool property used in bare-bool and NOT-bool predicate tests.
    private class FlagEntity
    {
        public int Id { get; set; }
        public bool IsActive { get; set; }
    }

    // Entity with new BSON-primitive types (TimeSpan, Guid) and nullable properties.
    private class SpecialTypesEntity
    {
        public int Id { get; set; }
        public TimeSpan Duration { get; set; }
        public Guid ExternalId { get; set; }
        public DateOnly CreatedDate { get; set; }
        public TimeOnly CreatedTime { get; set; }
        public double? Score { get; set; }
        public int? Count { get; set; }
        public TimeSpan? OptionalDuration { get; set; }
    }
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public BsonExpressionEvaluatorTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"bee_tests_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);

        _db.Users.InsertAsync(new User { Name = "Alice",   Age = 30 }).GetAwaiter().GetResult();
        _db.Users.InsertAsync(new User { Name = "Bob",     Age = 25 }).GetAwaiter().GetResult();
        _db.Users.InsertAsync(new User { Name = "Charlie", Age = 35 }).GetAwaiter().GetResult();
        _db.Users.InsertAsync(new User { Name = "Dave",    Age = 20 }).GetAwaiter().GetResult();
        _db.SaveChangesAsync().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ─── TryCompile: returns non-null for supported binary comparisons ────────

    [Fact]
    public void TryCompile_Int32Equal_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Age == 30;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_Int32NotEqual_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Age != 25;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_Int32GreaterThan_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Age > 25;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_Int32GreaterThanOrEqual_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Age >= 30;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_Int32LessThan_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Age < 40;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_Int32LessThanOrEqual_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Age <= 30;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_StringEqual_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Name == "Alice";
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_StringNotEqual_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Name != "Alice";
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_EqualsMethodCall_ReturnsNonNull()
    {
        // e.Name.Equals("Alice") → .Equals() on a direct member
        Expression<Func<User, bool>> lambda = x => x.Name.Equals("Alice");
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_FlippedOperands_ReturnsNonNull()
    {
        // Constant on the left: 30 == x.Age → gets flipped internally
        Expression<Func<User, bool>> lambda = x => 30 == x.Age;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_ClosureCapture_ReturnsNonNull()
    {
        int target = 30;
        Expression<Func<User, bool>> lambda = x => x.Age == target;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_AndAlso_NullGuardPlusEquality_ReturnsNonNull()
    {
        // Pattern: (other != null) && x.Name.Equals(other.Name)
        // Left side doesn't touch the parameter; right side does → returns right-side predicate
        User other = new() { Name = "Alice" };
        Expression<Func<User, bool>> lambda = x => other != null && x.Name.Equals(other.Name);
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    // ─── TryCompile: bool member and logical NOT ─────────────────────────────

    [Fact]
    public void TryCompile_BareBoolMember_ReturnsNonNull()
    {
        Expression<Func<FlagEntity, bool>> lambda = x => x.IsActive;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<FlagEntity>(lambda));
    }

    [Fact]
    public void TryCompile_LogicalNotBoolMember_ReturnsNonNull()
    {
        Expression<Func<FlagEntity, bool>> lambda = x => !x.IsActive;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<FlagEntity>(lambda));
    }

    [Fact]
    public void TryCompile_BareBoolMember_PredicateMatchesTrue()
    {
        Expression<Func<FlagEntity, bool>> lambda = x => x.IsActive;
        var predicate = BsonExpressionEvaluator.TryCompile<FlagEntity>(lambda);
        Assert.NotNull(predicate);

        var keyMap = new Dictionary<string, ushort> { ["isactive"] = 1 };
        var reverseKeyMap = new System.Collections.Concurrent.ConcurrentDictionary<ushort, string>(
            new Dictionary<ushort, string> { [1] = "isactive" });
        var activeDoc   = BsonDocument.Create(keyMap, reverseKeyMap, b => b.AddBoolean("isactive", true));
        var inactiveDoc = BsonDocument.Create(keyMap, reverseKeyMap, b => b.AddBoolean("isactive", false));

        Assert.True(predicate!(activeDoc.GetReader()));
        Assert.False(predicate!(inactiveDoc.GetReader()));
    }

    [Fact]
    public void TryCompile_LogicalNotBoolMember_PredicateMatchesFalse()
    {
        Expression<Func<FlagEntity, bool>> lambda = x => !x.IsActive;
        var predicate = BsonExpressionEvaluator.TryCompile<FlagEntity>(lambda);
        Assert.NotNull(predicate);

        var keyMap = new Dictionary<string, ushort> { ["isactive"] = 1 };
        var reverseKeyMap = new System.Collections.Concurrent.ConcurrentDictionary<ushort, string>(
            new Dictionary<ushort, string> { [1] = "isactive" });
        var activeDoc   = BsonDocument.Create(keyMap, reverseKeyMap, b => b.AddBoolean("isactive", true));
        var inactiveDoc = BsonDocument.Create(keyMap, reverseKeyMap, b => b.AddBoolean("isactive", false));

        Assert.False(predicate!(activeDoc.GetReader()));
        Assert.True(predicate!(inactiveDoc.GetReader()));
    }

    // ─── TryCompile: returns null for unsupported expressions ────────────────

    [Fact]
    public void TryCompile_StringStartsWith_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Name.StartsWith("A");
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_NestedPath_ReturnsNull()
    {
        Expression<Func<ComplexUser, bool>> lambda = x => x.MainAddress.Street == "Main St";
        Assert.Null(BsonExpressionEvaluator.TryCompile<ComplexUser>(lambda));
    }

    [Fact]
    public void TryCompile_NullTargetValue_ReturnsNonNull()
    {
        // Null check: x.Name == null is now supported and returns a non-null predicate
        string? nullName = null;
        Expression<Func<User, bool>> lambda = x => x.Name == nullName;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_BothSidesAreLambdaParameter_ReturnsNull()
    {
        // x.Age == x.Age — right side also references the parameter, not a constant
        Expression<Func<User, bool>> lambda = x => x.Age == x.Age;
        // Right side x.Age is a MemberExpression on param, so TryEvaluate fails → null
        Assert.Null(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    // ─── Integration: LINQ Where uses BsonExpressionEvaluator internally ─────

    [Fact]
    public void Integration_IntEqual_FiltersCorrectly()
    {
        var results = _db.Users.AsQueryable().Where(x => x.Age == 30).ToList();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public void Integration_IntGreaterThan_FiltersCorrectly()
    {
        var results = _db.Users.AsQueryable().Where(x => x.Age > 25).ToList();
        Assert.Equal(2, results.Count); // Alice (30), Charlie (35)
        Assert.All(results, u => Assert.True(u.Age > 25));
    }

    [Fact]
    public void Integration_IntLessThan_FiltersCorrectly()
    {
        var results = _db.Users.AsQueryable().Where(x => x.Age < 30).ToList();
        Assert.Equal(2, results.Count); // Bob (25), Dave (20)
        Assert.All(results, u => Assert.True(u.Age < 30));
    }

    [Fact]
    public void Integration_IntLessThanOrEqual_FiltersCorrectly()
    {
        var results = _db.Users.AsQueryable().Where(x => x.Age <= 25).ToList();
        Assert.Equal(2, results.Count); // Bob (25), Dave (20)
    }

    [Fact]
    public void Integration_IntGreaterThanOrEqual_FiltersCorrectly()
    {
        var results = _db.Users.AsQueryable().Where(x => x.Age >= 35).ToList();
        Assert.Single(results);
        Assert.Equal("Charlie", results[0].Name);
    }

    [Fact]
    public void Integration_IntNotEqual_FiltersCorrectly()
    {
        var results = _db.Users.AsQueryable().Where(x => x.Age != 30).ToList();
        Assert.Equal(3, results.Count);
        Assert.DoesNotContain(results, u => u.Name == "Alice");
    }

    [Fact]
    public void Integration_StringEqual_FiltersCorrectly()
    {
        var results = _db.Users.AsQueryable().Where(x => x.Name == "Bob").ToList();
        Assert.Single(results);
        Assert.Equal(25, results[0].Age);
    }

    [Fact]
    public void Integration_ClosureCapture_FiltersCorrectly()
    {
        int targetAge = 30;
        var results = _db.Users.AsQueryable().Where(x => x.Age == targetAge).ToList();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public void Integration_NoMatchingResult_ReturnsEmpty()
    {
        var results = _db.Users.AsQueryable().Where(x => x.Age == 999).ToList();
        Assert.Empty(results);
    }

    [Fact]
    public void Integration_AllMatchingResult_ReturnsAll()
    {
        var results = _db.Users.AsQueryable().Where(x => x.Age >= 0).ToList();
        Assert.Equal(4, results.Count);
    }

    // ─── Phase 1: OrElse ──────────────────────────────────────────────────────

    [Fact]
    public void TryCompile_OrElse_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Age == 25 || x.Age == 30;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void Integration_OrElse_FiltersCorrectly()
    {
        var results = _db.Users.AsQueryable().Where(x => x.Age == 25 || x.Age == 30).ToList();
        Assert.Equal(2, results.Count);
        Assert.Contains(results, u => u.Name == "Alice");
        Assert.Contains(results, u => u.Name == "Bob");
    }

    [Fact]
    public void Integration_OrElse_AllMatch()
    {
        var results = _db.Users.AsQueryable().Where(x => x.Age >= 0 || x.Name == "Alice").ToList();
        Assert.Equal(4, results.Count);
    }

    // ─── Phase 1: Null checks ────────────────────────────────────────────────

    [Fact]
    public void TryCompile_NullCheckEqual_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Name == null;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_NullCheckNotEqual_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Name != null;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void Integration_NullCheckNotEqual_ReturnsNonNullDocuments()
    {
        // All users have non-null Name — should return all
        var results = _db.Users.AsQueryable().Where(x => x.Name != null).ToList();
        Assert.Equal(4, results.Count);
    }

    // ─── Phase 1: String methods ─────────────────────────────────────────────

    [Fact]
    public void TryCompile_StringContains_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Name.Contains("li");
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_StringEndsWith_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Name.EndsWith("e");
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void Integration_StringStartsWith_FiltersCorrectly()
    {
        var results = _db.Users.AsQueryable().Where(x => x.Name.StartsWith("A")).ToList();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public void Integration_StringContains_FiltersCorrectly()
    {
        var results = _db.Users.AsQueryable().Where(x => x.Name.Contains("o")).ToList();
        Assert.Single(results);
        Assert.Equal("Bob", results[0].Name);
    }

    [Fact]
    public void Integration_StringEndsWith_FiltersCorrectly()
    {
        var results = _db.Users.AsQueryable().Where(x => x.Name.EndsWith("e")).ToList();
        Assert.Equal(3, results.Count); // Alice, Charlie, Dave all end with 'e'
        Assert.All(results, u => Assert.EndsWith("e", u.Name));
    }

    [Fact]
    public void BsonLevel_StringContains_WorksWithUnicodePattern()
    {
        // Confirms the UTF-8 byte-level matching is semantically correct for
        // non-ASCII characters (multi-byte UTF-8 sequences).
        var keyMap     = new Dictionary<string, ushort> { ["name"] = 1 };
        var reverseMap = new System.Collections.Concurrent.ConcurrentDictionary<ushort, string>(
            new Dictionary<ushort, string> { [1] = "name" });

        var matchDoc  = BsonDocument.Create(keyMap, reverseMap, b => b.AddString("name", "Héloïse"));
        var noMatchDoc = BsonDocument.Create(keyMap, reverseMap, b => b.AddString("name", "Alice"));

        Expression<Func<User, bool>> lambda = x => x.Name.Contains("ïse");
        var predicate = BsonExpressionEvaluator.TryCompile<User>(lambda);
        Assert.NotNull(predicate);

        Assert.True(predicate!(matchDoc.GetReader()),  "Should match document containing 'ïse'");
        Assert.False(predicate!(noMatchDoc.GetReader()), "Should not match document without 'ïse'");
    }

    [Fact]
    public void BsonLevel_StringStartsWith_WorksWithUnicodePattern()
    {
        var keyMap     = new Dictionary<string, ushort> { ["name"] = 1 };
        var reverseMap = new System.Collections.Concurrent.ConcurrentDictionary<ushort, string>(
            new Dictionary<ushort, string> { [1] = "name" });

        var matchDoc   = BsonDocument.Create(keyMap, reverseMap, b => b.AddString("name", "Ångström"));
        var noMatchDoc = BsonDocument.Create(keyMap, reverseMap, b => b.AddString("name", "Angstrom"));

        Expression<Func<User, bool>> lambda = x => x.Name.StartsWith("Å");
        var predicate = BsonExpressionEvaluator.TryCompile<User>(lambda);
        Assert.NotNull(predicate);

        Assert.True(predicate!(matchDoc.GetReader()),   "Should match document starting with 'Å'");
        Assert.False(predicate!(noMatchDoc.GetReader()), "Should not match document starting with 'A'");
    }

    [Fact]
    public void BsonLevel_StringEndsWith_WorksWithUnicodePattern()
    {
        var keyMap     = new Dictionary<string, ushort> { ["name"] = 1 };
        var reverseMap = new System.Collections.Concurrent.ConcurrentDictionary<ushort, string>(
            new Dictionary<ushort, string> { [1] = "name" });

        var matchDoc   = BsonDocument.Create(keyMap, reverseMap, b => b.AddString("name", "Üniversität"));
        var noMatchDoc = BsonDocument.Create(keyMap, reverseMap, b => b.AddString("name", "University"));

        Expression<Func<User, bool>> lambda = x => x.Name.EndsWith("ät");
        var predicate = BsonExpressionEvaluator.TryCompile<User>(lambda);
        Assert.NotNull(predicate);

        Assert.True(predicate!(matchDoc.GetReader()),   "Should match document ending with 'ät'");
        Assert.False(predicate!(noMatchDoc.GetReader()), "Should not match document ending with 'ty'");
    }

    // ─── Phase 1: static string.IsNullOrEmpty ────────────────────────────────

    [Fact]
    public void TryCompile_StringIsNullOrEmpty_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => string.IsNullOrEmpty(x.Name);
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void Integration_StringIsNullOrEmpty_ReturnsNoneForSeededData()
    {
        // All seeded users have non-empty names
        var results = _db.Users.AsQueryable().Where(x => string.IsNullOrEmpty(x.Name)).ToList();
        Assert.Empty(results);
    }

    // ─── Phase 1: IN operator ────────────────────────────────────────────────

    [Fact]
    public void TryCompile_InOperator_InstanceContains_ReturnsNonNull()
    {
        var names = new List<string> { "Alice", "Bob" };
        Expression<Func<User, bool>> lambda = x => names.Contains(x.Name);
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_InOperator_EnumerableContains_ReturnsNonNull()
    {
        var ages = new[] { 25, 30 };
        Expression<Func<User, bool>> lambda = x => ages.Contains(x.Age);
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void Integration_InOperator_FiltersCorrectly()
    {
        var names = new List<string> { "Alice", "Charlie" };
        var results = _db.Users.AsQueryable().Where(x => names.Contains(x.Name)).ToList();
        Assert.Equal(2, results.Count);
        Assert.All(results, u => Assert.Contains(u.Name, names));
    }

    [Fact]
    public void Integration_InOperator_EmptyCollection_ReturnsNone()
    {
        var names = new List<string>();
        var results = _db.Users.AsQueryable().Where(x => names.Contains(x.Name)).ToList();
        Assert.Empty(results);
    }

    // ─── Phase 1: Enum support ───────────────────────────────────────────────

    [Fact]
    public void TryCompile_EnumComparison_ReturnsNonNull()
    {
        Expression<Func<EnumEntity, bool>> lambda = x => x.Role == UserRole.Admin;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<EnumEntity>(lambda));
    }

    // ─── Phase 1: New primitive types (TimeSpan, Guid, DateOnly, TimeOnly) ───

    [Fact]
    public void TryCompile_TimeSpanEqual_ReturnsNonNull()
    {
        var duration = TimeSpan.FromMinutes(30);
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.Duration == duration;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    [Fact]
    public void TryCompile_TimeSpanGreaterThan_ReturnsNonNull()
    {
        var duration = TimeSpan.FromMinutes(30);
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.Duration > duration;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    [Fact]
    public void TryCompile_GuidEqual_ReturnsNonNull()
    {
        var id = Guid.NewGuid();
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.ExternalId == id;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    [Fact]
    public void TryCompile_GuidNotEqual_ReturnsNonNull()
    {
        var id = Guid.NewGuid();
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.ExternalId != id;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    [Fact]
    public void TryCompile_DateOnlyEqual_ReturnsNonNull()
    {
        var date = new DateOnly(2024, 1, 15);
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.CreatedDate == date;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    [Fact]
    public void TryCompile_DateOnlyGreaterThan_ReturnsNonNull()
    {
        var date = new DateOnly(2024, 1, 1);
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.CreatedDate > date;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    [Fact]
    public void TryCompile_TimeOnlyEqual_ReturnsNonNull()
    {
        var time = new TimeOnly(10, 30);
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.CreatedTime == time;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    [Fact]
    public void TryCompile_TimeOnlyLessThan_ReturnsNonNull()
    {
        var time = new TimeOnly(12, 0);
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.CreatedTime < time;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    // ─── Phase 1: Predicate correctness for new types ────────────────────────

    [Fact]
    public void BsonLevel_TimeSpanEqual_MatchesCorrectly()
    {
        var duration = TimeSpan.FromMinutes(30);
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.Duration == duration;
        var predicate = BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda);
        Assert.NotNull(predicate);

        var keyMap = new Dictionary<string, ushort> { ["duration"] = 1 };
        var reverseMap = new System.Collections.Concurrent.ConcurrentDictionary<ushort, string>(
            new Dictionary<ushort, string> { [1] = "duration" });

        var matchDoc    = BsonDocument.Create(keyMap, reverseMap, b => b.AddTimeSpan("duration", TimeSpan.FromMinutes(30)));
        var noMatchDoc  = BsonDocument.Create(keyMap, reverseMap, b => b.AddTimeSpan("duration", TimeSpan.FromMinutes(60)));

        Assert.True(predicate!(matchDoc.GetReader()),   "Should match 30-minute duration");
        Assert.False(predicate!(noMatchDoc.GetReader()), "Should not match 60-minute duration");
    }

    [Fact]
    public void BsonLevel_GuidEqual_MatchesCorrectly()
    {
        var id = Guid.Parse("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.ExternalId == id;
        var predicate = BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda);
        Assert.NotNull(predicate);

        var keyMap = new Dictionary<string, ushort> { ["externalid"] = 1 };
        var reverseMap = new System.Collections.Concurrent.ConcurrentDictionary<ushort, string>(
            new Dictionary<ushort, string> { [1] = "externalid" });

        var matchDoc   = BsonDocument.Create(keyMap, reverseMap, b => b.AddGuid("externalid", id));
        var noMatchDoc = BsonDocument.Create(keyMap, reverseMap, b => b.AddGuid("externalid", Guid.NewGuid()));

        Assert.True(predicate!(matchDoc.GetReader()),   "Should match the specific Guid");
        Assert.False(predicate!(noMatchDoc.GetReader()), "Should not match a different Guid");
    }

    [Fact]
    public void BsonLevel_DateOnlyEqual_MatchesCorrectly()
    {
        var date = new DateOnly(2024, 6, 15);
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.CreatedDate == date;
        var predicate = BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda);
        Assert.NotNull(predicate);

        var keyMap = new Dictionary<string, ushort> { ["createddate"] = 1 };
        var reverseMap = new System.Collections.Concurrent.ConcurrentDictionary<ushort, string>(
            new Dictionary<ushort, string> { [1] = "createddate" });

        var matchDoc   = BsonDocument.Create(keyMap, reverseMap, b => b.AddDateOnly("createddate", date));
        var noMatchDoc = BsonDocument.Create(keyMap, reverseMap, b => b.AddDateOnly("createddate", new DateOnly(2024, 1, 1)));

        Assert.True(predicate!(matchDoc.GetReader()),   "Should match 2024-06-15");
        Assert.False(predicate!(noMatchDoc.GetReader()), "Should not match 2024-01-01");
    }

    [Fact]
    public void BsonLevel_TimeOnlyEqual_MatchesCorrectly()
    {
        var time = new TimeOnly(10, 30);
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.CreatedTime == time;
        var predicate = BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda);
        Assert.NotNull(predicate);

        var keyMap = new Dictionary<string, ushort> { ["createdtime"] = 1 };
        var reverseMap = new System.Collections.Concurrent.ConcurrentDictionary<ushort, string>(
            new Dictionary<ushort, string> { [1] = "createdtime" });

        var matchDoc   = BsonDocument.Create(keyMap, reverseMap, b => b.AddTimeOnly("createdtime", time));
        var noMatchDoc = BsonDocument.Create(keyMap, reverseMap, b => b.AddTimeOnly("createdtime", new TimeOnly(14, 0)));

        Assert.True(predicate!(matchDoc.GetReader()),   "Should match 10:30");
        Assert.False(predicate!(noMatchDoc.GetReader()), "Should not match 14:00");
    }

    // ─── Phase 1: Nullable.HasValue pattern ──────────────────────────────────

    [Fact]
    public void TryCompile_NullableHasValue_ReturnsNonNull()
    {
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.Score.HasValue;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    [Fact]
    public void TryCompile_NullableIntHasValue_ReturnsNonNull()
    {
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.Count.HasValue;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    [Fact]
    public void BsonLevel_NullableHasValue_MatchesNonNullField()
    {
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.Score.HasValue;
        var predicate = BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda);
        Assert.NotNull(predicate);

        var keyMap = new Dictionary<string, ushort> { ["score"] = 1 };
        var reverseMap = new System.Collections.Concurrent.ConcurrentDictionary<ushort, string>(
            new Dictionary<ushort, string> { [1] = "score" });

        var presentDoc = BsonDocument.Create(keyMap, reverseMap, b => b.AddDouble("score", 9.5));
        var nullDoc    = BsonDocument.Create(keyMap, reverseMap, b => b.AddNull("score"));

        Assert.True(predicate!(presentDoc.GetReader()),  "HasValue should be true for non-null field");
        Assert.False(predicate!(nullDoc.GetReader()),    "HasValue should be false for null field");
    }

    // ─── Phase 1: Nullable.Value accessor ────────────────────────────────────

    [Fact]
    public void TryCompile_NullableValueAccess_ReturnsNonNull()
    {
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.Score.Value > 5.0;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    [Fact]
    public void TryCompile_NullableValueEqual_ReturnsNonNull()
    {
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.Score.Value == 9.5;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    [Fact]
    public void BsonLevel_NullableValueAccess_MatchesCorrectly()
    {
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.Score.Value > 5.0;
        var predicate = BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda);
        Assert.NotNull(predicate);

        var keyMap = new Dictionary<string, ushort> { ["score"] = 1 };
        var reverseMap = new System.Collections.Concurrent.ConcurrentDictionary<ushort, string>(
            new Dictionary<ushort, string> { [1] = "score" });

        var highDoc = BsonDocument.Create(keyMap, reverseMap, b => b.AddDouble("score", 9.5));
        var lowDoc  = BsonDocument.Create(keyMap, reverseMap, b => b.AddDouble("score", 2.0));

        Assert.True(predicate!(highDoc.GetReader()),  "Score 9.5 > 5.0 should match");
        Assert.False(predicate!(lowDoc.GetReader()),  "Score 2.0 > 5.0 should not match");
    }

    // ─── Phase 1: x.NullableProp == v (Nullable<T> lift) ────────────────────

    [Fact]
    public void TryCompile_NullableEqualityComparison_ReturnsNonNull()
    {
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.Score == 9.5;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    [Fact]
    public void TryCompile_NullableIntEqualityComparison_ReturnsNonNull()
    {
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.Count == 42;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    [Fact]
    public void TryCompile_NullableTimeSpanComparison_ReturnsNonNull()
    {
        var duration = TimeSpan.FromHours(1);
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => x.OptionalDuration == duration;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    // ─── Phase 1: CompareTo(v) op 0 pattern ─────────────────────────────────

    [Fact]
    public void TryCompile_CompareToGreaterThan_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Age.CompareTo(25) > 0;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_CompareToEqual_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Age.CompareTo(30) == 0;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_CompareToLessThan_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Age.CompareTo(35) < 0;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_StringCompareTo_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Name.CompareTo("Alice") == 0;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void BsonLevel_CompareTo_MatchesCorrectly()
    {
        // x.Age.CompareTo(25) > 0 → equivalent to x.Age > 25
        Expression<Func<User, bool>> lambda = x => x.Age.CompareTo(25) > 0;
        var predicate = BsonExpressionEvaluator.TryCompile<User>(lambda);
        Assert.NotNull(predicate);

        var keyMap = new Dictionary<string, ushort> { ["age"] = 1 };
        var reverseMap = new System.Collections.Concurrent.ConcurrentDictionary<ushort, string>(
            new Dictionary<ushort, string> { [1] = "age" });

        var matchDoc   = BsonDocument.Create(keyMap, reverseMap, b => b.AddInt32("age", 30));
        var noMatchDoc = BsonDocument.Create(keyMap, reverseMap, b => b.AddInt32("age", 20));

        Assert.True(predicate!(matchDoc.GetReader()),   "Age 30 > 25 should match");
        Assert.False(predicate!(noMatchDoc.GetReader()), "Age 20 > 25 should not match");
    }

    [Fact]
    public void Integration_CompareToGreaterThan_FiltersCorrectly()
    {
        // x.Age.CompareTo(25) > 0 → equivalent to x.Age > 25
        var results = _db.Users.AsQueryable().Where(x => x.Age.CompareTo(25) > 0).ToList();
        Assert.Equal(2, results.Count); // Alice (30), Charlie (35)
        Assert.All(results, u => Assert.True(u.Age > 25));
    }

    [Fact]
    public void Integration_CompareToEqual_FiltersCorrectly()
    {
        // x.Age.CompareTo(30) == 0 → equivalent to x.Age == 30
        var results = _db.Users.AsQueryable().Where(x => x.Age.CompareTo(30) == 0).ToList();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    // ─── Phase 1: !(x.Prop.Contains(s)) — negated Contains ──────────────────

    [Fact]
    public void TryCompile_NegatedContains_ReturnsNonNull()
    {
        Expression<Func<User, bool>> lambda = x => !x.Name.Contains("li");
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void Integration_NegatedContains_FiltersCorrectly()
    {
        // Names NOT containing "li": Bob (25), Charlie (35), Dave (20)
        // "Alice" contains "li", "Charlie" contains "li"
        var results = _db.Users.AsQueryable().Where(x => !x.Name.Contains("li")).ToList();
        // Alice and Charlie contain "li" — both should be excluded
        Assert.DoesNotContain(results, u => u.Name == "Alice");
        Assert.DoesNotContain(results, u => u.Name == "Charlie");
        Assert.Contains(results, u => u.Name == "Bob");
        Assert.Contains(results, u => u.Name == "Dave");
    }

    // ─── Phase 1: Deep nested && / || (depth > 2) ────────────────────────────

    [Fact]
    public void TryCompile_TripleAndAlso_ReturnsNonNull()
    {
        // Depth 3 AND chain
        Expression<Func<User, bool>> lambda = x => x.Age >= 20 && x.Age <= 40 && x.Name != "Bob";
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_TripleOrElse_ReturnsNonNull()
    {
        // Depth 3 OR chain
        Expression<Func<User, bool>> lambda = x => x.Age == 20 || x.Age == 25 || x.Age == 30;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_MixedAndOrDeep_ReturnsNonNull()
    {
        // Mixed && / || with depth > 2
        Expression<Func<User, bool>> lambda = x => (x.Age == 25 && x.Name == "Bob") || x.Age == 35;
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void Integration_TripleAndAlso_FiltersCorrectly()
    {
        var results = _db.Users.AsQueryable()
            .Where(x => x.Age >= 20 && x.Age <= 40 && x.Name != "Bob")
            .ToList();
        // Alice(30), Charlie(35), Dave(20) — all in 20-40 and not Bob
        Assert.DoesNotContain(results, u => u.Name == "Bob");
        Assert.All(results, u => Assert.True(u.Age is >= 20 and <= 40));
    }

    [Fact]
    public void Integration_TripleOrElse_FiltersCorrectly()
    {
        var results = _db.Users.AsQueryable()
            .Where(x => x.Age == 20 || x.Age == 25 || x.Age == 30)
            .ToList();
        Assert.Equal(3, results.Count); // Dave(20), Bob(25), Alice(30)
        Assert.All(results, u => Assert.Contains(u.Age, new[] { 20, 25, 30 }));
    }

    [Fact]
    public void Integration_MixedAndOrDeep_FiltersCorrectly()
    {
        var results = _db.Users.AsQueryable()
            .Where(x => (x.Age == 25 && x.Name == "Bob") || x.Age == 35)
            .ToList();
        Assert.Equal(2, results.Count);
        Assert.Contains(results, u => u.Name == "Bob");
        Assert.Contains(results, u => u.Name == "Charlie");
    }

    // ─── Phase 1: IN operator with new types ─────────────────────────────────

    [Fact]
    public void TryCompile_InOperator_GuidCollection_ReturnsNonNull()
    {
        var ids = new List<Guid> { Guid.NewGuid(), Guid.NewGuid() };
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => ids.Contains(x.ExternalId);
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }

    [Fact]
    public void TryCompile_InOperator_TimeSpanCollection_ReturnsNonNull()
    {
        var durations = new List<TimeSpan> { TimeSpan.FromMinutes(30), TimeSpan.FromHours(1) };
        Expression<Func<SpecialTypesEntity, bool>> lambda = x => durations.Contains(x.Duration);
        Assert.NotNull(BsonExpressionEvaluator.TryCompile<SpecialTypesEntity>(lambda));
    }
}

