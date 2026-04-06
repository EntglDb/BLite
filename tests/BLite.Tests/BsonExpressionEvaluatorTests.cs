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
}

