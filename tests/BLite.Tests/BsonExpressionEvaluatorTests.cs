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
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public BsonExpressionEvaluatorTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"bee_tests_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);

        _db.Users.Insert(new User { Name = "Alice",   Age = 30 });
        _db.Users.Insert(new User { Name = "Bob",     Age = 25 });
        _db.Users.Insert(new User { Name = "Charlie", Age = 35 });
        _db.Users.Insert(new User { Name = "Dave",    Age = 20 });
        _db.SaveChanges();
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

    // ─── TryCompile: returns null for unsupported expressions ────────────────

    [Fact]
    public void TryCompile_MethodCallOnProperty_ReturnsNull()
    {
        Expression<Func<User, bool>> lambda = x => x.Name.StartsWith("A");
        Assert.Null(BsonExpressionEvaluator.TryCompile<User>(lambda));
    }

    [Fact]
    public void TryCompile_NestedPath_ReturnsNull()
    {
        Expression<Func<ComplexUser, bool>> lambda = x => x.MainAddress.Street == "Main St";
        Assert.Null(BsonExpressionEvaluator.TryCompile<ComplexUser>(lambda));
    }

    [Fact]
    public void TryCompile_NullTargetValue_ReturnsNull()
    {
        // Null target: IsKnownBsonPrimitive(null) → false → returns null predicate
        string? nullName = null;
        Expression<Func<User, bool>> lambda = x => x.Name == nullName;
        Assert.Null(BsonExpressionEvaluator.TryCompile<User>(lambda));
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
}
