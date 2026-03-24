using System.Linq;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;
using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Mutation-coverage tests for IndexOptimizer, BTreeQueryProvider, and BsonProjectionCompiler.
/// Exercises LINQ query paths that hit index optimization, OrderBy.Take, push-down SELECT,
/// and various terminal operators through the typed query pipeline.
/// Targets NoCoverage mutants in these areas.
/// </summary>
public class IndexOptimizerLinqCoverageTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public IndexOptimizerLinqCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"idx_linq_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
        SeedData().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _db.Dispose();
        TryDelete(_dbPath);
        TryDelete(Path.ChangeExtension(_dbPath, ".wal"));
    }

    private static void TryDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); } catch { }
    }

    private async Task SeedData()
    {
        await _db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
        await _db.Users.InsertAsync(new User { Name = "Bob", Age = 25 });
        await _db.Users.InsertAsync(new User { Name = "Charlie", Age = 35 });
        await _db.Users.InsertAsync(new User { Name = "Diana", Age = 28 });
        await _db.Users.InsertAsync(new User { Name = "Eve", Age = 22 });

        await _db.Products.InsertAsync(new Product { Id = 1, Title = "Widget", Price = 9.99m });
        await _db.Products.InsertAsync(new Product { Id = 2, Title = "Gadget", Price = 19.99m });
        await _db.Products.InsertAsync(new Product { Id = 3, Title = "Thingy", Price = 4.99m });
        await _db.Products.InsertAsync(new Product { Id = 4, Title = "Doohickey", Price = 29.99m });
        await _db.Products.InsertAsync(new Product { Id = 5, Title = "Whatchamacallit", Price = 14.99m });

        // People collection has an index on Age (configured in TestDbContext)
        await _db.People.InsertAsync(new Person { Id = 1, Name = "P1", Age = 30 });
        await _db.People.InsertAsync(new Person { Id = 2, Name = "P2", Age = 25 });
        await _db.People.InsertAsync(new Person { Id = 3, Name = "P3", Age = 40 });
        await _db.People.InsertAsync(new Person { Id = 4, Name = "P4", Age = 20 });
        await _db.People.InsertAsync(new Person { Id = 5, Name = "P5", Age = 35 });

        await _db.SaveChangesAsync();
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Index-optimized WHERE queries (Person has index on Age)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Linq_Where_Equal_WithIndex_ReturnsCorrectResult()
    {
        var results = _db.People.AsQueryable().Where(x => x.Age == 30).ToList();
        Assert.Single(results);
        Assert.Equal("P1", results[0].Name);
    }

    [Fact]
    public void Linq_Where_GreaterThan_WithIndex_ReturnsRange()
    {
        var results = _db.People.AsQueryable().Where(x => x.Age > 30).ToList();
        Assert.Equal(2, results.Count); // 35, 40
    }

    [Fact]
    public void Linq_Where_GreaterThanOrEqual_WithIndex()
    {
        var results = _db.People.AsQueryable().Where(x => x.Age >= 35).ToList();
        Assert.Equal(2, results.Count); // 35, 40
    }

    [Fact]
    public void Linq_Where_LessThan_WithIndex()
    {
        var results = _db.People.AsQueryable().Where(x => x.Age < 25).ToList();
        Assert.Single(results);
        Assert.Equal("P4", results[0].Name);
    }

    [Fact]
    public void Linq_Where_LessThanOrEqual_WithIndex()
    {
        var results = _db.People.AsQueryable().Where(x => x.Age <= 25).ToList();
        Assert.Equal(2, results.Count); // 20, 25
    }

    [Fact]
    public void Linq_Where_Range_AndAlso_WithIndex()
    {
        // Should merge into a range query on the Age index
        var results = _db.People.AsQueryable().Where(x => x.Age >= 25 && x.Age <= 35).ToList();
        Assert.Equal(3, results.Count); // 25, 30, 35
    }

    [Fact]
    public void Linq_Where_ClosureCapture_WithIndex()
    {
        int minAge = 30;
        var results = _db.People.AsQueryable().Where(x => x.Age == minAge).ToList();
        Assert.Single(results);
    }

    [Fact]
    public void Linq_Where_FlippedComparison_WithIndex()
    {
        // 30 == x.Age (constant on left, property on right)
        var results = _db.People.AsQueryable().Where(x => 30 == x.Age).ToList();
        Assert.Single(results);
    }

    [Fact]
    public void Linq_Where_NoResults_ReturnsEmpty()
    {
        var results = _db.People.AsQueryable().Where(x => x.Age == 999).ToList();
        Assert.Empty(results);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Count fast path (no WHERE)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Linq_Count_NoWhere_FastPath()
    {
        var count = _db.People.AsQueryable().Count();
        Assert.Equal(5, count);
    }

    [Fact]
    public void Linq_Count_WithWhere()
    {
        var count = _db.People.AsQueryable().Where(x => x.Age > 25).Count();
        Assert.Equal(3, count);
    }

    [Fact]
    public void Linq_LongCount_NoWhere_FastPath()
    {
        var count = _db.People.AsQueryable().LongCount();
        Assert.Equal(5L, count);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  SELECT push-down (single field projection)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Linq_Select_SingleStringField()
    {
        var names = _db.Users.AsQueryable()
            .Where(x => x.Age >= 22)
            .Select(x => x.Name).ToList();
        Assert.Equal(5, names.Count);
        Assert.Contains("Alice", names);
    }

    [Fact]
    public void Linq_Select_SingleIntField()
    {
        var ages = _db.Users.AsQueryable()
            .Where(x => x.Age >= 22)
            .Select(x => x.Age).ToList();
        Assert.Equal(5, ages.Count);
        Assert.Contains(30, ages);
    }

    [Fact]
    public void Linq_Select_DecimalField()
    {
        var prices = _db.Products.AsQueryable()
            .Where(x => x.Price > 0)
            .Select(x => x.Price).ToList();
        Assert.Equal(5, prices.Count);
        Assert.Contains(9.99m, prices);
    }

    [Fact]
    public void Linq_Select_WithWhere()
    {
        var names = _db.Users.AsQueryable()
            .Where(x => x.Age > 28)
            .Select(x => x.Name)
            .ToList();
        Assert.Equal(2, names.Count);
        Assert.Contains("Alice", names);
        Assert.Contains("Charlie", names);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  OrderBy + Take (index optimization path)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Linq_OrderBy_Take_ViaIndex()
    {
        // Price index on Products — OrderBy(Price).Take(3) should use index
        var cheapest = _db.Products.AsQueryable()
            .OrderBy(x => x.Price)
            .Take(3)
            .ToList();
        Assert.Equal(3, cheapest.Count);
        Assert.Equal("Thingy", cheapest[0].Title); // 4.99
    }

    [Fact]
    public void Linq_OrderByDescending_Take_ViaIndex()
    {
        var expensive = _db.Products.AsQueryable()
            .OrderByDescending(x => x.Price)
            .Take(2)
            .ToList();
        Assert.Equal(2, expensive.Count);
        Assert.Equal("Doohickey", expensive[0].Title); // 29.99
    }

    [Fact]
    public void Linq_OrderBy_Take_OnPersonAge()
    {
        // People has index on Age
        var youngest = _db.People.AsQueryable()
            .OrderBy(x => x.Age)
            .Take(2)
            .ToList();
        Assert.Equal(2, youngest.Count);
        Assert.Equal("P4", youngest[0].Name); // Age 20
    }

    [Fact]
    public void Linq_OrderByDescending_Take_OnPersonAge()
    {
        var oldest = _db.People.AsQueryable()
            .OrderByDescending(x => x.Age)
            .Take(2)
            .ToList();
        Assert.Equal(2, oldest.Count);
        Assert.Equal("P3", oldest[0].Name); // Age 40
    }

    // ══════════════════════════════════════════════════════════════════════
    //  OrderBy + Skip + Take (pipeline path)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Linq_OrderBy_Skip_Take()
    {
        var page = _db.Users.AsQueryable()
            .OrderBy(x => x.Age)
            .Skip(1)
            .Take(2)
            .ToList();
        Assert.Equal(2, page.Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Terminal operators
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Linq_First_ReturnsFirstElement()
    {
        var first = _db.Users.AsQueryable().First();
        Assert.NotNull(first);
    }

    [Fact]
    public void Linq_FirstOrDefault_WithWhere_ReturnsMatch()
    {
        var user = _db.Users.AsQueryable().Where(x => x.Name == "Alice").FirstOrDefault();
        Assert.NotNull(user);
        Assert.Equal("Alice", user!.Name);
    }

    [Fact]
    public void Linq_FirstOrDefault_NoMatch_ReturnsNull()
    {
        var user = _db.Users.AsQueryable().Where(x => x.Name == "Nobody").FirstOrDefault();
        Assert.Null(user);
    }

    [Fact]
    public void Linq_Any_WithData_ReturnsTrue()
    {
        Assert.True(_db.Users.AsQueryable().Any());
    }

    [Fact]
    public void Linq_Any_WithPredicate()
    {
        Assert.True(_db.Users.AsQueryable().Any(x => x.Age > 30));
        Assert.False(_db.Users.AsQueryable().Any(x => x.Age > 100));
    }

    [Fact]
    public void Linq_ToArray()
    {
        var arr = _db.Users.AsQueryable().ToArray();
        Assert.Equal(5, arr.Length);
    }

    [Fact]
    public void Linq_ToList()
    {
        var list = _db.Users.AsQueryable().ToList();
        Assert.Equal(5, list.Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Complex operator fallback (GroupBy, Join → EnumerableRewriter)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Linq_GroupBy_FallsBackCorrectly()
    {
        var groups = _db.Users.AsQueryable()
            .GroupBy(x => x.Age > 28)
            .ToList();
        Assert.Equal(2, groups.Count);
    }

    [Fact]
    public void Linq_Where_String_Equals()
    {
        var results = _db.Users.AsQueryable().Where(x => x.Name == "Bob").ToList();
        Assert.Single(results);
        Assert.Equal(25, results[0].Age);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Async enumerable through typed query
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Linq_AsAsyncEnumerable_ReturnsAll()
    {
        var results = new List<User>();
        await foreach (var user in (IAsyncEnumerable<User>)_db.Users.AsQueryable())
        {
            results.Add(user);
        }
        Assert.Equal(5, results.Count);
    }

    [Fact]
    public async Task Linq_AsAsyncEnumerable_WithWhere()
    {
        // Use People collection (int ID) to avoid cross-collection BSON type conflicts
        var query = _db.People.AsQueryable().Where(x => x.Age > 30);
        var results = new List<Person>();
        await foreach (var person in (IAsyncEnumerable<Person>)query)
        {
            results.Add(person);
        }
        Assert.Equal(2, results.Count); // P3 (40), P5 (35)
    }

    // ══════════════════════════════════════════════════════════════════════
    //  StartsWith optimization (BTree prefix scan)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Linq_Where_StartsWith_NoIndex_ReturnsFiltered()
    {
        // Users don't have an index on Name, but StartsWith should still work via scan
        var results = _db.Users.AsQueryable().Where(x => x.Name.StartsWith("A")).ToList();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Equals method call optimization
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Linq_Where_EqualsMethod()
    {
        var target = "Charlie";
        var results = _db.Users.AsQueryable().Where(x => x.Name.Equals(target)).ToList();
        Assert.Single(results);
        Assert.Equal(35, results[0].Age);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Select + OrderBy + Take (combined)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Linq_Select_After_OrderBy_Take()
    {
        // OrderBy + Take + Select (order matters: Select after Take)
        var names = _db.Products.AsQueryable()
            .OrderBy(x => x.Price)
            .Take(3)
            .Select(x => x.Title)
            .ToList();
        Assert.Equal(3, names.Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  IntEntity (int key) and LongEntity (long key) tests
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Linq_IntEntity_Count()
    {
        await _db.IntEntities.InsertAsync(new IntEntity { Id = 1, Name = "A" });
        await _db.IntEntities.InsertAsync(new IntEntity { Id = 2, Name = "B" });
        Assert.Equal(2, _db.IntEntities.AsQueryable().Count());
    }

    [Fact]
    public async Task Linq_LongEntity_SelectField()
    {
        await _db.LongEntities.InsertAsync(new LongEntity { Id = 1, Name = "X" });
        await _db.LongEntities.InsertAsync(new LongEntity { Id = 2, Name = "Y" });
        await _db.SaveChangesAsync();
        var results = await _db.LongEntities.AsQueryable().ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.Name == "X");
    }

    [Fact]
    public async Task Linq_LongEntity_SelectId()
    {
        await _db.LongEntities.InsertAsync(new LongEntity { Id = 100, Name = "Z" });
        var ids = await _db.LongEntities.AsQueryable().Select(x => x.Id).ToListAsync();
        Assert.Contains(100L, ids);
    }
}
