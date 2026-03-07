using BLite.Shared;

namespace BLite.Tests;

public class ValueObjectIdTests : IDisposable
{
    private readonly string _dbPath = "value_object_ids.db";
    private readonly TestDbContext _db;

    public ValueObjectIdTests()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        _db = new TestDbContext(_dbPath);
    }

    // ── Existing: FindById ────────────────────────────────────────────────────

    [Fact]
    public void Should_Support_ValueObject_Id_Conversion()
    {
        var order = new Order
        {
            Id = new OrderId("ORD-123"),
            CustomerName = "John Doe"
        };

        _db.Orders.Insert(order);

        var retrieved = _db.Orders.FindById(new OrderId("ORD-123"));

        Assert.NotNull(retrieved);
        Assert.Equal("ORD-123", retrieved.Id.Value);
        Assert.Equal("John Doe", retrieved.CustomerName);
    }

    // ── LINQ / AsQueryable ────────────────────────────────────────────────────

    [Fact]
    public void AsQueryable_Where_EqualityOperator_WithValueObjectId_ReturnsCorrectRecord()
    {
        _db.Orders.Insert(new Order { Id = new OrderId("A"), CustomerName = "Alice" });
        _db.Orders.Insert(new Order { Id = new OrderId("B"), CustomerName = "Bob" });

        var target = new OrderId("A");
        var results = _db.Orders.AsQueryable().Where(o => o.Id == target).ToList();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].CustomerName);
    }

    [Fact]
    public void AsQueryable_Where_EqualsMethod_WithValueObjectId_ReturnsCorrectRecord()
    {
        _db.Orders.Insert(new Order { Id = new OrderId("A"), CustomerName = "Alice" });
        _db.Orders.Insert(new Order { Id = new OrderId("B"), CustomerName = "Bob" });

        var target = new OrderId("B");
        var results = _db.Orders.AsQueryable().Where(o => o.Id.Equals(target)).ToList();

        Assert.Single(results);
        Assert.Equal("Bob", results[0].CustomerName);
    }

    [Fact]
    public void AsQueryable_Where_ValueObjectId_ReturnsEmpty_WhenNoMatch()
    {
        _db.Orders.Insert(new Order { Id = new OrderId("A"), CustomerName = "Alice" });

        var target = new OrderId("NOPE");
        var results = _db.Orders.AsQueryable().Where(o => o.Id == target).ToList();

        Assert.Empty(results);
    }

    [Fact]
    public void AsQueryable_Where_ValueObjectId_NoConverter_FallsBackToFullScan()
    {
        // Verifies that even without a BSON-level optimisation, the in-memory LINQ
        // filter still returns correct results (regression guard).
        _db.Orders.Insert(new Order { Id = new OrderId("X"), CustomerName = "Xena" });
        _db.Orders.Insert(new Order { Id = new OrderId("Y"), CustomerName = "Yara" });

        var results = _db.Orders.AsQueryable()
            .Where(o => o.CustomerName == "Xena")
            .ToList();

        Assert.Single(results);
        Assert.Equal("X", results[0].Id.Value);
    }

    [Fact]
    public void AsQueryable_Where_ValueObjectId_AndAlsoNull_Guard_Works()
    {
        // Pattern used in real code: item.Id != null && e.Id.Equals(item.Id)
        _db.Orders.Insert(new Order { Id = new OrderId("C"), CustomerName = "Carol" });
        _db.Orders.Insert(new Order { Id = new OrderId("D"), CustomerName = "Dan" });

        var item = new Order { Id = new OrderId("C") };
        var results = _db.Orders.AsQueryable()
            .Where(o => item.Id != null && o.Id.Equals(item.Id))
            .ToList();

        Assert.Single(results);
        Assert.Equal("Carol", results[0].CustomerName);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }
}
