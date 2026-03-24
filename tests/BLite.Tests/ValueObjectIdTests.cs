using BLite.Core.Query;
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
    public async Task Should_Support_ValueObject_Id_Conversion()
    {
        var order = new Order
        {
            Id = new OrderId("ORD-123"),
            CustomerName = "John Doe"
        };

        await _db.Orders.InsertAsync(order);

        var retrieved = await _db.Orders.FindByIdAsync(new OrderId("ORD-123"));

        Assert.NotNull(retrieved);
        Assert.Equal("ORD-123", retrieved.Id.Value);
        Assert.Equal("John Doe", retrieved.CustomerName);
    }

    // ── LINQ / AsQueryable ────────────────────────────────────────────────────

    [Fact]
    public async Task AsQueryable_Where_EqualityOperator_WithValueObjectId_ReturnsCorrectRecord()
    {
        await _db.Orders.InsertAsync(new Order { Id = new OrderId("A"), CustomerName = "Alice" });
        await _db.Orders.InsertAsync(new Order { Id = new OrderId("B"), CustomerName = "Bob" });

        var target = new OrderId("A");
        var results = await _db.Orders.AsQueryable().Where(o => o.Id == target).ToListAsync();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].CustomerName);
    }

    [Fact]
    public async Task AsQueryable_Where_EqualsMethod_WithValueObjectId_ReturnsCorrectRecord()
    {
        await _db.Orders.InsertAsync(new Order { Id = new OrderId("A"), CustomerName = "Alice" });
        await _db.Orders.InsertAsync(new Order { Id = new OrderId("B"), CustomerName = "Bob" });

        var target = new OrderId("B");
        var results = await _db.Orders.AsQueryable().Where(o => o.Id.Equals(target)).ToListAsync();

        Assert.Single(results);
        Assert.Equal("Bob", results[0].CustomerName);
    }

    [Fact]
    public async Task AsQueryable_Where_ValueObjectId_ReturnsEmpty_WhenNoMatch()
    {
        await _db.Orders.InsertAsync(new Order { Id = new OrderId("A"), CustomerName = "Alice" });

        var target = new OrderId("NOPE");
        var results = await _db.Orders.AsQueryable().Where(o => o.Id == target).ToListAsync();

        Assert.Empty(results);
    }

    [Fact]
    public async Task AsQueryable_Where_ValueObjectId_NoConverter_FallsBackToFullScan()
    {
        // Verifies that even without a BSON-level optimisation, the in-memory LINQ
        // filter still returns correct results (regression guard).
        await _db.Orders.InsertAsync(new Order { Id = new OrderId("X"), CustomerName = "Xena" });
        await _db.Orders.InsertAsync(new Order { Id = new OrderId("Y"), CustomerName = "Yara" });

        var results = await _db.Orders.AsQueryable()
            .Where(o => o.CustomerName == "Xena")
            .ToListAsync();
        Assert.Single(results);
        Assert.Equal("X", results[0].Id.Value);
    }

    [Fact]
    public async Task AsQueryable_Where_ValueObjectId_AndAlsoNull_Guard_Works()
    {
        // Pattern used in real code: item.Id != null && e.Id.Equals(item.Id)
        await _db.Orders.InsertAsync(new Order { Id = new OrderId("C"), CustomerName = "Carol" });
        await _db.Orders.InsertAsync(new Order { Id = new OrderId("D"), CustomerName = "Dan" });
        var item = new Order { Id = new OrderId("C") };
        var results = await _db.Orders.AsQueryable()
            .Where(o => item.Id != null && o.Id.Equals(item.Id))
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("Carol", results[0].CustomerName);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }
}
