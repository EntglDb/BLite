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

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }
}
