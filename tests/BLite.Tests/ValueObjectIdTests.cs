using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using Xunit;

namespace BLite.Tests;

public record OrderId(string Value)
{
    public OrderId() : this(string.Empty) { }
}

public class OrderIdConverter : ValueConverter<OrderId, string>
{
    public override string ConvertToProvider(OrderId model) => model?.Value ?? string.Empty;
    public override OrderId ConvertFromProvider(string provider) => new OrderId(provider);
}

public class Order
{
    public OrderId Id { get; set; } = null!;
    public string CustomerName { get; set; } = "";
}

public partial class TestDbContext : DocumentDbContext
{
    public DocumentCollection<OrderId, Order> Orders { get; set; } = null!;
}

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
