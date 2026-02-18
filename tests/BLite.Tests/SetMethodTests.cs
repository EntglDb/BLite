using BLite.Bson;
using BLite.Core.Collections;
using BLite.Shared;

namespace BLite.Tests;

public class SetMethodTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public SetMethodTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_set_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Fact]
    public void Set_ObjectId_ReturnsCorrectCollection()
    {
        var collection = _db.Set<ObjectId, User>();
        Assert.NotNull(collection);
        Assert.Same(_db.Users, collection);
    }

    [Fact]
    public void Set_Shorthand_ReturnsCorrectCollection()
    {
        var collection = _db.Set<User>();
        Assert.NotNull(collection);
        Assert.Same(_db.Users, collection);
    }

    [Fact]
    public void Set_Int_ReturnsCorrectCollection()
    {
        var collection = _db.Set<int, Person>();
        Assert.NotNull(collection);
        Assert.Same(_db.People, collection);
    }

    [Fact]
    public void Set_String_ReturnsCorrectCollection()
    {
        var collection = _db.Set<string, StringEntity>();
        Assert.NotNull(collection);
        Assert.Same(_db.StringEntities, collection);
    }

    [Fact]
    public void Set_Guid_ReturnsCorrectCollection()
    {
        var collection = _db.Set<Guid, GuidEntity>();
        Assert.NotNull(collection);
        Assert.Same(_db.GuidEntities, collection);
    }

    [Fact]
    public void Set_CustomKey_ReturnsCorrectCollection()
    {
        var collection = _db.Set<OrderId, Order>();
        Assert.NotNull(collection);
        Assert.Same(_db.Orders, collection);
    }

    [Fact]
    public void Set_AllObjectIdCollections_ReturnCorrectInstances()
    {
        Assert.Same(_db.AnnotatedUsers, _db.Set<ObjectId, AnnotatedUser>());
        Assert.Same(_db.ComplexUsers, _db.Set<ObjectId, ComplexUser>());
        Assert.Same(_db.TestDocuments, _db.Set<ObjectId, TestDocument>());
        Assert.Same(_db.OrderDocuments, _db.Set<ObjectId, OrderDocument>());
        Assert.Same(_db.ComplexDocuments, _db.Set<ObjectId, ComplexDocument>());
        Assert.Same(_db.PeopleV2, _db.Set<ObjectId, PersonV2>());
        Assert.Same(_db.VectorItems, _db.Set<ObjectId, VectorEntity>());
        Assert.Same(_db.GeoItems, _db.Set<ObjectId, GeoEntity>());
    }

    [Fact]
    public void Set_AllIntCollections_ReturnCorrectInstances()
    {
        Assert.Same(_db.AutoInitEntities, _db.Set<int, AutoInitEntity>());
        Assert.Same(_db.Products, _db.Set<int, Product>());
        Assert.Same(_db.IntEntities, _db.Set<int, IntEntity>());
        Assert.Same(_db.AsyncDocs, _db.Set<int, AsyncDoc>());
        Assert.Same(_db.SchemaUsers, _db.Set<int, SchemaUser>());
    }

    [Fact]
    public void Set_StringKeyCollections_ReturnCorrectInstances()
    {
        Assert.Same(_db.CustomKeyEntities, _db.Set<string, CustomKeyEntity>());
    }

    [Fact]
    public void Set_UnregisteredEntity_ThrowsInvalidOperationException()
    {
        Assert.Throws<InvalidOperationException>(() => _db.Set<ObjectId, Address>());
    }

    [Fact]
    public void Set_WrongKeyType_ThrowsInvalidOperationException()
    {
        Assert.Throws<InvalidOperationException>(() => _db.Set<string, User>());
    }

    [Fact]
    public void Set_CanPerformOperations()
    {
        var users = _db.Set<User>();

        var user = new User { Name = "Alice", Age = 30 };
        var id = users.Insert(user);

        var found = users.FindById(id);
        Assert.NotNull(found);
        Assert.Equal("Alice", found.Name);
        Assert.Equal(30, found.Age);
    }

    [Fact]
    public void Set_WithIntKey_CanPerformOperations()
    {
        var products = _db.Set<int, Product>();

        var product = new Product { Id = 1, Title = "Widget", Price = 9.99m };
        products.Insert(product);

        var found = products.FindById(1);
        Assert.NotNull(found);
        Assert.Equal("Widget", found.Title);
        Assert.Equal(9.99m, found.Price);
    }
}
