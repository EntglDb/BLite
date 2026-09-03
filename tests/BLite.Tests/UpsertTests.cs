using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

public class UpsertTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;
    private readonly TestDbContext _db;

    public UpsertTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_upsert_{Guid.NewGuid()}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"test_upsert_{Guid.NewGuid()}.wal");

        _db = new TestDbContext(_dbPath);
    }

    [Fact]
    public async Task Upsert_With_No_Id_Inserts_New_Document()
    {
        var user = new User { Name = "Alice", Age = 30 };

        var result = await _db.Users.UpsertAsync(user);
        await _db.SaveChangesAsync();

        Assert.True(result.Inserted);
        Assert.NotEqual(default, result.Id);

        var found = await _db.Users.FindByIdAsync(result.Id);
        Assert.NotNull(found);
        Assert.Equal("Alice", found.Name);
    }

    [Fact]
    public async Task Upsert_With_Explicit_Unused_Id_Inserts()
    {
        var id = 42;
        var product = new Product { Id = id, Title = "Widget", Price = 9.99m };

        var result = await _db.Products.UpsertAsync(product);
        await _db.SaveChangesAsync();

        Assert.True(result.Inserted);
        Assert.Equal(id, result.Id);

        var found = await _db.Products.FindByIdAsync(id);
        Assert.NotNull(found);
        Assert.Equal("Widget", found.Title);
        Assert.Equal(1, await _db.Products.CountAsync());
    }

    [Fact]
    public async Task Upsert_With_Existing_Id_Replaces_Document()
    {
        var id = await _db.Products.InsertAsync(new Product { Title = "Widget", Price = 9.99m });
        await _db.SaveChangesAsync();

        var result = await _db.Products.UpsertAsync(new Product { Id = id, Title = "Widget Pro", Price = 19.99m });
        await _db.SaveChangesAsync();

        Assert.False(result.Inserted);
        Assert.Equal(id, result.Id);

        var found = await _db.Products.FindByIdAsync(id);
        Assert.NotNull(found);
        Assert.Equal("Widget Pro", found.Title);
        Assert.Equal(19.99m, found.Price);
        Assert.Equal(1, await _db.Products.CountAsync());
    }

    [Fact]
    public async Task Upsert_Does_Not_Throw_Duplicate_Key_Unlike_Insert()
    {
        var id = await _db.Products.InsertAsync(new Product { Title = "Widget", Price = 9.99m });
        await _db.SaveChangesAsync();

        // A plain InsertAsync of the same id would throw "Duplicate key violation".
        // UpsertAsync must instead replace it.
        var result = await _db.Products.UpsertAsync(new Product { Id = id, Title = "Widget v2", Price = 12.0m });
        await _db.SaveChangesAsync();

        Assert.False(result.Inserted);
        Assert.Equal(1, await _db.Products.CountAsync());
    }

    [Fact]
    public async Task UpsertBulk_Mixes_Inserts_And_Updates()
    {
        var existingId = await _db.Products.InsertAsync(new Product { Title = "Widget", Price = 9.99m });
        await _db.SaveChangesAsync();

        var newId = 999;
        var results = await _db.Products.UpsertBulkAsync(new[]
        {
            new Product { Id = existingId, Title = "Widget Pro", Price = 19.99m },
            new Product { Id = newId, Title = "Gadget", Price = 5.0m },
        });
        await _db.SaveChangesAsync();

        Assert.Equal(2, results.Count);
        Assert.False(results[0].Inserted);
        Assert.True(results[1].Inserted);

        Assert.Equal(2, await _db.Products.CountAsync());

        var updated = await _db.Products.FindByIdAsync(existingId);
        Assert.Equal("Widget Pro", updated!.Title);

        var inserted = await _db.Products.FindByIdAsync(newId);
        Assert.NotNull(inserted);
        Assert.Equal("Gadget", inserted!.Title);
    }

    public void Dispose()
    {
        _db?.Dispose();
    }
}
