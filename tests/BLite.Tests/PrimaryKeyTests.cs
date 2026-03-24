using BLite.Shared;

namespace BLite.Tests;

public class PrimaryKeyTests : IDisposable
{
    private readonly string _dbPath = "primary_key_tests.db";

    public PrimaryKeyTests()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Fact]
    public async Task Test_Int_PrimaryKey()
    {
        using var db = new TestDbContext(_dbPath);

        var entity = new IntEntity { Id = 1, Name = "Test 1" };
        await db.IntEntities.InsertAsync(entity);
        await db.SaveChangesAsync();

        var retrieved = await db.IntEntities.FindByIdAsync(1);
        Assert.NotNull(retrieved);
        Assert.Equal(1, retrieved.Id);
        Assert.Equal("Test 1", retrieved.Name);

        entity.Name = "Updated";
        await db.IntEntities.UpdateAsync(entity);

        retrieved = await db.IntEntities.FindByIdAsync(1);
        Assert.Equal("Updated", retrieved?.Name);

        await db.IntEntities.DeleteAsync(1);
        Assert.Null(await db.IntEntities.FindByIdAsync(1));
    }

    [Fact]
    public async Task Test_String_PrimaryKey()
    {
        using var db = new TestDbContext(_dbPath);

        var entity = new StringEntity { Id = "key1", Value = "Value 1" };
        await db.StringEntities.InsertAsync(entity);
        await db.SaveChangesAsync();

        var retrieved = await db.StringEntities.FindByIdAsync("key1");
        Assert.NotNull(retrieved);
        Assert.Equal("key1", retrieved.Id);
        Assert.Equal("Value 1", retrieved.Value);

        await db.StringEntities.DeleteAsync("key1");
        await db.SaveChangesAsync();
        Assert.Null(await db.StringEntities.FindByIdAsync("key1"));
    }

    [Fact]
    public async Task Test_Guid_PrimaryKey()
    {
        using var db = new TestDbContext(_dbPath);

        var id = Guid.NewGuid();
        var entity = new GuidEntity { Id = id, Name = "Guid Test" };
        await db.GuidEntities.InsertAsync(entity);
        await db.SaveChangesAsync();

        var retrieved = await db.GuidEntities.FindByIdAsync(id);
        Assert.NotNull(retrieved);
        Assert.Equal(id, retrieved.Id);

        await db.GuidEntities.DeleteAsync(id);
        await db.SaveChangesAsync();
        Assert.Null(await db.GuidEntities.FindByIdAsync(id));
    }

    [Fact]
    public async Task Test_String_PrimaryKey_With_Custom_Name()
    {
        // Test entity with string key NOT named "Id" (named "Code" instead)
        using var db = new TestDbContext(_dbPath);

        var entity = new CustomKeyEntity { Code = "ABC123", Description = "Test Description" };
        await db.CustomKeyEntities.InsertAsync(entity);
        await db.SaveChangesAsync();

        // Verify retrieval works correctly
        var retrieved = await db.CustomKeyEntities.FindByIdAsync("ABC123");
        Assert.NotNull(retrieved);
        Assert.Equal("ABC123", retrieved.Code);
        Assert.Equal("Test Description", retrieved.Description);

        // Verify update works
        entity.Description = "Updated Description";
        await db.CustomKeyEntities.UpdateAsync(entity);
        await db.SaveChangesAsync();

        retrieved = await db.CustomKeyEntities.FindByIdAsync("ABC123");
        Assert.Equal("Updated Description", retrieved?.Description);

        // Verify delete works
        await db.CustomKeyEntities.DeleteAsync("ABC123");
        await db.SaveChangesAsync();
        Assert.Null(await db.CustomKeyEntities.FindByIdAsync("ABC123"));
    }
}
