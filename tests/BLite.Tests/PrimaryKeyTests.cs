using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using BLite.Shared;
using System;
using System.Buffers;

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
    public void Test_Int_PrimaryKey()
    {
        using var db = new TestDbContext(_dbPath);

        var entity = new IntEntity { Id = 1, Name = "Test 1" };
        db.IntEntities.Insert(entity);
        db.SaveChanges();

        var retrieved = db.IntEntities.FindById(1);
        Assert.NotNull(retrieved);
        Assert.Equal(1, retrieved.Id);
        Assert.Equal("Test 1", retrieved.Name);

        entity.Name = "Updated";
        db.IntEntities.Update(entity);

        retrieved = db.IntEntities.FindById(1);
        Assert.Equal("Updated", retrieved?.Name);

        db.IntEntities.Delete(1);
        Assert.Null(db.IntEntities.FindById(1));
    }

    [Fact]
    public void Test_String_PrimaryKey()
    {
        using var db = new TestDbContext(_dbPath);

        var entity = new StringEntity { Id = "key1", Value = "Value 1" };
        db.StringEntities.Insert(entity);
        db.SaveChanges();

        var retrieved = db.StringEntities.FindById("key1");
        Assert.NotNull(retrieved);
        Assert.Equal("key1", retrieved.Id);
        Assert.Equal("Value 1", retrieved.Value);

        db.StringEntities.Delete("key1");
        db.SaveChanges();
        Assert.Null(db.StringEntities.FindById("key1"));
    }

    [Fact]
    public void Test_Guid_PrimaryKey()
    {
        using var db = new TestDbContext(_dbPath);

        var id = Guid.NewGuid();
        var entity = new GuidEntity { Id = id, Name = "Guid Test" };
        db.GuidEntities.Insert(entity);
        db.SaveChanges();

        var retrieved = db.GuidEntities.FindById(id);
        Assert.NotNull(retrieved);
        Assert.Equal(id, retrieved.Id);

        db.GuidEntities.Delete(id);
        db.SaveChanges();
        Assert.Null(db.GuidEntities.FindById(id));
    }

    [Fact]
    public void Test_String_PrimaryKey_With_Custom_Name()
    {
        // Test entity with string key NOT named "Id" (named "Code" instead)
        using var db = new TestDbContext(_dbPath);

        var entity = new CustomKeyEntity { Code = "ABC123", Description = "Test Description" };
        db.CustomKeyEntities.Insert(entity);
        db.SaveChanges();

        // Verify retrieval works correctly
        var retrieved = db.CustomKeyEntities.FindById("ABC123");
        Assert.NotNull(retrieved);
        Assert.Equal("ABC123", retrieved.Code);
        Assert.Equal("Test Description", retrieved.Description);

        // Verify update works
        entity.Description = "Updated Description";
        db.CustomKeyEntities.Update(entity);
        db.SaveChanges();

        retrieved = db.CustomKeyEntities.FindById("ABC123");
        Assert.Equal("Updated Description", retrieved?.Description);

        // Verify delete works
        db.CustomKeyEntities.Delete("ABC123");
        db.SaveChanges();
        Assert.Null(db.CustomKeyEntities.FindById("ABC123"));
    }
}
