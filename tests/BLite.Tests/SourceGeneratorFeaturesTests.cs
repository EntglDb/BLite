using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Tests for Source Generator enhancements:
/// 1. Property inheritance from base classes (including Id)
/// 2. Exclusion of computed getter-only properties
/// 3. Recognition of advanced collection types (HashSet, ISet, LinkedList, etc.)
/// </summary>
public class SourceGeneratorFeaturesTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;
    private readonly TestDbContext _db;

    public SourceGeneratorFeaturesTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_sg_features_{Guid.NewGuid()}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"test_sg_features_{Guid.NewGuid()}.wal");
        
        _db = new TestDbContext(_dbPath);
    }

    #region Inheritance Tests

    [Fact]
    public async Task DerivedEntity_InheritsId_FromBaseClass()
    {
        // Arrange
        var entity = new DerivedEntity
        {
            Name = "Test Entity",
            Description = "Testing inheritance",
            CreatedAt = DateTime.UtcNow
        };

        // Act
        var id = await _db.DerivedEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();
        var retrieved = await _db.DerivedEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(retrieved);
        Assert.Equal(id, retrieved.Id); // Id from base class should work
        Assert.Equal("Test Entity", retrieved.Name);
        Assert.Equal("Testing inheritance", retrieved.Description);
        Assert.Equal(entity.CreatedAt.Date, retrieved.CreatedAt.Date); // Compare just date part
    }

    [Fact]
    public async Task DerivedEntity_Update_WorksWithInheritedId()
    {
        // Arrange
        var entity = new DerivedEntity
        {
            Name = "Original",
            Description = "Original Description",
            CreatedAt = DateTime.UtcNow
        };
        var id = await _db.DerivedEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        // Act
        var retrieved = await _db.DerivedEntities.FindByIdAsync(id);
        Assert.NotNull(retrieved);
        retrieved.Name = "Updated";
        retrieved.Description = "Updated Description";
        await _db.DerivedEntities.UpdateAsync(retrieved);
        await _db.SaveChangesAsync();

        var updated = await _db.DerivedEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(updated);
        Assert.Equal(id, updated.Id);
        Assert.Equal("Updated", updated.Name);
        Assert.Equal("Updated Description", updated.Description);
    }

    [Fact]
    public async Task DerivedEntity_Query_WorksWithInheritedProperties()
    {
        // Arrange
        var now = DateTime.UtcNow;
        var entity1 = new DerivedEntity { Name = "Entity1", CreatedAt = now.AddDays(-2) };
        var entity2 = new DerivedEntity { Name = "Entity2", CreatedAt = now.AddDays(-1) };
        var entity3 = new DerivedEntity { Name = "Entity3", CreatedAt = now };

        await _db.DerivedEntities.InsertAsync(entity1);
        await _db.DerivedEntities.InsertAsync(entity2);
        await _db.DerivedEntities.InsertAsync(entity3);
        await _db.SaveChangesAsync();

        // Act - Query using inherited property
        var recent = await _db.DerivedEntities.FindAsync(e => e.CreatedAt >= now.AddDays(-1.5)).ToListAsync();

        // Assert
        Assert.Equal(2, recent.Count);
        Assert.Contains(recent, e => e.Name == "Entity2");
        Assert.Contains(recent, e => e.Name == "Entity3");
    }

    #endregion

    #region Computed Properties Tests

    [Fact]
    public async Task ComputedProperties_AreNotSerialized()
    {
        // Arrange
        var entity = new EntityWithComputedProperties
        {
            FirstName = "John",
            LastName = "Doe",
            BirthYear = 1990
        };

        // Act
        var id = await _db.ComputedPropertyEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();
        var retrieved = await _db.ComputedPropertyEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(retrieved);
        Assert.Equal("John", retrieved.FirstName);
        Assert.Equal("Doe", retrieved.LastName);
        Assert.Equal(1990, retrieved.BirthYear);
        
        // Computed properties should still work after deserialization
        Assert.Equal("John Doe", retrieved.FullName);
        Assert.True(retrieved.Age >= 34); // Born in 1990, so at least 34 in 2024+
        Assert.Contains("John Doe", retrieved.DisplayInfo);
    }

    [Fact]
    public async Task ComputedProperties_UpdateDoesNotBreak()
    {
        // Arrange
        var entity = new EntityWithComputedProperties
        {
            FirstName = "Jane",
            LastName = "Smith",
            BirthYear = 1985
        };
        var id = await _db.ComputedPropertyEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        // Act - UpdateAsync stored properties
        var retrieved = await _db.ComputedPropertyEntities.FindByIdAsync(id);
        Assert.NotNull(retrieved);
        retrieved.FirstName = "Janet";
        retrieved.BirthYear = 1986;
        await _db.ComputedPropertyEntities.UpdateAsync(retrieved);
        await _db.SaveChangesAsync();

        var updated = await _db.ComputedPropertyEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(updated);
        Assert.Equal("Janet", updated.FirstName);
        Assert.Equal("Smith", updated.LastName);
        Assert.Equal(1986, updated.BirthYear);
        Assert.Equal("Janet Smith", updated.FullName); // Computed property reflects new data
    }

    #endregion

    #region Advanced Collections Tests

    [Fact]
    public async Task HashSet_SerializesAndDeserializes()
    {
        // Arrange
        var entity = new EntityWithAdvancedCollections
        {
            Name = "Test HashSet"
        };
        entity.Tags.Add("tag1");
        entity.Tags.Add("tag2");
        entity.Tags.Add("tag3");

        // Act
        var id = await _db.AdvancedCollectionEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();
        var retrieved = await _db.AdvancedCollectionEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.Tags);
        Assert.IsType<HashSet<string>>(retrieved.Tags);
        Assert.Equal(3, retrieved.Tags.Count);
        Assert.Contains("tag1", retrieved.Tags);
        Assert.Contains("tag2", retrieved.Tags);
        Assert.Contains("tag3", retrieved.Tags);
    }

    [Fact]
    public async Task ISet_SerializesAndDeserializes()
    {
        // Arrange
        var entity = new EntityWithAdvancedCollections
        {
            Name = "Test ISet"
        };
        entity.Numbers.Add(10);
        entity.Numbers.Add(20);
        entity.Numbers.Add(30);
        entity.Numbers.Add(10); // Duplicate - should be ignored by set

        // Act
        var id = await _db.AdvancedCollectionEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();
        var retrieved = await _db.AdvancedCollectionEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.Numbers);
        Assert.IsAssignableFrom<ISet<int>>(retrieved.Numbers);
        Assert.Equal(3, retrieved.Numbers.Count); // Only unique values
        Assert.Contains(10, retrieved.Numbers);
        Assert.Contains(20, retrieved.Numbers);
        Assert.Contains(30, retrieved.Numbers);
    }

    [Fact]
    public async Task LinkedList_SerializesAndDeserializes()
    {
        // Arrange
        var entity = new EntityWithAdvancedCollections
        {
            Name = "Test LinkedList"
        };
        entity.History.AddLast("first");
        entity.History.AddLast("second");
        entity.History.AddLast("third");

        // Act
        var id = await _db.AdvancedCollectionEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();
        var retrieved = await _db.AdvancedCollectionEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.History);
        // LinkedList may be deserialized as List, then need conversion
        var historyList = retrieved.History.ToList();
        Assert.Equal(3, historyList.Count);
        Assert.Equal("first", historyList[0]);
        Assert.Equal("second", historyList[1]);
        Assert.Equal("third", historyList[2]);
    }

    [Fact]
    public async Task Queue_SerializesAndDeserializes()
    {
        // Arrange
        var entity = new EntityWithAdvancedCollections
        {
            Name = "Test Queue"
        };
        entity.PendingItems.Enqueue("item1");
        entity.PendingItems.Enqueue("item2");
        entity.PendingItems.Enqueue("item3");

        // Act
        var id = await _db.AdvancedCollectionEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();
        var retrieved = await _db.AdvancedCollectionEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.PendingItems);
        Assert.Equal(3, retrieved.PendingItems.Count);
        var items = retrieved.PendingItems.ToList();
        Assert.Contains("item1", items);
        Assert.Contains("item2", items);
        Assert.Contains("item3", items);
    }

    [Fact]
    public async Task Stack_SerializesAndDeserializes()
    {
        // Arrange
        var entity = new EntityWithAdvancedCollections
        {
            Name = "Test Stack"
        };
        entity.UndoStack.Push("action1");
        entity.UndoStack.Push("action2");
        entity.UndoStack.Push("action3");

        // Act
        var id = await _db.AdvancedCollectionEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();
        var retrieved = await _db.AdvancedCollectionEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.UndoStack);
        Assert.Equal(3, retrieved.UndoStack.Count);
        var items = retrieved.UndoStack.ToList();
        Assert.Contains("action1", items);
        Assert.Contains("action2", items);
        Assert.Contains("action3", items);
    }

    [Fact]
    public async Task HashSet_WithNestedObjects_SerializesAndDeserializes()
    {
        // Arrange
        var entity = new EntityWithAdvancedCollections
        {
            Name = "Test Nested HashSet"
        };
        entity.Addresses.Add(new Address { Street = "123 Main St", City = new City { Name = "NYC", ZipCode = "10001" } });
        entity.Addresses.Add(new Address { Street = "456 Oak Ave", City = new City { Name = "LA", ZipCode = "90001" } });

        // Act
        var id = await _db.AdvancedCollectionEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();
        var retrieved = await _db.AdvancedCollectionEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.Addresses);
        Assert.IsType<HashSet<Address>>(retrieved.Addresses);
        Assert.Equal(2, retrieved.Addresses.Count);
        
        var addressList = retrieved.Addresses.ToList();
        Assert.Contains(addressList, a => a.Street == "123 Main St" && a.City.Name == "NYC");
        Assert.Contains(addressList, a => a.Street == "456 Oak Ave" && a.City.Name == "LA");
    }

    [Fact]
    public async Task ISet_WithNestedObjects_SerializesAndDeserializes()
    {
        // Arrange
        var entity = new EntityWithAdvancedCollections
        {
            Name = "Test Nested ISet"
        };
        entity.FavoriteCities.Add(new City { Name = "Paris", ZipCode = "75001" });
        entity.FavoriteCities.Add(new City { Name = "Tokyo", ZipCode = "100-0001" });
        entity.FavoriteCities.Add(new City { Name = "London", ZipCode = "SW1A" });

        // Act
        var id = await _db.AdvancedCollectionEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();
        var retrieved = await _db.AdvancedCollectionEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.FavoriteCities);
        Assert.IsAssignableFrom<ISet<City>>(retrieved.FavoriteCities);
        Assert.Equal(3, retrieved.FavoriteCities.Count);
        
        var cityNames = retrieved.FavoriteCities.Select(c => c.Name).ToList();
        Assert.Contains("Paris", cityNames);
        Assert.Contains("Tokyo", cityNames);
        Assert.Contains("London", cityNames);
    }

    [Fact]
    public async Task AdvancedCollections_AllTypesInSingleEntity()
    {
        // Arrange - Test all collection types at once
        var entity = new EntityWithAdvancedCollections
        {
            Name = "Complete Test"
        };
        
        entity.Tags.Add("tag1");
        entity.Tags.Add("tag2");
        
        entity.Numbers.Add(1);
        entity.Numbers.Add(2);
        
        entity.History.AddLast("h1");
        entity.History.AddLast("h2");
        
        entity.PendingItems.Enqueue("p1");
        entity.PendingItems.Enqueue("p2");
        
        entity.UndoStack.Push("u1");
        entity.UndoStack.Push("u2");
        
        entity.Addresses.Add(new Address { Street = "Street1" });
        entity.FavoriteCities.Add(new City { Name = "City1" });

        // Act
        var id = await _db.AdvancedCollectionEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();
        var retrieved = await _db.AdvancedCollectionEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(retrieved);
        Assert.Equal("Complete Test", retrieved.Name);
        Assert.Equal(2, retrieved.Tags.Count);
        Assert.Equal(2, retrieved.Numbers.Count);
        Assert.Equal(2, retrieved.History.Count);
        Assert.Equal(2, retrieved.PendingItems.Count);
        Assert.Equal(2, retrieved.UndoStack.Count);
        Assert.Single(retrieved.Addresses);
        Assert.Single(retrieved.FavoriteCities);
    }

    #endregion

    #region Private Setters Tests

    [Fact]
    public async Task EntityWithPrivateSetters_CanBeDeserialized()
    {
        // Arrange
        var entity = EntityWithPrivateSetters.Create("John Doe", 30);

        // Act
        var id = await _db.PrivateSetterEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();
        var retrieved = await _db.PrivateSetterEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(retrieved);
        Assert.Equal(id, retrieved.Id);
        Assert.Equal("John Doe", retrieved.Name);
        Assert.Equal(30, retrieved.Age);
    }

    [Fact]
    public async Task EntityWithPrivateSetters_Update_Works()
    {
        // Arrange
        var entity1 = EntityWithPrivateSetters.Create("Alice", 25);
        var id1 = await _db.PrivateSetterEntities.InsertAsync(entity1);
        
        var entity2 = EntityWithPrivateSetters.Create("Bob", 35);
        entity2.GetType().GetProperty("Id")!.SetValue(entity2, id1); // Force same Id
        
        await _db.PrivateSetterEntities.UpdateAsync(entity2);
        await _db.SaveChangesAsync();

        // Act
        var retrieved = await _db.PrivateSetterEntities.FindByIdAsync(id1);
        // Assert
        Assert.NotNull(retrieved);
        Assert.Equal(id1, retrieved.Id);
        Assert.Equal("Bob", retrieved.Name);
        Assert.Equal(35, retrieved.Age);
    }

    [Fact]
    public async Task EntityWithPrivateSetters_Query_Works()
    {
        // Arrange
        var entity1 = EntityWithPrivateSetters.Create("Charlie", 20);
        var entity2 = EntityWithPrivateSetters.Create("Diana", 30);
        var entity3 = EntityWithPrivateSetters.Create("Eve", 40);

        await _db.PrivateSetterEntities.InsertAsync(entity1);
        await _db.PrivateSetterEntities.InsertAsync(entity2);
        await _db.PrivateSetterEntities.InsertAsync(entity3);
        await _db.SaveChangesAsync();

        // Act
        var adults = await _db.PrivateSetterEntities.FindAsync(e => e.Age >= 30).ToListAsync();

        // Assert
        Assert.Equal(2, adults.Count);
        Assert.Contains(adults, e => e.Name == "Diana");
        Assert.Contains(adults, e => e.Name == "Eve");
    }

    #endregion

    #region Inherited Private Setters Tests

    [Fact]
    public async Task EntityWithInheritedPrivateSetters_CanBeDeserialized()
    {
        // Arrange
        var entity = EntityWithInheritedPrivateSetters.Create("Alice", 25, "system");

        await _db.InheritedPrivateSetterEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        // Act
        var result = await _db.InheritedPrivateSetterEntities.FindByIdAsync(entity.Id);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(entity.Name, result.Name);
        Assert.Equal(entity.Age, result.Age);
        Assert.Equal(entity.CreatedBy, result.CreatedBy);
        Assert.Equal(entity.IsDeleted, result.IsDeleted);
    }

    [Fact]
    public async Task EntityWithInheritedPrivateSetters_Update_Works()
    {
        // Arrange
        var entity = EntityWithInheritedPrivateSetters.Create("Bob", 35, "system");

        await _db.InheritedPrivateSetterEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        // Act — update via replace
        var fetched = await _db.InheritedPrivateSetterEntities.FindByIdAsync(entity.Id);
        Assert.NotNull(fetched);
        await _db.InheritedPrivateSetterEntities.UpdateAsync(fetched);
        await _db.SaveChangesAsync();

        var updated = await _db.InheritedPrivateSetterEntities.FindByIdAsync(entity.Id);

        // Assert
        Assert.NotNull(updated);
        Assert.Equal(fetched.Name, updated.Name);
        Assert.Equal(fetched.Age, updated.Age);
        Assert.Equal(fetched.CreatedBy, updated.CreatedBy);
    }

    [Fact]
    public async Task EntityWithInheritedPrivateSetters_Query_Works()
    {
        // Arrange
        var entity1 = EntityWithInheritedPrivateSetters.Create("Carol", 20, "system");
        var entity2 = EntityWithInheritedPrivateSetters.Create("David", 40, "admin");
        var entity3 = EntityWithInheritedPrivateSetters.Create("Eve", 55, "admin");

        await _db.InheritedPrivateSetterEntities.InsertAsync(entity1);
        await _db.InheritedPrivateSetterEntities.InsertAsync(entity2);
        await _db.InheritedPrivateSetterEntities.InsertAsync(entity3);
        await _db.SaveChangesAsync();

        // Act
        var admins = await _db.InheritedPrivateSetterEntities.FindAsync(e => e.CreatedBy == "admin").ToListAsync();

        // Assert
        Assert.Equal(2, admins.Count);
        Assert.Contains(admins, e => e.Name == "David");
        Assert.Contains(admins, e => e.Name == "Eve");
    }

    #endregion

    #region Init-Only Setters Tests

    [Fact]
    public async Task EntityWithInitSetters_CanBeDeserialized()
    {
        // Arrange
        var entity = new EntityWithInitSetters
        {
            Id = ObjectId.NewObjectId(),
            Name = "Jane Doe",
            Age = 28,
            CreatedAt = DateTime.UtcNow
        };

        // Act
        var id = await _db.InitSetterEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();
        var retrieved = await _db.InitSetterEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(retrieved);
        Assert.Equal(id, retrieved.Id);
        Assert.Equal("Jane Doe", retrieved.Name);
        Assert.Equal(28, retrieved.Age);
    }

    [Fact]
    public async Task EntityWithInitSetters_Query_Works()
    {
        // Arrange
        var entity1 = new EntityWithInitSetters { Id = ObjectId.NewObjectId(), Name = "Alpha", Age = 20, CreatedAt = DateTime.UtcNow };
        var entity2 = new EntityWithInitSetters { Id = ObjectId.NewObjectId(), Name = "Beta", Age = 30, CreatedAt = DateTime.UtcNow };
        var entity3 = new EntityWithInitSetters { Id = ObjectId.NewObjectId(), Name = "Gamma", Age = 40, CreatedAt = DateTime.UtcNow };

        await _db.InitSetterEntities.InsertAsync(entity1);
        await _db.InitSetterEntities.InsertAsync(entity2);
        await _db.InitSetterEntities.InsertAsync(entity3);
        await _db.SaveChangesAsync();

        // Act
        var results = await _db.InitSetterEntities.FindAsync(e => e.Age > 25).ToListAsync();

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Contains(results, e => e.Name == "Beta");
        Assert.Contains(results, e => e.Name == "Gamma");
    }

    [Fact]
    public async Task EntityWithInitIdAndNullables_NullProperties_RoundTrip()
    {
        // Regression test: entities with an init-only Id and nullable value-type properties
        // must deserialize null as null, not as the default value (0).
        var withNulls = new EntityWithInitIdAndNullables { Id = ObjectId.NewObjectId(), Name = "Alice" };
        var withValues = new EntityWithInitIdAndNullables { Id = ObjectId.NewObjectId(), Name = "Bob", Age = 30, Weight = 70.5m };

        var idAlice = await _db.InitIdNullableEntities.InsertAsync(withNulls);
        var idBob = await _db.InitIdNullableEntities.InsertAsync(withValues);
        await _db.SaveChangesAsync();

        var alice = await _db.InitIdNullableEntities.FindByIdAsync(idAlice);
        var bob = await _db.InitIdNullableEntities.FindByIdAsync(idBob);

        Assert.NotNull(alice);
        Assert.Null(alice.Age);
        Assert.Null(alice.Weight);

        Assert.NotNull(bob);
        Assert.Equal(30, bob.Age);
        Assert.Equal(70.5m, bob.Weight);
    }

    #endregion

    #region DDD Private Backing Field Tests

    [Fact]
    public async Task DddAggregate_WithPrivateBackingField_CanPersistAndReload()
    {
        var agg = DddAggregate.Create("Invoice #1");
        agg.AddItem("Widget", 3);
        agg.AddItem("Gadget", 1);
        agg.AddTag("urgent");
        agg.AddTag("b2b");

        await _db.DddAggregates.InsertAsync(agg);
        await _db.SaveChangesAsync();

        var loaded = await _db.DddAggregates.FindByIdAsync(agg.Id);

        Assert.NotNull(loaded);
        Assert.Equal("Invoice #1", loaded.Title);
        Assert.Equal(2, loaded.Items.Count);
        Assert.Contains(loaded.Items, i => i.Name == "Widget" && i.Quantity == 3);
        Assert.Contains(loaded.Items, i => i.Name == "Gadget" && i.Quantity == 1);
        Assert.Equal(2, loaded.Tags.Count);
        Assert.Contains(loaded.Tags, t => t == "urgent");
        Assert.Contains(loaded.Tags, t => t == "b2b");
    }

    [Fact]
    public async Task DddAggregate_EmptyCollections_RoundTrip()
    {
        var agg = DddAggregate.Create("Empty Invoice");
        await _db.DddAggregates.InsertAsync(agg);
        await _db.SaveChangesAsync();
        var loaded = await _db.DddAggregates.FindByIdAsync(agg.Id);

        Assert.NotNull(loaded);
        Assert.Equal("Empty Invoice", loaded.Title);
        Assert.Empty(loaded.Items);
        Assert.Empty(loaded.Tags);
    }

    [Fact]
    public async Task DddAggregate_MultipleDocuments_FindAll_Works()
    {
        var agg1 = DddAggregate.Create("Order A");
        agg1.AddItem("X", 10);

        var agg2 = DddAggregate.Create("Order B");
        agg2.AddItem("Y", 5);
        agg2.AddItem("Z", 2);

        await _db.DddAggregates.InsertAsync(agg1);
        await _db.DddAggregates.InsertAsync(agg2);
        await _db.SaveChangesAsync();

        var all = (await _db.DddAggregates.FindAllAsync().ToListAsync());

        Assert.Equal(2, all.Count);
        var a = all.Single(x => x.Title == "Order A");
        var b = all.Single(x => x.Title == "Order B");
        Assert.Single(a.Items);
        Assert.Equal(2, b.Items.Count);
    }

    #endregion

    #region HasConversion on Non-ID Properties

    [Fact]
    public async Task Device_NonIdProperty_HasConversion_Serializes_Correctly()
    {
        // ulong is not natively supported by BSON; the converter maps it to long.
        var device = new Device
        {
            Id            = "dev-001",
            SearchIndexId = ulong.MaxValue,   // largest ulong — exercises the full range
            Name          = "Test Device",
        };

        var id = await _db.Devices.InsertAsync(device);
        await _db.SaveChangesAsync();

        var retrieved = await _db.Devices.FindByIdAsync(id);

        Assert.NotNull(retrieved);
        Assert.Equal("dev-001", retrieved.Id);
        Assert.Equal(ulong.MaxValue, retrieved.SearchIndexId);
        Assert.Equal("Test Device", retrieved.Name);
    }

    [Fact]
    public async Task Device_NonIdProperty_HasConversion_RoundTrips_Multiple_Values()
    {
        var devices = new[]
        {
            new Device { Id = "d1", SearchIndexId = 0UL,               Name = "Zero"    },
            new Device { Id = "d2", SearchIndexId = 1UL,               Name = "One"     },
            new Device { Id = "d3", SearchIndexId = 9_999_999_999UL,   Name = "Large"   },
            new Device { Id = "d4", SearchIndexId = ulong.MaxValue,    Name = "MaxVal"  },
        };

        foreach (var d in devices)
            await _db.Devices.InsertAsync(d);
        await _db.SaveChangesAsync();

        foreach (var original in devices)
        {
            var loaded = await _db.Devices.FindByIdAsync(original.Id);
            Assert.NotNull(loaded);
            Assert.Equal(original.SearchIndexId, loaded.SearchIndexId);
            Assert.Equal(original.Name, loaded.Name);
        }
    }

    [Fact]
    public async Task Device_NonIdProperty_HasConversion_Update_Works()
    {
        var device = new Device { Id = "dev-upd", SearchIndexId = 42UL, Name = "Before" };
        await _db.Devices.InsertAsync(device);
        await _db.SaveChangesAsync();

        device.SearchIndexId = 99UL;
        device.Name          = "After";
        await _db.Devices.UpdateAsync(device);
        await _db.SaveChangesAsync();

        var loaded = await _db.Devices.FindByIdAsync("dev-upd");
        Assert.NotNull(loaded);
        Assert.Equal(99UL, loaded.SearchIndexId);
        Assert.Equal("After", loaded.Name);
    }

    #endregion

    public void Dispose()
    {
        _db?.Dispose();
        
        if (File.Exists(_dbPath)) 
            File.Delete(_dbPath);
        if (File.Exists(_walPath)) 
            File.Delete(_walPath);
    }
}
