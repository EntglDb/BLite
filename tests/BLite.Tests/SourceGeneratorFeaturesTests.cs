using BLite.Bson;
using BLite.Shared;
using System.Linq;

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
    public void DerivedEntity_InheritsId_FromBaseClass()
    {
        // Arrange
        var entity = new DerivedEntity
        {
            Name = "Test Entity",
            Description = "Testing inheritance",
            CreatedAt = DateTime.UtcNow
        };

        // Act
        var id = _db.DerivedEntities.Insert(entity);
        _db.SaveChanges();
        var retrieved = _db.DerivedEntities.FindById(id);

        // Assert
        Assert.NotNull(retrieved);
        Assert.Equal(id, retrieved.Id); // Id from base class should work
        Assert.Equal("Test Entity", retrieved.Name);
        Assert.Equal("Testing inheritance", retrieved.Description);
        Assert.Equal(entity.CreatedAt.Date, retrieved.CreatedAt.Date); // Compare just date part
    }

    [Fact]
    public void DerivedEntity_Update_WorksWithInheritedId()
    {
        // Arrange
        var entity = new DerivedEntity
        {
            Name = "Original",
            Description = "Original Description",
            CreatedAt = DateTime.UtcNow
        };
        var id = _db.DerivedEntities.Insert(entity);
        _db.SaveChanges();

        // Act
        var retrieved = _db.DerivedEntities.FindById(id);
        Assert.NotNull(retrieved);
        retrieved.Name = "Updated";
        retrieved.Description = "Updated Description";
        _db.DerivedEntities.Update(retrieved);
        _db.SaveChanges();

        var updated = _db.DerivedEntities.FindById(id);

        // Assert
        Assert.NotNull(updated);
        Assert.Equal(id, updated.Id);
        Assert.Equal("Updated", updated.Name);
        Assert.Equal("Updated Description", updated.Description);
    }

    [Fact]
    public void DerivedEntity_Query_WorksWithInheritedProperties()
    {
        // Arrange
        var now = DateTime.UtcNow;
        var entity1 = new DerivedEntity { Name = "Entity1", CreatedAt = now.AddDays(-2) };
        var entity2 = new DerivedEntity { Name = "Entity2", CreatedAt = now.AddDays(-1) };
        var entity3 = new DerivedEntity { Name = "Entity3", CreatedAt = now };

        _db.DerivedEntities.Insert(entity1);
        _db.DerivedEntities.Insert(entity2);
        _db.DerivedEntities.Insert(entity3);
        _db.SaveChanges();

        // Act - Query using inherited property
        var recent = _db.DerivedEntities.Find(e => e.CreatedAt >= now.AddDays(-1.5)).ToList();

        // Assert
        Assert.Equal(2, recent.Count);
        Assert.Contains(recent, e => e.Name == "Entity2");
        Assert.Contains(recent, e => e.Name == "Entity3");
    }

    #endregion

    #region Computed Properties Tests

    [Fact]
    public void ComputedProperties_AreNotSerialized()
    {
        // Arrange
        var entity = new EntityWithComputedProperties
        {
            FirstName = "John",
            LastName = "Doe",
            BirthYear = 1990
        };

        // Act
        var id = _db.ComputedPropertyEntities.Insert(entity);
        _db.SaveChanges();
        var retrieved = _db.ComputedPropertyEntities.FindById(id);

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
    public void ComputedProperties_UpdateDoesNotBreak()
    {
        // Arrange
        var entity = new EntityWithComputedProperties
        {
            FirstName = "Jane",
            LastName = "Smith",
            BirthYear = 1985
        };
        var id = _db.ComputedPropertyEntities.Insert(entity);
        _db.SaveChanges();

        // Act - Update stored properties
        var retrieved = _db.ComputedPropertyEntities.FindById(id);
        Assert.NotNull(retrieved);
        retrieved.FirstName = "Janet";
        retrieved.BirthYear = 1986;
        _db.ComputedPropertyEntities.Update(retrieved);
        _db.SaveChanges();

        var updated = _db.ComputedPropertyEntities.FindById(id);

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
    public void HashSet_SerializesAndDeserializes()
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
        var id = _db.AdvancedCollectionEntities.Insert(entity);
        _db.SaveChanges();
        var retrieved = _db.AdvancedCollectionEntities.FindById(id);

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
    public void ISet_SerializesAndDeserializes()
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
        var id = _db.AdvancedCollectionEntities.Insert(entity);
        _db.SaveChanges();
        var retrieved = _db.AdvancedCollectionEntities.FindById(id);

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
    public void LinkedList_SerializesAndDeserializes()
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
        var id = _db.AdvancedCollectionEntities.Insert(entity);
        _db.SaveChanges();
        var retrieved = _db.AdvancedCollectionEntities.FindById(id);

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
    public void Queue_SerializesAndDeserializes()
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
        var id = _db.AdvancedCollectionEntities.Insert(entity);
        _db.SaveChanges();
        var retrieved = _db.AdvancedCollectionEntities.FindById(id);

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
    public void Stack_SerializesAndDeserializes()
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
        var id = _db.AdvancedCollectionEntities.Insert(entity);
        _db.SaveChanges();
        var retrieved = _db.AdvancedCollectionEntities.FindById(id);

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
    public void HashSet_WithNestedObjects_SerializesAndDeserializes()
    {
        // Arrange
        var entity = new EntityWithAdvancedCollections
        {
            Name = "Test Nested HashSet"
        };
        entity.Addresses.Add(new Address { Street = "123 Main St", City = new City { Name = "NYC", ZipCode = "10001" } });
        entity.Addresses.Add(new Address { Street = "456 Oak Ave", City = new City { Name = "LA", ZipCode = "90001" } });

        // Act
        var id = _db.AdvancedCollectionEntities.Insert(entity);
        _db.SaveChanges();
        var retrieved = _db.AdvancedCollectionEntities.FindById(id);

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
    public void ISet_WithNestedObjects_SerializesAndDeserializes()
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
        var id = _db.AdvancedCollectionEntities.Insert(entity);
        _db.SaveChanges();
        var retrieved = _db.AdvancedCollectionEntities.FindById(id);

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
    public void AdvancedCollections_AllTypesInSingleEntity()
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
        var id = _db.AdvancedCollectionEntities.Insert(entity);
        _db.SaveChanges();
        var retrieved = _db.AdvancedCollectionEntities.FindById(id);

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

    public void Dispose()
    {
        _db?.Dispose();
        
        if (File.Exists(_dbPath)) 
            File.Delete(_dbPath);
        if (File.Exists(_walPath)) 
            File.Delete(_walPath);
    }
}
