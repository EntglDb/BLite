using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Tests for secondary indexes on embedded (nested) object properties,
/// including nullable chain handling.
/// </summary>
public class EmbeddedIndexTests : IDisposable
{
    private readonly string _dbPath;

    public EmbeddedIndexTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_embedded_idx_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
    }

    [Fact]
    public void Index_On_Nested_Property_Is_Created()
    {
        using var db = new TestDbContext(_dbPath);

        // Verify index was created on nested path
        var indexes = db.PeopleWithEmbeddedAddress.GetIndexes();
        Assert.Contains(indexes, idx => idx.PropertyPaths.Contains("MainAddress.City.Name"));
    }

    [Fact]
    public void Insert_With_Valid_Nested_Property_Succeeds()
    {
        using var db = new TestDbContext(_dbPath);

        var person = new PersonWithEmbeddedAddress
        {
            Id = 1,
            Name = "Alice",
            MainAddress = new Address
            {
                Street = "123 Main St",
                City = new City { Name = "Seattle", ZipCode = "98101" }
            }
        };

        db.PeopleWithEmbeddedAddress.Insert(person);

        var retrieved = db.PeopleWithEmbeddedAddress.FindById(1);
        Assert.NotNull(retrieved);
        Assert.Equal("Seattle", retrieved.MainAddress?.City?.Name);
    }

    [Fact]
    public void Insert_With_Null_Intermediate_Skips_Index()
    {
        using var db = new TestDbContext(_dbPath);

        // Person with null MainAddress (intermediate property is null)
        var person1 = new PersonWithEmbeddedAddress { Id = 1, Name = "Bob", MainAddress = null };
        
        // Person with MainAddress but null City
        var person2 = new PersonWithEmbeddedAddress
        {
            Id = 2,
            Name = "Charlie",
            MainAddress = new Address { Street = "456 Oak Ave", City = new City() }
        };

        // Both should insert successfully without throwing NullReferenceException
        db.PeopleWithEmbeddedAddress.Insert(person1);
        db.PeopleWithEmbeddedAddress.Insert(person2);

        Assert.Equal(2, db.PeopleWithEmbeddedAddress.Count());
    }

    [Fact]
    public void Query_On_Nested_Property_Uses_Index()
    {
        using var db = new TestDbContext(_dbPath);

        // Insert test data
        db.PeopleWithEmbeddedAddress.Insert(new PersonWithEmbeddedAddress
        {
            Id = 1,
            Name = "Alice",
            MainAddress = new Address
            {
                City = new City { Name = "Seattle", ZipCode = "98101" }
            }
        });

        db.PeopleWithEmbeddedAddress.Insert(new PersonWithEmbeddedAddress
        {
            Id = 2,
            Name = "Bob",
            MainAddress = new Address
            {
                City = new City { Name = "Portland", ZipCode = "97201" }
            }
        });

        db.PeopleWithEmbeddedAddress.Insert(new PersonWithEmbeddedAddress
        {
            Id = 3,
            Name = "Charlie",
            MainAddress = null // Null intermediate - should be skipped
        });

        // Query using nested property - should use index
        var results = db.PeopleWithEmbeddedAddress.AsQueryable()
            .Where(p => p.MainAddress!.City.Name == "Seattle")
            .ToList();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public void Update_Nested_Property_Updates_Index()
    {
        using var db = new TestDbContext(_dbPath);

        var person = new PersonWithEmbeddedAddress
        {
            Id = 1,
            Name = "Alice",
            MainAddress = new Address
            {
                City = new City { Name = "Seattle", ZipCode = "98101" }
            }
        };

        db.PeopleWithEmbeddedAddress.Insert(person);

        // Update nested city
        person.MainAddress.City.Name = "Portland";
        db.PeopleWithEmbeddedAddress.Update(person);

        // Query with new value should find it
        var results = db.PeopleWithEmbeddedAddress.AsQueryable()
            .Where(p => p.MainAddress!.City.Name == "Portland")
            .ToList();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);

        // Query with old value should return empty
        var oldResults = db.PeopleWithEmbeddedAddress.AsQueryable()
            .Where(p => p.MainAddress!.City.Name == "Seattle")
            .ToList();

        Assert.Empty(oldResults);
    }

    [Fact]
    public void Update_From_NonNull_To_Null_Removes_From_Index()
    {
        using var db = new TestDbContext(_dbPath);

        var person = new PersonWithEmbeddedAddress
        {
            Id = 1,
            Name = "Alice",
            MainAddress = new Address
            {
                City = new City { Name = "Seattle" }
            }
        };

        db.PeopleWithEmbeddedAddress.Insert(person);

        // Set MainAddress to null
        person.MainAddress = null;
        db.PeopleWithEmbeddedAddress.Update(person);

        // Should still retrieve by Id
        var retrieved = db.PeopleWithEmbeddedAddress.FindById(1);
        Assert.NotNull(retrieved);
        Assert.Null(retrieved.MainAddress);

        // Query on nested property should return empty (index entry removed)
        var results = db.PeopleWithEmbeddedAddress.AsQueryable()
            .Where(p => p.MainAddress!.City.Name == "Seattle")
            .ToList();

        Assert.Empty(results);
    }

    [Fact]
    public void Delete_With_Nested_Property_Removes_From_Index()
    {
        using var db = new TestDbContext(_dbPath);

        var person = new PersonWithEmbeddedAddress
        {
            Id = 1,
            Name = "Alice",
            MainAddress = new Address
            {
                City = new City { Name = "Seattle" }
            }
        };

        db.PeopleWithEmbeddedAddress.Insert(person);
        
        // Verify it's queryable
        var beforeDelete = db.PeopleWithEmbeddedAddress.AsQueryable()
            .Where(p => p.MainAddress!.City.Name == "Seattle")
            .ToList();
        Assert.Single(beforeDelete);

        // Delete
        db.PeopleWithEmbeddedAddress.Delete(1);

        // Should not be found in index
        var afterDelete = db.PeopleWithEmbeddedAddress.AsQueryable()
            .Where(p => p.MainAddress!.City.Name == "Seattle")
            .ToList();
        Assert.Empty(afterDelete);
    }

    [Fact]
    public void Multiple_Nested_Indexes_Work_Independently()
    {
        using var db = new TestDbContext(_dbPath);

        var person = new PersonWithEmbeddedAddress
        {
            Id = 1,
            Name = "Alice",
            MainAddress = new Address
            {
                City = new City { Name = "Seattle", ZipCode = "98101" }
            },
            BillingAddress = new Address
            {
                City = new City { Name = "Portland", ZipCode = "97201" }
            }
        };

        db.PeopleWithEmbeddedAddress.Insert(person);

        // Query on MainAddress index
        var mainResults = db.PeopleWithEmbeddedAddress.AsQueryable()
            .Where(p => p.MainAddress!.City.Name == "Seattle")
            .ToList();
        Assert.Single(mainResults);

        // Query on BillingAddress index
        var billingResults = db.PeopleWithEmbeddedAddress.AsQueryable()
            .Where(p => p.BillingAddress!.City.Name == "Portland")
            .ToList();
        Assert.Single(billingResults);
    }
}
