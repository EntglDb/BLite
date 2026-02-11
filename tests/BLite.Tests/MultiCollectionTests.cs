using BLite.Core;
using BLite.Core.Storage;
using BLite.Core.Collections;
using BLite.Core.Indexing;
using BLite.Core.Metadata;
using Xunit;
using BLite.Bson;

namespace BLite.Tests;

public class MultiCollectionTests : IDisposable
{
    private readonly string _dbPath = "multi_collection.db";

    public MultiCollectionTests()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    public class Person
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Age { get; set; }
    }

    public class Product
    {
        public int Id { get; set; }
        public string Title { get; set; } = "";
        public decimal Price { get; set; }
    }

    private class TestContext : DocumentDbContext
    {
        public DocumentCollection<ObjectId,Person> People { get; private set; } = null!;
        public DocumentCollection<ObjectId, Product> Products { get; private set; } = null!;

        public TestContext(string path) : base(path, PageFileConfig.Default)
        {
            People = CreateCollection(new PersonMapper());
            Products = CreateCollection(new ProductMapper());
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Person>()
                .ToCollection("people_collection")
                .HasIndex(p => p.Age, name: "Age");

            modelBuilder.Entity<Product>()
                .ToCollection("products_collection")
                .HasIndex(p => p.Price, name: "Price", unique: true);
        }
    }

    [Fact]
    public void Can_Store_Multiple_Collections_In_One_File()
    {
        using (var db = new TestContext(_dbPath))
        {
            // Verify names
            Assert.Contains(db.People.GetIndexes(), i => i.Name == "Age");
            Assert.Contains(db.Products.GetIndexes(), i => i.Name == "Price");
        }

        // Reopen and check persistence
        using (var db = new TestContext(_dbPath))
        {
            Assert.Contains(db.People.GetIndexes(), i => i.Name == "Age");
            Assert.Contains(db.Products.GetIndexes(), i => i.Name == "Price");
        }
    }
}

// Mock mappers for the test
public class PersonMapper : ObjectIdMapperBase<MultiCollectionTests.Person>
{
    public override string CollectionName => "People"; // Should be overridden by model!
    public override int Serialize(MultiCollectionTests.Person entity, BsonSpanWriter writer) => 0;
    public override MultiCollectionTests.Person Deserialize(BsonSpanReader reader) => new();
    public override BLite.Bson.ObjectId GetId(MultiCollectionTests.Person entity) => default;
    public override void SetId(MultiCollectionTests.Person entity, BLite.Bson.ObjectId id) { }
}

public class ProductMapper : ObjectIdMapperBase<MultiCollectionTests.Product>
{
    public override string CollectionName => "Products"; // Should be overridden by model!
    public override int Serialize(MultiCollectionTests.Product entity, BsonSpanWriter writer) => 0;
    public override MultiCollectionTests.Product Deserialize(BsonSpanReader reader) => new();
    public override BLite.Bson.ObjectId GetId(MultiCollectionTests.Product entity) => default;
    public override void SetId(MultiCollectionTests.Product entity, BLite.Bson.ObjectId id) { }
}
