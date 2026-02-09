using DocumentDb.Core;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Collections;
using DocumentDb.Core.Indexing;
using DocumentDb.Core.Metadata;
using Xunit;

namespace DocumentDb.Tests;

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
        public DocumentCollection<Person> People { get; private set; } = null!;
        public DocumentCollection<Product> Products { get; private set; } = null!;

        public TestContext(string path) : base(path, PageFileConfig.Default) { }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Person>()
                .ToCollection("people_collection")
                .HasIndex(p => p.Age, name: "Age");

            modelBuilder.Entity<Product>()
                .ToCollection("products_collection")
                .HasIndex(p => p.Price, name: "Price", unique: true);
        }

        protected override void InitializeCollections()
        {
            People = CreateCollection<Person>(new PersonMapper());
            Products = CreateCollection<Product>(new ProductMapper());
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
public class PersonMapper : IDocumentMapper<MultiCollectionTests.Person>
{
    public string CollectionName => "People"; // Should be overridden by model!
    public int Serialize(MultiCollectionTests.Person entity, Span<byte> buffer) => 0;
    public void Serialize(MultiCollectionTests.Person entity, System.Buffers.IBufferWriter<byte> writer) { }
    public MultiCollectionTests.Person Deserialize(ReadOnlySpan<byte> buffer) => new();
    public DocumentDb.Bson.ObjectId GetId(MultiCollectionTests.Person entity) => default;
    public void SetId(MultiCollectionTests.Person entity, DocumentDb.Bson.ObjectId id) { }
}

public class ProductMapper : IDocumentMapper<MultiCollectionTests.Product>
{
    public string CollectionName => "Products"; // Should be overridden by model!
    public int Serialize(MultiCollectionTests.Product entity, Span<byte> buffer) => 0;
    public void Serialize(MultiCollectionTests.Product entity, System.Buffers.IBufferWriter<byte> writer) { }
    public MultiCollectionTests.Product Deserialize(ReadOnlySpan<byte> buffer) => new();
    public DocumentDb.Bson.ObjectId GetId(MultiCollectionTests.Product entity) => default;
    public void SetId(MultiCollectionTests.Product entity, DocumentDb.Bson.ObjectId id) { }
}
