using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using BLite.Core.Indexing;
using BLite.Shared;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Test context with manual collection initialization
/// (Source Generator will automate this in the future)
/// </summary>
public partial class TestDbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, AnnotatedUser> AnnotatedUsers { get; set; } = null!;
    public DocumentCollection<OrderId, Order> Orders { get; set; } = null!;
    public DocumentCollection<ObjectId, TestDocument> TestDocuments { get; set; } = null!;
    public DocumentCollection<ObjectId, OrderDocument> OrderDocuments { get; set; } = null!;
    public DocumentCollection<ObjectId, ComplexDocument> ComplexDocuments { get; set; } = null!;
    public DocumentCollection<ObjectId, User> Users { get; set; } = null!;
    public DocumentCollection<ObjectId, ComplexUser> ComplexUsers { get; set; } = null!;
    public DocumentCollection<int, AutoInitEntity> AutoInitEntities { get; set; } = null!;
    public DocumentCollection<int, Person> People { get; set; } = null!;
    public DocumentCollection<ObjectId, PersonV2> PeopleV2 { get; set; } = null!;
    public DocumentCollection<int, Product> Products { get; set; } = null!;
    public DocumentCollection<int, IntEntity> IntEntities { get; set; } = null!;
    public DocumentCollection<string, StringEntity> StringEntities { get; set; } = null!;
    public DocumentCollection<Guid, GuidEntity> GuidEntities { get; set; } = null!;
    public DocumentCollection<int, AsyncDoc> AsyncDocs { get; set; } = null!;
    public DocumentCollection<int, SchemaUser> SchemaUsers { get; set; } = null!;
    public DocumentCollection<ObjectId, VectorEntity> VectorItems { get; set; } = null!;
    public DocumentCollection<ObjectId, GeoEntity> GeoItems { get; set; } = null!;
    public DocumentCollection<ObjectId, CustomerWithContact> CustomersWithContact { get; set; } = null!;
    public DocumentCollection<ObjectId, CompanyWithContacts> CompaniesWithContacts { get; set; } = null!;

    public TestDbContext(string databasePath) : base(databasePath)
    {
        InitializeCollections();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AnnotatedUser>();
        modelBuilder.Entity<User>().ToCollection("users");
        modelBuilder.Entity<ComplexUser>().ToCollection("complex_users");
        modelBuilder.Entity<AutoInitEntity>().ToCollection("auto_init_entities");
        modelBuilder.Entity<Person>().ToCollection("people_collection").HasIndex(p => p.Age);
        modelBuilder.Entity<PersonV2>().ToCollection("peoplev2_collection").HasIndex(p => p.Age);
        modelBuilder.Entity<Product>().ToCollection("products_collection").HasIndex(p => p.Price);
        modelBuilder.Entity<IntEntity>().HasKey(e => e.Id);
        modelBuilder.Entity<StringEntity>().HasKey(e => e.Id);
        modelBuilder.Entity<GuidEntity>().HasKey(e => e.Id);
        modelBuilder.Entity<AsyncDoc>().ToCollection("async_docs");
        modelBuilder.Entity<SchemaUser>().ToCollection("schema_users").HasKey(e => e.Id);
        modelBuilder.Entity<TestDocument>();
        modelBuilder.Entity<OrderDocument>();
        modelBuilder.Entity<ComplexDocument>();
        
        modelBuilder.Entity<VectorEntity>()
            .ToCollection("vector_items")
            .HasVectorIndex(x => x.Embedding, dimensions: 3, metric: VectorMetric.L2, name: "idx_vector");

        modelBuilder.Entity<GeoEntity>()
            .ToCollection("geo_items")
            .HasSpatialIndex(x => x.Location, name: "idx_spatial");

        modelBuilder.Entity<Order>()
            .HasKey(x => x.Id)
            .HasConversion<OrderIdConverter>();
        
        // Test entities for nested objects with Id fields
        modelBuilder.Entity<CustomerWithContact>().ToCollection("customers_with_contact");
        modelBuilder.Entity<CompanyWithContacts>().ToCollection("companies_with_contacts");
    }

    public void ForceCheckpoint()
    {
        _storage.Checkpoint();
    }

    public StorageEngine Storage => _storage;  
}
