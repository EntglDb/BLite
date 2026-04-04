using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using BLite.Core.Indexing;
using BLite.Shared;
using BLite.Core.Storage;
using System;

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
    public DocumentCollection<string, CustomerOrder> CustomerOrders { get; set; } = null!;
    public DocumentCollection<ObjectId, PersonV2> PeopleV2 { get; set; } = null!;
    public DocumentCollection<int, Product> Products { get; set; } = null!;
    public DocumentCollection<int, IntEntity> IntEntities { get; set; } = null!;
    public DocumentCollection<long, LongEntity> LongEntities { get; set; } = null!;
    public DocumentCollection<string, StringEntity> StringEntities { get; set; } = null!;
    public DocumentCollection<Guid, GuidEntity> GuidEntities { get; set; } = null!;
    public DocumentCollection<string, CustomKeyEntity> CustomKeyEntities { get; set; } = null!;
    public DocumentCollection<int, AsyncDoc> AsyncDocs { get; set; } = null!;
    public DocumentCollection<int, SchemaUser> SchemaUsers { get; set; } = null!;
    public DocumentCollection<ObjectId, VectorEntity> VectorItems { get; set; } = null!;
    public DocumentCollection<ObjectId, GeoEntity> GeoItems { get; set; } = null!;
    
    // Source Generator Feature Tests
    public DocumentCollection<ObjectId, DerivedEntity> DerivedEntities { get; set; } = null!;
    public DocumentCollection<ObjectId, EntityWithComputedProperties> ComputedPropertyEntities { get; set; } = null!;
    public DocumentCollection<ObjectId, EntityWithAdvancedCollections> AdvancedCollectionEntities { get; set; } = null!;
    public DocumentCollection<ObjectId, EntityWithPrivateSetters> PrivateSetterEntities { get; set; } = null!;
    public DocumentCollection<ObjectId, EntityWithInheritedPrivateSetters> InheritedPrivateSetterEntities { get; set; } = null!;
    public DocumentCollection<ObjectId, EntityWithInitSetters> InitSetterEntities { get; set; } = null!;
    public DocumentCollection<ObjectId, EntityWithInitIdAndNullables> InitIdNullableEntities { get; set; } = null!;
    
    // Circular Reference Tests
    public DocumentCollection<ObjectId, Employee> Employees { get; set; } = null!;
    public DocumentCollection<ObjectId, CategoryRef> CategoryRefs { get; set; } = null!;
    public DocumentCollection<ObjectId, ProductRef> ProductRefs { get; set; } = null!;
    
    // Nullable String Id Test (UuidEntity scenario with inheritance)
    public DocumentCollection<string, MockCounter> MockCounters { get; set; } = null!;
    
    // Temporal Types Test (DateTimeOffset, TimeSpan, DateOnly, TimeOnly)
    public DocumentCollection<ObjectId, TemporalEntity> TemporalEntities { get; set; } = null!;
    
    // Enum Serialization Tests
    public DocumentCollection<ObjectId, EnumEntity> EnumEntities { get; set; } = null!;

    // NestedObject with [Key] Id – regression tests
    public DocumentCollection<ObjectId, PersonWithContact> PeopleWithContacts { get; set; } = null!;

    // Embedded Index Tests
    public DocumentCollection<int, PersonWithEmbeddedAddress> PeopleWithEmbeddedAddress { get; set; } = null!;

    // Self-Referencing Collection Tests
    public DocumentCollection<ObjectId, TreeNode> TreeNodes { get; set; } = null!;

    // DDD private backing field Tests
    public DocumentCollection<ObjectId, DddAggregate> DddAggregates { get; set; } = null!;

    // FullyPrivateEntity – private constructor + all private setters
    public DocumentCollection<ObjectId, FullyPrivateEntity> FullyPrivateEntities { get; set; } = null!;

    // SensorReading – TimeSeries via DocumentDbContext fluent API
    public DocumentCollection<ObjectId, SensorReading> SensorReadings { get; set; } = null!;

    // Device – HasConversion on a non-ID property (ulong → long)
    public DocumentCollection<string, Device> Devices { get; set; } = null!;

    // Binary Property Tests
    public DocumentCollection<ObjectId, BinaryEntity> BinaryEntities { get; set; } = null!;

    public TestDbContext(string databasePath) : base(databasePath)
    {
        InitializeCollections();
    }

    /// <summary>
    /// Creates a new context with an explicit page-file configuration.
    /// Use <see cref="PageFileConfig.Server(string, PageFileConfig?)"/> to open a multi-file database.
    /// </summary>
    public TestDbContext(string databasePath, PageFileConfig config) : base(databasePath, config)
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
        modelBuilder.Entity<LongEntity>().HasKey(e => e.Id);
        modelBuilder.Entity<StringEntity>().HasKey(e => e.Id);
        modelBuilder.Entity<GuidEntity>().HasKey(e => e.Id);
        modelBuilder.Entity<CustomKeyEntity>().HasKey(e => e.Code);
        modelBuilder.Entity<AsyncDoc>().ToCollection("async_docs");
        modelBuilder.Entity<SchemaUser>().ToCollection("schema_users").HasKey(e => e.Id);
        modelBuilder.Entity<TestDocument>().HasIndex(x => x.Category);
        modelBuilder.Entity<OrderDocument>();
        modelBuilder.Entity<ComplexDocument>();
        
        modelBuilder.Entity<VectorEntity>()
            .ToCollection("vector_items")
            .HasVectorIndex(x => x.Embedding, dimensions: 3, metric: VectorMetric.L2, name: "idx_vector");

        modelBuilder.Entity<GeoEntity>()
            .ToCollection("geo_items")
            .HasSpatialIndex(x => x.Location, name: "idx_spatial");

        // Custom value converter for complex Id type - now clearly tied to the property
        modelBuilder.Entity<Order>()
            .Property(x => x.Id)
            .HasConversion<OrderIdConverter>();

        // Source Generator Feature Tests
        modelBuilder.Entity<DerivedEntity>().ToCollection("derived_entities");
        modelBuilder.Entity<EntityWithComputedProperties>().ToCollection("computed_property_entities");
        modelBuilder.Entity<EntityWithAdvancedCollections>().ToCollection("advanced_collection_entities");
        modelBuilder.Entity<EntityWithPrivateSetters>().ToCollection("private_setter_entities");
        modelBuilder.Entity<EntityWithInitSetters>().ToCollection("init_setter_entities");
        modelBuilder.Entity<EntityWithInitIdAndNullables>().ToCollection("init_id_nullable_entities");
        
        // Circular Reference Tests
        modelBuilder.Entity<Employee>().ToCollection("employees");
        modelBuilder.Entity<CategoryRef>().ToCollection("category_refs");
        modelBuilder.Entity<ProductRef>().ToCollection("product_refs");
        
        // Nullable String Id Test (UuidEntity scenario)
        modelBuilder.Entity<MockCounter>().ToCollection("mock_counters").HasKey(e => e.Id);
        
        // Temporal Types Test
        modelBuilder.Entity<TemporalEntity>().ToCollection("temporal_entities").HasKey(e => e.Id);

        // Enum Serialization Test
        modelBuilder.Entity<EnumEntity>().ToCollection("enum_entities").HasKey(e => e.Id);

        // NestedObject with [Key] Id – regression
        modelBuilder.Entity<PersonWithContact>().ToCollection("people_with_contacts");

        // Embedded Index Tests - secondary indexes on nested nullable properties
        modelBuilder.Entity<PersonWithEmbeddedAddress>()
            .ToCollection("people_with_embedded_address")
            .HasKey(p => p.Id)
            .HasIndex(p => p.MainAddress!.City.Name) // Index on nested property path
            .HasIndex(p => p.BillingAddress!.City.Name); // Second nested index

        // Self-Referencing Collection Tests
        modelBuilder.Entity<TreeNode>()
            .ToCollection("tree_nodes")
            .HasKey(t => t.Id);

        // DDD private backing field Tests
        modelBuilder.Entity<DddAggregate>().ToCollection("ddd_aggregates");

        // FullyPrivateEntity
        modelBuilder.Entity<FullyPrivateEntity>().ToCollection("fully_private_entities");

        // SensorReading TimeSeries
        modelBuilder.Entity<SensorReading>()
            .ToCollection("sensor_readings")
            .HasTimeSeries(r => r.Timestamp, retention: TimeSpan.FromDays(7));

        // Device – HasConversion on a non-ID property (ulong → long)
        modelBuilder.Entity<Device>()
            .ToCollection("devices")
            .Property(x => x.SearchIndexId)
            .HasConversion<UlongToInt64Converter>();

        // Benchmark entities
        modelBuilder.Entity<CustomerOrder>().ToCollection("customer_orders").HasKey(e => e.Id);

        // Binary Property Tests
        modelBuilder.Entity<BinaryEntity>().ToCollection("binary_entities");
    }

    public void ForceCheckpoint()
    {
        _storage.CheckpointAsync().GetAwaiter().GetResult();
    }

    public StorageEngine Storage => _storage;  
}
