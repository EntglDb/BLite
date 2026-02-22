using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace BLite.Shared
{
    // --- Basic Entities ---

    public class User
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; } = "";
        public int Age { get; set; }
    }

    // --- Complex Entities (Nested) ---

    public class ComplexUser
    {
        [BsonId]
        public ObjectId Id { get; set; }
        public string Name { get; set; } = "";

        // Direct nested object
        public Address MainAddress { get; set; } = new();

        // Collection of nested objects
        public List<Address> OtherAddresses { get; set; } = new();

        // Primitive collection
        public List<string> Tags { get; set; } = new();

        [BsonIgnore]
        public string Secret { get; set; } = "";
    }

    public class Address
    {
        public string Street { get; set; } = "";
        public City City { get; set; } = new(); // Depth 2
    }

    public class City
    {
        public string Name { get; set; } = "";
        public string ZipCode { get; set; } = "";
    }

    // --- Primary Key Test Entities ---

    public class IntEntity
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    public class StringEntity
    {
        public required string Id { get; set; }
        public string? Value { get; set; }
    }

    public class GuidEntity
    {
        public Guid Id { get; set; }
        public string? Name { get; set; }
    }

    /// <summary>
    /// Entity with string key NOT named "Id" - tests custom key name support
    /// </summary>
    public class CustomKeyEntity
    {
        [System.ComponentModel.DataAnnotations.Key]
        public required string Code { get; set; }
        public string? Description { get; set; }
    }

    // --- Multi-collection / Auto-init entities ---

    public class AutoInitEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
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

    public class AsyncDoc
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    public class SchemaUser
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public Address Address { get; set; } = new();
    }

    public class VectorEntity
    {
        public ObjectId Id { get; set; }
        public string Title { get; set; } = "";
        public float[] Embedding { get; set; } = Array.Empty<float>();
    }

    public class GeoEntity
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; } = "";
        public (double Latitude, double Longitude) Location { get; set; }
    }

    public record OrderId(string Value)
    {
        public OrderId() : this(string.Empty) { }
    }

    public class OrderIdConverter : ValueConverter<OrderId, string>
    {
        public override string ConvertToProvider(OrderId model) => model?.Value ?? string.Empty;
        public override OrderId ConvertFromProvider(string provider) => new OrderId(provider);
    }

    public class Order
    {
        public OrderId Id { get; set; } = null!;
        public string CustomerName { get; set; } = "";
    }

    public class TestDocument
    {
        public ObjectId Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Amount { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class OrderDocument
    {
        public ObjectId Id { get; set; }
        public string ItemName { get; set; } = string.Empty;
        public int Quantity { get; set; }
    }

    public class OrderItem
    {
        public string Name { get; set; } = string.Empty;
        public int Price { get; set; }
    }

    public class ComplexDocument
    {
        public ObjectId Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public Address ShippingAddress { get; set; } = new();
        public List<OrderItem> Items { get; set; } = new();
    }

    [Table("custom_users", Schema = "test")]
    public class AnnotatedUser
    {
        [Key]
        public ObjectId Id { get; set; }

        [Required]
        [Column("display_name")]
        [StringLength(50, MinimumLength = 3)]
        public string Name { get; set; } = "";

        [Range(0, 150)]
        public int Age { get; set; }

        [NotMapped]
        public string ComputedInfo => $"{Name} ({Age})";

        [Column(TypeName = "geopoint")]
        public (double Lat, double Lon) Location { get; set; }
    }
    public class PersonV2
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
    }

    /// <summary>
    /// Entity used to test DbContext inheritance
    /// </summary>
    public class ExtendedEntity
    {
        public int Id { get; set; }
        public string Description { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
    }

    // ===== SOURCE GENERATOR FEATURE TESTS =====

    /// <summary>
    /// Base entity with Id property - test inheritance
    /// </summary>
    public class BaseEntityWithId
    {
        public ObjectId Id { get; set; }
        public DateTime CreatedAt { get; set; }
    }

    /// <summary>
    /// Derived entity that inherits Id from base class
    /// </summary>
    public class DerivedEntity : BaseEntityWithId
    {
        public string Name { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
    }

    /// <summary>
    /// Entity with computed getter-only properties (should be excluded from serialization)
    /// </summary>
    public class EntityWithComputedProperties
    {
        public ObjectId Id { get; set; }
        public string FirstName { get; set; } = string.Empty;
        public string LastName { get; set; } = string.Empty;
        public int BirthYear { get; set; }

        // Computed properties - should NOT be serialized
        public string FullName => $"{FirstName} {LastName}";
        public int Age => DateTime.Now.Year - BirthYear;
        public string DisplayInfo => $"{FullName} (Age: {Age})";
    }

    /// <summary>
    /// Entity with advanced collection types (HashSet, ISet, LinkedList, etc.)
    /// </summary>
    public class EntityWithAdvancedCollections
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; } = string.Empty;

        // Various collection types that should all be recognized
        public HashSet<string> Tags { get; set; } = new();
        public ISet<int> Numbers { get; set; } = new HashSet<int>();
        public LinkedList<string> History { get; set; } = new();
        public Queue<string> PendingItems { get; set; } = new();
        public Stack<string> UndoStack { get; set; } = new();
        
        // Nested objects in collections
        public HashSet<Address> Addresses { get; set; } = new();
        public ISet<City> FavoriteCities { get; set; } = new HashSet<City>();
    }

    /// <summary>
    /// Entity with private setters (requires reflection-based deserialization)
    /// </summary>
    public class EntityWithPrivateSetters
    {
        public ObjectId Id { get; private set; }
        public string Name { get; private set; } = string.Empty;
        public int Age { get; private set; }
        public DateTime CreatedAt { get; private set; }

        // Factory method for creation
        public static EntityWithPrivateSetters Create(string name, int age)
        {
            return new EntityWithPrivateSetters
            {
                Id = ObjectId.NewObjectId(),
                Name = name,
                Age = age,
                CreatedAt = DateTime.UtcNow
            };
        }
    }

    /// <summary>
    /// Entity with init-only setters (can use object initializer)
    /// </summary>
    public class EntityWithInitSetters
    {
        public ObjectId Id { get; init; }
        public required string Name { get; init; }
        public int Age { get; init; }
        public DateTime CreatedAt { get; init; }
    }

    // ========================================
    // Circular Reference Test Entities
    // ========================================

    /// <summary>
    /// Employee with self-referencing via ObjectIds (organizational hierarchy)
    /// Tests: self-reference using referencing (BEST PRACTICE)
    /// Recommended: Avoids embedding which can lead to large/circular documents
    /// </summary>
    public class Employee
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Department { get; set; } = string.Empty;
        public ObjectId? ManagerId { get; set; } // Reference to manager
        public List<ObjectId>? DirectReportIds { get; set; } // References to direct reports (best practice)
    }

    /// <summary>
    /// Category with referenced products (N-N using ObjectId references)
    /// Tests: N-N relationships using referencing (BEST PRACTICE for document databases)
    /// Recommended: Avoids large documents, better for queries and updates
    /// </summary>
    public class CategoryRef
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public List<ObjectId>? ProductIds { get; set; } // Only IDs - no embedding
    }

    /// <summary>
    /// Product with referenced categories (N-N using ObjectId references)
    /// Tests: N-N relationships using referencing (BEST PRACTICE for document databases)
    /// Recommended: Avoids large documents, better for queries and updates
    /// </summary>
    public class ProductRef
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Price { get; set; }
        public List<ObjectId>? CategoryIds { get; set; } // Only IDs - no embedding
    }

    // ========================================
    // Nullable String Key Test (UuidEntity scenario)
    // ========================================

    /// <summary>
    /// Base entity class that simulates CleanCore's BaseEntity{TId, TEntity}
    /// This is the root of the hierarchy that causes the generator bug
    /// </summary>
    public abstract class MockBaseEntity<TId, TEntity>
        where TId : IEquatable<TId>
        where TEntity : class
    {
        [System.ComponentModel.DataAnnotations.Key]
        public virtual TId? Id { get; set; }

        protected MockBaseEntity() { }
        
        protected MockBaseEntity(TId? id)
        {
            Id = id;
        }
    }

    /// <summary>
    /// Simulates CleanCore's UuidEntity{TEntity} which inherits from BaseEntity{string, TEntity}
    /// Tests the bug where generator incorrectly chooses ObjectIdMapperBase instead of StringMapperBase
    /// when the Id property is inherited and nullable
    /// </summary>
    public abstract class MockUuidEntity<TEntity> : MockBaseEntity<string, TEntity>
        where TEntity : class
    {
        protected MockUuidEntity() : base() { }
        
        protected MockUuidEntity(string? id) : base(id) { }
    }

    /// <summary>
    /// Concrete entity that inherits from MockUuidEntity, simulating Counter from CleanCore
    /// This is the actual entity that will be stored in the collection
    /// </summary>
    public class MockCounter : MockUuidEntity<MockCounter>
    {
        public MockCounter() : base() { }
        
        public MockCounter(string? id) : base(id) { }
        
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    // ========================================
    // Benchmark Entities – Realistic Document
    // ========================================

    public class PostalAddress
    {
        public string Street  { get; set; } = string.Empty;
        public string City    { get; set; } = string.Empty;
        public string ZipCode { get; set; } = string.Empty;
        public string Country { get; set; } = string.Empty;
    }

    public class CustomerContact
    {
        public string FullName       { get; set; } = string.Empty;
        public string Email          { get; set; } = string.Empty;
        public string Phone          { get; set; } = string.Empty;
        public PostalAddress BillingAddress { get; set; } = new();
    }

    public class OrderLine
    {
        public string  Sku         { get; set; } = string.Empty;
        public string  ProductName { get; set; } = string.Empty;
        public int     Quantity    { get; set; }
        public decimal UnitPrice   { get; set; }
        public decimal Subtotal    { get; set; }
        public List<string> Tags   { get; set; } = new();
    }

    public class ShippingInfo
    {
        public string       Carrier           { get; set; } = string.Empty;
        public string       TrackingNumber    { get; set; } = string.Empty;
        public PostalAddress Destination      { get; set; } = new();
        public DateTime?    EstimatedDelivery { get; set; }
    }

    public class OrderNote
    {
        public string   Author    { get; set; } = string.Empty;
        public string   Text      { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
    }

    /// <summary>
    /// Realistic e-commerce order document for benchmarking.
    /// Structure: 2 nested objects (Customer, Shipping), each with a nested PostalAddress;
    /// 3 collections (Lines with sub-Tags, Notes, Tags) — good stress test for
    /// nested serialization in BLite vs LiteDB vs SQLite+JSON.
    /// </summary>
    public class CustomerOrder
    {
        public string          Id          { get; set; } = string.Empty;
        public string          OrderNumber { get; set; } = string.Empty;
        public DateTime        PlacedAt    { get; set; }
        public string          Status      { get; set; } = string.Empty;
        public string          Currency    { get; set; } = "EUR";
        public decimal         Subtotal    { get; set; }
        public decimal         TaxAmount   { get; set; }
        public decimal         Total       { get; set; }
        public CustomerContact Customer    { get; set; } = new();
        public ShippingInfo    Shipping    { get; set; } = new();
        public List<OrderLine> Lines       { get; set; } = new();
        public List<OrderNote> Notes       { get; set; } = new();
        public List<string>    Tags        { get; set; } = new();
    }

    /// <summary>
    /// Entity for testing temporal types: DateTimeOffset, TimeSpan, DateOnly, TimeOnly
    /// </summary>
    public class TemporalEntity
    {
        [Key]
        public ObjectId Id { get; set; }
        
        public string Name { get; set; } = string.Empty;
        
        // DateTime types
        public DateTime CreatedAt { get; set; }
        public DateTimeOffset UpdatedAt { get; set; }
        public DateTimeOffset? LastAccessedAt { get; set; }
        
        // TimeSpan
        public TimeSpan Duration { get; set; }
        public TimeSpan? OptionalDuration { get; set; }
        
        // DateOnly and TimeOnly (.NET 6+)
        public DateOnly BirthDate { get; set; }
        public DateOnly? Anniversary { get; set; }
        
        public TimeOnly OpeningTime { get; set; }
        public TimeOnly? ClosingTime { get; set; }
    }

    // ========================================
    // Enum Serialization Test Entities
    // ========================================

    /// <summary>Default underlying type (int)</summary>
    public enum UserRole
    {
        Guest = 0,
        User = 1,
        Moderator = 2,
        Admin = 3
    }

    /// <summary>int-based enum with explicit values</summary>
    public enum OrderStatus : int
    {
        Pending = 0,
        Processing = 10,
        Shipped = 20,
        Delivered = 30,
        Cancelled = -1
    }

    /// <summary>byte-based enum</summary>
    public enum Priority : byte
    {
        Low = 0,
        Normal = 1,
        High = 2,
        Critical = 3
    }

    /// <summary>long-based enum</summary>
    public enum AuditAction : long
    {
        None = 0L,
        Created = 1L,
        Updated = 2L,
        Deleted = 3L,
        Archived = 100L
    }

    /// <summary>[Flags] enum to verify bitwise values survive round-trip</summary>
    [Flags]
    public enum Permissions
    {
        None = 0,
        Read = 1,
        Write = 2,
        Execute = 4,
        Admin = 8,
        All = Read | Write | Execute | Admin
    }

    /// <summary>
    /// Entity with various enum property types:
    /// required, nullable, collections, different underlying types, flags.
    /// </summary>
    public class EnumEntity
    {
        public ObjectId Id { get; set; }

        // Required enum (default underlying = int)
        public UserRole Role { get; set; }

        // Nullable enum
        public OrderStatus? Status { get; set; }

        // byte-based enum
        public Priority Priority { get; set; }

        // Nullable byte-based enum
        public Priority? FallbackPriority { get; set; }

        // long-based enum
        public AuditAction LastAction { get; set; }

        // [Flags] enum stored as int
        public Permissions Permissions { get; set; }

        // Collection of enums (List<T>)
        public List<UserRole> AssignableRoles { get; set; } = new();

        // Array of enums
        public OrderStatus[] StatusHistory { get; set; } = Array.Empty<OrderStatus>();

        // A simple string field to prove other fields still work
        public string Label { get; set; } = string.Empty;
    }
}
