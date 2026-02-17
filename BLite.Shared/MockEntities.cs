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

    // --- Test entities for nested objects with Id fields ---

    /// <summary>
    /// A nested object that has its own Id field.
    /// This is used to test that Id in nested objects doesn't cause mapper generation issues.
    /// </summary>
    public class ContactInfo
    {
        [Key]
        public int Id { get; set; }
        public string Email { get; set; } = "";
        public string Phone { get; set; } = "";
    }

    /// <summary>
    /// Root entity with a nested object that has an Id field.
    /// </summary>
    public class CustomerWithContact
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; } = "";
        public ContactInfo Contact { get; set; } = new();
    }

    /// <summary>
    /// Root entity with a collection of nested objects that have Id fields.
    /// </summary>
    public class CompanyWithContacts
    {
        public ObjectId Id { get; set; }
        public string CompanyName { get; set; } = "";
        public List<ContactInfo> Contacts { get; set; } = new();
    }
}
