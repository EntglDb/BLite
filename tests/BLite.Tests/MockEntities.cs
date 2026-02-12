using System;
using System.Collections.Generic;
using BLite.Bson;
using BLite.Core.Collections;

namespace BLite.Tests
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
}
