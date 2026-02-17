using BLite.Bson;
using BLite.Shared;
using BLite.Tests.TestDbContext_TestDbContext_Mappers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace BLite.Tests;

/// <summary>
/// Tests for nested objects that have their own Id fields.
/// This validates that Id fields in nested objects don't cause mapper generation issues.
/// </summary>
public class NestedObjectWithIdTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public NestedObjectWithIdTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_nested_id_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db?.Dispose();
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
    }

    [Fact]
    public void Should_Serialize_NestedObject_WithId()
    {
        // Arrange
        var customer = new CustomerWithContact
        {
            Id = ObjectId.NewObjectId(),
            Name = "John Doe",
            Contact = new ContactInfo
            {
                Id = 123,
                Email = "john@example.com",
                Phone = "+1234567890"
            }
        };

        // Act - Insert the entity
        _db.CustomersWithContact.Insert(customer);

        // Assert - Retrieve and verify
        var retrieved = _db.CustomersWithContact.FindById(customer.Id);
        Assert.NotNull(retrieved);
        Assert.Equal(customer.Name, retrieved.Name);
        Assert.NotNull(retrieved.Contact);
        Assert.Equal(123, retrieved.Contact.Id);
        Assert.Equal("john@example.com", retrieved.Contact.Email);
        Assert.Equal("+1234567890", retrieved.Contact.Phone);
    }

    [Fact]
    public void Should_Serialize_CollectionOfNestedObjects_WithId()
    {
        // Arrange
        var company = new CompanyWithContacts
        {
            Id = ObjectId.NewObjectId(),
            CompanyName = "Acme Corp",
            Contacts = new List<ContactInfo>
            {
                new ContactInfo { Id = 1, Email = "ceo@acme.com", Phone = "+1111111111" },
                new ContactInfo { Id = 2, Email = "cto@acme.com", Phone = "+2222222222" },
                new ContactInfo { Id = 3, Email = "cfo@acme.com", Phone = "+3333333333" }
            }
        };

        // Act - Insert the entity
        _db.CompaniesWithContacts.Insert(company);

        // Assert - Retrieve and verify
        var retrieved = _db.CompaniesWithContacts.FindById(company.Id);
        Assert.NotNull(retrieved);
        Assert.Equal("Acme Corp", retrieved.CompanyName);
        Assert.Equal(3, retrieved.Contacts.Count);
        
        // Verify all contacts were serialized and deserialized correctly
        Assert.Equal(1, retrieved.Contacts[0].Id);
        Assert.Equal("ceo@acme.com", retrieved.Contacts[0].Email);
        
        Assert.Equal(2, retrieved.Contacts[1].Id);
        Assert.Equal("cto@acme.com", retrieved.Contacts[1].Email);
        
        Assert.Equal(3, retrieved.Contacts[2].Id);
        Assert.Equal("cfo@acme.com", retrieved.Contacts[2].Email);
    }

    [Fact]
    public void Should_Update_NestedObject_WithId()
    {
        // Arrange
        var customer = new CustomerWithContact
        {
            Id = ObjectId.NewObjectId(),
            Name = "Jane Smith",
            Contact = new ContactInfo
            {
                Id = 456,
                Email = "jane@example.com",
                Phone = "+9876543210"
            }
        };
        _db.CustomersWithContact.Insert(customer);

        // Act - Update the nested object's Id and other fields
        customer.Contact.Id = 789;
        customer.Contact.Email = "jane.new@example.com";
        _db.CustomersWithContact.Update(customer);

        // Assert
        var retrieved = _db.CustomersWithContact.FindById(customer.Id);
        Assert.NotNull(retrieved);
        Assert.Equal(789, retrieved.Contact.Id);
        Assert.Equal("jane.new@example.com", retrieved.Contact.Email);
    }

    [Fact]
    public void Should_Query_By_NestedObject_Field()
    {
        // Arrange
        var customer1 = new CustomerWithContact
        {
            Id = ObjectId.NewObjectId(),
            Name = "Alice",
            Contact = new ContactInfo { Id = 100, Email = "alice@test.com", Phone = "+1111111111" }
        };
        var customer2 = new CustomerWithContact
        {
            Id = ObjectId.NewObjectId(),
            Name = "Bob",
            Contact = new ContactInfo { Id = 200, Email = "bob@test.com", Phone = "+2222222222" }
        };

        _db.CustomersWithContact.Insert(customer1);
        _db.CustomersWithContact.Insert(customer2);

        // Act - Query by nested email field
        var result = _db.CustomersWithContact
            .AsQueryable()
            .Where(c => c.Contact.Email == "alice@test.com")
            .ToList();

        // Assert
        Assert.Single(result);
        Assert.Equal("Alice", result[0].Name);
        Assert.Equal(100, result[0].Contact.Id);
    }

    [Fact]
    public void Should_Handle_Null_NestedObject_WithId()
    {
        // Arrange
        var customer = new CustomerWithContact
        {
            Id = ObjectId.NewObjectId(),
            Name = "No Contact Person",
            Contact = null!
        };

        // Act
        _db.CustomersWithContact.Insert(customer);

        // Assert
        var retrieved = _db.CustomersWithContact.FindById(customer.Id);
        Assert.NotNull(retrieved);
        Assert.Equal("No Contact Person", retrieved.Name);
        Assert.Null(retrieved.Contact);
    }

    [Fact]
    public void Should_Verify_ContactInfo_Mapper_HasSerializeFields()
    {
        // This test verifies that the mapper for ContactInfo has the SerializeFields method
        // which is required for nested object serialization
        var mapper = new BLite_Shared_ContactInfoMapper();
        
        // If this compiles and runs, it means SerializeFields exists
        var contact = new ContactInfo { Id = 999, Email = "test@test.com", Phone = "+0000000000" };
        
        var keyMap = new System.Collections.Concurrent.ConcurrentDictionary<string, ushort>(StringComparer.OrdinalIgnoreCase);
        keyMap["id"] = 1;
        keyMap["email"] = 2;
        keyMap["phone"] = 3;
        
        var buffer = new byte[1024];
        var writer = new BsonSpanWriter(buffer, keyMap);
        
        // This should not throw
        mapper.SerializeFields(contact, ref writer);
        
        Assert.True(writer.Position > 0, "Mapper should have written data");
    }

    [Fact]
    public void Should_Serialize_NestedId_AsRegularField_NotAsUnderscore()
    {
        // This test verifies that the Id in a nested object is serialized as "id" not "_id"
        var contact = new ContactInfo { Id = 555, Email = "verify@test.com", Phone = "+5555555555" };
        
        var keyMap = new System.Collections.Concurrent.ConcurrentDictionary<string, ushort>(StringComparer.OrdinalIgnoreCase);
        keyMap["id"] = 1;
        keyMap["email"] = 2;
        keyMap["phone"] = 3;
        
        var keys = new System.Collections.Concurrent.ConcurrentDictionary<ushort, string>();
        keys[1] = "id";
        keys[2] = "email";
        keys[3] = "phone";
        
        var buffer = new byte[1024];
        var writer = new BsonSpanWriter(buffer, keyMap);
        var mapper = new BLite_Shared_ContactInfoMapper();
        
        // Serialize the nested object
        int bytesWritten = mapper.Serialize(contact, writer);
        
        // Read back the BSON to verify structure
        var reader = new BsonSpanReader(buffer.AsSpan(0, bytesWritten), keys);
        
        // Read document
        var docSize = reader.ReadDocumentSize();
        Assert.True(docSize > 0);
        
        bool foundIdField = false;
        bool foundUnderscoreIdField = false;
        
        // ReadDocumentSize returns the full document size including the 4-byte size field itself
        // Position is already advanced by 4 bytes after ReadDocumentSize
        // So we read until position reaches the original position + docSize - 4
        while (reader.Position < docSize - 4)
        {
            var bsonType = reader.ReadBsonType();
            if (bsonType == BsonType.EndOfDocument) break;
            
            var fieldName = reader.ReadElementHeader();
            
            if (fieldName == "id")
                foundIdField = true;
            if (fieldName == "_id")
                foundUnderscoreIdField = true;
                
            reader.SkipValue(bsonType);
        }
        
        // For nested objects, Id should be serialized as "id", NOT "_id"
        Assert.True(foundIdField, "Nested object should have 'id' field");
        Assert.False(foundUnderscoreIdField, "Nested object should NOT have '_id' field");
    }
}
