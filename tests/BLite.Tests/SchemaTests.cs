using BLite.Core.Collections;
using BLite.Bson;
using Xunit;
using System.Text;
using BLite.Core.Indexing;

namespace BLite.Tests;

public class SchemaTests
{
    // 1. Manual User Entity
    public class User
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public Address Address { get; set; } = new();
    }

    public class Address
    {
        public string City { get; set; } = string.Empty;
        public string Street { get; set; } = string.Empty;
    }

    // 2. Manual Mapper for Address (Nested)
    public class AddressMapper
    {
        public static BsonDocument Serialize(Address addr)
        {
            var doc = new BsonDocumentBuilder();
            if (addr.City != null) doc.AddString("City", addr.City);
            if (addr.Street != null) doc.AddString("Street", addr.Street);
            return doc.Build();
        }

        public static IEnumerable<string> UsedKeys => new[] { "City", "Street" };

        public static BsonDocument GetSchemaManifest()
        {
            return BsonDocument.Create(b =>
            {
                b.AddString("City", "String");
                b.AddString("Street", "String");
            });
        }
    }

    // 3. Manual Mapper for User (Root)
    public class UserMapper : Int32MapperBase<User>
    {
        public override string CollectionName => "users";

        public override int GetId(User entity) => entity.Id;
        public override void SetId(User entity, int id) => entity.Id = id;

        public override int Serialize(User entity, Span<byte> buffer)
        {
            // Dummy implementation for Schema test
            return 0;
        }

        public override User Deserialize(ReadOnlySpan<byte> buffer)
        {
             // Dummy implementation
             return new User();
        }

        public override IEnumerable<string> UsedKeys
        {
            get
            {
                var keys = new List<string> { "_id", "Name", "Address" };
                // In manual implementation, we decide if we expose nested keys strictly or just top level.
                // The requirement mentions 'UsedKeys' which usually implies indexes.
                // But for schema, GetSchemaManifest is the authority.
                return keys;
            }
        }

        public override BsonSchema GetSchema()
        {
            var schema = new BsonSchema { Title = "User" };
            
            // _id
            schema.Fields.Add(new BsonField 
            { 
                Name = "_id", 
                Type = BsonType.Int32 
            });

            // Name
            schema.Fields.Add(new BsonField 
            { 
                Name = "Name", 
                Type = BsonType.String 
            });

            // Address (Nested)
            var addressSchema = new BsonSchema { Title = "Address" };
            addressSchema.Fields.Add(new BsonField { Name = "City", Type = BsonType.String });
            addressSchema.Fields.Add(new BsonField { Name = "Street", Type = BsonType.String });

            schema.Fields.Add(new BsonField 
            { 
                Name = "Address", 
                Type = BsonType.Document,
                NestedSchema = addressSchema
            });

            return schema;
        }
    }

    [Fact]
    public void UsedKeys_ShouldReturnAllKeys()
    {
        var mapper = new UserMapper();
        var keys = mapper.UsedKeys.ToList();
        
        Assert.Contains("_id", keys);
        Assert.Contains("Name", keys);
        Assert.Contains("Address", keys);
    }
    
    [Fact]
    public void GetSchema_ShouldReturnBsonSchema()
    {
        var mapper = new UserMapper();
        var schema = mapper.GetSchema();
        
        var idField = schema.Fields.FirstOrDefault(f => f.Name == "_id");
        Assert.NotNull(idField);
        Assert.Equal(BsonType.Int32, idField.Type);
        
        var nameField = schema.Fields.FirstOrDefault(f => f.Name == "Name");
        Assert.NotNull(nameField);
        Assert.Equal(BsonType.String, nameField.Type);

        var addressField = schema.Fields.FirstOrDefault(f => f.Name == "Address");
        Assert.NotNull(addressField);
        Assert.Equal(BsonType.Document, addressField.Type);
        Assert.NotNull(addressField.NestedSchema);
        Assert.Contains(addressField.NestedSchema.Fields, f => f.Name == "City");
    }
}
