using BLite.Core.Collections;
using BLite.Bson;
using Xunit;
using System.Text;
using BLite.Core.Indexing;

namespace BLite.Tests;

public class SchemaTests
{
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, ushort> _testKeyMap = new(StringComparer.OrdinalIgnoreCase);
    static SchemaTests()
    {
        ushort id = 1;
        foreach (var k in new[] { "_id", "name", "address", "city", "street" }) _testKeyMap[k] = id++;
    }
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
            var doc = new BsonDocumentBuilder(_testKeyMap);
            if (addr.City != null) doc.AddString("city", addr.City);
            if (addr.Street != null) doc.AddString("street", addr.Street);
            return doc.Build();
        }

        public static IEnumerable<string> UsedKeys => new[] { "City", "Street" };

        public static BsonDocument GetSchemaManifest()
        {
            return BsonDocument.Create(_testKeyMap, b =>
            {
                b.AddString("city", "String");
                b.AddString("street", "String");
            });
        }
    }

    // 3. Manual Mapper for User (Root)
    public class UserMapper : Int32MapperBase<User>
    {
        public override string CollectionName => "users";

        public override int GetId(User entity) => entity.Id;
        public override void SetId(User entity, int id) => entity.Id = id;

        public override int Serialize(User entity, BsonSpanWriter writer)
        {
            // Dummy implementation for Schema test
            return 0;
        }

        public override User Deserialize(BsonSpanReader reader)
        {
             // Dummy implementation
             return new User();
        }

        public override IEnumerable<string> UsedKeys
        {
            get
            {
                var keys = new List<string> { "_id", "name", "address" };
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
                Name = "name", 
                Type = BsonType.String 
            });

            // Address (Nested)
            var addressSchema = new BsonSchema { Title = "Address" };
            addressSchema.Fields.Add(new BsonField { Name = "city", Type = BsonType.String });
            addressSchema.Fields.Add(new BsonField { Name = "street", Type = BsonType.String });

            schema.Fields.Add(new BsonField 
            { 
                Name = "address", 
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
        Assert.Contains("name", keys);
        Assert.Contains("address", keys);
    }
    
    [Fact]
    public void GetSchema_ShouldReturnBsonSchema()
    {
        var mapper = new UserMapper();
        var schema = mapper.GetSchema();
        
        var idField = schema.Fields.FirstOrDefault(f => f.Name == "_id");
        Assert.NotNull(idField);
        Assert.Equal(BsonType.Int32, idField.Type);
        
        var nameField = schema.Fields.FirstOrDefault(f => f.Name == "name");
        Assert.NotNull(nameField);
        Assert.Equal(BsonType.String, nameField.Type);

        var addressField = schema.Fields.FirstOrDefault(f => f.Name == "address");
        Assert.NotNull(addressField);
        Assert.Equal(BsonType.Document, addressField.Type);
        Assert.NotNull(addressField.NestedSchema);
        Assert.Contains(addressField.NestedSchema.Fields, f => f.Name == "city");
    }
}
