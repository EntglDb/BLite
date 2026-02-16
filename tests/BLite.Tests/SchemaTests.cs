using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Indexing;
using BLite.Tests.TestDbContext_TestDbContext_Mappers;
using System.Text;
using Xunit;

namespace BLite.Tests;

public class SchemaTests
{
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, ushort> _testKeyMap = new(StringComparer.OrdinalIgnoreCase);
    static SchemaTests()
    {
        ushort id = 1;
        foreach (var k in new[] { "_id", "name", "mainaddress", "otheraddresses","tags","secret","street","city" }) _testKeyMap[k] = id++;
    }

    [Fact]
    public void UsedKeys_ShouldReturnAllKeys()
    {
        var mapper = new BLite_Shared_ComplexUserMapper();
        var keys = mapper.UsedKeys.ToList();
        
        Assert.Contains("_id", keys);
        Assert.Contains("name", keys);
        Assert.Contains("mainaddress", keys);
        Assert.Contains("otheraddresses", keys);
        Assert.Contains("tags", keys);
        Assert.Contains("secret", keys);
        Assert.Contains("street", keys);
        Assert.Contains("city", keys);

    }
    
    [Fact]
    public void GetSchema_ShouldReturnBsonSchema()
    {
        var mapper = new BLite_Shared_ComplexUserMapper();
        var schema = mapper.GetSchema();
        
        var idField = schema.Fields.FirstOrDefault(f => f.Name == "_id");
        Assert.NotNull(idField);
        Assert.Equal(BsonType.ObjectId, idField.Type);
        
        var nameField = schema.Fields.FirstOrDefault(f => f.Name == "name");
        Assert.NotNull(nameField);
        Assert.Equal(BsonType.String, nameField.Type);

        var addressField = schema.Fields.FirstOrDefault(f => f.Name == "mainaddress");
        Assert.NotNull(addressField);
        Assert.Equal(BsonType.Document, addressField.Type);
        Assert.NotNull(addressField.NestedSchema);
        // Address in MockEntities has City (Nested)
        Assert.Contains(addressField.NestedSchema.Fields, f => f.Name == "city");
    }
}
