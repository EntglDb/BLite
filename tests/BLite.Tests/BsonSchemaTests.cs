using BLite.Bson;
using BLite.Core.Collections;
using Xunit;
using System.Collections.Generic;
using System;
using System.Linq;

namespace BLite.Tests;

public class BsonSchemaTests
{
    public class SimpleEntity
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public bool IsActive { get; set; }
    }

    [Fact]
    public void GenerateSchema_SimpleEntity()
    {
        var schema = BsonSchemaGenerator.FromType<SimpleEntity>();

        Assert.Equal("SimpleEntity", schema.Title);
        Assert.Equal(4, schema.Fields.Count);

        var idField = schema.Fields.First(f => f.Name == "_id");
        Assert.Equal(BsonType.ObjectId, idField.Type);

        var nameField = schema.Fields.First(f => f.Name == "name");
        Assert.Equal(BsonType.String, nameField.Type);

        var ageField = schema.Fields.First(f => f.Name == "age");
        Assert.Equal(BsonType.Int32, ageField.Type);
    }

    public class CollectionEntity
    {
        public List<string> Tags { get; set; } = new();
        public int[] Scores { get; set; } = Array.Empty<int>();
    }

    [Fact]
    public void GenerateSchema_Collections()
    {
        var schema = BsonSchemaGenerator.FromType<CollectionEntity>();

        var tags = schema.Fields.First(f => f.Name == "tags");
        Assert.Equal(BsonType.Array, tags.Type);
        Assert.Equal(BsonType.String, tags.ArrayItemType);

        var scores = schema.Fields.First(f => f.Name == "scores");
        Assert.Equal(BsonType.Array, scores.Type);
        Assert.Equal(BsonType.Int32, scores.ArrayItemType);
    }

    public class NestedEntity
    {
        public SimpleEntity Parent { get; set; } = new();
    }

    [Fact]
    public void GenerateSchema_Nested()
    {
        var schema = BsonSchemaGenerator.FromType<NestedEntity>();
        
        var parent = schema.Fields.First(f => f.Name == "parent");
        Assert.Equal(BsonType.Document, parent.Type);
        Assert.NotNull(parent.NestedSchema);
        Assert.Contains(parent.NestedSchema.Fields, f => f.Name == "_id");
    }

    public class ComplexCollectionEntity
    {
        public List<SimpleEntity> Items { get; set; } = new();
    }

    [Fact]
    public void GenerateSchema_ComplexCollection()
    {
        var schema = BsonSchemaGenerator.FromType<ComplexCollectionEntity>();
        
        var items = schema.Fields.First(f => f.Name == "items");
        Assert.Equal(BsonType.Array, items.Type);
        // Assert.Equal(BsonType.Document, items.ArrayItemType); // Wait, my generator logic might return Array here? No, item type logic...
        
        // Let's verify generator logic for complex array item type
        // In generator: (BsonType.Array, itemNested, itemBsonType)
        // itemBsonType for SimpleEntity should be Document
        
        Assert.Equal(BsonType.Document, items.ArrayItemType);
        Assert.NotNull(items.NestedSchema);
        Assert.Contains(items.NestedSchema.Fields, f => f.Name == "_id");
    }
}
