using BLite.Bson;
using BLite.Core.Collections;
using Xunit;
using System.Collections.Generic;
using System;
using System.Linq;

namespace BLite.Tests;

public class RobustnessTests
{
    public struct Point
    {
        public int X { get; set; }
        public int Y { get; set; }
    }

    public class RobustEntity
    {
        public List<int?> NullableInts { get; set; } = new();
        public Dictionary<string, int> Map { get; set; } = new();
        public IEnumerable<string> EnumerableStrings { get; set; } = Array.Empty<string>();
        public Point Location { get; set; }
        public Point? NullableLocation { get; set; }
    }

    [Fact]
    public void GenerateSchema_RobustnessChecks()
    {
        var schema = BsonSchemaGenerator.FromType<RobustEntity>();
        
        // 1. Nullable Value Types in List
        var nullableInts = schema.Fields.First(f => f.Name == "nullableints");
        Assert.Equal(BsonType.Array, nullableInts.Type);
        Assert.Equal(BsonType.Int32, nullableInts.ArrayItemType);
        // Note: Current Schema doesn't capture "ItemIsNullable", but verifying it doesn't crash/return Undefined

        // 2. Dictionary (likely treated as Array of KVPs currently, or Undefined if structs fail)
        // With current logic: Dictionary implements IEnumerable<KVP>. KVP is struct. 
        // If generator doesn't handle structs as Documents, item type might be Undefined.
        var map = schema.Fields.First(f => f.Name == "map");
        Assert.Equal(BsonType.Array, map.Type);
        
        // 3. IEnumerable property
        var enumerable = schema.Fields.First(f => f.Name == "enumerablestrings");
        Assert.Equal(BsonType.Array, enumerable.Type);
        Assert.Equal(BsonType.String, enumerable.ArrayItemType);

        // 4. Struct
        var location = schema.Fields.First(f => f.Name == "location");
        // Structs should be treated as Documents in BSON if not primitive
        Assert.Equal(BsonType.Document, location.Type); 
        Assert.NotNull(location.NestedSchema);
        Assert.Contains(location.NestedSchema.Fields, f => f.Name == "x");

        // 5. Nullable Struct
        var nullableLocation = schema.Fields.First(f => f.Name == "nullablelocation");
        Assert.Equal(BsonType.Document, nullableLocation.Type);
        Assert.True(nullableLocation.IsNullable);
        Assert.NotNull(nullableLocation.NestedSchema);
    }
}
