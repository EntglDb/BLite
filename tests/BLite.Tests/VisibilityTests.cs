using BLite.Bson;
using BLite.Core.Collections;
using Xunit;
using System.Linq;

namespace BLite.Tests;

public class VisibilityTests
{
    public class VisibilityEntity
    {
        // Should be included
        public int NormalProp { get; set; }
        
        // Should be included (serialization usually writes it)
        public int PrivateSetProp { get; private set; }
        
        // Should be included
        public int InitProp { get; init; }

        // Fields - typically included in BSON if public, but reflection need GetFields
        public string PublicField = string.Empty;

        // Should NOT be included
        private int _privateField;
        
        // Helper to set private
        public void SetPrivate(int val) => _privateField = val;
    }

    [Fact]
    public void GenerateSchema_VisibilityChecks()
    {
        var schema = BsonSchemaGenerator.FromType<VisibilityEntity>();
        
        Assert.Contains(schema.Fields, f => f.Name == "NormalProp");
        Assert.Contains(schema.Fields, f => f.Name == "PrivateSetProp");
        Assert.Contains(schema.Fields, f => f.Name == "InitProp");

        // Verify assumption about fields
        // Current implementation uses GetProperties, so PublicField might be missing.
        // We will assert current status and then fix if requested/failed.
        Assert.Contains(schema.Fields, f => f.Name == "PublicField"); // This will likely fail currently
        
        Assert.DoesNotContain(schema.Fields, f => f.Name == "_privateField");
    }
}
