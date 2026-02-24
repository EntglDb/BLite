using BLite.Core.Collections;

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
        
        Assert.Contains(schema.Fields, f => f.Name == "normalprop");
        Assert.Contains(schema.Fields, f => f.Name == "privatesetprop");
        Assert.Contains(schema.Fields, f => f.Name == "initprop");

        // Verify assumption about fields
        // Current implementation uses GetProperties, so publicfield might be missing.
        // We will assert current status and then fix if requested/failed.
        Assert.Contains(schema.Fields, f => f.Name == "publicfield"); // This will likely fail currently
        
        Assert.DoesNotContain(schema.Fields, f => f.Name == "_privatefield");
    }
}
