using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;

// Deliberately declared in the GLOBAL namespace (no namespace block).
// Regression fixture: ContainingNamespace.ToDisplayString() used to return the
// literal "<global namespace>" for these types, crashing the mapper generator
// with an invalid hint-name character (CS8785).

public partial class GlobalNsDbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, GlobalNsPerson> People { get; set; } = null!;

    public GlobalNsDbContext(string databasePath) : base(databasePath)
    {
        InitializeCollections();
    }
}

public class GlobalNsPerson
{
    public ObjectId Id { get; set; }
    public string Name { get; set; } = "";

    // Nested type: exercises the generator's NestedTypeInfo path for entities
    // declared in the global namespace.
    public GlobalNsMetrics Metrics { get; set; } = new();

    public class GlobalNsMetrics
    {
        public int Score { get; set; }
    }
}
