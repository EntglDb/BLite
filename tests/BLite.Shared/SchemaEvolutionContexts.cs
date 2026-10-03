using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;

namespace BLite.Shared;

public class EvoA { public ObjectId Id { get; set; } public string Name { get; set; } = ""; }
public class EvoB { public ObjectId Id { get; set; } public string Name { get; set; } = ""; }

/// <summary>"App v1": declares only collection A.</summary>
public partial class EvoV1DbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, EvoA> A { get; set; } = null!;
    public EvoV1DbContext(string path) : base(path) => InitializeCollections();
}

/// <summary>"App v2": keeps A and adds collection B.</summary>
public partial class EvoV2DbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, EvoA> A { get; set; } = null!;
    public DocumentCollection<ObjectId, EvoB> B { get; set; } = null!;
    public EvoV2DbContext(string path) : base(path) => InitializeCollections();
}

/// <summary>Declares only collection B (A becomes an orphan and is dropped on open).</summary>
public partial class EvoOnlyBDbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, EvoB> B { get; set; } = null!;
    public EvoOnlyBDbContext(string path) : base(path) => InitializeCollections();
}
