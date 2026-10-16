using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;

namespace BLite.Shared;

// Contexts for SignedZeroIndexTests (#249): the same collection with and without indexes on a
// double and a float property.

public sealed class SzDoc
{
    public ObjectId Id { get; set; }
    public string Name { get; set; } = "";
    public double Ratio { get; set; }
    public float Level { get; set; }
}

/// <summary>Indexes on <see cref="SzDoc.Ratio"/> and <see cref="SzDoc.Level"/>.</summary>
public partial class SzIndexedContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, SzDoc> Docs { get; set; } = null!;
    public SzIndexedContext(string path) : base(path) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SzDoc>().ToCollection("sz_docs")
            .HasIndex(x => x.Ratio)
            .HasIndex(x => x.Level);
    }
}

/// <summary>The same collection without secondary indexes.</summary>
public partial class SzPlainContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, SzDoc> Docs { get; set; } = null!;
    public SzPlainContext(string path) : base(path) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SzDoc>().ToCollection("sz_docs");
    }
}
