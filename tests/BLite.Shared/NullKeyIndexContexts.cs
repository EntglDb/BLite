using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;

namespace BLite.Shared;

// Contexts for NullKeyIndexPlanTests (#247): the same collection with and without indexes on
// nullable and non-nullable properties.

public sealed class NkDoc
{
    public ObjectId Id { get; set; }
    public string Name { get; set; } = "";
    public string? Nick { get; set; }
    public int? MaybeCount { get; set; }
    public bool Flag { get; set; }
    public int Rank { get; set; }
}

/// <summary>Indexes on every property the tests query or order by.</summary>
public partial class NkIndexedContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, NkDoc> Docs { get; set; } = null!;
    public NkIndexedContext(string path) : base(path) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<NkDoc>().ToCollection("nk_docs")
            .HasIndex(x => x.Nick)
            .HasIndex(x => x.MaybeCount)
            .HasIndex(x => x.Flag)
            .HasIndex(x => x.Rank);
    }
}

/// <summary>The same collection without secondary indexes.</summary>
public partial class NkPlainContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, NkDoc> Docs { get; set; } = null!;
    public NkPlainContext(string path) : base(path) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<NkDoc>().ToCollection("nk_docs");
    }
}
