using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;

namespace BLite.Shared;

// Contexts for NullComparisonTests (#248): the same collection with and without indexes on the
// nullable properties, and an older shape of the entity that lacks them, so documents written
// through it have the fields absent.

public enum NcState { Open = 1, Closed = 2, Archived = 3 }

public sealed class NcDoc
{
    public ObjectId Id { get; set; }
    public string Name { get; set; } = "";
    public int? MaybeCount { get; set; }
    public string? Nick { get; set; }
    public NcState? State { get; set; }
    public NcState Kind { get; set; }
}

/// <summary>An older version of <see cref="NcDoc"/>: only the name.</summary>
public sealed class NcDocV1
{
    public ObjectId Id { get; set; }
    public string Name { get; set; } = "";
}

/// <summary>Indexes on every property the tests compare.</summary>
public partial class NcIndexedContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, NcDoc> Docs { get; set; } = null!;
    public NcIndexedContext(string path) : base(path) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<NcDoc>().ToCollection("nc_docs")
            .HasIndex(x => x.MaybeCount)
            .HasIndex(x => x.Nick)
            .HasIndex(x => x.State)
            .HasIndex(x => x.Kind);
    }
}

/// <summary>The same collection without secondary indexes.</summary>
public partial class NcPlainContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, NcDoc> Docs { get; set; } = null!;
    public NcPlainContext(string path) : base(path) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<NcDoc>().ToCollection("nc_docs");
    }
}

/// <summary>Writes <see cref="NcDocV1"/> into the same collection.</summary>
public partial class NcV1Context : DocumentDbContext
{
    public DocumentCollection<ObjectId, NcDocV1> Docs { get; set; } = null!;
    public NcV1Context(string path) : base(path) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<NcDocV1>().ToCollection("nc_docs");
    }
}
