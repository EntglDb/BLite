using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;

namespace BLite.Benchmark;

/// <summary>
/// Simulates a real-world photo library document: one UUID primary key,
/// a SourceId (folder / album) foreign-key field indexed for 1-to-N queries,
/// and a unique FilePath field indexed for 1-to-1 exact-match queries.
/// </summary>
public class RealWorldPhotoPo
{
    public Guid   Id         { get; init; }
    public string SourceId   { get; init; } = null!;
    public string FilePath   { get; init; } = null!;
    public DateTime DateTaken { get; set; }
    public long   FileSize   { get; set; }
    public int    Width      { get; set; }
    public int    Height     { get; set; }
}

public partial class RealWorldPhotoDbContext : DocumentDbContext
{
    public RealWorldPhotoDbContext(string path) : base(path)
    {
        InitializeCollections();
    }

    public DocumentCollection<Guid, RealWorldPhotoPo> Photos { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<RealWorldPhotoPo>()
            .ToCollection("Photos")
            .HasIndex(x => x.Id,       unique: true)
            .HasIndex(x => x.SourceId)
            .HasIndex(x => x.FilePath, unique: true);
    }
}
