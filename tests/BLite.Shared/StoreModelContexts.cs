using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using BLite.Core.Storage;

namespace BLite.Shared;

/// <summary>
/// A document for the model-based store test: an explicit int key, a non-unique indexed
/// <see cref="Count"/> with few distinct values, a unique indexed nullable <see cref="Code"/>, and a
/// <see cref="Payload"/> whose size varies from nothing to several pages.
/// </summary>
public class StoreDoc
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Count { get; set; }
    public string? Code { get; set; }
    public string Payload { get; set; } = string.Empty;
}

public partial class StoreModelDbContext : DocumentDbContext
{
    public DocumentCollection<int, StoreDoc> Docs { get; set; } = null!;

    public StoreModelDbContext(string path) : base(path) => InitializeCollections();

    public StoreModelDbContext(string path, PageFileConfig config) : base(path, config) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<StoreDoc>().ToCollection("store_docs")
            .HasKey(d => d.Id)
            .HasIndex(d => d.Count)
            .HasIndex(d => d.Code, unique: true);
    }
}
