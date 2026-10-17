using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using BLite.Core.Storage;

namespace AotSmoke;

/// <summary>The entity of the model-based store test (tests/BLite.Shared/StoreModelContexts.cs).</summary>
public class StoreDoc
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Count { get; set; }
    public string? Code { get; set; }
    public string Payload { get; set; } = string.Empty;
}

/// <summary>
/// The context of the model-based store test, source-generated: the mapper and
/// <c>InitializeCollections</c> come from BLite.SourceGenerators, the indexes are declared in
/// <see cref="OnModelCreating"/> rather than created with <c>CreateIndexAsync</c>.
/// </summary>
public partial class AotDbContext : DocumentDbContext
{
    public DocumentCollection<int, StoreDoc> Docs { get; set; } = null!;

    /// <summary>The engine passed to the constructor.</summary>
    public StorageEngine Engine { get; }

    /// <summary>Over an engine the caller opened, so that the caller can back it up; the context disposes it.</summary>
    public AotDbContext(StorageEngine storage) : base(storage)
    {
        Engine = storage;
        InitializeCollections();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<StoreDoc>().ToCollection("store_docs")
            .HasKey(d => d.Id)
            .HasIndex(d => d.Count)
            .HasIndex(d => d.Code, unique: true);
    }
}
