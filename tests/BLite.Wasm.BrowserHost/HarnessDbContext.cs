using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using BLite.Core.Storage;

namespace BLite.Wasm.BrowserHost;

public class HarnessItem
{
    public ObjectId Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Value { get; set; }
}

/// <summary>Typed context over a browser storage backend, built through the pluggable StorageEngine constructor.</summary>
public partial class HarnessDbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, HarnessItem> Items { get; set; } = null!;

    public HarnessDbContext(StorageEngine storage) : base(storage)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<HarnessItem>().ToCollection("items").HasIndex(x => x.Value);
    }
}

/// <summary>The same entity without a secondary index, to separate index building from the rest of the typed path.</summary>
public partial class HarnessPlainDbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, HarnessItem> Items { get; set; } = null!;

    public HarnessPlainDbContext(StorageEngine storage) : base(storage)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<HarnessItem>().ToCollection("plain_items");
    }
}
