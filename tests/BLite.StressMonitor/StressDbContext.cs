using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;

namespace BLite.StressMonitor;

/// <summary>Document used by the stress load workers.</summary>
public class StressDoc
{
    [BsonId]
    public int Id { get; set; }

    public string Name { get; set; } = "";

    /// <summary>One of five fixed categories — used to exercise the secondary index.</summary>
    public string Category { get; set; } = "";

    public double Value { get; set; }

    /// <summary>Incremented on each update to verify round-trip correctness.</summary>
    public long Version { get; set; }
}

/// <summary>
/// BLite DbContext for the stress-monitor load workers.
/// The source generator emits <c>InitializeCollections()</c> automatically.
/// A secondary index on <see cref="StressDoc.Category"/> is configured via
/// <c>OnModelCreating</c> so that find workers exercise the index path.
/// </summary>
public partial class StressDbContext : DocumentDbContext
{
    public DocumentCollection<int, StressDoc> Docs { get; set; } = null!;

    public StressDbContext(string path) : base(path)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<StressDoc>()
            .ToCollection("stress_docs")
            .HasIndex(d => d.Category);
    }
}
