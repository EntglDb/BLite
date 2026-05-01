using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Minimal DocumentDbContext that defines a secondary index on <see cref="MultiFileEntry.Tag"/>.
/// Used by encryption integration tests to verify that encrypted index files are read and
/// queried correctly after a context close/reopen cycle.
/// </summary>
public partial class EncIndexedContext : DocumentDbContext
{
    public DocumentCollection<int, MultiFileEntry> Entries { get; set; } = null!;

    public EncIndexedContext(string databasePath, PageFileConfig config)
        : base(databasePath, config)
    {
        InitializeCollections();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MultiFileEntry>()
            .ToCollection("enc_idx_entries")
            .HasKey(e => e.Id)
            .HasIndex(e => e.Tag);
    }
}
