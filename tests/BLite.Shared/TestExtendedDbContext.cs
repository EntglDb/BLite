using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Extended test context that inherits from TestDbContext.
/// Used to verify that collection initialization works correctly with inheritance.
/// </summary>
public partial class TestExtendedDbContext : TestDbContext
{
    public DocumentCollection<int, ExtendedEntity> ExtendedEntities { get; set; } = null!;

    public TestExtendedDbContext(string databasePath) : base(databasePath)
    {
        InitializeCollections();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        
        modelBuilder.Entity<ExtendedEntity>()
            .ToCollection("extended_entities")
            .HasKey(e => e.Id);
    }
}
