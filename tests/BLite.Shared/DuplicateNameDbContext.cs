using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using System;
using ModuleA = BLite.Shared.ModuleA;
using ModuleB = BLite.Shared.ModuleB;

namespace BLite.Tests;

/// <summary>
/// Test context for verifying that the Source Generator correctly handles
/// entities with duplicate class names from different namespaces.
/// </summary>
public partial class DuplicateNameDbContext : DocumentDbContext
{
    public DocumentCollection<int, ModuleA.Widget> ModuleAWidgets { get; set; } = null!;
    public DocumentCollection<int, ModuleB.Widget> ModuleBWidgets { get; set; } = null!;

    public DuplicateNameDbContext(string databasePath) : base(databasePath)
    {
        InitializeCollections();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ModuleA.Widget>().ToCollection("module_a_widgets").HasKey(e => e.Id);
        modelBuilder.Entity<ModuleB.Widget>().ToCollection("module_b_widgets").HasKey(e => e.Id);
    }
}
