using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using System;
using ModuleA = BLite.Shared.ModuleA;
using ModuleB = BLite.Shared.ModuleB;
using Module_A = BLite.Shared.Module_A;
using Module = BLite.Shared.Module;

namespace BLite.Tests;

/// <summary>
/// Test context for verifying that the Source Generator correctly handles
/// entities with duplicate class names from different namespaces.
/// Also validates GetMapperName collision-resistance when type names contain underscores.
/// </summary>
public partial class DuplicateNameDbContext : DocumentDbContext
{
    public DocumentCollection<int, ModuleA.Widget> ModuleAWidgets { get; set; } = null!;
    public DocumentCollection<int, ModuleB.Widget> ModuleBWidgets { get; set; } = null!;

    // These two entities had colliding mapper names before the underscore-escaping fix:
    //   BLite.Shared.Module_A.Gadget  →  BLite_Shared_Module_A_GadgetMapper  (old, collides)
    //   BLite.Shared.Module.A_Gadget  →  BLite_Shared_Module_A_GadgetMapper  (old, collides)
    // After the fix:
    //   BLite.Shared.Module_A.Gadget  →  BLite_Shared_Module__A_GadgetMapper  (unique)
    //   BLite.Shared.Module.A_Gadget  →  BLite_Shared_Module_A__GadgetMapper  (unique)
    public DocumentCollection<int, Module_A.Gadget> Module_AGadgets { get; set; } = null!;
    public DocumentCollection<int, Module.A_Gadget> ModuleAGadgets { get; set; } = null!;

    public DuplicateNameDbContext(string databasePath) : base(databasePath)
    {
        InitializeCollections();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ModuleA.Widget>().ToCollection("module_a_widgets").HasKey(e => e.Id);
        modelBuilder.Entity<ModuleB.Widget>().ToCollection("module_b_widgets").HasKey(e => e.Id);
        modelBuilder.Entity<Module_A.Gadget>().ToCollection("module_underscore_a_gadgets").HasKey(e => e.Id);
        modelBuilder.Entity<Module.A_Gadget>().ToCollection("module_a_underscore_gadgets").HasKey(e => e.Id);
    }
}
