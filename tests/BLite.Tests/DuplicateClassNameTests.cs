using BLite.Bson;
using System.IO;
using ModuleA = BLite.Shared.ModuleA;
using ModuleB = BLite.Shared.ModuleB;
using Module_A = BLite.Shared.Module_A;
using Module = BLite.Shared.Module;

namespace BLite.Tests;

/// <summary>
/// Tests verifying that the Source Generator correctly handles entities
/// with duplicate class names from different namespaces.
/// Both root entities and their nested types (also with duplicate names) 
/// must get unique mapper class names and correct collection names.
/// </summary>
public class DuplicateClassNameTests : IDisposable
{
    private readonly string _dbPath;
    private readonly DuplicateNameDbContext _db;

    public DuplicateClassNameTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_duplicate_{System.Guid.NewGuid()}.db");
        _db = new DuplicateNameDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Fact]
    public void Insert_And_Retrieve_ModuleA_Widget()
    {
        var widget = new ModuleA.Widget
        {
            Id = 1,
            Name = "ModuleA Widget",
            Label = new ModuleA.Tag { Key = "color", Value = "red" }
        };

        _db.ModuleAWidgets.Insert(widget);
        _db.SaveChanges();

        var result = _db.ModuleAWidgets.FindById(1);
        Assert.NotNull(result);
        Assert.Equal("ModuleA Widget", result.Name);
        Assert.Equal("color", result.Label.Key);
        Assert.Equal("red", result.Label.Value);
    }

    [Fact]
    public void Insert_And_Retrieve_ModuleB_Widget()
    {
        var widget = new ModuleB.Widget
        {
            Id = 1,
            Title = "ModuleB Widget",
            Category = new ModuleB.Tag { Name = "electronics", Priority = 5 }
        };

        _db.ModuleBWidgets.Insert(widget);
        _db.SaveChanges();

        var result = _db.ModuleBWidgets.FindById(1);
        Assert.NotNull(result);
        Assert.Equal("ModuleB Widget", result.Title);
        Assert.Equal("electronics", result.Category.Name);
        Assert.Equal(5, result.Category.Priority);
    }

    [Fact]
    public void Both_Collections_Are_Independent()
    {
        // Insert into both collections
        var widgetA = new ModuleA.Widget { Id = 42, Name = "A-Widget" };
        var widgetB = new ModuleB.Widget { Id = 42, Title = "B-Widget" };

        _db.ModuleAWidgets.Insert(widgetA);
        _db.ModuleBWidgets.Insert(widgetB);
        _db.SaveChanges();

        // Ensure each collection stores and retrieves its own type
        var resultA = _db.ModuleAWidgets.FindById(42);
        var resultB = _db.ModuleBWidgets.FindById(42);

        Assert.NotNull(resultA);
        Assert.NotNull(resultB);
        Assert.Equal("A-Widget", resultA.Name);
        Assert.Equal("B-Widget", resultB.Title);

        // Verify collections are truly independent (different collection names)
        Assert.Equal(1, _db.ModuleAWidgets.Count());
        Assert.Equal(1, _db.ModuleBWidgets.Count());
    }

    [Fact]
    public void Nested_Types_With_Duplicate_Names_Serialize_Correctly()
    {
        var widgetA = new ModuleA.Widget
        {
            Id = 1,
            Name = "Widget A",
            Label = new ModuleA.Tag { Key = "size", Value = "large" }
        };
        var widgetB = new ModuleB.Widget
        {
            Id = 1,
            Title = "Widget B",
            Category = new ModuleB.Tag { Name = "category-1", Priority = 10 }
        };

        _db.ModuleAWidgets.Insert(widgetA);
        _db.ModuleBWidgets.Insert(widgetB);
        _db.SaveChanges();

        var resultA = _db.ModuleAWidgets.FindById(1);
        var resultB = _db.ModuleBWidgets.FindById(1);

        Assert.NotNull(resultA);
        Assert.NotNull(resultB);
        Assert.Equal("size", resultA.Label.Key);
        Assert.Equal("large", resultA.Label.Value);
        Assert.Equal("category-1", resultB.Category.Name);
        Assert.Equal(10, resultB.Category.Priority);
    }

    /// <summary>
    /// Verifies the GetMapperName collision-resistance fix: types whose names differ only by
    /// where underscores vs dots appear must map to distinct mapper class names.
    /// Before the fix: "Module_A.Gadget" and "Module.A_Gadget" both produced "Module_A_GadgetMapper".
    /// After the fix:  "Module_A.Gadget" → "Module__A_GadgetMapper"
    ///                 "Module.A_Gadget" → "Module_A__GadgetMapper"
    /// </summary>
    [Fact]
    public void Underscore_In_TypeName_Does_Not_Cause_Mapper_Name_Collision()
    {
        var gadget1 = new Module_A.Gadget { Id = 1, Model = "Gadget-1" };
        var gadget2 = new Module.A_Gadget { Id = 1, Variant = "Variant-1" };

        _db.Module_AGadgets.Insert(gadget1);
        _db.ModuleAGadgets.Insert(gadget2);
        _db.SaveChanges();

        var result1 = _db.Module_AGadgets.FindById(1);
        var result2 = _db.ModuleAGadgets.FindById(1);

        Assert.NotNull(result1);
        Assert.NotNull(result2);
        Assert.Equal("Gadget-1", result1.Model);
        Assert.Equal("Variant-1", result2.Variant);
    }
}
