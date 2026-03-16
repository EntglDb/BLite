using System.Collections.Generic;

// Two separate namespaces each containing a class named "Widget" and "Tag"
// to test the Source Generator's behavior with duplicate class names.

namespace BLite.Shared.ModuleA
{
    public class Widget
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public Tag Label { get; set; } = new();
    }

    public class Tag
    {
        public string Key { get; set; } = "";
        public string Value { get; set; } = "";
    }
}

namespace BLite.Shared.ModuleB
{
    public class Widget
    {
        public int Id { get; set; }
        public string Title { get; set; } = "";
        public Tag Category { get; set; } = new();
    }

    public class Tag
    {
        public string Name { get; set; } = "";
        public int Priority { get; set; }
    }
}

// Additional entities to verify the GetMapperName collision-resistance fix:
// "Module_A" namespace with class "Gadget" vs "Module" namespace with class "A_Gadget"
// Before the fix, both would map to the same mapper name "BLite_Shared_Module_A_GadgetMapper".
// After the fix (underscore escaping), they map to distinct names.

namespace BLite.Shared.Module_A
{
    public class Gadget
    {
        public int Id { get; set; }
        public string Model { get; set; } = "";
    }
}

namespace BLite.Shared.Module
{
    public class A_Gadget
    {
        public int Id { get; set; }
        public string Variant { get; set; } = "";
    }
}
