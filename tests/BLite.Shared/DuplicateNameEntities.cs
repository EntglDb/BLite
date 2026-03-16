using BLite.Bson;
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
