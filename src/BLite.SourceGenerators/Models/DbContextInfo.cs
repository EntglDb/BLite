using System.Collections.Generic;

namespace BLite.SourceGenerators.Models
{
    public class DbContextInfo
    {
        public string ClassName { get; set; } = "";
        public string Namespace { get; set; } = "";
        public string FilePath { get; set; } = "";
        public List<EntityInfo> Entities { get; } = new List<EntityInfo>();
        public Dictionary<string, NestedTypeInfo> GlobalNestedTypes { get; } = new Dictionary<string, NestedTypeInfo>();
    }
}
