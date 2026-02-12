using System.Collections.Generic;

namespace BLite.SourceGenerators.Models
{
    public class EntityInfo
    {
        public string Name { get; set; } = "";
        public string Namespace { get; set; } = "";
        public string FullTypeName { get; set; } = "";
        public string CollectionName { get; set; } = "";
        public string? CollectionPropertyName { get; set; }
        
        public PropertyInfo? IdProperty => Properties.FirstOrDefault(p => p.IsKey);
        public bool AutoId { get; set; }
        
        public List<PropertyInfo> Properties { get; } = new List<PropertyInfo>();
        public Dictionary<string, NestedTypeInfo> NestedTypes { get; } = new Dictionary<string, NestedTypeInfo>();
        public HashSet<string> IgnoredProperties { get; } = new HashSet<string>();
    }

    public class PropertyInfo
    {
        public string Name { get; set; } = "";
        public string TypeName { get; set; } = "";
        public string BsonFieldName { get; set; } = "";
        public bool IsNullable { get; set; }
        
        public bool HasPublicSetter { get; set; }
        public bool HasInitOnlySetter { get; set; }
        public string? BackingFieldName { get; set; }
        
        public bool IsKey { get; set; }
        public bool IsCollection { get; set; }
        public bool IsArray { get; set; }
        public string? CollectionItemType { get; set; }
        
        public bool IsNestedObject { get; set; }
        public bool IsCollectionItemNested { get; set; }
        public string? NestedTypeName { get; set; }
        public string? NestedTypeFullName { get; set; }
    }

    public class NestedTypeInfo
    {
        public string Name { get; set; } = "";
        public string Namespace { get; set; } = "";
        public string FullTypeName { get; set; } = "";
        public int Depth { get; set; }
        
        public List<PropertyInfo> Properties { get; } = new List<PropertyInfo>();
        public Dictionary<string, NestedTypeInfo> NestedTypes { get; } = new Dictionary<string, NestedTypeInfo>();
    }
}
