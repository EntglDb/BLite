using System.Collections.Generic;
using System.Linq;

namespace BLite.SourceGenerators.Models
{
    public class EntityInfo
    {
        public string Name { get; set; } = "";
        public string Namespace { get; set; } = "";
        public string FullTypeName { get; set; } = "";
        public string CollectionName { get; set; } = "";
        public string? CollectionPropertyName { get; set; }
        public string? CollectionIdTypeFullName { get; set; }
        
        public PropertyInfo? IdProperty => Properties.FirstOrDefault(p => p.IsKey);
        public bool AutoId { get; set; }
        public bool HasPrivateSetters { get; set; }
        public bool HasPrivateOrNoConstructor { get; set; }
        
        /// <summary>
        /// True when this EntityInfo represents a nested type (not a root DocumentCollection entity).
        /// In this case, even if a property has IsKey=true, the mapper is generated as a plain class
        /// and the key field is serialized using its BsonFieldName (e.g. "id") instead of "_id".
        /// </summary>
        public bool IsNestedTypeMapper { get; set; }
        
        public List<PropertyInfo> Properties { get; } = new List<PropertyInfo>();
        public Dictionary<string, NestedTypeInfo> NestedTypes { get; } = new Dictionary<string, NestedTypeInfo>();
        public HashSet<string> IgnoredProperties { get; } = new HashSet<string>();
    }

    public class PropertyInfo
    {
        public string Name { get; set; } = "";
        public string TypeName { get; set; } = "";
        public string BsonFieldName { get; set; } = "";
        public string? ColumnTypeName { get; set; }
        public bool IsNullable { get; set; }
        
        public bool HasPublicSetter { get; set; }
        public bool HasInitOnlySetter { get; set; }
        public bool HasAnySetter { get; set; }
        public bool IsReadOnlyGetter { get; set; }
        public string? BackingFieldName { get; set; }
        
        public bool IsKey { get; set; }
        public bool IsRequired { get; set; }
        public int? MaxLength { get; set; }
        public int? MinLength { get; set; }
        public double? RangeMin { get; set; }
        public double? RangeMax { get; set; }

        public bool IsCollection { get; set; }
        public bool IsArray { get; set; }
        public string? CollectionItemType { get; set; }
        public string? CollectionConcreteTypeName { get; set; }
        
        public bool IsEnum { get; set; }
        public string? EnumUnderlyingTypeName { get; set; }
        public string? EnumFullTypeName { get; set; }
        
        public bool IsNestedObject { get; set; }
        public bool IsCollectionItemNested { get; set; }
        public bool IsCollectionItemEnum { get; set; }
        public string? CollectionItemEnumUnderlyingTypeName { get; set; }
        public string? CollectionItemEnumFullTypeName { get; set; }
        public string? NestedTypeName { get; set; }
        public string? NestedTypeFullName { get; set; }
        public string? ConverterTypeName { get; set; }
        public string? ProviderTypeName { get; set; }
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
