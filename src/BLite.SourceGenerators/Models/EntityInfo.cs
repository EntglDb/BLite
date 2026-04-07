using System.Collections.Generic;
using System.Linq;
using BLite.SourceGenerators;

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

        /// <summary>
        /// True when the DbContext property is typed as <c>IDocumentCollection&lt;TId,T&gt;</c>
        /// (interface) rather than the concrete <c>DocumentCollection&lt;TId,T&gt;</c>.
        /// Controls whether a downcast is emitted in the generated <c>InitializeCollections</c>.
        /// </summary>
        public bool CollectionPropertyIsInterface { get; set; }
        
        public PropertyInfo? IdProperty => Properties.FirstOrDefault(p => p.IsKey);
        public bool AutoId { get; set; }
        public bool HasPrivateSetters { get; set; }
        public bool HasPrivateOrNoConstructor { get; set; }

        /// <summary>
        /// Null = no viable constructor; use GetUninitializedObject (field initializers won't run).
        /// Empty list = use parameterless constructor.
        /// Non-empty = use N-param constructor; each entry maps a ctor param to a property.
        /// </summary>
        public List<ConstructorParameterInfo>? SelectedConstructorParameters { get; set; }

        /// <summary>
        /// True when the selected constructor is public, allowing <c>new T(...)</c>.
        /// False = non-public; use [UnsafeAccessor(Constructor)] (NET8+) or Activator.CreateInstance (netstandard2.1).
        /// Also false for public parameterless ctors on types that have C# 11 <c>required</c> members
        /// (to avoid CS9035 in the generated <c>new T()</c> call site).
        /// </summary>
        public bool SelectedConstructorIsPublic { get; set; } = true;
        
        /// <summary>
        /// True when this EntityInfo represents a nested type (not a root DocumentCollection entity).
        /// In this case, even if a property has IsKey=true, the mapper is generated as a plain class
        /// and the key field is serialized using its BsonFieldName (e.g. "id") instead of "_id".
        /// </summary>
        public bool IsNestedTypeMapper { get; set; }
        
        public List<PropertyInfo> Properties { get; } = new List<PropertyInfo>();
        public Dictionary<string, NestedTypeInfo> NestedTypes { get; } = new Dictionary<string, NestedTypeInfo>();
        public HashSet<string> IgnoredProperties { get; } = new HashSet<string>();

        /// <summary>
        /// Index declarations parsed from <c>OnModelCreating</c> for this entity.
        /// Each entry represents one <c>HasIndex(lambda)</c> call.
        /// Used by the source generator to emit the <c>{Entity}Filter</c> class.
        /// </summary>
        public List<IndexInfo> Indexes { get; } = new List<IndexInfo>();

        public List<BLiteDiagnostic> Diagnostics { get; } = new List<BLiteDiagnostic>();
    }

    /// <summary>
    /// Metadata for a single index declaration captured from <c>HasIndex</c> in
    /// <c>OnModelCreating</c>.  The source generator uses this to decide which
    /// filter methods return an <see cref="IndexQueryPlan"/> (B-Tree seek) vs. a
    /// <see cref="BsonReaderPredicate"/> (full scan).
    /// </summary>
    public class IndexInfo
    {
        /// <summary>
        /// The CLR property name used as the B-Tree index key, as extracted from
        /// the <c>HasIndex(x =&gt; x.PropertyName)</c> lambda.
        /// For simple (single-property) indexes this is a single element.
        /// </summary>
        public List<string> PropertyPaths { get; } = new List<string>();

        /// <summary>
        /// Optional explicit index name supplied via <c>HasIndex(lambda, name: "...")</c>.
        /// <c>null</c> means the engine will auto-derive the name from the property path(s).
        /// </summary>
        public string? Name { get; set; }
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

        /// <summary>
        /// The fully-qualified name (without global:: prefix) of the type that declares this property.
        /// Used to generate [UnsafeAccessor] setters correctly when the property is inherited.
        /// </summary>
        public string DeclaringTypeName { get; set; } = "";

        /// <summary>
        /// True when this is a getter-only property backed by a conventional private field
        /// following the DDD pattern: <c>private List&lt;T&gt; _items</c> + <c>public IReadOnlyCollection&lt;T&gt; Items =&gt; _items.AsReadOnly()</c>.
        /// The <see cref="BackingFieldName"/> holds the private field name (e.g. <c>_items</c>).
        /// </summary>
        public bool HasPrivateBackingFieldAccess { get; set; }
        
        public bool IsKey { get; set; }
        public bool IsRequired { get; set; }
        /// <summary>True when the property has the C# 11 <c>required</c> keyword (not the [Required] DataAnnotations attribute).</summary>
        public bool HasCSharpRequiredKeyword { get; set; }
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

        // Dictionary<TKey, TValue> support
        public bool IsDictionary { get; set; }
        public string? DictionaryKeyType { get; set; }
        public string? DictionaryValueType { get; set; }
    }

    public class NestedTypeInfo
    {
        public string Name { get; set; } = "";
        public string Namespace { get; set; } = "";
        public string FullTypeName { get; set; } = "";
        public int Depth { get; set; }
        public bool HasPrivateOrNoConstructor { get; set; }
        public bool HasPrivateSetters { get; set; }
        public List<ConstructorParameterInfo>? SelectedConstructorParameters { get; set; }
        public bool SelectedConstructorIsPublic { get; set; } = true;
        
        public List<PropertyInfo> Properties { get; } = new List<PropertyInfo>();
        public Dictionary<string, NestedTypeInfo> NestedTypes { get; } = new Dictionary<string, NestedTypeInfo>();
    }

    /// <summary>Maps a constructor parameter to the entity property it was matched to (by name, case-insensitive).</summary>
    public sealed class ConstructorParameterInfo
    {
        public string Name { get; }
        public string TypeName { get; }
        public string MatchedPropertyName { get; }
        public bool IsNullable { get; }
        public ConstructorParameterInfo(string name, string typeName, string matchedPropertyName, bool isNullable)
        {
            Name = name; TypeName = typeName; MatchedPropertyName = matchedPropertyName; IsNullable = isNullable;
        }
    }
}
