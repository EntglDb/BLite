namespace BLite.SourceGenerators.Helpers
{
    /// <summary>
    /// Centralises every convention string/number used across the source-generator
    /// so that a single change here propagates everywhere.
    /// </summary>
    internal static class BLiteConventions
    {
        // ── DbContext discovery ────────────────────────────────────────────────
        /// <summary>Base class name that marks a user class as a BLite DbContext.</summary>
        public const string DocumentDbContextBaseName = "DocumentDbContext";
        /// <summary>Suffix expected on DbContext class names to filter candidates early.</summary>
        public const string DbContextClassSuffix = "Context";
        /// <summary>File suffix used to skip already-generated files.</summary>
        public const string GeneratedFileSuffix = ".g.cs";

        // ── Fluent model-builder API names ────────────────────────────────────
        public const string OnModelCreatingMethodName = "OnModelCreating";
        public const string EntityMethodName = "Entity";
        public const string PropertyMethodName = "Property";
        public const string HasConversionMethodName = "HasConversion";
        public const string ValueConverterBaseName = "ValueConverter";

        // ── DocumentCollection generic shape ──────────────────────────────────
        public const string DocumentCollectionTypeName = "DocumentCollection";
        public const string IDocumentCollectionTypeName = "IDocumentCollection";
        /// <summary>Expected number of type parameters: &lt;TId, TEntity&gt;.</summary>
        public const int DocumentCollectionTypeArgCount = 2;

        // ── Direct-mapper attribute ───────────────────────────────────────────
        public const string DocumentMapperAttributeName = "DocumentMapper";

        // ── BSON serialization ────────────────────────────────────────────────
        /// <summary>Reserved BSON field name for the root-entity primary key.</summary>
        public const string BsonIdFieldName = "_id";
        /// <summary>
        /// Bytes occupied by the document-size prefix in the BSON wire format.
        /// Must be subtracted from the stored size to get the payload end position.
        /// </summary>
        public const int BsonDocumentSizeOverhead = 4;

        // ── Generated code naming ─────────────────────────────────────────────
        public const string MapperClassSuffix = "Mapper";
        public const string MapperNamespaceSuffix = "Mappers";
        public const string FallbackMapperNamespace = "Mappers";
        public const string ConverterFieldName = "_idConverter";
        public const string SetterFieldPrefix = "_setter_";

        // ── Write-method name helpers ─────────────────────────────────────────
        /// <summary>Prefix shared by all BsonSpanWriter write methods.</summary>
        public const string WriteMethodPrefix = "Write";
        /// <summary>
        /// Length of <see cref="WriteMethodPrefix"/>. Used to strip the prefix
        /// when deriving the array-variant method name (e.g. "WriteString" → "WriteArrayString").
        /// </summary>
        public const int WriteMethodPrefixLength = 5;

        // ── Entity / property conventions ─────────────────────────────────────
        /// <summary>Default primary-key property name used when no [Key] attribute is present.</summary>
        public const string DefaultIdPropertyName = "Id";
        /// <summary>Suffix appended to the lower-cased entity name to derive its default collection name.</summary>
        public const string DefaultCollectionNameSuffix = "s";

        // ── C# compiler / backing-field conventions ───────────────────────────
        /// <summary>
        /// Suffix used by the C# compiler for auto-property backing fields.
        /// Full pattern: &lt;PropertyName&gt;k__BackingField
        /// </summary>
        public const string CompilerBackingFieldSuffix = ">k__BackingField";
        /// <summary>Prefix used by the DDD private backing-field convention (_propertyName).</summary>
        public const string BackingFieldPrefix = "_";

        // ── Attribute names ───────────────────────────────────────────────────
        public const string BsonIgnoreAttribute = "BsonIgnore";
        public const string JsonIgnoreAttribute = "JsonIgnore";
        public const string NotMappedAttribute = "NotMapped";
        public const string KeyAttribute = "Key";
        public const string BsonIdAttribute = "BsonId";
        public const string TableAttribute = "Table";
        public const string ColumnAttribute = "Column";
        public const string BsonPropertyAttribute = "BsonProperty";
        public const string JsonPropertyNameAttribute = "JsonPropertyName";
        public const string RequiredAttribute = "Required";
        public const string MaxLengthAttribute = "MaxLength";
        public const string MinLengthAttribute = "MinLength";
        public const string StringLengthAttribute = "StringLength";
        public const string MinimumLengthNamedArg = "MinimumLength";
        public const string RangeAttribute = "Range";
        public const string SchemaNamedArg = "Schema";
        public const string TypeNameNamedArg = "TypeName";

        // ── Recursion / chain limits ──────────────────────────────────────────
        /// <summary>Maximum depth for recursive nested-type traversal during entity analysis.</summary>
        public const int DefaultMaxNestedTypeDepth = 20;
        /// <summary>
        /// Maximum steps to walk backwards through a fluent builder chain when searching for
        /// <c>Entity&lt;T&gt;()</c> from a <c>HasConversion&lt;TC&gt;()</c> call.
        /// Realistic depth is ≤ 3 (Entity → ToCollection → Property → HasConversion);
        /// 20 is a generous safety cap against pathological or generated chains.
        /// </summary>
        public const int MaxFluentChainDepth = 20;

        // ── AutoId type names ─────────────────────────────────────────────────
        public const string AutoIdTypeInt = "int";
        public const string AutoIdTypeInt32 = "Int32";
        public const string AutoIdTypeLong = "long";
        public const string AutoIdTypeInt64 = "Int64";
    }
}
