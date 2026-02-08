using System.Linq.Expressions;
using DocumentDb.Bson;

namespace DocumentDb.Core.Indexing;

/// <summary>
/// High-level metadata and configuration for a custom index on a document collection.
/// Wraps low-level IndexOptions and provides strongly-typed expression-based key extraction.
/// </summary>
/// <typeparam name="T">Document type</typeparam>
public sealed class CollectionIndexDefinition<T> where T : class
{
    /// <summary>
    /// Unique name for this index (auto-generated or user-specified)
    /// </summary>
    public string Name { get; }
    
    /// <summary>
    /// Property paths that make up this index key.
    /// Examples: ["Age"] for simple index, ["City", "Age"] for compound index
    /// </summary>
    public string[] PropertyPaths { get; }
    
    /// <summary>
    /// If true, enforces uniqueness constraint on the indexed values
    /// </summary>
    public bool IsUnique { get; }
    
    /// <summary>
    /// Type of index structure (from existing IndexType enum)
    /// </summary>
    public IndexType Type { get; }
    
    /// <summary>
    /// Compiled function to extract the index key from a document.
    /// Compiled for maximum performance (10-100x faster than interpreting Expression).
    /// </summary>
    public Func<T, object> KeySelector { get; }
    
    /// <summary>
    /// Original expression for the key selector (for analysis and serialization)
    /// </summary>
    public Expression<Func<T, object>> KeySelectorExpression { get; }
    
    /// <summary>
    /// If true, this is the primary key index (_id)
    /// </summary>
    public bool IsPrimary { get; }

    /// <summary>
    /// Creates a new index definition
    /// </summary>
    /// <param name="name">Index name</param>
    /// <param name="propertyPaths">Property paths for the index</param>
    /// <param name="keySelectorExpression">Expression to extract key from document</param>
    /// <param name="isUnique">Enforce uniqueness</param>
    /// <param name="type">Index structure type (BTree or Hash)</param>
    /// <param name="isPrimary">Is this the primary key index</param>
    public CollectionIndexDefinition(
        string name,
        string[] propertyPaths,
        Expression<Func<T, object>> keySelectorExpression,
        bool isUnique = false,
        IndexType type = IndexType.BTree,
        bool isPrimary = false)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Index name cannot be empty", nameof(name));
        
        if (propertyPaths == null || propertyPaths.Length == 0)
            throw new ArgumentException("Property paths cannot be empty", nameof(propertyPaths));
        
        Name = name;
        PropertyPaths = propertyPaths;
        KeySelectorExpression = keySelectorExpression ?? throw new ArgumentNullException(nameof(keySelectorExpression));
        KeySelector = keySelectorExpression.Compile(); // Compile for performance
        IsUnique = isUnique;
        Type = type;
        IsPrimary = isPrimary;
    }
    
    /// <summary>
    /// Converts this high-level definition to low-level IndexOptions for BTreeIndex
    /// </summary>
    public IndexOptions ToIndexOptions()
    {
        return new IndexOptions
        {
            Type = Type,
            Unique = IsUnique,
            Fields = PropertyPaths
        };
    }
    
    /// <summary>
    /// Checks if this index can be used for a query on the specified property path
    /// </summary>
    public bool CanSupportQuery(string propertyPath)
    {
        // Simple index: exact match required
        if (PropertyPaths.Length == 1)
            return PropertyPaths[0].Equals(propertyPath, StringComparison.OrdinalIgnoreCase);
        
        // Compound index: can support if queried property is the first component
        // e.g., index on ["City", "Age"] can support query on "City" but not just "Age"
        return PropertyPaths[0].Equals(propertyPath, StringComparison.OrdinalIgnoreCase);
    }
    
    /// <summary>
    /// Checks if this index can support queries on multiple properties (compound queries)
    /// </summary>
    public bool CanSupportCompoundQuery(string[] propertyPaths)
    {
        if (propertyPaths == null || propertyPaths.Length == 0)
            return false;
        
        // Check if queried paths are a prefix of this index
        // e.g., index on ["City", "Age", "Name"] can support ["City"] or ["City", "Age"]
        if (propertyPaths.Length > PropertyPaths.Length)
            return false;
        
        for (int i = 0; i < propertyPaths.Length; i++)
        {
            if (!PropertyPaths[i].Equals(propertyPaths[i], StringComparison.OrdinalIgnoreCase))
                return false;
        }
        
        return true;
    }

    public override string ToString()
    {
        var uniqueStr = IsUnique ? "Unique" : "Non-Unique";
        var paths = string.Join(", ", PropertyPaths);
        return $"{Name} ({uniqueStr} {Type} on [{paths}])";
    }
}

/// <summary>
/// Information about an existing index (for querying index metadata)
/// </summary>
public sealed class CollectionIndexInfo
{
    public string Name { get; init; } = string.Empty;
    public string[] PropertyPaths { get; init; } = Array.Empty<string>();
    public bool IsUnique { get; init; }
    public IndexType Type { get; init; }
    public bool IsPrimary { get; init; }
    public long EstimatedDocumentCount { get; init; }
    public long EstimatedSizeBytes { get; init; }

    public override string ToString()
    {
        return $"{Name}: {string.Join(", ", PropertyPaths)} ({EstimatedDocumentCount} docs, {EstimatedSizeBytes:N0} bytes)";
    }
}
