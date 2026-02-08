using System.Linq.Expressions;
using DocumentDb.Bson;
using DocumentDb.Core.Collections;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Transactions;

namespace DocumentDb.Core.Indexing;

/// <summary>
/// Manages a collection of secondary indexes on a document collection.
/// Handles index creation, deletion, automatic selection, and maintenance.
/// </summary>
/// <typeparam name="T">Document type</typeparam>
public sealed class CollectionIndexManager<T> : IDisposable where T : class
{
    private readonly Dictionary<string, CollectionSecondaryIndex<T>> _indexes;
    private readonly PageFile _pageFile;
    private readonly IDocumentMapper<T> _mapper;
    private readonly object _lock = new();
    private bool _disposed;

    public CollectionIndexManager(PageFile pageFile, IDocumentMapper<T> mapper)
    {
        _pageFile = pageFile ?? throw new ArgumentNullException(nameof(pageFile));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        _indexes = new Dictionary<string, CollectionSecondaryIndex<T>>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Creates a new secondary index
    /// </summary>
    /// <param name="definition">Index definition</param>
    /// <returns>The created secondary index</returns>
    public CollectionSecondaryIndex<T> CreateIndex(CollectionIndexDefinition<T> definition)
    {
        if (definition == null)
            throw new ArgumentNullException(nameof(definition));

        lock (_lock)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CollectionIndexManager<T>));

            // Check if index with this name already exists
            if (_indexes.ContainsKey(definition.Name))
                throw new InvalidOperationException($"Index '{definition.Name}' already exists");

            // Create secondary index
            var secondaryIndex = new CollectionSecondaryIndex<T>(definition, _pageFile, _mapper);
            _indexes[definition.Name] = secondaryIndex;

            return secondaryIndex;
        }
    }

    /// <summary>
    /// Creates a simple index on a single property
    /// </summary>
    /// <typeparam name="TKey">Key type</typeparam>
    /// <param name="keySelector">Expression to extract key from document</param>
    /// <param name="name">Optional index name (auto-generated if null)</param>
    /// <param name="unique">Enforce uniqueness constraint</param>
    /// <returns>The created secondary index</returns>
    public CollectionSecondaryIndex<T> CreateIndex<TKey>(
        Expression<Func<T, TKey>> keySelector,
        string? name = null,
        bool unique = false)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));

        // Extract property paths from expression
        var propertyPaths = ExpressionAnalyzer.ExtractPropertyPaths(keySelector);
        
        // Generate name if not provided
        name ??= GenerateIndexName(propertyPaths);

        // Convert expression to object-returning expression (required for definition)
        var objectSelector = Expression.Lambda<Func<T, object>>(
            Expression.Convert(keySelector.Body, typeof(object)),
            keySelector.Parameters);

        // Create definition
        var definition = new CollectionIndexDefinition<T>(
            name,
            propertyPaths,
            objectSelector,
            unique);

        return CreateIndex(definition);
    }

    /// <summary>
    /// Drops an existing index by name
    /// </summary>
    /// <param name="name">Index name</param>
    /// <returns>True if index was found and dropped, false otherwise</returns>
    public bool DropIndex(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Index name cannot be empty", nameof(name));

        lock (_lock)
        {
            if (_indexes.TryGetValue(name, out var index))
            {
                index.Dispose();
                _indexes.Remove(name);
                
                // TODO: Free pages used by index in PageFile
                
                return true;
            }

            return false;
        }
    }

    /// <summary>
    /// Gets an index by name
    /// </summary>
    public CollectionSecondaryIndex<T>? GetIndex(string name)
    {
        lock (_lock)
        {
            return _indexes.TryGetValue(name, out var index) ? index : null;
        }
    }

    /// <summary>
    /// Gets all indexes
    /// </summary>
    public IEnumerable<CollectionSecondaryIndex<T>> GetAllIndexes()
    {
        lock (_lock)
        {
            return _indexes.Values.ToList(); // Return copy to avoid lock issues
        }
    }

    /// <summary>
    /// Gets information about all indexes
    /// </summary>
    public IEnumerable<CollectionIndexInfo> GetIndexInfo()
    {
        lock (_lock)
        {
            return _indexes.Values.Select(idx => idx.GetInfo()).ToList();
        }
    }

    /// <summary>
    /// Finds the best index to use for a query on the specified property.
    /// Returns null if no suitable index found (requires full scan).
    /// </summary>
    /// <param name="propertyPath">Property path being queried</param>
    /// <returns>Best index for the query, or null if none suitable</returns>
    public CollectionSecondaryIndex<T>? FindBestIndex(string propertyPath)
    {
        if (string.IsNullOrWhiteSpace(propertyPath))
            return null;

        lock (_lock)
        {
            // Find all indexes that can support this query
            var candidates = _indexes.Values
                .Where(idx => idx.Definition.CanSupportQuery(propertyPath))
                .ToList();

            if (candidates.Count == 0)
                return null;

            // Simple strategy: prefer unique indexes, then shortest property path
            return candidates
                .OrderByDescending(idx => idx.Definition.IsUnique)
                .ThenBy(idx => idx.Definition.PropertyPaths.Length)
                .First();
        }
    }

    /// <summary>
    /// Finds the best index for a compound query on multiple properties
    /// </summary>
    public CollectionSecondaryIndex<T>? FindBestCompoundIndex(string[] propertyPaths)
    {
        if (propertyPaths == null || propertyPaths.Length == 0)
            return null;

        lock (_lock)
        {
            var candidates = _indexes.Values
                .Where(idx => idx.Definition.CanSupportCompoundQuery(propertyPaths))
                .ToList();

            if (candidates.Count == 0)
                return null;

            // Prefer longest matching index (more selective)
            return candidates
                .OrderByDescending(idx => idx.Definition.PropertyPaths.Length)
                .ThenByDescending(idx => idx.Definition.IsUnique)
                .First();
        }
    }

    /// <summary>
    /// Inserts a document into all indexes
    /// </summary>
    public void InsertIntoAll(T document, ITransaction? transaction = null)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));

        lock (_lock)
        {
            foreach (var index in _indexes.Values)
            {
                index.Insert(document, transaction);
            }
        }
    }

    /// <summary>
    /// Updates a document in all indexes
    /// </summary>
    public void UpdateInAll(T oldDocument, T newDocument, ITransaction? transaction = null)
    {
        if (oldDocument == null)
            throw new ArgumentNullException(nameof(oldDocument));
        if (newDocument == null)
            throw new ArgumentNullException(nameof(newDocument));

        lock (_lock)
        {
            foreach (var index in _indexes.Values)
            {
                index.Update(oldDocument, newDocument, transaction);
            }
        }
    }

    /// <summary>
    /// Deletes a document from all indexes
    /// </summary>
    public void DeleteFromAll(T document, ITransaction? transaction = null)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));

        lock (_lock)
        {
            foreach (var index in _indexes.Values)
            {
                index.Delete(document, transaction);
            }
        }
    }

    /// <summary>
    /// Generates an index name from property paths
    /// </summary>
    private static string GenerateIndexName(string[] propertyPaths)
    {
        return $"idx_{string.Join("_", propertyPaths)}";
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        lock (_lock)
        {
            foreach (var index in _indexes.Values)
            {
                try { index.Dispose(); } catch { /* Best effort */ }
            }
            
            _indexes.Clear();
            _disposed = true;
        }

        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// Helper class to analyze LINQ expressions and extract property paths
/// </summary>
public static class ExpressionAnalyzer
{
    /// <summary>
    /// Extracts property paths from a lambda expression.
    /// Supports simple property access (p => p.Age) and anonymous types (p => new { p.City, p.Age }).
    /// </summary>
    public static string[] ExtractPropertyPaths<T, TKey>(Expression<Func<T, TKey>> expression)
    {
        if (expression.Body is MemberExpression memberExpr)
        {
            // Simple property: p => p.Age
            return new[] { memberExpr.Member.Name };
        }
        else if (expression.Body is NewExpression newExpr)
        {
            // Compound key via anonymous type: p => new { p.City, p.Age }
            return newExpr.Arguments
                .OfType<MemberExpression>()
                .Select(m => m.Member.Name)
                .ToArray();
        }
        else if (expression.Body is UnaryExpression { NodeType: ExpressionType.Convert } unaryExpr
                 && unaryExpr.Operand is MemberExpression innerMember)
        {
            // Wrapped property: p => (object)p.Age
            return new[] { innerMember.Member.Name };
        }

        throw new ArgumentException(
            "Expression must be a property accessor (p => p.Property) or anonymous type (p => new { p.Prop1, p.Prop2 })",
            nameof(expression));
    }
}
