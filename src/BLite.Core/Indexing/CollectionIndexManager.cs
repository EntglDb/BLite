using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace BLite.Core.Indexing;

/// <summary>
/// Manages a collection of secondary indexes on a document collection.
/// Handles index creation, deletion, automatic selection, and maintenance.
/// </summary>
/// <typeparam name="TId">Primary key type</typeparam>
/// <typeparam name="T">Document type</typeparam>
public sealed class CollectionIndexManager<TId, T> : IDisposable where T : class
{
    private readonly Dictionary<string, CollectionSecondaryIndex<TId, T>> _indexes;
    private readonly StorageEngine _storage;
    private readonly IDocumentMapper<TId, T> _mapper;
    private readonly object _lock = new();
    private bool _disposed;
    private readonly string _collectionName;
    private CollectionMetadata _metadata;

    public CollectionIndexManager(StorageEngine storage, IDocumentMapper<TId, T> mapper, string? collectionName = null)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        _collectionName = collectionName ?? _mapper.CollectionName;
        _indexes = new Dictionary<string, CollectionSecondaryIndex<TId, T>>(StringComparer.OrdinalIgnoreCase);
        
        // Load existing metadata via storage
        _metadata = _storage.GetCollectionMetadata(_collectionName) ?? new CollectionMetadata { Name = _collectionName };
        
        // Initialize indexes from metadata
        foreach (var idxMeta in _metadata.Indexes)
        {
            var definition = RebuildDefinition(idxMeta.Name, idxMeta.PropertyPaths, idxMeta.IsUnique, idxMeta.Type, idxMeta.Dimensions, idxMeta.Metric);
            var index = new CollectionSecondaryIndex<TId, T>(definition, _storage, _mapper, idxMeta.RootPageId);
            _indexes[idxMeta.Name] = index;
        }
    }

    private void UpdateMetadata()
    {
        _metadata.Indexes.Clear();
        foreach (var index in _indexes.Values)
        {
            var info = index.GetInfo();
            _metadata.Indexes.Add(new IndexMetadata
            {
                Name = info.Name,
                IsUnique = info.IsUnique,
                Type = info.Type,
                PropertyPaths = info.PropertyPaths,
                Dimensions = index.Definition.Dimensions,
                Metric = index.Definition.Metric,
                RootPageId = index.RootPageId
            });
        }
    }

    /// <summary>
    /// Creates a new secondary index
    /// </summary>
    /// <param name="definition">Index definition</param>
    /// <returns>The created secondary index</returns>
    public CollectionSecondaryIndex<TId, T> CreateIndex(CollectionIndexDefinition<T> definition)
    {
        if (definition == null)
            throw new ArgumentNullException(nameof(definition));

        lock (_lock)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CollectionIndexManager<TId, T>));

            // Check if index with this name already exists
            if (_indexes.ContainsKey(definition.Name))
                throw new InvalidOperationException($"Index '{definition.Name}' already exists");

            // Create secondary index
            var secondaryIndex = new CollectionSecondaryIndex<TId, T>(definition, _storage, _mapper);
            _indexes[definition.Name] = secondaryIndex;
            
            // Persist metadata
            UpdateMetadata();
            _storage.SaveCollectionMetadata(_metadata);

            return secondaryIndex;
        }
    }

    // ... methods ...

    /// <summary>
    /// Creates a simple index on a single property
    /// </summary>
    /// <typeparam name="TKey">Key type</typeparam>
    /// <param name="keySelector">Expression to extract key from document</param>
    /// <param name="name">Optional index name (auto-generated if null)</param>
    /// <param name="unique">Enforce uniqueness constraint</param>
    /// <returns>The created secondary index</returns>
    public CollectionSecondaryIndex<TId, T> CreateIndex<TKey>(
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

    public CollectionSecondaryIndex<TId, T> CreateVectorIndex<TKey>(Expression<Func<T, TKey>> keySelector, int dimensions, VectorMetric metric = VectorMetric.Cosine, string? name = null)
    {
        var propertyPaths = ExpressionAnalyzer.ExtractPropertyPaths(keySelector);
        var indexName = name ?? GenerateIndexName(propertyPaths);

        lock (_lock)
        {
            if (_indexes.TryGetValue(indexName, out var existing))
                return existing;

            var param = Expression.Parameter(typeof(T), "u");
            var body = Expression.Convert(keySelector.Body, typeof(object));
            var lambda = Expression.Lambda<Func<T, object>>(body, param);

            var definition = new CollectionIndexDefinition<T>(indexName, propertyPaths, lambda, false, IndexType.Vector, false, dimensions, metric);
            return CreateIndex(definition);
        }
    }

    public CollectionSecondaryIndex<TId, T> EnsureIndex(
        Expression<Func<T, object>> keySelector,
        string? name = null,
        bool unique = false)
    {
        var propertyPaths = ExpressionAnalyzer.ExtractPropertyPaths(keySelector);
        name ??= GenerateIndexName(propertyPaths);

        lock (_lock)
        {
            if (_indexes.TryGetValue(name, out var existing))
                return existing;

            return CreateIndex(keySelector, name, unique);
        }
    }

    internal CollectionSecondaryIndex<TId, T> EnsureIndexUntyped(
        LambdaExpression keySelector,
        string? name = null,
        bool unique = false)
    {
        // Convert LambdaExpression to Expression<Func<T, object>> properly by sharing parameters
        var body = keySelector.Body;
        if (body.Type != typeof(object))
        {
            body = Expression.Convert(body, typeof(object));
        }
        
        var lambda = Expression.Lambda<Func<T, object>>(body, keySelector.Parameters);

        return EnsureIndex(lambda, name, unique);
    }

    public CollectionSecondaryIndex<TId, T> CreateVectorIndexUntyped(
        LambdaExpression keySelector,
        int dimensions,
        VectorMetric metric = VectorMetric.Cosine,
        string? name = null)
    {
        var propertyPaths = ExpressionAnalyzer.ExtractPropertyPaths(keySelector);
        var indexName = name ?? GenerateIndexName(propertyPaths);

        lock (_lock)
        {
            if (_indexes.TryGetValue(indexName, out var existing))
                return existing;

            var body = keySelector.Body;
            if (body.Type != typeof(object))
            {
                body = Expression.Convert(body, typeof(object));
            }

            var lambda = Expression.Lambda<Func<T, object>>(body, keySelector.Parameters);

            var definition = new CollectionIndexDefinition<T>(indexName, propertyPaths, lambda, false, IndexType.Vector, false, dimensions, metric);
            return CreateIndex(definition);
        }
    }

    public CollectionSecondaryIndex<TId, T> CreateSpatialIndexUntyped(
        LambdaExpression keySelector,
        string? name = null)
    {
        var propertyPaths = ExpressionAnalyzer.ExtractPropertyPaths(keySelector);
        var indexName = name ?? GenerateIndexName(propertyPaths);

        lock (_lock)
        {
            if (_indexes.TryGetValue(indexName, out var existing))
                return existing;

            var body = keySelector.Body;
            if (body.Type != typeof(object))
            {
                body = Expression.Convert(body, typeof(object));
            }
            
            var lambda = Expression.Lambda<Func<T, object>>(body, keySelector.Parameters);

            var definition = new CollectionIndexDefinition<T>(indexName, propertyPaths, lambda, false, IndexType.Spatial);
            return CreateIndex(definition);
        }
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
                
                SaveMetadata(); // Save metadata after dropping index
                return true;
            }

            return false;
        }
    }

    /// <summary>
    /// Gets an index by name
    /// </summary>
    public CollectionSecondaryIndex<TId, T>? GetIndex(string name)
    {
        lock (_lock)
        {
            return _indexes.TryGetValue(name, out var index) ? index : null;
        }
    }

    /// <summary>
    /// Gets all indexes
    /// </summary>
    public IEnumerable<CollectionSecondaryIndex<TId, T>> GetAllIndexes()
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
    public CollectionSecondaryIndex<TId, T>? FindBestIndex(string propertyPath)
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
    public CollectionSecondaryIndex<TId, T>? FindBestCompoundIndex(string[] propertyPaths)
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
    /// <param name="document">Document to insert</param>
    /// <param name="location">Physical location of the document</param>
    /// <param name="transaction">Transaction context</param>
    public void InsertIntoAll(T document, DocumentLocation location, ITransaction transaction)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));

        lock (_lock)
        {
            foreach (var index in _indexes.Values)
            {
                index.Insert(document, location, transaction);
            }
        }
    }

    /// <summary>
    /// Updates a document in all indexes
    /// </summary>
    /// <param name="oldDocument">Old version of document</param>
    /// <param name="newDocument">New version of document</param>
    /// <param name="oldLocation">Physical location of old document</param>
    /// <param name="newLocation">Physical location of new document</param>
    /// <param name="transaction">Transaction context</param>
    public void UpdateInAll(T oldDocument, T newDocument, DocumentLocation oldLocation, DocumentLocation newLocation, ITransaction transaction)
    {
        if (oldDocument == null)
            throw new ArgumentNullException(nameof(oldDocument));
        if (newDocument == null)
            throw new ArgumentNullException(nameof(newDocument));

        lock (_lock)
        {
            foreach (var index in _indexes.Values)
            {
                index.Update(oldDocument, newDocument, oldLocation, newLocation, transaction);
            }
        }
    }

    /// <summary>
    /// Deletes a document from all indexes
    /// </summary>
    /// <param name="document">Document to delete</param>
    /// <param name="location">Physical location of the document</param>
    /// <param name="transaction">Transaction context</param>
    public void DeleteFromAll(T document, DocumentLocation location, ITransaction transaction)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));

        lock (_lock)
        {
            foreach (var index in _indexes.Values)
            {
                index.Delete(document, location, transaction);
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

    private CollectionIndexDefinition<T> RebuildDefinition(string name, string[] paths, bool isUnique, IndexType type, int dimensions = 0, VectorMetric metric = VectorMetric.Cosine)
    {
        var param = Expression.Parameter(typeof(T), "u");
        Expression body;
        
        if (paths.Length == 1)
        {
            body = Expression.PropertyOrField(param, paths[0]);
        }
        else
        {
            body = Expression.NewArrayInit(typeof(object), 
                paths.Select(p => Expression.Convert(Expression.PropertyOrField(param, p), typeof(object))));
        }
        
        var objectBody = Expression.Convert(body, typeof(object));
        var lambda = Expression.Lambda<Func<T, object>>(objectBody, param);
        
        return new CollectionIndexDefinition<T>(name, paths, lambda, isUnique, type, false, dimensions, metric);
    }

    public uint PrimaryRootPageId => _metadata.PrimaryRootPageId;

    public void SetPrimaryRootPageId(uint pageId)
    {
        lock (_lock)
        {
            if (_metadata.PrimaryRootPageId != pageId)
            {
                _metadata.PrimaryRootPageId = pageId;
                _storage.SaveCollectionMetadata(_metadata);
            }
        }
    }

    public CollectionMetadata GetMetadata() => _metadata;

    private void SaveMetadata()
    {
        UpdateMetadata();
        _storage.SaveCollectionMetadata(_metadata);
    }

    public void Dispose()
    {
        if (_disposed)
            return;
            
        // No auto-save on dispose to avoid unnecessary I/O if no changes
        
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
    public static string[] ExtractPropertyPaths(LambdaExpression expression)
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
        else if (expression.Body is UnaryExpression { NodeType: ExpressionType.Convert } unaryExpr)
        {
            // Handle Convert(Member) or Convert(New)
            if (unaryExpr.Operand is MemberExpression innerMember)
            {
                // Wrapped property: p => (object)p.Age
                return new[] { innerMember.Member.Name };
            }
            else if (unaryExpr.Operand is NewExpression innerNew)
            {
                 // Wrapped anonymous type: p => (object)new { p.City, p.Age }
                 return innerNew.Arguments
                    .OfType<MemberExpression>()
                    .Select(m => m.Member.Name)
                    .ToArray();
            }
        }

        throw new ArgumentException(
            "Expression must be a property accessor (p => p.Property) or anonymous type (p => new { p.Prop1, p.Prop2 })",
            nameof(expression));
    }
}
