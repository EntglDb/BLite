using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
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
    private volatile IReadOnlyList<CollectionIndexInfo>? _cachedIndexInfo;
    // Whether any index is a unique BTree index, so EnsureUniqueInAll can skip the loop.
    // Refreshed under _lock whenever _indexes changes.
    private volatile bool _hasUniqueBTreeIndex;

    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    [RequiresUnreferencedCode("Index creation uses reflection (Expression.PropertyOrField) to access type members. Ensure all entity types and their members are preserved.")]
    public CollectionIndexManager(StorageEngine storage, IDocumentMapper<TId, T> mapper, string? collectionName = null)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        _collectionName = collectionName ?? _mapper.CollectionName;
        _indexes = new Dictionary<string, CollectionSecondaryIndex<TId, T>>(StringComparer.OrdinalIgnoreCase);
        
        // Load existing metadata via storage
        _metadata = _storage.GetCollectionMetadata(_collectionName) ?? new CollectionMetadata { Name = _collectionName };

        // Refuse a collection that requires features this version cannot maintain, before anything
        // below writes to it.
        CollectionFeatures.EnsureCanOpen(_metadata, _storage.IsReadOnly);
        
        // Initialize indexes from metadata. Root page ids are fixed for the lifetime of an index
        // (a root split rewrites the root page in place), so they only need persisting when the
        // constructor allocated one (an index persisted before its root was allocated eagerly).
        bool rootAllocated = false;
        foreach (var idxMeta in _metadata.Indexes)
        {
            var indexName = idxMeta.Name;
            var definition = RebuildDefinition(idxMeta.Name, idxMeta.PropertyPaths, idxMeta.IsUnique, idxMeta.Type, idxMeta.Dimensions, idxMeta.Metric);
            var index = new CollectionSecondaryIndex<TId, T>(definition, _storage, _mapper, idxMeta.RootPageId)
            {
                KeyFormat = idxMeta.KeyFormat
            };
            _indexes[indexName] = index;
            rootAllocated |= index.RootPageId != idxMeta.RootPageId;
        }

        RefreshHasUniqueBTreeIndex();

        // Re-record the features the indexes require if a release that predates them rewrote the
        // record and dropped them (a read-only open cannot, and the indexes still protect it).
        if (rootAllocated || (!_storage.IsReadOnly && CollectionFeatures.SyncIndexFeatures(_metadata)))
            SaveMetadata();
    }

    private void RefreshHasUniqueBTreeIndex()
    {
        var any = false;
        foreach (var index in _indexes.Values)
            any |= index.IsUniqueBTree;
        _hasUniqueBTreeIndex = any;
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
                RootPageId = index.RootPageId,
                KeyFormat = index.KeyFormat
            });
        }
        CollectionFeatures.SyncIndexFeatures(_metadata);
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
            _cachedIndexInfo = null; // invalidate cache
            RefreshHasUniqueBTreeIndex();
            
            // Persist metadata
            UpdateMetadata();
            _storage.SaveCollectionMetadata(_metadata);

            return secondaryIndex;
        }
    }

    /// <summary>
    /// The name an index on <paramref name="keySelector"/> gets when none is given. Full-text
    /// indexes get their own prefix, so they can sit next to a B-Tree index on the same property.
    /// </summary>
    internal static string ResolveIndexName(LambdaExpression keySelector, string? name, IndexType type = IndexType.BTree)
    {
        if (name != null) return name;
        var propertyPaths = ExpressionAnalyzer.ExtractPropertyPaths(keySelector);
        return type == IndexType.FullText
            ? $"idx_fts_{string.Join("_", propertyPaths)}"
            : GenerateIndexName(propertyPaths);
    }

    /// <summary>
    /// Builds an index definition from an untyped key selector, as declared in the model.
    /// </summary>
    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    internal static CollectionIndexDefinition<T> BuildDefinition(
        LambdaExpression keySelector,
        string? name,
        bool unique,
        IndexType type,
        int dimensions,
        VectorMetric metric)
    {
        var propertyPaths = ExpressionAnalyzer.ExtractPropertyPaths(keySelector);
        var body = keySelector.Body.Type == typeof(object)
            ? keySelector.Body
            : Expression.Convert(keySelector.Body, typeof(object));
        var lambda = Expression.Lambda<Func<T, object>>(body, keySelector.Parameters);

        return new CollectionIndexDefinition<T>(
            name ?? GenerateIndexName(propertyPaths), propertyPaths, lambda, unique, type, false, dimensions, metric);
    }

    /// <summary>
    /// Creates an index for <paramref name="definition"/> without registering or persisting it,
    /// so it can be filled before queries or the collection metadata see it. The index owns its
    /// root page: hand it to <see cref="Register"/> or <see cref="ReplaceIndex"/>, or release its
    /// pages.
    /// </summary>
    internal CollectionSecondaryIndex<TId, T> CreateUnregistered(CollectionIndexDefinition<T> definition)
    {
        lock (_lock)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CollectionIndexManager<TId, T>));

            return new CollectionSecondaryIndex<TId, T>(definition, _storage, _mapper);
        }
    }

    /// <summary>
    /// Registers an index built by <see cref="CreateUnregistered"/> and persists it. If the
    /// metadata cannot be saved, the index is not registered and remains the caller's to release.
    /// </summary>
    internal void Register(CollectionSecondaryIndex<TId, T> index)
    {
        lock (_lock)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CollectionIndexManager<TId, T>));
            if (_indexes.ContainsKey(index.Name))
                throw new InvalidOperationException($"Index '{index.Name}' already exists");

            PublishLocked(index.Name, index, previous: null);
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
    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
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

    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
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

    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
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

    [Obsolete("Use model HasVectorIndex/HasSpatialIndex or CreateIndex(definition); will be removed in 6.0")]
    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
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

    [Obsolete("Use model HasVectorIndex/HasSpatialIndex or CreateIndex(definition); will be removed in 6.0")]
    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
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
                // Free all pages owned by this index before removing it
                index.FreeAllPages(_storage);
                index.Dispose();
                _indexes.Remove(name);
                _cachedIndexInfo = null; // invalidate cache
                RefreshHasUniqueBTreeIndex();
                
                SaveMetadata(); // Save metadata after dropping index
                return true;
            }

            return false;
        }
    }

    /// <summary>
    /// Registers <paramref name="fresh"/> under <paramref name="name"/>, persists its root page
    /// and frees the pages of the index it replaces. If the metadata cannot be saved, the old
    /// index stays registered and <paramref name="fresh"/> remains the caller's to release.
    /// </summary>
    internal void ReplaceIndex(string name, CollectionSecondaryIndex<TId, T> fresh)
    {
        lock (_lock)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CollectionIndexManager<TId, T>));
            if (!_indexes.TryGetValue(name, out var old))
                throw new InvalidOperationException($"Index '{name}' does not exist");

            // Persist the new root before freeing the old tree: a crash in between leaks the old
            // pages, whereas the reverse order would let the next open free them a second time.
            PublishLocked(name, fresh, old);
            old.FreeAllPages(_storage);
            old.Dispose();
        }
    }

    /// <summary>
    /// Registers <paramref name="index"/> under <paramref name="name"/> in place of
    /// <paramref name="previous"/> (null: none) and saves the metadata. If the save fails, the
    /// previous registration is restored, so memory keeps matching the metadata on disk, and the
    /// exception propagates. Must be called under <see cref="_lock"/>.
    /// </summary>
    private void PublishLocked(string name, CollectionSecondaryIndex<TId, T> index, CollectionSecondaryIndex<TId, T>? previous)
    {
        _indexes[name] = index;
        _cachedIndexInfo = null; // invalidate cache
        RefreshHasUniqueBTreeIndex();

        try
        {
            SaveMetadata();
        }
        catch
        {
            if (previous != null)
                _indexes[name] = previous;
            else
                _indexes.Remove(name);
            _cachedIndexInfo = null;
            RefreshHasUniqueBTreeIndex();
            UpdateMetadata();
            throw;
        }
    }

    /// <summary>Records the key format an index's pages are written with and persists it.</summary>
    internal void SetKeyFormat(string name, byte keyFormat)
    {
        lock (_lock)
        {
            if (!_indexes.TryGetValue(name, out var index))
                throw new InvalidOperationException($"Index '{name}' does not exist");

            index.KeyFormat = keyFormat;
            SaveMetadata();
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
    /// Gets information about all indexes. Result is cached and rebuilt only when indexes change.
    /// </summary>
    public IReadOnlyList<CollectionIndexInfo> GetIndexInfo()
    {
        var cached = _cachedIndexInfo;
        if (cached != null) return cached;
        return BuildIndexInfoCache();
    }

    private IReadOnlyList<CollectionIndexInfo> BuildIndexInfoCache()
    {
        lock (_lock)
        {
            // Re-check under lock in case another thread already built the cache.
            var cached = _cachedIndexInfo;
            if (cached != null) return cached;

            var result = new CollectionIndexInfo[_indexes.Count];
            int i = 0;
            foreach (var idx in _indexes.Values)
                result[i++] = idx.GetInfo();
            _cachedIndexInfo = result;
            return result;
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
    /// Throws if <paramref name="document"/> would violate any unique index.
    /// Call before any page is written, so a rejected write leaves nothing behind.
    /// On update, pass the stored version as <paramref name="oldDocument"/>: only indexes whose
    /// key changed are checked, so documents that already share a value can still be updated.
    /// </summary>
    /// <param name="document">Document about to be inserted or written by an update</param>
    /// <param name="oldDocument">Stored version of the document on update; null on insert</param>
    /// <param name="transaction">Transaction context</param>
    public void EnsureUniqueInAll(T document, T? oldDocument, ITransaction transaction)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));

        if (!_hasUniqueBTreeIndex)
            return;

        lock (_lock)
        {
            foreach (var index in _indexes.Values)
            {
                index.EnsureUnique(document, oldDocument, transaction);
            }
        }
    }

    /// <summary>
    /// Throws <see cref="BLiteValidationException"/> if <paramref name="document"/>'s key could not
    /// be written to one of the indexes (vector dimension mismatch, key too large).
    /// Call before any page is written, together with <see cref="EnsureUniqueInAll"/>.
    /// </summary>
    /// <param name="oldDocument">The stored version on update: keys equal to its keys are not checked again.</param>
    public void ValidateKeysInAll(T document, T? oldDocument = default)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));

        lock (_lock)
        {
            foreach (var index in _indexes.Values)
            {
                index.ValidateKey(document, oldDocument);
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

    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    [RequiresUnreferencedCode("Index creation uses Expression.PropertyOrField which requires type metadata to be preserved.")]
    private CollectionIndexDefinition<T> RebuildDefinition(string name, string[] paths, bool isUnique, IndexType type, int dimensions = 0, VectorMetric metric = VectorMetric.Cosine)
    {
        var param = Expression.Parameter(typeof(T), "u");
        Expression body;
        
        if (paths.Length == 1)
        {
            // Handle nested paths like "MainAddress.City.Name"
            body = BuildNestedPropertyAccess(param, paths[0]);
        }
        else
        {
            // Compound key with multiple paths
            body = Expression.NewArrayInit(typeof(object), 
                paths.Select(p => Expression.Convert(BuildNestedPropertyAccess(param, p), typeof(object))));
        }
        
        var objectBody = Expression.Convert(body, typeof(object));
        var lambda = Expression.Lambda<Func<T, object>>(objectBody, param);
        
        return new CollectionIndexDefinition<T>(name, paths, lambda, isUnique, type, false, dimensions, metric);
    }

    /// <summary>
    /// Builds nested property access expression from dot-notation path.
    /// Example: "MainAddress.City.Name" -> param.MainAddress.City.Name
    /// </summary>
    [RequiresUnreferencedCode("Builds nested property access using Expression.PropertyOrField which requires type member metadata to be preserved.")]
    private static Expression BuildNestedPropertyAccess(Expression param, string path)
    {
        var parts = path.Split('.');
        Expression current = param;
        
        foreach (var part in parts)
        {
            current = Expression.PropertyOrField(current, part);
        }
        
        return current;
    }

    public uint PrimaryRootPageId => _metadata.PrimaryRootPageId;

    public void SetPrimaryRootPageId(uint pageId)
    {
        lock (_lock)
        {
            if (_metadata.PrimaryRootPageId != pageId)
            {
                _metadata.PrimaryRootPageId = pageId;
                RefreshSequenceValue();
                _storage.SaveCollectionMetadata(_metadata);
            }
        }
    }

    public CollectionMetadata GetMetadata() => _metadata;

    private void SaveMetadata()
    {
        UpdateMetadata();
        RefreshSequenceValue();
        _storage.SaveCollectionMetadata(_metadata);
    }

    /// <summary>
    /// Reads the current SequenceValue from the page file so that this in-memory
    /// <c>_metadata</c> snapshot does not overwrite the auto-increment counter
    /// that <see cref="StorageEngine.GetNextSequence"/> maintains independently.
    /// </summary>
    private void RefreshSequenceValue()
    {
        var current = _storage.GetCollectionMetadata(_collectionName);
        if (current != null)
            _metadata.SequenceValue = current.SequenceValue;
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
            _hasUniqueBTreeIndex = false;
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
    /// Supports simple property access (p => p.Age), nested properties (p => p.Address.City.Name),
    /// and anonymous types (p => new { p.City, p.Age }).
    /// Returns full dot-notation paths for nested properties (e.g., "Address.City.Name").
    /// </summary>
    public static string[] ExtractPropertyPaths(LambdaExpression expression)
    {
        if (expression.Body is MemberExpression memberExpr)
        {
            // Simple or nested property: p => p.Age or p => p.Address.City.Name
            return new[] { ExtractFullPath(memberExpr) };
        }
        else if (expression.Body is NewExpression newExpr)
        {
            // Compound key via anonymous type: p => new { p.City, p.Address.Country }
            return newExpr.Arguments
                .OfType<MemberExpression>()
                .Select(ExtractFullPath)
                .ToArray();
        }
        else if (expression.Body is UnaryExpression { NodeType: ExpressionType.Convert } unaryExpr)
        {
            // Handle Convert(Member) or Convert(New)
            if (unaryExpr.Operand is MemberExpression innerMember)
            {
                // Wrapped property: p => (object)p.Age
                return new[] { ExtractFullPath(innerMember) };
            }
            else if (unaryExpr.Operand is NewExpression innerNew)
            {
                 // Wrapped anonymous type: p => (object)new { p.City, p.Age }
                 return innerNew.Arguments
                    .OfType<MemberExpression>()
                    .Select(ExtractFullPath)
                    .ToArray();
            }
        }

        throw new ArgumentException(
            "Expression must be a property accessor (p => p.Property) or anonymous type (p => new { p.Prop1, p.Prop2 })",
            nameof(expression));
    }

    /// <summary>
    /// Extracts full dot-notation path from a MemberExpression chain.
    /// Example: p.Address.City.Name -> "Address.City.Name"
    /// </summary>
    private static string ExtractFullPath(MemberExpression memberExpr)
    {
        var parts = new List<string>();
        Expression? current = memberExpr;

        while (current is MemberExpression member)
        {
            parts.Insert(0, member.Member.Name);
            current = member.Expression;
        }

        // Current should now be the parameter (e.g., 'p' in p => p.Name)
        // If we hit something else (e.g., a method call), that's an error
        if (current is not ParameterExpression)
        {
            throw new ArgumentException(
                $"Property path must start from the parameter, got {current?.GetType().Name}",
                nameof(memberExpr));
        }

        return string.Join(".", parts);
    }
}
