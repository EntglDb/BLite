using DocumentDb.Bson;
using DocumentDb.Core.Collections;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Transactions;

namespace DocumentDb.Core.Indexing;

/// <summary>
/// Represents a secondary (non-primary) index on a document collection.
/// Provides a high-level, strongly-typed wrapper around the low-level BTreeIndex.
/// Handles automatic key extraction from documents using compiled expressions.
/// </summary>
/// <typeparam name="T">Document type</typeparam>
public sealed class CollectionSecondaryIndex<T> : IDisposable where T : class
{
    private readonly CollectionIndexDefinition<T> _definition;
    private readonly BTreeIndex _btreeIndex;
    private readonly IDocumentMapper<T> _mapper;
    private bool _disposed;

    /// <summary>
    /// Gets the index definition
    /// </summary>
    public CollectionIndexDefinition<T> Definition => _definition;
    
    /// <summary>
    /// Gets the underlying BTree index (for advanced scenarios)
    /// </summary>
    public BTreeIndex BTreeIndex => _btreeIndex;

    public CollectionSecondaryIndex(
        CollectionIndexDefinition<T> definition,
        PageFile pageFile,
        IDocumentMapper<T> mapper)
    {
        _definition = definition ?? throw new ArgumentNullException(nameof(definition));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        
        // Create underlying BTree index using converted options
        var indexOptions = definition.ToIndexOptions();
        _btreeIndex = new BTreeIndex(pageFile, indexOptions);
    }

    /// <summary>
    /// Inserts a document into this index
    /// </summary>
    /// <param name="document">Document to index</param>
    /// <param name="transaction">Optional transaction</param>
    public void Insert(T document, ITransaction? transaction = null)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));
        
        // Extract key using compiled selector (fast!)
        var keyValue = _definition.KeySelector(document);
        if (keyValue == null)
            return; // Skip null keys
        
        // Convert CLR object to IndexKey
        var indexKey = ConvertToIndexKey(keyValue);
        
        // Get document ID
        var documentId = _mapper.GetId(document);
        
        // Insert into underlying BTree (OLD single-value method)
        _btreeIndex.Insert(indexKey, documentId, transaction);
    }

    /// <summary>
    /// Updates a document in this index (delete old, insert new).
    /// Only updates if the indexed key has changed.
    /// </summary>
    /// <param name="oldDocument">Old version of document</param>
    /// <param name="newDocument">New version of document</param>
    /// <param name="transaction">Optional transaction</param>
    public void Update(T oldDocument, T newDocument, ITransaction? transaction = null)
    {
        if (oldDocument == null)
            throw new ArgumentNullException(nameof(oldDocument));
        if (newDocument == null)
            throw new ArgumentNullException(nameof(newDocument));
        
        // Extract keys from both versions
        var oldKey = _definition.KeySelector(oldDocument);
        var newKey = _definition.KeySelector(newDocument);
        
        // If keys are the same, no index update needed (optimization)
        if (Equals(oldKey, newKey))
            return;
        
        var documentId = _mapper.GetId(oldDocument);
        
        // Delete old entry if it had a key
        if (oldKey != null)
        {
            var oldIndexKey = ConvertToIndexKey(oldKey);
            _btreeIndex.Delete(oldIndexKey, documentId, transaction);
        }
        
        // Insert new entry if it has a key
        if (newKey != null)
        {
            var newIndexKey = ConvertToIndexKey(newKey);
            _btreeIndex.Insert(newIndexKey, documentId, transaction);
        }
    }

    /// <summary>
    /// Deletes a document from this index
    /// </summary>
    /// <param name="document">Document to remove from index</param>
    /// <param name="transaction">Optional transaction</param>
    public void Delete(T document, ITransaction? transaction = null)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));
        
        // Extract key
        var keyValue = _definition.KeySelector(document);
        if (keyValue == null)
            return; // Nothing to delete
        
        var indexKey = ConvertToIndexKey(keyValue);
        var documentId = _mapper.GetId(document);
        
        _btreeIndex.Delete(indexKey, documentId, transaction);
    }

    /// <summary>
    /// Seeks a single document by exact key match (O(log n))
    /// </summary>
    /// <param name="key">Key value to seek</param>
    /// <returns>Document ID if found, null otherwise</returns>
    public ObjectId? Seek(object key)
    {
        if (key == null)
            return null;
        
        var indexKey = ConvertToIndexKey(key);
        
        if (_btreeIndex.TryFind(indexKey, out var documentId))
            return documentId;
        
        return null;
    }

    /// <summary>
    /// Scans a range of keys (O(log n + k) where k is result count)
    /// </summary>
    /// <param name="minKey">Minimum key (inclusive), null for unbounded</param>
    /// <param name="maxKey">Maximum key (inclusive), null for unbounded</param>
    /// <returns>Enumerable of document IDs in key order</returns>
    public IEnumerable<ObjectId> Range(object? minKey, object? maxKey)
    {
        // Handle unbounded ranges
        IndexKey actualMinKey;
        IndexKey actualMaxKey;
        
        if (minKey == null && maxKey == null)
        {
            // Full scan - use extreme values
            actualMinKey = new IndexKey(new byte[0]); // Empty = smallest
            actualMaxKey = new IndexKey(new byte[255]); // Max byte array
        }
        else if (minKey == null)
        {
            actualMinKey = new IndexKey(new byte[0]);
            actualMaxKey = ConvertToIndexKey(maxKey!);
        }
        else if (maxKey == null)
        {
            actualMinKey = ConvertToIndexKey(minKey);
            actualMaxKey = new IndexKey(new byte[255]);
        }
        else
        {
            actualMinKey = ConvertToIndexKey(minKey);
            actualMaxKey = ConvertToIndexKey(maxKey);
        }
        
        // Use BTreeIndex.Range which returns IndexEntry
        foreach (var entry in _btreeIndex.Range(actualMinKey, actualMaxKey))
        {
            yield return entry.DocumentId;
        }
    }

    /// <summary>
    /// Gets statistics about this index
    /// </summary>
    public CollectionIndexInfo GetInfo()
    {
        return new CollectionIndexInfo
        {
            Name = _definition.Name,
            PropertyPaths = _definition.PropertyPaths,
            IsUnique = _definition.IsUnique,
            Type = _definition.Type,
            IsPrimary = _definition.IsPrimary,
            EstimatedDocumentCount = 0, // TODO: Track or calculate document count
            EstimatedSizeBytes = 0      // TODO: Calculate index size
        };
    }

    /// <summary>
    /// Converts a CLR value to an IndexKey for BTree storage.
    /// Supports all common .NET types.
    /// </summary>
    private IndexKey ConvertToIndexKey(object value)
    {
        return value switch
        {
            ObjectId objectId => new IndexKey(objectId),
            string str => new IndexKey(str),
            int intVal => new IndexKey(intVal),
            long longVal => new IndexKey(longVal),
            DateTime dateTime => new IndexKey(dateTime.Ticks),
            bool boolVal => new IndexKey(boolVal ? 1 : 0),
            byte[] byteArray => new IndexKey(byteArray),
            
            // For compound keys or complex types, use ToString and serialize
            // TODO: Better compound key serialization
            _ => new IndexKey(value.ToString() ?? string.Empty)
        };
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        
        // BTreeIndex doesn't currently implement IDisposable
        // Future: may need to flush buffers, close resources
        
        _disposed = true;
        GC.SuppressFinalize(this);
    }
}
