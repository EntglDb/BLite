using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Core.Indexing.Internal;
using System;
using System.Linq;
using System.Collections.Generic;

namespace BLite.Core.Indexing;

/// <summary>
/// Represents a secondary (non-primary) index on a document collection.
/// Provides a high-level, strongly-typed wrapper around the low-level BTreeIndex.
/// Handles automatic key extraction from documents using compiled expressions.
/// </summary>
/// <typeparam name="TId">Primary key type</typeparam>
/// <typeparam name="T">Document type</typeparam>
public sealed class CollectionSecondaryIndex<TId, T> : IDisposable where T : class
{
    private readonly CollectionIndexDefinition<T> _definition;
    private readonly BTreeIndex? _btreeIndex;
    private readonly VectorSearchIndex? _vectorIndex;
    private readonly RTreeIndex? _spatialIndex;
    private readonly IDocumentMapper<TId, T> _mapper;
    private bool _disposed;

    /// <summary>
    /// Gets the index definition
    /// </summary>
    public CollectionIndexDefinition<T> Definition => _definition;
    
    /// <summary>
    /// Gets the underlying BTree index (for advanced scenarios)
    /// </summary>
    public BTreeIndex? BTreeIndex => _btreeIndex;

    public uint RootPageId => _btreeIndex?.RootPageId ?? _vectorIndex?.RootPageId ?? _spatialIndex?.RootPageId ?? 0;

    public CollectionSecondaryIndex(
        CollectionIndexDefinition<T> definition,
        StorageEngine storage,
        IDocumentMapper<TId, T> mapper,
        uint rootPageId = 0)
    {
        _definition = definition ?? throw new ArgumentNullException(nameof(definition));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        
        var indexOptions = definition.ToIndexOptions();
        
        if (indexOptions.Type == IndexType.Vector)
        {
            _vectorIndex = new VectorSearchIndex(storage, indexOptions, rootPageId);
            _btreeIndex = null;
            _spatialIndex = null;
        }
        else if (indexOptions.Type == IndexType.Spatial)
        {
            _spatialIndex = new RTreeIndex(storage, indexOptions, rootPageId);
            _btreeIndex = null;
            _vectorIndex = null;
        }
        else
        {
            _btreeIndex = new BTreeIndex(storage, indexOptions, rootPageId);
            _vectorIndex = null;
            _spatialIndex = null;
        }
    }

    /// <summary>
    /// Inserts a document into this index
    /// </summary>
    /// <param name="document">Document to index</param>
    /// <param name="location">Physical location of the document</param>
    /// <param name="transaction">Optional transaction</param>
    public void Insert(T document, DocumentLocation location, ITransaction transaction)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));
        
        // Extract key using compiled selector (fast!)
        var keyValue = _definition.KeySelector(document);
        if (keyValue == null)
            return; // Skip null keys
        
        if (_vectorIndex != null)
        {
            // Vector Index Support
            if (keyValue is float[] singleVector)
            {
                _vectorIndex.Insert(singleVector, location, transaction);
            }
            else if (keyValue is IEnumerable<float[]> vectors)
            {
                foreach (var v in vectors)
                {
                    _vectorIndex.Insert(v, location, transaction);
                }
            }
        }
        else if (_spatialIndex != null)
        {
            // Geospatial Index Support
            if (keyValue is ValueTuple<double, double> t)
            {
                _spatialIndex.Insert(GeoBox.FromPoint(new GeoPoint(t.Item1, t.Item2)), location, transaction);
            }
        }
        else if (_btreeIndex != null)
        {
            // BTree Index logic
            var userKey = ConvertToIndexKey(keyValue);
            var documentId = _mapper.GetId(document);
            var compositeKey = CreateCompositeKey(userKey, _mapper.ToIndexKey(documentId));
            _btreeIndex.Insert(compositeKey, location, transaction?.TransactionId);
        }
    }

    /// <summary>
    /// Updates a document in this index (delete old, insert new).
    /// Only updates if the indexed key has changed.
    /// </summary>
    /// <param name="oldDocument">Old version of document</param>
    /// <param name="newDocument">New version of document</param>
    /// <param name="oldLocation">Physical location of old document</param>
    /// <param name="newLocation">Physical location of new document</param>
    /// <param name="transaction">Optional transaction</param>
    public void Update(T oldDocument, T newDocument, DocumentLocation oldLocation, DocumentLocation newLocation, ITransaction transaction)
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
            var oldUserKey = ConvertToIndexKey(oldKey);
            var oldCompositeKey = CreateCompositeKey(oldUserKey, _mapper.ToIndexKey(documentId));
            _btreeIndex?.Delete(oldCompositeKey, oldLocation, transaction?.TransactionId);
        }
        
        // Insert new entry if it has a key
        if (newKey != null)
        {
            var newUserKey = ConvertToIndexKey(newKey);
            var newCompositeKey = CreateCompositeKey(newUserKey, _mapper.ToIndexKey(documentId));
            _btreeIndex?.Insert(newCompositeKey, newLocation, transaction?.TransactionId);
        }
    }

    /// <summary>
    /// Deletes a document from this index
    /// </summary>
    /// <param name="document">Document to remove from index</param>
    /// <param name="location">Physical location of the document</param>
    /// <param name="transaction">Optional transaction</param>
    public void Delete(T document, DocumentLocation location, ITransaction transaction)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));
        
        // Extract key
        var keyValue = _definition.KeySelector(document);
        if (keyValue == null)
            return; // Nothing to delete
        
        var userKey = ConvertToIndexKey(keyValue);
        var documentId = _mapper.GetId(document);
        
        // Create composite key and delete
        var compositeKey = CreateCompositeKey(userKey, _mapper.ToIndexKey(documentId));
        _btreeIndex?.Delete(compositeKey, location, transaction?.TransactionId);
    }

    /// <summary>
    /// Seeks a single document by exact key match (O(log n))
    /// </summary>
    /// <param name="key">Key value to seek</param>
    /// <param name="transaction">Optional transaction to read uncommitted changes</param>
    /// <returns>Document location if found, null otherwise</returns>
    public DocumentLocation? Seek(object key, ITransaction? transaction = null)
    {
        if (key == null)
            return null;
        
        if (_vectorIndex != null && key is float[] query)
        {
            return _vectorIndex.Search(query, 1, transaction: transaction).FirstOrDefault().Location;
        }

        if (_btreeIndex != null)
        {
            var userKey = ConvertToIndexKey(key);
            var minComposite = CreateCompositeKeyBoundary(userKey, useMinObjectId: true);
            var maxComposite = CreateCompositeKeyBoundary(userKey, useMinObjectId: false);
            var firstEntry = _btreeIndex.Range(minComposite, maxComposite, IndexDirection.Forward, transaction?.TransactionId).FirstOrDefault();
            return firstEntry.Location.PageId == 0 ? null : (DocumentLocation?)firstEntry.Location;
        }
        
        return null;
    }

    public IEnumerable<VectorSearchResult> VectorSearch(float[] query, int k, int efSearch = 100, ITransaction? transaction = null)
    {
        if (_vectorIndex == null)
            throw new InvalidOperationException("This index is not a vector index.");
            
        return _vectorIndex.Search(query, k, efSearch, transaction);
    }

    /// <summary>
    /// Performs geospatial distance search
    /// </summary>
    public IEnumerable<DocumentLocation> Near((double Latitude, double Longitude) center, double radiusKm, ITransaction? transaction = null)
    {
        if (_spatialIndex == null)
            throw new InvalidOperationException("This index is not a spatial index.");

        var queryBox = SpatialMath.BoundingBox(center.Latitude, center.Longitude, radiusKm);
        foreach (var loc in _spatialIndex.Search(queryBox, transaction))
        {
            yield return loc;
        }
    }

    /// <summary>
    /// Performs geospatial bounding box search
    /// </summary>
    public IEnumerable<DocumentLocation> Within((double Latitude, double Longitude) min, (double Latitude, double Longitude) max, ITransaction? transaction = null)
    {
        if (_spatialIndex == null)
            throw new InvalidOperationException("This index is not a spatial index.");

        var area = new GeoBox(min.Latitude, min.Longitude, max.Latitude, max.Longitude);
        return _spatialIndex.Search(area, transaction);
    }

    /// <summary>
    /// Scans a range of keys (O(log n + k) where k is result count)
    /// </summary>
    /// <param name="minKey">Minimum key (inclusive), null for unbounded</param>
    /// <param name="maxKey">Maximum key (inclusive), null for unbounded</param>
    /// <param name="transaction">Optional transaction to read uncommitted changes</param>
    /// <returns>Enumerable of document locations in key order</returns>
    public IEnumerable<DocumentLocation> Range(object? minKey, object? maxKey, IndexDirection direction = IndexDirection.Forward, ITransaction? transaction = null)
    {
        if (_btreeIndex == null) yield break;
        
        // Handle unbounded ranges
        IndexKey actualMinKey;
        IndexKey actualMaxKey;
        
        if (minKey == null && maxKey == null)
        {
            // Full scan - use extreme values
            actualMinKey = new IndexKey(new byte[0]); // Empty = smallest
            actualMaxKey = new IndexKey(Enumerable.Repeat((byte)0xFF, 255).ToArray()); // Max bytes
        }
        else if (minKey == null)
        {
            actualMinKey = new IndexKey(new byte[0]);
            var userMaxKey = ConvertToIndexKey(maxKey!);
            actualMaxKey = CreateCompositeKeyBoundary(userMaxKey, useMinObjectId: false); // Max boundary
        }
        else if (maxKey == null)
        {
            var userMinKey = ConvertToIndexKey(minKey);
            actualMinKey = CreateCompositeKeyBoundary(userMinKey, useMinObjectId: true); // Min boundary
            actualMaxKey = new IndexKey(Enumerable.Repeat((byte)0xFF, 255).ToArray());
        }
        else
        {
            // Both bounds specified
            var userMinKey = ConvertToIndexKey(minKey);
            var userMaxKey = ConvertToIndexKey(maxKey);
            
            // Create composite boundaries:
            // Min: (userMinKey, ObjectId.Empty) - captures all docs with key >= userMinKey
            // Max: (userMaxKey, ObjectId.MaxValue) - captures all docs with key <= userMaxKey
            actualMinKey = CreateCompositeKeyBoundary(userMinKey, useMinObjectId: true);
            actualMaxKey = CreateCompositeKeyBoundary(userMaxKey, useMinObjectId: false);
        }
        
        // Use BTreeIndex.Range with WAL-aware reads and direction
        // Extract DocumentLocation from each entry
        foreach (var entry in _btreeIndex.Range(actualMinKey, actualMaxKey, direction, transaction?.TransactionId))
        {
            yield return entry.Location;
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

    #region Composite Key Support (SQLite-style for Duplicate Keys)

    /// <summary>
    /// Creates a composite key by concatenating user key with document ID.
    /// This allows duplicate user keys while maintaining BTree uniqueness.
    /// Format: [UserKeyBytes] + [DocumentIdKey]
    /// </summary>
    private IndexKey CreateCompositeKey(IndexKey userKey, IndexKey documentIdKey)
    {
        // Allocate buffer: user key + document ID key length
        var compositeBytes = new byte[userKey.Data.Length + documentIdKey.Data.Length];
        
        // Copy user key
        userKey.Data.CopyTo(compositeBytes.AsSpan(0, userKey.Data.Length));
        
        // Append document ID key
        documentIdKey.Data.CopyTo(compositeBytes.AsSpan(userKey.Data.Length));
        
        return new IndexKey(compositeBytes);
    }

    /// <summary>
    /// Creates a composite key for range query boundary.
    /// Uses MIN or MAX ID representation to capture all documents with the user key.
    /// </summary>
    private IndexKey CreateCompositeKeyBoundary(IndexKey userKey, bool useMinObjectId)
    {
        // For range boundaries, we use an empty key for Min and a very large key for Max 
        // to wrap around all possible IDs for this user key.
        IndexKey idBoundary = useMinObjectId 
            ? new IndexKey(Array.Empty<byte>()) 
            : new IndexKey(Enumerable.Repeat((byte)0xFF, 16).ToArray()); // Using 16 as a safe max for GUID/ObjectId
            
        return CreateCompositeKey(userKey, idBoundary);
    }

    /// <summary>
    /// Extracts the original user key from a composite key by removing the ObjectId suffix.
    /// Used when we need to return the original indexed value.
    /// </summary>
    private IndexKey ExtractUserKey(IndexKey compositeKey)
    {
        // Composite key = UserKey + ObjectId(12 bytes)
        var userKeyLength = compositeKey.Data.Length - 12;
        if (userKeyLength <= 0)
            return compositeKey; // Fallback for malformed keys
        
        var userKeyBytes = compositeKey.Data.Slice(0, userKeyLength);
        return new IndexKey(userKeyBytes);
    }

    #endregion

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
