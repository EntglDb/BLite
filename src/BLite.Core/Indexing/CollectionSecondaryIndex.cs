using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Core.Indexing.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core.Indexing;

/// <summary>
/// Represents a secondary (non-primary) index on a document collection.
/// Provides a high-level, strongly-typed wrapper around the low-level BTreeIndex.
/// Handles automatic key extraction from documents using compiled expressions.
/// </summary>
/// <typeparam name="TId">Primary key type</typeparam>
/// <typeparam name="T">Document type</typeparam>
public sealed class CollectionSecondaryIndex<TId, T> : IDisposable, ICollectionIndex<TId, T> where T : class
{
    // Pre-allocated boundary sentinels — single allocation at type-init time.
    // 255 bytes of 0xFF covers any composite key length used in this implementation.
    private static readonly IndexKey s_maxBoundaryKey;
    // Upper bound for the document-id part of a composite key: one byte above every id key prefix.
    // Must not be longer: [user key][0xFF..] also covers the entries of longer values that start with
    // the user key ("p70", "p71".. for "p7"), see CreateCompositeKeyBoundary.
    private static readonly IndexKey s_maxIdBoundaryKey;
    // Id types whose index key always has the same length (used by EnsureUnique).
    private static readonly bool s_fixedSizeId =
        typeof(TId) == typeof(ObjectId) || typeof(TId) == typeof(Guid) ||
        typeof(TId) == typeof(int) || typeof(TId) == typeof(long) ||
        typeof(TId) == typeof(double) || typeof(TId) == typeof(decimal);

    static CollectionSecondaryIndex()
    {
        // Fill the pre-allocated boundary buffers with 0xFF.
        var buf255 = new byte[256]; Array.Fill(buf255, (byte)0xFF);
        // Raw (un-prefixed) so it sorts above keys of every prefix, including DecimalKeyPrefix (0x03).
        s_maxBoundaryKey   = IndexKey.FromOwnedArray(buf255);
        s_maxIdBoundaryKey = IndexKey.FromOwnedArray([IndexKey.PrefixUpperBound]); // raw: above ids of every key prefix
    }

    private readonly CollectionIndexDefinition<T> _definition;
    private readonly BTreeIndex? _btreeIndex;
    private readonly VectorSearchIndex? _vectorIndex;
    private readonly RTreeIndex? _spatialIndex;
    private readonly FullTextIndex? _fullTextIndex;
    private readonly IDocumentMapper<TId, T> _mapper;
    private bool _disposed;
    private long _documentCount;

    /// <summary>
    /// Gets the index definition
    /// </summary>
    public CollectionIndexDefinition<T> Definition => _definition;
    
    /// <summary>
    /// Gets the underlying BTree index (for advanced scenarios)
    /// </summary>
    public BTreeIndex? BTreeIndex => _btreeIndex;

    public uint RootPageId => _btreeIndex?.RootPageId ?? _vectorIndex?.RootPageId ?? _spatialIndex?.RootPageId ?? _fullTextIndex?.RootPageId ?? 0;

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
        else if (indexOptions.Type == IndexType.FullText)
        {
            _fullTextIndex = new FullTextIndex(storage, indexOptions, rootPageId);
            _btreeIndex = null;
            _vectorIndex = null;
            _spatialIndex = null;
            KeyFormat = FullTextIndex.FormatVersion;
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
        
        // Extract key with null-safe traversal for embedded properties
        if (!_definition.TryGetKey(document, out var keyValue) || keyValue == null)
            return; // Skip null keys or failed traversal
        
        InsertKey(keyValue, document, location, transaction);

        System.Threading.Interlocked.Increment(ref _documentCount);
    }

    /// <summary>
    /// Updates a document in this index, for both in-place and relocating updates.
    /// If the key is unchanged and the document did not move, the existing entry is kept;
    /// if only the location changed, the entry is re-pointed to <paramref name="newLocation"/>;
    /// otherwise the old entry is removed and the new one inserted.
    /// </summary>
    /// <param name="oldDocument">Old version of document</param>
    /// <param name="newDocument">New version of document</param>
    /// <param name="oldLocation">Physical location of old document</param>
    /// <param name="newLocation">Physical location of new document (may differ from <paramref name="oldLocation"/>)</param>
    /// <param name="transaction">Optional transaction</param>
    public void Update(T oldDocument, T newDocument, DocumentLocation oldLocation, DocumentLocation newLocation, ITransaction transaction)
    {
        if (oldDocument == null)
            throw new ArgumentNullException(nameof(oldDocument));
        if (newDocument == null)
            throw new ArgumentNullException(nameof(newDocument));
        
        // Extract keys with null-safe traversal
        _definition.TryGetKey(oldDocument, out var oldKey);
        _definition.TryGetKey(newDocument, out var newKey);
        
        bool moved = oldLocation.PageId != newLocation.PageId || oldLocation.SlotIndex != newLocation.SlotIndex;

        if (KeysEqual(oldKey, newKey))
        {
            // Same key at the same location: the existing entry stays valid.
            if (!moved || oldKey == null)
                return;

            // Same key, new location: re-point the entry. An unchanged vector keeps its HNSW
            // node instead of leaving a deleted node behind; an unchanged B-Tree key is rewritten
            // in place, so a key stored before MaxKeySize was lowered (#217) is not inserted again.
            if (_btreeIndex != null
                && _btreeIndex.Relocate(CreateCompositeKey(ConvertToIndexKey(oldKey), _mapper.ToIndexKey(_mapper.GetId(oldDocument))),
                                        oldLocation, newLocation, transaction?.TransactionId))
                return;

            if (_vectorIndex == null || !TryRelocateVectors(oldKey, oldLocation, newLocation, transaction))
            {
                DeleteKey(oldKey, oldDocument, oldLocation, transaction);
                InsertKey(newKey!, newDocument, newLocation, transaction);
            }
            return;
        }
        
        // Delete old entry if it had a key
        if (oldKey != null)
            DeleteKey(oldKey, oldDocument, oldLocation, transaction);
        
        // Insert new entry if it has a key
        if (newKey != null)
            InsertKey(newKey, newDocument, newLocation, transaction);

        // Keep the count in line with Insert/Delete, which skip documents without a key.
        if (oldKey == null && newKey != null)
            System.Threading.Interlocked.Increment(ref _documentCount);
        else if (oldKey != null && newKey == null)
            System.Threading.Interlocked.Decrement(ref _documentCount);
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
        
        // Extract key with null-safe traversal
        if (!_definition.TryGetKey(document, out var keyValue) || keyValue == null)
            return; // Nothing to delete
        
        DeleteKey(keyValue, document, location, transaction);

        System.Threading.Interlocked.Decrement(ref _documentCount);
    }

    private void InsertKey(object keyValue, T document, DocumentLocation location, ITransaction? transaction)
    {
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
        else if (_fullTextIndex != null)
        {
            if (keyValue is string text)
            {
                _fullTextIndex.Insert(text, location, transaction);
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

    /// <summary>Re-points every vector of <paramref name="keyValue"/>; false if any node was missing.</summary>
    private bool TryRelocateVectors(object keyValue, DocumentLocation oldLocation, DocumentLocation newLocation, ITransaction? transaction)
    {
        if (keyValue is float[] singleVector)
            return _vectorIndex!.Relocate(singleVector, oldLocation, newLocation, transaction);

        if (keyValue is IEnumerable<float[]> vectors)
        {
            bool all = true;
            foreach (var v in vectors)
                all &= _vectorIndex!.Relocate(v, oldLocation, newLocation, transaction);
            return all;
        }

        return false;
    }

    private void DeleteKey(object keyValue, T document, DocumentLocation location, ITransaction? transaction)
    {
        if (_vectorIndex != null)
        {
            if (keyValue is float[] singleVector)
            {
                _vectorIndex.Delete(singleVector, location, transaction);
            }
            else if (keyValue is IEnumerable<float[]> vectors)
            {
                foreach (var v in vectors)
                {
                    _vectorIndex.Delete(v, location, transaction);
                }
            }
        }
        else if (_spatialIndex != null)
        {
            if (keyValue is ValueTuple<double, double> t)
            {
                _spatialIndex.Delete(GeoBox.FromPoint(new GeoPoint(t.Item1, t.Item2)), location, transaction);
            }
        }
        else if (_fullTextIndex != null)
        {
            if (keyValue is string text)
            {
                _fullTextIndex.Delete(text, location, transaction);
            }
        }
        else if (_btreeIndex != null)
        {
            var userKey = ConvertToIndexKey(keyValue);
            var documentId = _mapper.GetId(document);
            var compositeKey = CreateCompositeKey(userKey, _mapper.ToIndexKey(documentId));
            _btreeIndex.Delete(compositeKey, location, transaction?.TransactionId);
        }
    }

    /// <summary>
    /// Key equality for skipping index maintenance. Vectors compare by value: every load
    /// materializes a new array, so reference equality would rewrite the vector index on
    /// every update.
    /// </summary>
    private static bool KeysEqual(object? oldKey, object? newKey)
    {
        if (oldKey is float[] oldVector && newKey is float[] newVector)
            return oldVector.AsSpan().SequenceEqual(newVector);

        if (oldKey is IEnumerable<float[]> oldVectors && newKey is IEnumerable<float[]> newVectors)
        {
            using var oldEnum = oldVectors.GetEnumerator();
            using var newEnum = newVectors.GetEnumerator();
            while (true)
            {
                bool hasOld = oldEnum.MoveNext();
                if (hasOld != newEnum.MoveNext()) return false;
                if (!hasOld) return true;
                if (!oldEnum.Current.AsSpan().SequenceEqual(newEnum.Current)) return false;
            }
        }

        return Equals(oldKey, newKey);
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
            return _btreeIndex.TryFindFirst(minComposite, maxComposite, transaction?.TransactionId ?? 0, out var loc)
                ? loc
                : null;
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

        return _spatialIndex.SearchNear(center.Latitude, center.Longitude, radiusKm, transaction);
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
    /// Locations of the documents whose indexed text contains every term of <paramref name="query"/>.
    /// </summary>
    public IReadOnlyList<DocumentLocation> FullTextMatch(string query, ITransaction? transaction = null)
    {
        if (_fullTextIndex == null)
            throw new InvalidOperationException("This index is not a full-text index.");

        return _fullTextIndex.Match(query, transaction);
    }

    /// <summary>
    /// The <paramref name="k"/> best BM25 matches for <paramref name="query"/>, best first.
    /// </summary>
    public IReadOnlyList<(DocumentLocation Location, double Score)> FullTextSearch(string query, int k, bool matchAllTerms = true, ITransaction? transaction = null)
    {
        if (_fullTextIndex == null)
            throw new InvalidOperationException("This index is not a full-text index.");

        return _fullTextIndex.Search(query, k, matchAllTerms, transaction);
    }

    /// <summary>
    /// The BM25 matches for <paramref name="query"/>, best first, starting with the
    /// <paramref name="k"/> best and continuing past them for as long as the caller enumerates.
    /// </summary>
    internal IEnumerable<(DocumentLocation Location, double Score)> FullTextSearchExpanding(string query, int k, bool matchAllTerms, ITransaction? transaction = null)
    {
        if (_fullTextIndex == null)
            throw new InvalidOperationException("This index is not a full-text index.");

        return _fullTextIndex.SearchExpanding(query, k, matchAllTerms, transaction);
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
            actualMinKey = IndexKey.MinKey; // Empty = smallest
            actualMaxKey = s_maxBoundaryKey; // Max bytes
        }
        else if (minKey == null)
        {
            actualMinKey = IndexKey.MinKey;
            var userMaxKey = ConvertToIndexKey(maxKey!);
            actualMaxKey = CreateCompositeKeyBoundary(userMaxKey, useMinObjectId: false); // Max boundary
        }
        else if (maxKey == null)
        {
            var userMinKey = ConvertToIndexKey(minKey);
            actualMinKey = CreateCompositeKeyBoundary(userMinKey, useMinObjectId: true); // Min boundary
            actualMaxKey = s_maxBoundaryKey;
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
    /// Counts index entries that fall within the specified key range, performing a
    /// key-only B-tree leaf scan (zero data-page reads, zero per-entry allocations).
    /// </summary>
    /// <param name="minKey">Lower bound value; <c>null</c> means unbounded.</param>
    /// <param name="maxKey">Upper bound value; <c>null</c> means unbounded.</param>
    /// <param name="startInclusive">
    ///   <c>true</c> for <c>&gt;=</c> / <c>==</c>; <c>false</c> for strict <c>&gt;</c>.
    /// </param>
    /// <param name="endInclusive">
    ///   <c>true</c> for <c>&lt;=</c> / <c>==</c>; <c>false</c> for strict <c>&lt;</c>.
    /// </param>
    /// <param name="transaction">Optional transaction for read-your-own-writes.</param>
    /// <returns>Count of matching index entries.</returns>
    public int CountRange(object? minKey, object? maxKey, bool startInclusive, bool endInclusive, ITransaction? transaction = null)
    {
        if (_btreeIndex == null) return 0;

        // Unbounded defaults use the global sentinels: IndexKey.MinKey for the lower bound
        // and s_maxBoundaryKey for the upper bound.
        // When a user key is specified, CreateCompositeKeyBoundary builds the composite
        // boundary for that user key so inclusive/exclusive range semantics are preserved.
        //
        // Composite key layout: [UserKeyBytes][DocumentIdBytes].
        //   Inclusive start: lower composite boundary [userKey][MinId] — all real entries
        //     [userKey][realId] are >= this boundary, so they are included.
        //   Exclusive start: upper composite boundary [userKey][MaxId] — all real entries
        //     [userKey][realId] are < this boundary (below it), so they are excluded.
        //   Inclusive end:   upper composite boundary [userKey][MaxId] — all real entries
        //     [userKey][realId] are <= this boundary, so they are included.
        //   Exclusive end:   lower composite boundary [userKey][MinId] — all real entries
        //     [userKey][realId] are > this boundary, so they are excluded.
        var (actualMinKey, actualMaxKey) = CompositeRange(minKey, maxKey, startInclusive, endInclusive);
        return _btreeIndex.CountRange(actualMinKey, actualMaxKey, transaction?.TransactionId);
    }

    /// <summary>
    /// Hands the stored keys of the entries in the range to <paramref name="visitor"/>, in ascending
    /// order and without allocating per entry; bounds as for <see cref="CountRange"/>. Each key is
    /// <c>[user key][document id]</c>, so the indexed value is its prefix.
    /// </summary>
    internal void ScanKeys<TVisitor>(object? minKey, object? maxKey, bool startInclusive, bool endInclusive, ref TVisitor visitor, ITransaction? transaction = null)
        where TVisitor : struct, BTreeIndex.IKeyVisitor
    {
        if (_btreeIndex == null) return;

        var (actualMinKey, actualMaxKey) = CompositeRange(minKey, maxKey, startInclusive, endInclusive);
        _btreeIndex.ScanRange(actualMinKey, actualMaxKey, transaction?.TransactionId, ref visitor);
    }

    /// <summary>
    /// The stored key of the last entry in the range (bounds as for <see cref="CountRange"/>), read
    /// from the end of the range; <c>null</c> when the range is empty.
    /// </summary>
    internal IndexKey? LastKey(object? minKey, object? maxKey, bool startInclusive, bool endInclusive, ITransaction? transaction = null)
    {
        if (_btreeIndex == null) return null;

        var (actualMinKey, actualMaxKey) = CompositeRange(minKey, maxKey, startInclusive, endInclusive);
        foreach (var entry in _btreeIndex.Range(actualMinKey, actualMaxKey, IndexDirection.Backward, transaction?.TransactionId))
            return entry.Key;
        return null;
    }

    /// <summary>
    /// The composite B-Tree bounds covering every entry whose user key lies between
    /// <paramref name="minKey"/> and <paramref name="maxKey"/> (<c>null</c> = unbounded).
    /// </summary>
    private (IndexKey Min, IndexKey Max) CompositeRange(object? minKey, object? maxKey, bool startInclusive, bool endInclusive)
    {
        IndexKey actualMinKey = IndexKey.MinKey;
        IndexKey actualMaxKey = s_maxBoundaryKey;

        if (minKey != null)
        {
            var userMin = ConvertToIndexKey(minKey);
            // inclusive start: useMinObjectId=true  → lower composite boundary for the user key
            // exclusive start: useMinObjectId=false → upper composite boundary for the user key
            actualMinKey = CreateCompositeKeyBoundary(userMin, useMinObjectId: startInclusive);
        }

        if (maxKey != null)
        {
            var userMax = ConvertToIndexKey(maxKey);
            // inclusive end:   useMinObjectId=false → upper composite boundary for the user key
            // exclusive end:   useMinObjectId=true  → lower composite boundary for the user key
            actualMaxKey = CreateCompositeKeyBoundary(userMax, useMinObjectId: !endInclusive);
        }

        return (actualMinKey, actualMaxKey);
    }

    /// <summary>
    /// Gets statistics about this index
    /// </summary>
    public CollectionIndexInfo GetInfo()
    {
        // Use CompareExchange as an atomic read (works on both net5+ and netstandard2.1)
        var count = System.Threading.Interlocked.CompareExchange(ref _documentCount, 0L, 0L);
        // Rough size estimate: key length prefix (4) + avg key bytes (20) + DocumentLocation (6) per entry
        var estimatedSize = count * (4 + 20 + DocumentLocation.SerializedSize);
        return new CollectionIndexInfo
        {
            Name = _definition.Name,
            PropertyPaths = _definition.PropertyPaths,
            IsUnique = _definition.IsUnique,
            Type = _definition.Type,
            IsPrimary = _definition.IsPrimary,
            EstimatedDocumentCount = count,
            EstimatedSizeBytes = estimatedSize
        };
    }

    /// <summary>
    /// True for a unique BTree index, the only kind <see cref="EnsureUnique"/> checks.
    /// </summary>
    internal bool IsUniqueBTree => _definition.IsUnique && _btreeIndex != null;

    /// <summary>
    /// Throws if this is a unique BTree index and another document already holds
    /// <paramref name="document"/>'s key. The document's own entry (same id) is ignored.
    /// On update, <paramref name="oldDocument"/> is the stored version: if its key equals the
    /// new one, <see cref="Update"/> keeps the existing entry and nothing is checked, so a
    /// document that already shares its value with another can still be updated.
    /// Entries are stored as <c>[user key][document id]</c>, so the BTree's own unique
    /// check never sees two equal keys; the collection calls this before writing any page.
    /// </summary>
    internal void EnsureUnique(T document, T? oldDocument, ITransaction? transaction)
    {
        if (!IsUniqueBTree)
            return;

        if (!_definition.TryGetKey(document, out var keyValue) || keyValue == null)
            return; // null keys are not indexed, so they never conflict

        var userKey = ConvertToIndexKey(keyValue);

        if (oldDocument != null
            && _definition.TryGetKey(oldDocument, out var oldKeyValue) && oldKeyValue != null
            && (Equals(oldKeyValue, keyValue) || ConvertToIndexKey(oldKeyValue).Data.SequenceEqual(userKey.Data)))
            return; // key unchanged

        // Every id key starts with its type's prefix byte, so the entries of this value lie in
        // [user key][prefix] .. [user key][prefix + 1]. Bounding the scan there skips the entries
        // of longer values starting with this one (e.g. "10", "11", ... when checking "1").
        var ownIdKey = _mapper.ToIndexKey(_mapper.GetId(document));
        var idPrefix = ownIdKey.Data[0];
        var minComposite = CreateIdPrefixBoundary(userKey, idPrefix);
        var maxComposite = CreateIdPrefixBoundary(userKey, (byte)(idPrefix + 1));

        foreach (var entry in _btreeIndex!.Range(minComposite, maxComposite, IndexDirection.Forward, transaction?.TransactionId))
        {
            var idPart = entry.Key.Data.Slice(userKey.Data.Length);
            if (idPart.SequenceEqual(ownIdKey.Data))
                continue; // this document's own entry

            // A longer value whose next byte is the prefix byte itself still falls in the range
            // (e.g. "ab\u0002c" when checking "ab"); for fixed-size ids its id part has the wrong length.
            if (s_fixedSizeId && idPart.Length != ownIdKey.Data.Length)
                continue;

            throw new BLiteValidationException(
                $"Duplicate key violation for unique index '{_definition.Name}'");
        }
    }

    /// <summary>
    /// Throws <see cref="BLiteValidationException"/> if <paramref name="document"/>'s key could not
    /// be written to this index: a vector whose length differs from the index dimensions, or a
    /// B-Tree key larger than a node page. Called before any page is written, so a rejected write
    /// leaves nothing behind instead of failing halfway through the indexes.
    /// </summary>
    /// <param name="oldDocument">
    /// The stored version on update. A key equal to the stored one keeps its entry (see
    /// <see cref="Update"/>), so it is not checked again: a key stored before
    /// <see cref="BTreeIndex.MaxKeySize"/> was lowered (#217) must not block every update.
    /// </param>
    internal void ValidateKey(T document, T? oldDocument = default)
    {
        if (!_definition.TryGetKey(document, out var keyValue) || keyValue == null)
            return; // null keys are not indexed

        if (oldDocument != null
            && _definition.TryGetKey(oldDocument, out var oldKeyValue)
            && KeysEqual(oldKeyValue, keyValue))
            return;

        if (_vectorIndex != null)
        {
            if (keyValue is float[] singleVector)
                ValidateVector(singleVector);
            else if (keyValue is IEnumerable<float[]> vectors)
                foreach (var v in vectors) ValidateVector(v);
        }
        else if (_btreeIndex != null && !keyValue.GetType().IsValueType)
        {
            // Value-type keys (numbers, dates, Guid, ObjectId, enums) have a small fixed size;
            // only strings, byte arrays and the ToString fallback can grow past a page.
            var keyLength = ConvertToIndexKey(keyValue).Data.Length
                            + _mapper.ToIndexKey(_mapper.GetId(document)).Data.Length;
            if (keyLength > _btreeIndex.MaxKeySize)
                throw new BLiteValidationException(
                    $"Key of {keyLength} bytes is too large for index '{_definition.Name}' (maximum {_btreeIndex.MaxKeySize} bytes).");
        }
    }

    private void ValidateVector(float[] vector)
    {
        if (vector.Length != _definition.Dimensions)
            throw new BLiteValidationException(
                $"Vector dimension mismatch for index '{_definition.Name}'. Expected {_definition.Dimensions}, got {vector.Length}");
    }

    // [user key][one byte]: a range bound between the entries of user key and those of longer values.
    private static IndexKey CreateIdPrefixBoundary(IndexKey userKey, byte idPrefixBound)
    {
        var bytes = new byte[userKey.Data.Length + 1];
        userKey.Data.CopyTo(bytes);
        bytes[^1] = idPrefixBound;
        return IndexKey.FromOwnedArray(bytes);
    }

    #region Composite Key Support (SQLite-style for Duplicate Keys)

    /// <summary>
    /// Creates a composite key by concatenating user key with document ID.
    /// This allows duplicate user keys while maintaining BTree uniqueness.
    /// Format: [UserKeyBytes] + [DocumentIdKey]
    /// </summary>
    private IndexKey CreateCompositeKey(IndexKey userKey, IndexKey documentIdKey)
    {
        // Allocate once — ownership is transferred to IndexKey.FromOwnedArray, no second copy.
        var compositeBytes = new byte[userKey.Data.Length + documentIdKey.Data.Length];
        userKey.Data.CopyTo(compositeBytes.AsSpan(0, userKey.Data.Length));
        documentIdKey.Data.CopyTo(compositeBytes.AsSpan(userKey.Data.Length));
        return IndexKey.FromOwnedArray(compositeBytes);
    }

    /// <summary>
    /// Creates a composite key for range query boundary.
    /// Uses MIN or MAX ID representation to capture all documents with the user key.
    /// </summary>
    private IndexKey CreateCompositeKeyBoundary(IndexKey userKey, bool useMinObjectId)
    {
        // Min: [user key] sorts before every [user key][id] entry.
        // Max: [user key][PrefixUpperBound] sorts after every [user key][id] entry (every id key starts
        // with KeyPrefix or DecimalKeyPrefix) but before the entries of a longer value starting with the
        // user key, whose next byte is that value's next byte (0x04 or above for every character but
        // U+0000..U+0003). An all-0xFF id bound sorted after those too, so "p7" matched "p70".."p79" (#206).
        IndexKey idBoundary = useMinObjectId 
            ? IndexKey.MinKey
            : s_maxIdBoundaryKey;
            
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
        // A decimal-keyed index only holds decimal keys: bounds given as int/long/double/float
        // (e.g. from a converted predicate) must be encoded the same way to be comparable.
        if (IsDecimalKeyed)
        {
            switch (value)
            {
                case int i: return new IndexKey((decimal)i);
                case long l: return new IndexKey((decimal)l);
                case double dd: return new IndexKey(ToDecimalClamped(dd));
                case float f: return new IndexKey(ToDecimalClamped(f));
            }
        }

        return value switch
        {
            // Pre-encoded IndexKey (passed from generated filter code via IndexPlanBuilder/IndexQueryPlan)
            IndexKey ik => ik,

            DBNull => IndexKey.NullSentinel, // explicit null equality query
            ObjectId objectId => new IndexKey(objectId),
            string str => new IndexKey(str),
            int intVal => new IndexKey(intVal),
            long longVal => new IndexKey(longVal),
            double doubleVal => new IndexKey(doubleVal),
            float floatVal => new IndexKey((double)floatVal),
            decimal decimalVal => new IndexKey(decimalVal),
            DateTime dateTime => new IndexKey(new DateTimeOffset(dateTime.ToUniversalTime()).ToUnixTimeMilliseconds()),
            DateTimeOffset dto => new IndexKey(dto.ToUnixTimeMilliseconds()),
            byte[] byteArray => new IndexKey(byteArray),
            
            // Enum values are boxed as the enum type (not the underlying int).
            // Convert to long to handle all enum underlying types (byte, short, int, long) consistently.
            _ when value.GetType().IsEnum => new IndexKey(Convert.ToInt64(value)),

            // Guid: common identity type, stored as 16 raw bytes
            Guid guid => new IndexKey(guid.ToByteArray()),

            // For truly unknown / compound types fall back to their UTF-8 string representation.
            // This produces consistent equality semantics; range ordering may not be meaningful.
            _ => new IndexKey(value.ToString() ?? string.Empty)
        };
    }

    // ── Decimal-keyed indexes ─────────────────────────────────────────────────
    // A decimal property is keyed with the exact IndexKey(decimal) encoding (DecimalKeyPrefix).
    // Bounds of another numeric type must be converted before encoding, and indexes written by
    // releases that keyed decimals as (double)value are rebuilt on open (NeedsKeyFormatMigration).

    private bool? _isDecimalKeyed;

    private bool IsDecimalKeyed => _isDecimalKeyed ??= SingleKeyType() == typeof(decimal);

    /// <summary>The type of the single indexed property (nullable unwrapped), or null for a compound key.</summary>
    private Type? SingleKeyType()
    {
        if (_definition.PropertyPaths.Length != 1) return null;
        // Only the boxing conversion to object is looked through: an explicit cast in the selector
        // (e.g. (double)p.Price) changes the key type on purpose and is encoded as that type.
        var body = _definition.KeySelectorExpression.Body;
        while (body is System.Linq.Expressions.UnaryExpression { NodeType: System.Linq.Expressions.ExpressionType.Convert } u
               && u.Type == typeof(object))
            body = u.Operand;
        return Nullable.GetUnderlyingType(body.Type) ?? body.Type;
    }

    // ── Double-keyed indexes ──────────────────────────────────────────────────
    // IndexKey(double) gives -0.0 the key of +0.0 (#249). Earlier releases keyed -0.0 apart, just
    // below +0.0; such an entry is neither found by a query nor removed when its document is updated
    // or deleted, so a double- or float-keyed index that still holds one is rebuilt on open
    // (NeedsKeyFormatMigration). The same holds for a NaN with the sign bit clear (what Arm64
    // computes), which was keyed above +Infinity before every NaN took the key of double.NaN.
    // Looking for them costs two seeks per index and open.

    // The former key of -0.0: every bit of 0x8000_0000_0000_0000 inverted, after KeyPrefix.
    private static readonly IndexKey s_legacyNegativeZeroKey =
        new IndexKey(new byte[] { 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF });

    // The former keys of the NaNs with the sign bit clear, 0x7FF0_0000_0000_0001..0x7FFF_FFFF_FFFF_FFFF
    // with the sign bit set, after KeyPrefix: every key above that of +Infinity.
    private static readonly IndexKey s_legacyPositiveNaNMinKey =
        new IndexKey(new byte[] { 0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 });
    private static readonly IndexKey s_legacyPositiveNaNMaxKey =
        new IndexKey(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF });

    private bool HoldsLegacyDoubleKey()
    {
        if (_btreeIndex == null) return false;
        var keyType = SingleKeyType();
        if (keyType != typeof(double) && keyType != typeof(float)) return false;

        return HoldsKeyBetween(s_legacyNegativeZeroKey, s_legacyNegativeZeroKey)
            || HoldsKeyBetween(s_legacyPositiveNaNMinKey, s_legacyPositiveNaNMaxKey);
    }

    private bool HoldsKeyBetween(IndexKey minUserKey, IndexKey maxUserKey)
    {
        foreach (var _ in _btreeIndex!.Range(
                     CreateCompositeKeyBoundary(minUserKey, useMinObjectId: true),
                     CreateCompositeKeyBoundary(maxUserKey, useMinObjectId: false),
                     IndexDirection.Forward, null))
            return true;
        return false;
    }

    private static decimal ToDecimalClamped(double d)
    {
        if (double.IsNaN(d)) return decimal.MinValue;
        if (d >= (double)decimal.MaxValue) return decimal.MaxValue;
        if (d <= (double)decimal.MinValue) return decimal.MinValue;
        return (decimal)d;
    }

    // Converted directly: going through double first would turn 1.1f into 1.10000002384186m
    // and miss a stored 1.1m.
    private static decimal ToDecimalClamped(float f)
    {
        if (float.IsNaN(f)) return decimal.MinValue;
        if (f >= (float)decimal.MaxValue) return decimal.MaxValue;
        if (f <= (float)decimal.MinValue) return decimal.MinValue;
        return (decimal)f;
    }

    /// <summary>
    /// Key format this index's pages were written with (see <see cref="IndexMetadata.KeyFormat"/>).
    /// New indexes start at <see cref="IndexMetadata.CurrentKeyFormat"/>; indexes loaded from
    /// metadata carry the persisted value until they are migrated.
    /// </summary>
    internal byte KeyFormat { get; set; } = IndexMetadata.CurrentKeyFormat;

    /// <summary>
    /// True when this B-tree index keys a <c>decimal</c> property and its pages still use the legacy
    /// format, i.e. hold <c>(double)value</c> keys that the exact decimal encoding cannot match; or
    /// when this full-text index was written with another analyzer version or holds keys it did not
    /// write (e.g. from an older BLite that read it as a plain B-tree); or when this double- or
    /// float-keyed B-tree index holds a key of -0.0 written before -0.0 and +0.0 shared a key, or
    /// a key of a NaN with the sign bit clear written before every NaN took the key of <see cref="double.NaN"/>.
    /// </summary>
    internal bool NeedsKeyFormatMigration =>
        (_btreeIndex != null && KeyFormat < IndexMetadata.CurrentKeyFormat && IsDecimalKeyed)
        || HoldsLegacyDoubleKey()
        || (_fullTextIndex != null && (KeyFormat != FullTextIndex.FormatVersion || _fullTextIndex.HasForeignKeys()));

    /// <summary>The key format a rebuilt copy of this index is written in.</summary>
    internal byte TargetKeyFormat => _fullTextIndex != null ? FullTextIndex.FormatVersion : IndexMetadata.CurrentKeyFormat;

    /// <summary>
    /// Whether the indexed text of <paramref name="document"/> contains every term of
    /// <paramref name="query"/> (or, with <paramref name="matchAllTerms"/> false, at least one).
    /// Used to re-check full-text hits, so a stale posting can never surface another document.
    /// </summary>
    internal bool FullTextContains(T document, string query, bool matchAllTerms)
    {
        var text = _definition.TryGetKey(document, out var key) ? key as string : null;
        return matchAllTerms ? Text.FullTextAnalyzer.Matches(text, query) : Text.FullTextAnalyzer.MatchesAny(text, query);
    }

    // ── ICollectionIndex metadata pass-throughs ─────────────────────────────

    public string Name => _definition.Name;
    public string[] PropertyPaths => _definition.PropertyPaths;
    IndexType ICollectionIndex<TId, T>.Type => _definition.Type;
    public bool IsUnique => _definition.IsUnique;
    public int Dimensions => _definition.Dimensions;
    public VectorMetric Metric => _definition.Metric;

    // ── Document reader (injected by DocumentCollection) ─────────────────────
    // Needed to resolve DocumentLocation → T inside Query / QueryAsync.

    private Func<DocumentLocation, CancellationToken, ValueTask<T?>>? _asyncDocReader;

    /// <summary>
    /// Injects the document resolver. Called by <c>DocumentCollection</c> before
    /// returning the index to the caller. Not set for indexes held internally.
    /// </summary>
    internal void SetDocumentReader(
        Func<DocumentLocation, CancellationToken, ValueTask<T?>> async)
    {
        _asyncDocReader = async;
    }

    // ── ICollectionIndex query methods ────────────────────────────────────────

    /// <inheritdoc cref="ICollectionIndex{TId,T}.QueryAsync"/>
    public async IAsyncEnumerable<T> QueryAsync(
        object? minKey = null,
        object? maxKey = null,
        bool ascending = true,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_asyncDocReader is null)
            throw new InvalidOperationException(
                "Document reader not available. Use collection.QueryIndexAsync() instead, " +
                "or obtain this index via collection.GetIndex() / CreateIndex().");

        var direction = ascending ? IndexDirection.Forward : IndexDirection.Backward;
        foreach (var loc in Range(minKey, maxKey, direction))
        {
            ct.ThrowIfCancellationRequested();
            var doc = await _asyncDocReader(loc, ct).ConfigureAwait(false);
            if (doc is not null) yield return doc;
        }
    }

    // Explicit interface implementation so the public method retains the ITransaction overload.
    IEnumerable<VectorSearchResult> ICollectionIndex<TId, T>.VectorSearch(float[] query, int k, int efSearch)
        => VectorSearch(query, k, efSearch, null);

    public async IAsyncEnumerable<T> VectorSearchAsync(
        float[] query, int k, int efSearch = 100,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_asyncDocReader is null)
            throw new InvalidOperationException(
                "Document reader not available. Use collection.VectorSearchAsync() instead, " +
                "or obtain this index via collection.CreateVectorIndex() / EnsureIndex().");

        foreach (var result in VectorSearch(query, k, efSearch, null))
        {
            ct.ThrowIfCancellationRequested();
            var doc = await _asyncDocReader(result.Location, ct).ConfigureAwait(false);
            if (doc is not null) yield return doc;
        }
    }

    // ── IDisposable ───────────────────────────────────────────────────────────

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Frees all physical pages owned by this index via the storage engine.
    /// Called from <see cref="CollectionIndexManager{TId,T}.DropIndex"/> to reclaim disk space.
    /// </summary>
    internal void FreeAllPages(StorageEngine storage)
    {
        foreach (var pageId in CollectAllPages(0))
            storage.FreePage(pageId);
    }

    /// <summary>
    /// The pages of this index as <paramref name="transactionId"/> sees them (0: committed state).
    /// </summary>
    internal IReadOnlyList<uint> CollectAllPages(ulong transactionId)
    {
        if (_btreeIndex != null) return _btreeIndex.CollectAllPages(transactionId);
        if (_vectorIndex != null) return _vectorIndex.CollectAllPages(transactionId);
        if (_spatialIndex != null) return _spatialIndex.CollectAllPages(transactionId);
        if (_fullTextIndex != null) return _fullTextIndex.CollectAllPages(transactionId);
        return Array.Empty<uint>();
    }
}
