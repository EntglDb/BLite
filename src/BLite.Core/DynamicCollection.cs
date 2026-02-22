using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BLite.Bson;
using BLite.Core.Indexing;
using BLite.Core.Indexing.Internal;
using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace BLite.Core;

/// <summary>
/// Schema-less document collection for dynamic/server mode.
/// Operates on BsonDocument and BsonId — no compile-time type information required.
/// Sits alongside DocumentCollection&lt;TId, T&gt; as an equally valid alternative
/// that insists directly on the StorageEngine.
/// </summary>
public sealed class DynamicCollection : IDisposable
{
    private readonly StorageEngine _storage;
    private readonly ITransactionHolder _transactionHolder;
    private readonly BTreeIndex _primaryIndex;
    private readonly string _collectionName;
    private readonly BsonIdType _idType;
    private readonly SemaphoreSlim _collectionLock = new(1, 1);
    private readonly Dictionary<uint, ushort> _freeSpaceMap = new();
    private readonly int _maxDocumentSizeForSinglePage;
    private uint _currentDataPage;

    // ── Discriminated union for secondary indexes ─────────────────────────────
    private enum DynamicIndexKind { BTree, Vector, Spatial }

    private sealed class DynamicSecondaryIndex
    {
        public DynamicIndexKind Kind { get; }
        public string FieldPath { get; }
        public IndexOptions Options { get; }
        public BTreeIndex? BTree { get; }
        public VectorSearchIndex? Vector { get; }
        public RTreeIndex? Spatial { get; }
        public uint RootPageId => BTree?.RootPageId ?? Vector?.RootPageId ?? Spatial?.RootPageId ?? 0;

        public DynamicSecondaryIndex(BTreeIndex btree, string fieldPath, IndexOptions options)
        { Kind = DynamicIndexKind.BTree; BTree = btree; FieldPath = fieldPath; Options = options; }

        public DynamicSecondaryIndex(VectorSearchIndex vector, string fieldPath, IndexOptions options)
        { Kind = DynamicIndexKind.Vector; Vector = vector; FieldPath = fieldPath; Options = options; }

        public DynamicSecondaryIndex(RTreeIndex spatial, string fieldPath, IndexOptions options)
        { Kind = DynamicIndexKind.Spatial; Spatial = spatial; FieldPath = fieldPath; Options = options; }
    }

    // Secondary indexes: name → DynamicSecondaryIndex
    private readonly Dictionary<string, DynamicSecondaryIndex> _secondaryIndexes = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Creates or opens a dynamic collection.
    /// </summary>
    /// <param name="storage">The storage engine instance</param>
    /// <param name="transactionHolder">Transaction holder for ACID operations</param>
    /// <param name="collectionName">Name of the collection</param>
    /// <param name="idType">The BSON type used for the _id field (default: ObjectId)</param>
    public DynamicCollection(StorageEngine storage, ITransactionHolder transactionHolder, string collectionName, BsonIdType idType = BsonIdType.ObjectId)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _transactionHolder = transactionHolder ?? throw new ArgumentNullException(nameof(transactionHolder));
        _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
        _idType = idType;
        _maxDocumentSizeForSinglePage = _storage.PageSize - 128;

        // Load or create collection metadata
        var metadata = _storage.GetCollectionMetadata(_collectionName);
        uint primaryRootPageId = 0;

        if (metadata != null)
        {
            primaryRootPageId = metadata.PrimaryRootPageId;

            // Restore secondary indexes from metadata
            foreach (var idxMeta in metadata.Indexes)
            {
                if (idxMeta.PropertyPaths.Length == 0) continue;
                var fieldPath = idxMeta.PropertyPaths[0];

                switch (idxMeta.Type)
                {
                    case IndexType.BTree:
                    {
                        var opts = idxMeta.IsUnique
                            ? IndexOptions.CreateUnique(idxMeta.PropertyPaths)
                            : IndexOptions.CreateBTree(idxMeta.PropertyPaths);
                        var btree = new BTreeIndex(_storage, opts, idxMeta.RootPageId);
                        _secondaryIndexes[idxMeta.Name] = new DynamicSecondaryIndex(btree, fieldPath, opts);
                        break;
                    }
                    case IndexType.Vector:
                    {
                        var opts = IndexOptions.CreateVector(idxMeta.Dimensions, idxMeta.Metric, 16, 200, idxMeta.PropertyPaths);
                        var vector = new VectorSearchIndex(_storage, opts, idxMeta.RootPageId);
                        _secondaryIndexes[idxMeta.Name] = new DynamicSecondaryIndex(vector, fieldPath, opts);
                        break;
                    }
                    case IndexType.Spatial:
                    {
                        var opts = IndexOptions.CreateSpatial(idxMeta.PropertyPaths);
                        var spatial = new RTreeIndex(_storage, opts, idxMeta.RootPageId);
                        _secondaryIndexes[idxMeta.Name] = new DynamicSecondaryIndex(spatial, fieldPath, opts);
                        break;
                    }
                }
            }
        }
        else
        {
            metadata = new CollectionMetadata { Name = _collectionName };
            _storage.SaveCollectionMetadata(metadata);
        }

        var indexOptions = IndexOptions.CreateBTree("_id");
        _primaryIndex = new BTreeIndex(_storage, indexOptions, primaryRootPageId);

        // Persist root page if newly allocated
        if (metadata.PrimaryRootPageId != _primaryIndex.RootPageId)
        {
            metadata.PrimaryRootPageId = _primaryIndex.RootPageId;
            _storage.SaveCollectionMetadata(metadata);
        }
    }

    /// <summary>The collection name.</summary>
    public string Name => _collectionName;

    /// <summary>The ID type used by this collection.</summary>
    public BsonIdType IdType => _idType;

    /// <summary>
    /// Creates a BsonDocument using the storage engine's key dictionary.
    /// Field names are automatically registered in the C-BSON key map.
    /// </summary>
    /// <param name="fieldNames">All field names that will be used in the document</param>
    /// <param name="buildAction">Builder action to populate the document</param>
    /// <returns>A new BsonDocument ready for insertion</returns>
    public BsonDocument CreateDocument(string[] fieldNames, Action<BsonDocumentBuilder> buildAction)
    {
        _storage.RegisterKeys(fieldNames);
        return BsonDocument.Create(_storage.GetKeyMap(), _storage.GetKeyReverseMap(), buildAction);
    }

    #region Insert

    /// <summary>
    /// Inserts a BsonDocument into the collection.
    /// If the document has no _id field, one is auto-generated.
    /// Returns the BsonId of the inserted document.
    /// </summary>
    public BsonId Insert(BsonDocument document)
    {
        if (document == null) throw new ArgumentNullException(nameof(document));

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        _collectionLock.Wait();
        try
        {
            return InsertCore(document, transaction);
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    private BsonId InsertCore(BsonDocument document, ITransaction transaction)
    {
        // Extract or generate ID
        BsonId id;
        if (!document.TryGetId(out id) || id.IsEmpty)
        {
            id = BsonId.NewId(_idType);
            // We need to rebuild the document with the _id field prepended
            document = PrependId(document, id);
        }

        // Write raw BSON to storage
        var docData = document.RawData;
        DocumentLocation location;

        if (docData.Length + SlotEntry.Size <= _maxDocumentSizeForSinglePage)
        {
            var pageId = FindPageWithSpace(docData.Length + SlotEntry.Size, transaction.TransactionId);
            if (pageId == 0) pageId = AllocateNewDataPage(transaction);
            var slotIndex = InsertIntoPage(pageId, docData, transaction);
            location = new DocumentLocation(pageId, slotIndex);
        }
        else
        {
            throw new InvalidOperationException($"Document size {docData.Length} exceeds maximum single page size. Overflow not yet supported in DynamicCollection.");
        }

        // Index the _id
        var key = new IndexKey(id.ToBytes());
        _primaryIndex.Insert(key, location, transaction.TransactionId);

        // Update secondary indexes
        foreach (var (_, idx) in _secondaryIndexes)
            IndexInsert(idx, document, location, transaction);

        return id;
    }

    private BsonDocument PrependId(BsonDocument document, BsonId id)
    {
        var keyMap = _storage.GetKeyMap();
        // Register _id key if not already present
        _storage.RegisterKeys(new[] { "_id" });

        // Estimate size: existing doc + id field overhead
        var estimatedSize = document.RawData.Length + 64;
        var buffer = new byte[estimatedSize];
        var writer = new BsonSpanWriter(buffer, keyMap);

        var sizePos = writer.BeginDocument();
        id.WriteTo(ref writer, "_id");

        // Copy all existing fields (skip _id if present)
        var reader = document.GetReader();
        reader.ReadDocumentSize();
        while (reader.Remaining > 1)
        {
            var type = reader.ReadBsonType();
            if (type == BsonType.EndOfDocument) break;
            var name = reader.ReadElementHeader();
            if (name == "_id")
            {
                reader.SkipValue(type);
                continue;
            }
            var value = BsonValue.ReadFrom(ref reader, type);
            value.WriteTo(ref writer, name);
        }

        writer.EndDocument(sizePos);

        return new BsonDocument(buffer[..writer.Position], _storage.GetKeyReverseMap());
    }

    /// <summary>
    /// Inserts a BsonDocument into the collection asynchronously.
    /// If the document has no _id field, one is auto-generated.
    /// Returns the BsonId of the inserted document.
    /// </summary>
    public async Task<BsonId> InsertAsync(BsonDocument document, CancellationToken ct = default)
    {
        if (document == null) throw new ArgumentNullException(nameof(document));

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        await _collectionLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return InsertCore(document, transaction);
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    /// <summary>
    /// Inserts multiple BsonDocuments into the collection in a single transaction.
    /// Returns the list of generated/existing BsonIds in insertion order.
    /// </summary>
    public List<BsonId> InsertBulk(IEnumerable<BsonDocument> documents)
    {
        if (documents == null) throw new ArgumentNullException(nameof(documents));

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        _collectionLock.Wait();
        try
        {
            var ids = new List<BsonId>();
            foreach (var doc in documents)
                ids.Add(InsertCore(doc, transaction));
            return ids;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    /// <summary>
    /// Inserts multiple BsonDocuments asynchronously in a single transaction.
    /// Returns the list of generated/existing BsonIds in insertion order.
    /// </summary>
    public async Task<List<BsonId>> InsertBulkAsync(IEnumerable<BsonDocument> documents, CancellationToken ct = default)
    {
        if (documents == null) throw new ArgumentNullException(nameof(documents));

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        await _collectionLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var ids = new List<BsonId>();
            foreach (var doc in documents)
            {
                ct.ThrowIfCancellationRequested();
                ids.Add(InsertCore(doc, transaction));
            }
            return ids;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    #endregion

    #region Find

    /// <summary>
    /// Finds a document by its BsonId.
    /// </summary>
    public BsonDocument? FindById(BsonId id)
    {
        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        var key = new IndexKey(id.ToBytes());

        if (!_primaryIndex.TryFind(key, out var location, transaction.TransactionId))
            return null;

        return ReadDocumentAt(location, transaction.TransactionId);
    }

    /// <summary>
    /// Returns all documents in the collection.
    /// </summary>
    public IEnumerable<BsonDocument> FindAll()
    {
        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        var txnId = transaction.TransactionId;
        var minKey = IndexKey.MinKey;
        var maxKey = IndexKey.MaxKey;

        foreach (var entry in _primaryIndex.Range(minKey, maxKey, IndexDirection.Forward, txnId))
        {
            var doc = ReadDocumentAt(entry.Location, txnId);
            if (doc != null)
                yield return doc;
        }
    }

    /// <summary>
    /// Returns the count of documents in the collection.
    /// </summary>
    public int Count()
    {
        int count = 0;
        foreach (var _ in FindAll()) count++;
        return count;
    }

    /// <summary>
    /// Scans all documents applying a predicate at the BSON level (no deserialization to T).
    /// The predicate receives a BsonSpanReader positioned at the start of each document.
    /// </summary>
    public IEnumerable<BsonDocument> Scan(Func<BsonSpanReader, bool> predicate)
    {
        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        var txnId = transaction.TransactionId;

        foreach (var entry in _primaryIndex.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txnId))
        {
            var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
            try
            {
                _storage.ReadPage(entry.Location.PageId, txnId, buffer);
                var header = SlottedPageHeader.ReadFrom(buffer);
                if (entry.Location.SlotIndex >= header.SlotCount) continue;

                var slotOffset = SlottedPageHeader.Size + (entry.Location.SlotIndex * SlotEntry.Size);
                var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));
                if (slot.Flags.HasFlag(SlotFlags.Deleted)) continue;

                var data = buffer.AsSpan(slot.Offset, slot.Length);
                var reader = new BsonSpanReader(data, _storage.GetKeyReverseMap());

                if (predicate(reader))
                {
                    var doc = ReadDocumentAt(entry.Location, txnId);
                    if (doc != null) yield return doc;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

    /// <summary>
    /// Queries a secondary index for documents matching a range.
    /// </summary>
    public IEnumerable<BsonDocument> QueryIndex(string indexName, object? minValue, object? maxValue)
    {
        if (!_secondaryIndexes.TryGetValue(indexName, out var entry))
            throw new ArgumentException($"Index '{indexName}' not found on collection '{_collectionName}'");
        if (entry.Kind != DynamicIndexKind.BTree || entry.BTree == null)
            throw new InvalidOperationException($"Index '{indexName}' is not a BTree index. Use VectorSearch/Near/Within for vector/spatial indexes.");

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        var txnId = transaction.TransactionId;

        var minKey = minValue != null ? CreateIndexKeyFromObject(minValue) : IndexKey.MinKey;
        var maxKey = maxValue != null ? CreateIndexKeyFromObject(maxValue) : IndexKey.MaxKey;

        foreach (var indexEntry in entry.BTree.Range(minKey, maxKey, IndexDirection.Forward, txnId))
        {
            var doc = ReadDocumentAt(indexEntry.Location, txnId);
            if (doc != null) yield return doc;
        }
    }

    /// <summary>
    /// Performs a vector similarity search using the named vector index.
    /// The field must have been indexed via <see cref="CreateVectorIndex"/>.
    /// </summary>
    /// <param name="indexName">Name of the vector index.</param>
    /// <param name="query">Query vector (must match the index dimensionality).</param>
    /// <param name="k">Maximum number of nearest neighbours to return.</param>
    /// <param name="efSearch">HNSW efSearch parameter (higher = more recall, slower). Default 100.</param>
    public IEnumerable<BsonDocument> VectorSearch(string indexName, float[] query, int k, int efSearch = 100)
    {
        if (!_secondaryIndexes.TryGetValue(indexName, out var entry) || entry.Kind != DynamicIndexKind.Vector || entry.Vector == null)
            throw new ArgumentException($"Vector index '{indexName}' not found on collection '{_collectionName}'");

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        foreach (var result in entry.Vector.Search(query, k, efSearch, transaction))
        {
            var doc = ReadDocumentAt(result.Location, transaction.TransactionId);
            if (doc != null) yield return doc;
        }
    }

    /// <summary>
    /// Returns documents within a radius (km) of a geographic centre point.
    /// The field must have been indexed via <see cref="CreateSpatialIndex"/>.
    /// </summary>
    public IEnumerable<BsonDocument> Near(string indexName, (double Latitude, double Longitude) center, double radiusKm)
    {
        if (!_secondaryIndexes.TryGetValue(indexName, out var entry) || entry.Kind != DynamicIndexKind.Spatial || entry.Spatial == null)
            throw new ArgumentException($"Spatial index '{indexName}' not found on collection '{_collectionName}'");

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        var queryBox = SpatialMath.BoundingBox(center.Latitude, center.Longitude, radiusKm);
        foreach (var loc in entry.Spatial.Search(queryBox, transaction))
        {
            var doc = ReadDocumentAt(loc, transaction.TransactionId);
            if (doc != null) yield return doc;
        }
    }

    /// <summary>
    /// Returns documents within a rectangular geographic area.
    /// The field must have been indexed via <see cref="CreateSpatialIndex"/>.
    /// </summary>
    public IEnumerable<BsonDocument> Within(string indexName, (double Latitude, double Longitude) min, (double Latitude, double Longitude) max)
    {
        if (!_secondaryIndexes.TryGetValue(indexName, out var entry) || entry.Kind != DynamicIndexKind.Spatial || entry.Spatial == null)
            throw new ArgumentException($"Spatial index '{indexName}' not found on collection '{_collectionName}'");

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        var area = new GeoBox(min.Latitude, min.Longitude, max.Latitude, max.Longitude);
        foreach (var loc in entry.Spatial.Search(area, transaction))
        {
            var doc = ReadDocumentAt(loc, transaction.TransactionId);
            if (doc != null) yield return doc;
        }
    }

    /// <summary>
    /// Returns documents matching the specified predicate.
    /// </summary>
    public IEnumerable<BsonDocument> Find(Func<BsonDocument, bool> predicate)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        foreach (var doc in FindAll())
            if (predicate(doc)) yield return doc;
    }

    /// <summary>
    /// Asynchronously yields documents matching the specified predicate.
    /// </summary>
    public async IAsyncEnumerable<BsonDocument> FindAsync(Func<BsonDocument, bool> predicate, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        await foreach (var doc in FindAllAsync(ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            if (predicate(doc)) yield return doc;
        }
    }

    #endregion

    #region Update

    /// <summary>
    /// Updates a document by its BsonId. Replaces the entire document.
    /// </summary>
    public bool Update(BsonId id, BsonDocument newDocument)
    {
        if (newDocument == null) throw new ArgumentNullException(nameof(newDocument));

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        _collectionLock.Wait();
        try
        {
            var key = new IndexKey(id.ToBytes());
            if (!_primaryIndex.TryFind(key, out var oldLocation, transaction.TransactionId))
                return false;

            // Read old document for secondary index cleanup
            var oldDoc = ReadDocumentAt(oldLocation, transaction.TransactionId);

            // Delete old slot
            DeleteSlot(oldLocation, transaction);

            // Ensure _id is present in new document
            if (!newDocument.TryGetId(out _))
            {
                newDocument = PrependId(newDocument, id);
            }

            // Insert updated document
            var docData = newDocument.RawData;
            DocumentLocation newLocation;

            if (docData.Length + SlotEntry.Size <= _maxDocumentSizeForSinglePage)
            {
                var pageId = FindPageWithSpace(docData.Length + SlotEntry.Size, transaction.TransactionId);
                if (pageId == 0) pageId = AllocateNewDataPage(transaction);
                var slotIndex = InsertIntoPage(pageId, docData, transaction);
                newLocation = new DocumentLocation(pageId, slotIndex);
            }
            else
            {
                throw new InvalidOperationException("Document too large for single page. Overflow not yet supported in DynamicCollection.");
            }

            // Update primary index: delete old, insert new
            _primaryIndex.Delete(key, oldLocation, transaction.TransactionId);
            _primaryIndex.Insert(key, newLocation, transaction.TransactionId);

            // Update secondary indexes
            foreach (var (_, idx) in _secondaryIndexes)
            {
                if (oldDoc != null) IndexDelete(idx, oldDoc, oldLocation, transaction);
                IndexInsert(idx, newDocument, newLocation, transaction);
            }

            return true;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    /// <summary>
    /// Updates a document by its BsonId asynchronously. Replaces the entire document.
    /// </summary>
    public async Task<bool> UpdateAsync(BsonId id, BsonDocument newDocument, CancellationToken ct = default)
    {
        if (newDocument == null) throw new ArgumentNullException(nameof(newDocument));

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        await _collectionLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var key = new IndexKey(id.ToBytes());
            if (!_primaryIndex.TryFind(key, out var oldLocation, transaction.TransactionId))
                return false;

            var oldDoc = ReadDocumentAt(oldLocation, transaction.TransactionId);
            DeleteSlot(oldLocation, transaction);

            if (!newDocument.TryGetId(out _))
                newDocument = PrependId(newDocument, id);

            var docData = newDocument.RawData;
            DocumentLocation newLocation;
            if (docData.Length + SlotEntry.Size <= _maxDocumentSizeForSinglePage)
            {
                var pageId = FindPageWithSpace(docData.Length + SlotEntry.Size, transaction.TransactionId);
                if (pageId == 0) pageId = AllocateNewDataPage(transaction);
                var slotIndex = InsertIntoPage(pageId, docData, transaction);
                newLocation = new DocumentLocation(pageId, slotIndex);
            }
            else
            {
                throw new InvalidOperationException("Document too large for single page. Overflow not yet supported in DynamicCollection.");
            }

            _primaryIndex.Delete(key, oldLocation, transaction.TransactionId);
            _primaryIndex.Insert(key, newLocation, transaction.TransactionId);

            foreach (var (_, idx) in _secondaryIndexes)
            {
                if (oldDoc != null) IndexDelete(idx, oldDoc, oldLocation, transaction);
                IndexInsert(idx, newDocument, newLocation, transaction);
            }

            return true;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    /// <summary>
    /// Updates multiple documents by their BsonIds in a single transaction.
    /// Returns the number of documents successfully updated.
    /// </summary>
    public int UpdateBulk(IEnumerable<(BsonId Id, BsonDocument Document)> updates)
    {
        if (updates == null) throw new ArgumentNullException(nameof(updates));

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        _collectionLock.Wait();
        try
        {
            var count = 0;
            foreach (var (id, doc) in updates)
            {
                var key = new IndexKey(id.ToBytes());
                if (!_primaryIndex.TryFind(key, out var oldLocation, transaction.TransactionId))
                    continue;

                var oldDoc = ReadDocumentAt(oldLocation, transaction.TransactionId);
                DeleteSlot(oldLocation, transaction);

                var newDoc = doc;
                if (!newDoc.TryGetId(out _))
                    newDoc = PrependId(newDoc, id);

                var docData = newDoc.RawData;
                if (docData.Length + SlotEntry.Size > _maxDocumentSizeForSinglePage)
                    throw new InvalidOperationException("Document too large for single page.");

                var pageId = FindPageWithSpace(docData.Length + SlotEntry.Size, transaction.TransactionId);
                if (pageId == 0) pageId = AllocateNewDataPage(transaction);
                var slotIndex = InsertIntoPage(pageId, docData, transaction);
                var newLocation = new DocumentLocation(pageId, slotIndex);

                _primaryIndex.Delete(key, oldLocation, transaction.TransactionId);
                _primaryIndex.Insert(key, newLocation, transaction.TransactionId);

                foreach (var (_, idx) in _secondaryIndexes)
                {
                    if (oldDoc != null) IndexDelete(idx, oldDoc, oldLocation, transaction);
                    IndexInsert(idx, newDoc, newLocation, transaction);
                }

                count++;
            }
            return count;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    /// <summary>
    /// Updates multiple documents asynchronously in a single transaction.
    /// Returns the number of documents successfully updated.
    /// </summary>
    public async Task<int> UpdateBulkAsync(IEnumerable<(BsonId Id, BsonDocument Document)> updates, CancellationToken ct = default)
    {
        if (updates == null) throw new ArgumentNullException(nameof(updates));

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        await _collectionLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var count = 0;
            foreach (var (id, doc) in updates)
            {
                ct.ThrowIfCancellationRequested();
                var key = new IndexKey(id.ToBytes());
                if (!_primaryIndex.TryFind(key, out var oldLocation, transaction.TransactionId))
                    continue;

                var oldDoc = ReadDocumentAt(oldLocation, transaction.TransactionId);
                DeleteSlot(oldLocation, transaction);

                var newDoc = doc;
                if (!newDoc.TryGetId(out _))
                    newDoc = PrependId(newDoc, id);

                var docData = newDoc.RawData;
                if (docData.Length + SlotEntry.Size > _maxDocumentSizeForSinglePage)
                    throw new InvalidOperationException("Document too large for single page.");

                var pageId = FindPageWithSpace(docData.Length + SlotEntry.Size, transaction.TransactionId);
                if (pageId == 0) pageId = AllocateNewDataPage(transaction);
                var slotIndex = InsertIntoPage(pageId, docData, transaction);
                var newLocation = new DocumentLocation(pageId, slotIndex);

                _primaryIndex.Delete(key, oldLocation, transaction.TransactionId);
                _primaryIndex.Insert(key, newLocation, transaction.TransactionId);

                foreach (var (_, idx) in _secondaryIndexes)
                {
                    if (oldDoc != null) IndexDelete(idx, oldDoc, oldLocation, transaction);
                    IndexInsert(idx, newDoc, newLocation, transaction);
                }

                count++;
            }
            return count;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    #endregion

    #region Delete

    /// <summary>
    /// Deletes a document by its BsonId.
    /// </summary>
    public bool Delete(BsonId id)
    {
        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        _collectionLock.Wait();
        try
        {
            var key = new IndexKey(id.ToBytes());
            if (!_primaryIndex.TryFind(key, out var location, transaction.TransactionId))
                return false;

            // Read doc for secondary index cleanup
            var doc = ReadDocumentAt(location, transaction.TransactionId);

            // Delete from primary index
            _primaryIndex.Delete(key, location, transaction.TransactionId);

            // Delete from secondary indexes
            if (doc != null)
            {
                foreach (var (_, idx) in _secondaryIndexes)
                    IndexDelete(idx, doc, location, transaction);
            }

            // Mark slot as deleted
            DeleteSlot(location, transaction);

            return true;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    /// <summary>
    /// Deletes a document by its BsonId asynchronously.
    /// </summary>
    public async Task<bool> DeleteAsync(BsonId id, CancellationToken ct = default)
    {
        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        await _collectionLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var key = new IndexKey(id.ToBytes());
            if (!_primaryIndex.TryFind(key, out var location, transaction.TransactionId))
                return false;

            var doc = ReadDocumentAt(location, transaction.TransactionId);
            _primaryIndex.Delete(key, location, transaction.TransactionId);

            if (doc != null)
            {
                foreach (var (_, idx) in _secondaryIndexes)
                    IndexDelete(idx, doc, location, transaction);
            }

            DeleteSlot(location, transaction);
            return true;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    /// <summary>
    /// Deletes multiple documents by their BsonIds in a single transaction.
    /// Returns the number of documents successfully deleted.
    /// </summary>
    public int DeleteBulk(IEnumerable<BsonId> ids)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        _collectionLock.Wait();
        try
        {
            var count = 0;
            foreach (var id in ids)
            {
                var key = new IndexKey(id.ToBytes());
                if (!_primaryIndex.TryFind(key, out var location, transaction.TransactionId))
                    continue;

                var doc = ReadDocumentAt(location, transaction.TransactionId);
                _primaryIndex.Delete(key, location, transaction.TransactionId);

                if (doc != null)
                {
                    foreach (var (_, idx) in _secondaryIndexes)
                        IndexDelete(idx, doc, location, transaction);
                }

                DeleteSlot(location, transaction);
                count++;
            }
            return count;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    /// <summary>
    /// Deletes multiple documents asynchronously in a single transaction.
    /// Returns the number of documents successfully deleted.
    /// </summary>
    public async Task<int> DeleteBulkAsync(IEnumerable<BsonId> ids, CancellationToken ct = default)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        await _collectionLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var count = 0;
            foreach (var id in ids)
            {
                ct.ThrowIfCancellationRequested();
                var key = new IndexKey(id.ToBytes());
                if (!_primaryIndex.TryFind(key, out var location, transaction.TransactionId))
                    continue;

                var doc = ReadDocumentAt(location, transaction.TransactionId);
                _primaryIndex.Delete(key, location, transaction.TransactionId);

                if (doc != null)
                {
                    foreach (var (_, idx) in _secondaryIndexes)
                        IndexDelete(idx, doc, location, transaction);
                }

                DeleteSlot(location, transaction);
                count++;
            }
            return count;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    #endregion

    #region Index Management

    /// <summary>Creates a secondary B-Tree index on a field path.</summary>
    public void CreateIndex(string fieldPath, string? name = null, bool unique = false)
    {
        name ??= $"idx_{fieldPath.ToLowerInvariant()}";
        fieldPath = fieldPath.ToLowerInvariant();

        if (_secondaryIndexes.ContainsKey(name))
            throw new InvalidOperationException($"Index '{name}' already exists");

        _storage.RegisterKeys(new[] { fieldPath });

        var opts = unique ? IndexOptions.CreateUnique(fieldPath) : IndexOptions.CreateBTree(fieldPath);
        var btree = new BTreeIndex(_storage, opts);
        var entry = new DynamicSecondaryIndex(btree, fieldPath, opts);
        _secondaryIndexes[name] = entry;

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        foreach (var e in _primaryIndex.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, transaction.TransactionId))
        {
            var doc = ReadDocumentAt(e.Location, transaction.TransactionId);
            if (doc != null) IndexInsert(entry, doc, e.Location, transaction);
        }

        PersistIndexMetadata();
    }

    /// <summary>
    /// Creates a vector (HNSW) index for similarity search on a float-array field.
    /// The field must be stored as a BSON Array of numeric values.
    /// </summary>
    public void CreateVectorIndex(string fieldPath, int dimensions, VectorMetric metric = VectorMetric.Cosine, string? name = null)
    {
        name ??= $"idx_vector_{fieldPath.ToLowerInvariant()}";
        fieldPath = fieldPath.ToLowerInvariant();

        if (_secondaryIndexes.ContainsKey(name))
            throw new InvalidOperationException($"Index '{name}' already exists");

        _storage.RegisterKeys(new[] { fieldPath });

        var opts = IndexOptions.CreateVector(dimensions, metric, 16, 200, fieldPath);
        var vector = new VectorSearchIndex(_storage, opts);
        var entry = new DynamicSecondaryIndex(vector, fieldPath, opts);
        _secondaryIndexes[name] = entry;

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        foreach (var e in _primaryIndex.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, transaction.TransactionId))
        {
            var doc = ReadDocumentAt(e.Location, transaction.TransactionId);
            if (doc != null) IndexInsert(entry, doc, e.Location, transaction);
        }

        PersistIndexMetadata();
    }

    /// <summary>
    /// Creates a geospatial (R-Tree) index for <c>Near</c> and <c>Within</c> queries.
    /// The field must be stored as a BSON coordinates array <c>[lat, lon]</c>.
    /// </summary>
    public void CreateSpatialIndex(string fieldPath, string? name = null)
    {
        name ??= $"idx_spatial_{fieldPath.ToLowerInvariant()}";
        fieldPath = fieldPath.ToLowerInvariant();

        if (_secondaryIndexes.ContainsKey(name))
            throw new InvalidOperationException($"Index '{name}' already exists");

        _storage.RegisterKeys(new[] { fieldPath });

        var opts = IndexOptions.CreateSpatial(fieldPath);
        var spatial = new RTreeIndex(_storage, opts, 0);
        var entry = new DynamicSecondaryIndex(spatial, fieldPath, opts);
        _secondaryIndexes[name] = entry;

        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        foreach (var e in _primaryIndex.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, transaction.TransactionId))
        {
            var doc = ReadDocumentAt(e.Location, transaction.TransactionId);
            if (doc != null) IndexInsert(entry, doc, e.Location, transaction);
        }

        PersistIndexMetadata();
    }

    /// <summary>Drops a secondary index by name.</summary>
    public bool DropIndex(string name)
    {
        if (!_secondaryIndexes.Remove(name))
            return false;

        PersistIndexMetadata();
        return true;
    }

    /// <summary>Lists all secondary index names.</summary>
    public IReadOnlyList<string> ListIndexes() => _secondaryIndexes.Keys.ToList();

    internal void PersistIndexMetadata()
    {
        var metadata = _storage.GetCollectionMetadata(_collectionName) ?? new CollectionMetadata { Name = _collectionName };
        metadata.PrimaryRootPageId = _primaryIndex.RootPageId;
        metadata.Indexes.Clear();

        foreach (var (name, idx) in _secondaryIndexes)
        {
            var idxMeta = new IndexMetadata
            {
                Name = name,
                RootPageId = idx.RootPageId,
                PropertyPaths = new[] { idx.FieldPath },
                IsUnique = false
            };

            switch (idx.Kind)
            {
                case DynamicIndexKind.BTree:
                    idxMeta.Type = IndexType.BTree;
                    idxMeta.IsUnique = idx.Options.Unique;
                    break;
                case DynamicIndexKind.Vector:
                    idxMeta.Type = IndexType.Vector;
                    idxMeta.Dimensions = idx.Options.Dimensions;
                    idxMeta.Metric = idx.Options.Metric;
                    break;
                case DynamicIndexKind.Spatial:
                    idxMeta.Type = IndexType.Spatial;
                    break;
            }

            metadata.Indexes.Add(idxMeta);
        }

        _storage.SaveCollectionMetadata(metadata);
    }

    // ── Index dispatch helpers ────────────────────────────────────────────────

    private void IndexInsert(DynamicSecondaryIndex idx, BsonDocument document, DocumentLocation location, ITransaction transaction)
    {
        if (!document.TryGetValue(idx.FieldPath, out var val)) return;

        switch (idx.Kind)
        {
            case DynamicIndexKind.BTree:
                var key = BsonValueToIndexKey(val);
                if (key.HasValue) idx.BTree!.Insert(key.Value, location, transaction.TransactionId);
                break;
            case DynamicIndexKind.Vector:
                var floats = ExtractFloatVector(val);
                if (floats != null) idx.Vector!.Insert(floats, location, transaction);
                break;
            case DynamicIndexKind.Spatial:
                var coords = ExtractCoordinates(val);
                if (coords.HasValue) idx.Spatial!.Insert(GeoBox.FromPoint(new GeoPoint(coords.Value.Lat, coords.Value.Lon)), location, transaction);
                break;
        }
    }

    private void IndexDelete(DynamicSecondaryIndex idx, BsonDocument document, DocumentLocation location, ITransaction transaction)
    {
        if (!document.TryGetValue(idx.FieldPath, out var val)) return;

        switch (idx.Kind)
        {
            case DynamicIndexKind.BTree:
                var key = BsonValueToIndexKey(val);
                if (key.HasValue) idx.BTree!.Delete(key.Value, location, transaction.TransactionId);
                break;
            // Vector and Spatial indexes do not support individual entry deletion;
            // their rebuild is handled at the collection level (compaction/reindex).
        }
    }

    #endregion

    #region Internal storage operations

    private BsonDocument? ReadDocumentAt(DocumentLocation location, ulong txnId)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            _storage.ReadPage(location.PageId, txnId, buffer);
            var header = SlottedPageHeader.ReadFrom(buffer);

            if (location.SlotIndex >= header.SlotCount)
                return null;

            var slotOffset = SlottedPageHeader.Size + (location.SlotIndex * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));

            if (slot.Flags.HasFlag(SlotFlags.Deleted))
                return null;

            // Copy document data (buffer is pooled, data must outlive it)
            var docData = buffer.AsSpan(slot.Offset, slot.Length).ToArray();
            return new BsonDocument(docData, _storage.GetKeyReverseMap());
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>Async version of <see cref="ReadDocumentAt"/>.</summary>
    private async ValueTask<BsonDocument?> ReadDocumentAtAsync(DocumentLocation location, ulong txnId, CancellationToken ct)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            await _storage.ReadPageAsync(location.PageId, txnId, buffer.AsMemory(0, _storage.PageSize), ct).ConfigureAwait(false);

            var header = SlottedPageHeader.ReadFrom(buffer);
            if (location.SlotIndex >= header.SlotCount) return null;

            var slotOffset = SlottedPageHeader.Size + (location.SlotIndex * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));
            if (slot.Flags.HasFlag(SlotFlags.Deleted)) return null;

            // Span created after the only await — safe; ToArray() copies before buffer is returned
            var docData = buffer.AsSpan(slot.Offset, slot.Length).ToArray();
            return new BsonDocument(docData, _storage.GetKeyReverseMap());
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>Async exact-match lookup.</summary>
    public async ValueTask<BsonDocument?> FindByIdAsync(BsonId id, CancellationToken ct = default)
    {
        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        var key = new IndexKey(id.ToBytes());
        var (found, location) = await _primaryIndex.TryFindAsync(key, transaction.TransactionId, ct).ConfigureAwait(false);
        if (!found) return null;
        return await ReadDocumentAtAsync(location, transaction.TransactionId, ct).ConfigureAwait(false);
    }

    /// <summary>Async full-collection scan.</summary>
    public async IAsyncEnumerable<BsonDocument> FindAllAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var transaction = _transactionHolder.GetCurrentTransactionOrStart();
        var txnId = transaction.TransactionId;

        await foreach (var entry in _primaryIndex
            .RangeAsync(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txnId, ct)
            .ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            var doc = await ReadDocumentAtAsync(entry.Location, txnId, ct).ConfigureAwait(false);
            if (doc != null) yield return doc;
        }
    }

    private uint FindPageWithSpace(int requiredBytes, ulong txnId)
    {
        if (_currentDataPage != 0)
        {
            if (_freeSpaceMap.TryGetValue(_currentDataPage, out var freeBytes) && freeBytes >= requiredBytes && !_storage.IsPageLocked(_currentDataPage, txnId))
                return _currentDataPage;
        }

        foreach (var (pageId, freeBytes) in _freeSpaceMap)
        {
            if (freeBytes >= requiredBytes && !_storage.IsPageLocked(pageId, txnId))
                return pageId;
        }
        return 0;
    }

    private uint AllocateNewDataPage(ITransaction transaction)
    {
        var pageId = _storage.AllocatePage();
        var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            buffer.AsSpan().Clear();
            var header = new SlottedPageHeader
            {
                PageId = pageId,
                PageType = PageType.Data,
                SlotCount = 0,
                FreeSpaceStart = SlottedPageHeader.Size,
                FreeSpaceEnd = (ushort)_storage.PageSize,
                NextOverflowPage = 0,
                TransactionId = 0
            };
            header.WriteTo(buffer);
            _storage.WritePage(pageId, transaction.TransactionId, buffer.AsSpan(0, _storage.PageSize));
            _freeSpaceMap[pageId] = (ushort)header.AvailableFreeSpace;
            _currentDataPage = pageId;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
        return pageId;
    }

    private ushort InsertIntoPage(uint pageId, ReadOnlySpan<byte> data, ITransaction transaction)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            _storage.ReadPage(pageId, transaction.TransactionId, buffer);
            var header = SlottedPageHeader.ReadFrom(buffer);

            if (header.PageType == PageType.Empty && header.FreeSpaceEnd == 0)
            {
                header = new SlottedPageHeader
                {
                    PageId = pageId,
                    PageType = PageType.Data,
                    SlotCount = 0,
                    FreeSpaceStart = SlottedPageHeader.Size,
                    FreeSpaceEnd = (ushort)_storage.PageSize,
                    TransactionId = (uint)transaction.TransactionId
                };
                header.WriteTo(buffer);
            }

            var requiredSpace = data.Length + SlotEntry.Size;
            if (header.AvailableFreeSpace < requiredSpace)
                throw new InvalidOperationException($"Not enough space in page {pageId}: need {requiredSpace}, have {header.AvailableFreeSpace}");

            // Find free slot
            ushort slotIndex = header.SlotCount;
            for (ushort i = 0; i < header.SlotCount; i++)
            {
                var so = SlottedPageHeader.Size + (i * SlotEntry.Size);
                var s = SlotEntry.ReadFrom(buffer.AsSpan(so));
                if (s.Flags.HasFlag(SlotFlags.Deleted))
                {
                    slotIndex = i;
                    break;
                }
            }

            // Write document data
            var docOffset = header.FreeSpaceEnd - data.Length;
            data.CopyTo(buffer.AsSpan(docOffset, data.Length));

            // Write slot entry
            var slotOffset = SlottedPageHeader.Size + (slotIndex * SlotEntry.Size);
            var slot = new SlotEntry
            {
                Offset = (ushort)docOffset,
                Length = (ushort)data.Length,
                Flags = SlotFlags.None
            };
            slot.WriteTo(buffer.AsSpan(slotOffset));

            // Update header
            if (slotIndex >= header.SlotCount)
                header.SlotCount = (ushort)(slotIndex + 1);
            header.FreeSpaceStart = (ushort)(SlottedPageHeader.Size + (header.SlotCount * SlotEntry.Size));
            header.FreeSpaceEnd = (ushort)docOffset;
            header.WriteTo(buffer);

            _storage.WritePage(pageId, transaction.TransactionId, buffer.AsSpan(0, _storage.PageSize));
            _freeSpaceMap[pageId] = (ushort)header.AvailableFreeSpace;

            return slotIndex;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private void DeleteSlot(DocumentLocation location, ITransaction transaction)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            _storage.ReadPage(location.PageId, transaction.TransactionId, buffer);
            var header = SlottedPageHeader.ReadFrom(buffer);

            if (location.SlotIndex < header.SlotCount)
            {
                var slotOffset = SlottedPageHeader.Size + (location.SlotIndex * SlotEntry.Size);
                var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));
                slot.Flags |= SlotFlags.Deleted;
                slot.WriteTo(buffer.AsSpan(slotOffset));
                header.WriteTo(buffer);
                _storage.WritePage(location.PageId, transaction.TransactionId, buffer.AsSpan(0, _storage.PageSize));
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static IndexKey? BsonValueToIndexKey(BsonValue value)
    {
        return value.Type switch
        {
            BsonType.Int32 => new IndexKey(value.AsInt32),
            BsonType.Int64 => new IndexKey(value.AsInt64),
            BsonType.String => new IndexKey(value.AsString),
            BsonType.ObjectId => new IndexKey(value.AsObjectId),
            BsonType.Double => new IndexKey(BitConverter.GetBytes(value.AsDouble)),
            _ => null // Can't index this type as BTree key
        };
    }

    /// <summary>Extracts a float[] from a BsonValue stored as a numeric BSON Array.</summary>
    private static float[]? ExtractFloatVector(BsonValue value)
    {
        if (value.Type != BsonType.Array) return null;
        var list = value.AsArray;
        if (list == null || list.Count == 0) return null;
        var result = new float[list.Count];
        for (int i = 0; i < list.Count; i++)
            result[i] = (float)list[i].AsDouble;
        return result;
    }

    /// <summary>Extracts (Lat, Lon) from a BsonValue stored as a BSON coordinates array.</summary>
    private static (double Lat, double Lon)? ExtractCoordinates(BsonValue value)
    {
        if (value.Type != BsonType.Array) return null;
        try { return value.AsCoordinates; }
        catch { return null; }
    }

    private static IndexKey CreateIndexKeyFromObject(object value) => value switch
    {
        int i => new IndexKey(i),
        long l => new IndexKey(l),
        string s => new IndexKey(s),
        ObjectId oid => new IndexKey(oid),
        Guid g => new IndexKey(g),
        double d => new IndexKey(BitConverter.GetBytes(d)),
        BsonId bid => new IndexKey(bid.ToBytes()),
        _ => throw new ArgumentException($"Cannot create IndexKey from type {value.GetType().Name}")
    };

    #endregion

    public void Dispose()
    {
        _collectionLock.Dispose();
    }
}
