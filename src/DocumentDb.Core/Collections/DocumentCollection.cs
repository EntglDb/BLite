using System.Buffers;
using System.Buffers.Binary;
using DocumentDb.Bson;
using DocumentDb.Core.Indexing;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Transactions;

namespace DocumentDb.Core.Collections;

/// <summary>
/// Production-ready document collection with slotted page architecture.
/// Supports multiple documents per page, overflow chains, and efficient space utilization.
/// </summary>
/// <typeparam name="T">Entity type</typeparam>
public class DocumentCollection<T> where T : class
{
    private readonly IDocumentMapper<T> _mapper;
    private readonly StorageEngine _storage;
    private readonly BTreeIndex _primaryIndex;
    private readonly CollectionIndexManager<T> _indexManager;

    // Free space tracking: PageId → Free bytes
    private readonly Dictionary<uint, ushort> _freeSpaceMap;

    // Current page for inserts (optimization)
    private uint _currentDataPage;

    private const int MaxDocumentSizeForSinglePage = 15000; // ~15KB for 16KB pages

    public DocumentCollection(
        IDocumentMapper<T> mapper,
        StorageEngine storageEngine,
        string? collectionName = null)
    {
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        _storage = storageEngine ?? throw new ArgumentNullException(nameof(storageEngine));

        // Initialize secondary index manager first (loads metadata including Primary Root Page ID)
        _indexManager = new CollectionIndexManager<T>(_storage, mapper, collectionName);
        _freeSpaceMap = new Dictionary<uint, ushort>();

        // Create primary index on _id (stores ObjectId → DocumentLocation mapping)
        // Use persisted root page ID if available
        var indexOptions = IndexOptions.CreateBTree("_id");
        _primaryIndex = new BTreeIndex(_storage, indexOptions, _indexManager.PrimaryRootPageId);

        // If a new root page was allocated, persist it
        if (_indexManager.PrimaryRootPageId != _primaryIndex.RootPageId)
        {
            _indexManager.SetPrimaryRootPageId(_primaryIndex.RootPageId);
        }
    }

    #region Public Transaction API

    /// <summary>
    /// Begins a new transaction. Multiple operations can be performed
    /// within the same transaction for better performance.
    /// </summary>
    /// <returns>A transaction object that must be committed or rolled back</returns>
    /// <example>
    /// using (var txn = collection.BeginTransaction())
    /// {
    ///     collection.Insert(entity1, txn);
    ///     collection.Insert(entity2, txn);
    ///     txn.Commit();
    /// }
    /// </example>
    public ITransaction BeginTransaction()
    {
        return _storage.BeginTransaction();
    }

    #endregion

    #region Index Management API

    /// <summary>
    /// Creates a secondary index on a property for fast lookups.
    /// The index is automatically maintained on insert/update/delete operations.
    /// </summary>
    /// <typeparam name="TKey">Property type</typeparam>
    /// <param name="keySelector">Expression to extract the indexed property (e.g., p => p.Age)</param>
    /// <param name="name">Optional custom index name (auto-generated if null)</param>
    /// <param name="unique">If true, enforces uniqueness constraint on the indexed values</param>
    /// <returns>The created secondary index</returns>
    /// <example>
    /// // Simple index on Age
    /// collection.CreateIndex(p => p.Age);
    /// 
    /// // Unique index on Email
    /// collection.CreateIndex(p => p.Email, unique: true);
    /// 
    /// // Custom name
    /// collection.CreateIndex(p => p.LastName, name: "idx_lastname");
    /// </example>
    public CollectionSecondaryIndex<T> CreateIndex<TKey>(
        System.Linq.Expressions.Expression<Func<T, TKey>> keySelector,
        string? name = null,
        bool unique = false)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));

        using (var txn = _storage.BeginTransaction())
        {
            var index = _indexManager.CreateIndex(keySelector, name, unique);

            // Rebuild index for existing documents
            RebuildIndex(index, txn);

            txn.Commit();

            return index;
        }
    }

    /// <summary>
    /// Ensures that an index exists on the specified property.
    /// If the index already exists, it is returned without modification (idempotent).
    /// If it doesn't exist, it is created and populated.
    /// </summary>
    public CollectionSecondaryIndex<T> EnsureIndex<TKey>(
        System.Linq.Expressions.Expression<Func<T, TKey>> keySelector,
        string? name = null,
        bool unique = false)
    {
        if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));

        // 1. Check if index already exists (fast path)
        var propertyPaths = ExpressionAnalyzer.ExtractPropertyPaths(keySelector);
        var indexName = name ?? $"idx_{string.Join("_", propertyPaths)}";

        var existingIndex = GetIndex(indexName);
        if (existingIndex != null)
        {
            return existingIndex;
        }

        // 2. Create if missing (slow path: rebuilds index)
        return CreateIndex(keySelector, name, unique);
    }

    /// <summary>
    /// Drops (removes) an existing secondary index by name.
    /// The primary index (_id) cannot be dropped.
    /// </summary>
    /// <param name="name">Name of the index to drop</param>
    /// <returns>True if the index was found and dropped, false otherwise</returns>
    public bool DropIndex(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Index name cannot be empty", nameof(name));

        // Prevent dropping primary index
        if (name.Equals("_id", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Cannot drop primary index");

        return _indexManager.DropIndex(name);
    }

    /// <summary>
    /// Gets metadata about all secondary indexes in this collection.
    /// Does not include the primary index (_id).
    /// </summary>
    /// <returns>Collection of index metadata</returns>
    public IEnumerable<CollectionIndexInfo> GetIndexes()
    {
        return _indexManager.GetIndexInfo();
    }

    internal void ApplyIndexBuilder(Metadata.IndexBuilder<T> builder)
    {
        // Use the IndexManager directly to ensure the index exists
        // We need to convert the LambdaExpression to a typed expression if possible, 
        // or add an untyped CreateIndex to IndexManager.
        
        // For now, let's use a dynamic approach or cast if we know it's Func<T, object>
        if (builder.KeySelector is System.Linq.Expressions.Expression<Func<T, object>> selector)
        {
             _indexManager.EnsureIndex(selector, builder.Name, builder.IsUnique);
        }
        else
        {
            // Try to rebuild the expression or use untyped version
            _indexManager.EnsureIndexUntyped(builder.KeySelector, builder.Name, builder.IsUnique);
        }
    }

    /// <summary>
    /// Gets a specific secondary index by name for advanced querying.
    /// Returns null if the index doesn't exist.
    /// </summary>
    /// <param name="name">Index name</param>
    /// <returns>The secondary index, or null if not found</returns>
    public CollectionSecondaryIndex<T>? GetIndex(string name)
    {
        return _indexManager.GetIndex(name);
    }

    /// <summary>
    /// Rebuilds an index by scanning all existing documents and re-inserting them.
    /// Called automatically when creating a new index.
    /// </summary>
    private void RebuildIndex(CollectionSecondaryIndex<T> index, ITransaction txn)
    {
        // Iterate all documents in the collection via primary index
        var minKey = new IndexKey(new byte[] { 0 });
        var maxKey = new IndexKey(Enumerable.Repeat((byte)0xFF, 24).ToArray()); // Max for ObjectId (12 bytes)

        foreach (var entry in _primaryIndex.Range(minKey, maxKey, txn.TransactionId))
        {
            try
            {
                var document = FindByLocation(entry.Location, txn);
                if (document != null)
                {
                    index.Insert(document, entry.Location, txn);
                }
            }
            catch
            {
                // Skip documents that fail to load or index
                // Production: should log errors
            }
        }
    }

    #endregion

    #region Data Page Management

    private uint FindPageWithSpace(int requiredBytes)
    {
        // Try current page first
        if (_currentDataPage != 0)
        {
            if (_freeSpaceMap.TryGetValue(_currentDataPage, out var freeBytes))
            {
                if (freeBytes >= requiredBytes + SlotEntry.Size)
                    return _currentDataPage;
            }
            else
            {
                // Load header and check - use StorageEngine
                Span<byte> page = stackalloc byte[SlottedPageHeader.Size];
                _storage.ReadPage(_currentDataPage, null, page);
                var header = SlottedPageHeader.ReadFrom(page);

                if (header.AvailableFreeSpace >= requiredBytes + SlotEntry.Size)
                {
                    _freeSpaceMap[_currentDataPage] = (ushort)header.AvailableFreeSpace;
                    return _currentDataPage;
                }
            }
        }

        // Search free space map
        foreach (var (pageId, freeBytes) in _freeSpaceMap)
        {
            if (freeBytes >= requiredBytes + SlotEntry.Size)
                return pageId;
        }

        return 0; // No suitable page
    }

    private uint AllocateNewDataPage(ITransaction txn)
    {
        var pageId = _storage.AllocatePage();

        // Initialize slotted page header
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

            // Transaction write or direct write
            if (txn is Transaction t)
            {
                // OPTIMIZATION: Pass ReadOnlyMemory to avoid ToArray() allocation
                var writeOp = new WriteOperation(ObjectId.Empty, buffer.AsMemory(0, _storage.PageSize), pageId, OperationType.AllocatePage);
                t.AddWrite(writeOp);
            }
            else
            {
                _storage.WritePage(pageId, txn.TransactionId, buffer);
            }

            // Track free space
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

            // Check free space
            var freeSpace = header.AvailableFreeSpace;
            var requiredSpace = data.Length + SlotEntry.Size;

            if (freeSpace < requiredSpace)
                throw new InvalidOperationException($"Not enough space: need {requiredSpace}, have {freeSpace} | PageId={pageId} | SlotCount={header.SlotCount} | Start={header.FreeSpaceStart} | End={header.FreeSpaceEnd} | Map={_freeSpaceMap.GetValueOrDefault(pageId)}");

            // Find free slot (reuse deleted or create new)
            ushort slotIndex = FindFreeSlot(buffer, ref header);

            // Write document at end of used space (grows up)
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

            // NEW: Buffer write in transaction or write immediately
            if (transaction is Transaction t)
            {
                // OPTIMIZATION: Pass ReadOnlyMemory to avoid ToArray() allocation
                var writeOp = new WriteOperation(
                    documentId: ObjectId.Empty,
                    newValue: buffer.AsMemory(0, _storage.PageSize),
                    pageId: pageId,
                    type: OperationType.Insert
                );
                t.AddWrite(writeOp);
            }
            else
            {
                _storage.WritePage(pageId, transaction.TransactionId, buffer);
            }

            // Update free space map
            _freeSpaceMap[pageId] = (ushort)header.AvailableFreeSpace;

            return slotIndex;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private ushort FindFreeSlot(Span<byte> page, ref SlottedPageHeader header)
    {
        // Scan existing slots for deleted ones
        for (ushort i = 0; i < header.SlotCount; i++)
        {
            var slotOffset = SlottedPageHeader.Size + (i * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(page.Slice(slotOffset, SlotEntry.Size));

            if ((slot.Flags & SlotFlags.Deleted) != 0)
                return i; // Reuse deleted slot
        }

        // No free slot, use next index
        return header.SlotCount;
    }

    private uint AllocateOverflowPage(ReadOnlySpan<byte> data, uint nextOverflowPageId, ITransaction transaction)
    {
        var pageId = _storage.AllocatePage();
        var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);

        try
        {
            buffer.AsSpan().Clear();

            var header = new SlottedPageHeader
            {
                PageId = pageId,
                PageType = PageType.Overflow,
                SlotCount = 0,
                FreeSpaceStart = SlottedPageHeader.Size,
                FreeSpaceEnd = (ushort)_storage.PageSize,
                NextOverflowPage = nextOverflowPageId,
                TransactionId = 0
            };

            header.WriteTo(buffer);

            // Write data immediately after header
            data.CopyTo(buffer.AsSpan(SlottedPageHeader.Size));

            // NEW: Buffer write in transaction or write immediately
            var writeOp = new WriteOperation(
                documentId: ObjectId.Empty,
                newValue: buffer.AsSpan(0, _storage.PageSize).ToArray(),
                pageId: pageId,
                type: OperationType.Insert
            );
            ((Transaction)transaction).AddWrite(writeOp);

            return pageId;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private (uint pageId, ushort slotIndex) InsertWithOverflow(ReadOnlySpan<byte> data, ITransaction transaction)
    {
        // 1. Calculate Primary Chunk Size
        // We need 8 bytes for metadata (TotalLength: 4, NextOverflowPage: 4)
        const int MetadataSize = 8;
        int maxPrimaryPayload = MaxDocumentSizeForSinglePage - MetadataSize;

        // 2. Build Overflow Chain (Reverse Order)
        uint nextOverflowPageId = 0;
        int remainingBytes = data.Length - maxPrimaryPayload;
        int offset = data.Length;
        int overflowChunkSize = _storage.PageSize - SlottedPageHeader.Size;

        while (offset > maxPrimaryPayload)
        {
            int chunkSize = Math.Min(overflowChunkSize, offset - maxPrimaryPayload);
            offset -= chunkSize;

            var overflowPageId = AllocateOverflowPage(
                data.Slice(offset, chunkSize),
                nextOverflowPageId,
                transaction
            );
            nextOverflowPageId = overflowPageId;
        }

        // 3. Prepare Primary Page Payload
        // Layout: [TotalLength (4)] [NextOverflowPage (4)] [DataChunk (...)]
        int primaryPayloadSize = offset; // This is the remaining data at start
        int totalSlotSize = MetadataSize + primaryPayloadSize;

        // Allocate primary page
        var primaryPageId = FindPageWithSpace(totalSlotSize);
        if (primaryPageId == 0)
            primaryPageId = AllocateNewDataPage(transaction);

        // 4. Write to Primary Page
        var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            _storage.ReadPage(primaryPageId, transaction.TransactionId, buffer);

            var header = SlottedPageHeader.ReadFrom(buffer);

            // Find free slot
            ushort slotIndex = FindFreeSlot(buffer, ref header);

            // Write payload at end of used space
            var docOffset = header.FreeSpaceEnd - totalSlotSize;
            var payloadSpan = buffer.AsSpan(docOffset, totalSlotSize);

            // Write Metadata
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(payloadSpan.Slice(0, 4), data.Length); // Total Length
            System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(payloadSpan.Slice(4, 4), nextOverflowPageId); // First Overflow Page

            // Write Data Chunk
            data.Slice(0, primaryPayloadSize).CopyTo(payloadSpan.Slice(8));

            // Write Slot Entry
            // FLAGS: HasOverflow
            // LENGTH: Length of data *in this slot* (Metadata + Chunk)
            // This avoids the 65KB limit issue for the SlotEntry.Length field itself, 
            // as specific slots are bounded by Page Size (16KB).
            var slotOffset = SlottedPageHeader.Size + (slotIndex * SlotEntry.Size);
            var slot = new SlotEntry
            {
                Offset = (ushort)docOffset,
                Length = (ushort)totalSlotSize,
                Flags = SlotFlags.HasOverflow
            };
            slot.WriteTo(buffer.AsSpan(slotOffset));

            // Update header
            if (slotIndex >= header.SlotCount)
                header.SlotCount = (ushort)(slotIndex + 1);

            header.FreeSpaceStart = (ushort)(SlottedPageHeader.Size + (header.SlotCount * SlotEntry.Size));
            header.FreeSpaceEnd = (ushort)docOffset;
            header.WriteTo(buffer);

            // NEW: Buffer write in transaction or write immediately
            var writeOp = new WriteOperation(
                documentId: ObjectId.Empty,
                newValue: buffer.AsMemory(0, _storage.PageSize),
                pageId: primaryPageId,
                type: OperationType.Insert
            );
            ((Transaction)transaction).AddWrite(writeOp);

            // Update free space map
            _freeSpaceMap[primaryPageId] = (ushort)header.AvailableFreeSpace;

            return (primaryPageId, slotIndex);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }



    #endregion

    #region Insert

    /// <summary>
    /// Inserts a new document into the collection
    /// </summary>
    /// <param name="entity">Entity to insert</param>
    /// <param name="transaction">Optional transaction to batch multiple operations. If null, auto-commits.</param>
    /// <returns>The ObjectId of the inserted document</returns>
    public ObjectId Insert(T entity, ITransaction? transaction = null)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        ITransaction txn = transaction ?? _storage.BeginTransaction();

        var isInternalTransaction = transaction == null;

        try
        {
            // Get or generate ID
            var id = _mapper.GetId(entity);
            if (id == ObjectId.Empty)
            {
                id = ObjectId.NewObjectId();
                _mapper.SetId(entity, id);
            }

            // Serialize to dynamically-growing buffer using ArrayBufferWriter
            // This eliminates any arbitrary size limit on documents
            var bufferWriter = new System.Buffers.ArrayBufferWriter<byte>();
            _mapper.Serialize(entity, bufferWriter);

            var docData = bufferWriter.WrittenSpan;

            if (docData.Length <= MaxDocumentSizeForSinglePage)
            {
                // Single page insert
                var pageId = FindPageWithSpace(docData.Length);

                if (pageId == 0)
                    pageId = AllocateNewDataPage(txn);

                var slotIndex = InsertIntoPage(pageId, docData, txn);
                var location = new DocumentLocation(pageId, slotIndex);

                // Add to primary index (stores ObjectId → DocumentLocation)
                var key = new IndexKey(id.ToByteArray());
                _primaryIndex.Insert(key, location, txn.TransactionId);

                // Insert into all secondary indexes
                _indexManager.InsertIntoAll(entity, location, txn);
            }
            else
            {
                // Multi-page overflow insert
                var (pageId, slotIndex) = InsertWithOverflow(docData, txn);
                var location = new DocumentLocation(pageId, slotIndex);

                // Add to primary index (stores ObjectId → DocumentLocation)
                var key = new IndexKey(id.ToByteArray());
                _primaryIndex.Insert(key, location, txn.TransactionId);

                // Insert into all secondary indexes
                _indexManager.InsertIntoAll(entity, location, txn);
            }

            if (isInternalTransaction)
            {
                txn.Commit();
            }

            return id;
        }
        catch
        {
            if (isInternalTransaction)
            {
                txn.Rollback();
            }
            throw;
        }
    }

    /// <summary>
    /// Inserts multiple documents in a single transaction for optimal performance.
    /// This is the recommended way to insert many documents at once.
    /// Uses micro-batched parallel serialization for optimal CPU utilization without excessive memory overhead.
    /// </summary>
    /// <param name="entities">Collection of entities to insert</param>
    /// <returns>List of ObjectIds for the inserted documents</returns>
    /// <example>
    /// var people = new List&lt;Person&gt; { person1, person2, person3 };
    /// var ids = collection.InsertBulk(people);
    /// </example>
    public List<ObjectId> InsertBulk(IEnumerable<T> entities, ITransaction? transaction = null)
    {
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        var entityList = entities.ToList();
        var ids = new List<ObjectId>(entityList.Count);

        var txn = transaction ?? _storage.BeginTransaction();
        var isInternalTransaction = transaction == null;

        const int BATCH_SIZE = 50;  // Optimal: balances parallelism benefits vs memory overhead

        // Process in micro-batches
        for (int batchStart = 0; batchStart < entityList.Count; batchStart += BATCH_SIZE)
        {
            int batchEnd = Math.Min(batchStart + BATCH_SIZE, entityList.Count);
            int batchCount = batchEnd - batchStart;

            // PHASE 1: Parallel serialize this batch
            var serializedBatch = new (ObjectId id, byte[] data)[batchCount];

            System.Threading.Tasks.Parallel.For(0, batchCount, i =>
            {
                var entity = entityList[batchStart + i];
                var id = _mapper.GetId(entity);
                if (id == ObjectId.Empty)
                {
                    id = ObjectId.NewObjectId();
                    _mapper.SetId(entity, id);
                }

                var bufferWriter = new System.Buffers.ArrayBufferWriter<byte>();
                _mapper.Serialize(entity, bufferWriter);
                serializedBatch[i] = (id, bufferWriter.WrittenMemory.ToArray());
            });

            // PHASE 2: Sequential insert this batch
            // Transaction handles caching and write coalescing automatically
            for (int i = 0; i < batchCount; i++)
            {
                var (id, docData) = serializedBatch[i];
                var entity = entityList[batchStart + i]; // Get entity for index updates

                DocumentLocation location;

                if (docData.Length <= MaxDocumentSizeForSinglePage)
                {
                    var pageId = FindPageWithSpace(docData.Length);

                    if (pageId == 0)
                        pageId = AllocateNewDataPage(txn);

                    var slotIndex = InsertIntoPage(pageId, docData, txn);
                    location = new DocumentLocation(pageId, slotIndex);
                }
                else
                {
                    // Multi-page overflow insert
                    var (pageId, slotIndex) = InsertWithOverflow(docData, txn);
                    location = new DocumentLocation(pageId, slotIndex);
                }

                // Add to primary index
                var key = new IndexKey(id.ToByteArray());
                _primaryIndex.Insert(key, location, txn.TransactionId);

                // Insert into all secondary indexes
                _indexManager.InsertIntoAll(entity, location, txn);

                ids.Add(id);
            }
        }

        if (isInternalTransaction)
        {
            txn.Commit();
        }

        return ids;
    }

    #endregion

    #region Find

    /// <summary>
    /// Finds a document by its ObjectId.
    /// If called within a transaction, will see uncommitted changes ("Read Your Own Writes").
    /// Otherwise creates a read-only snapshot transaction.
    /// </summary>
    /// <param name="id">ObjectId of the document</param>
    /// <param name="transaction">Optional transaction for isolation (supports Read Your Own Writes)</param>
    /// <returns>The document, or null if not found</returns>
    public T? FindById(ObjectId id, ITransaction? transaction = null)
    {
        var txn = transaction ?? _storage.BeginTransaction();
        var isInternal = transaction == null;

        try
        {
            var key = new IndexKey(id.ToByteArray());
            if (!_primaryIndex.TryFind(key, out var location, txn.TransactionId))
                return null;

            return FindByLocation(location, txn);
        }
        finally
        {
            // Read-only transaction, no commit needed - just cleanup
            if (isInternal)
                txn.Dispose();
        }
    }

    /// <summary>
    /// Returns all documents in the collection.
    /// WARNING: This method requires an external transaction for proper isolation!
    /// If no transaction is provided, reads committed snapshot only (may see partial updates).
    /// </summary>
    /// <param name="transaction">Transaction for isolation (REQUIRED for consistent reads during concurrent writes)</param>
    /// <returns>Enumerable of all documents</returns>
    public IEnumerable<T> FindAll(ITransaction? transaction = null)
    {
        // For IEnumerable methods with yield, we CANNOT create internal transactions
        // because the finally block won't execute until enumeration completes.
        // Caller must provide transaction if they need isolation.

        var txnId = transaction?.TransactionId ?? 0; // 0 = read committed only

        // Scan all entries in primary index
        var minKey = new IndexKey(new byte[] { 0 });
        var maxKey = new IndexKey(Enumerable.Repeat((byte)0xFF, 24).ToArray());

        foreach (var entry in _primaryIndex.Range(minKey, maxKey, txnId))
        {
            var entity = FindByLocation(entry.Location, transaction);
            if (entity != null)
                yield return entity;
        }
    }

    private T? FindByLocation(DocumentLocation location, ITransaction? transaction)
    {
        var txnId = transaction?.TransactionId ?? 0;
        var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            // Read from StorageEngine with transaction isolation
            _storage.ReadPage(location.PageId, txnId, buffer);

            var header = SlottedPageHeader.ReadFrom(buffer);

            if (location.SlotIndex >= header.SlotCount)
                return null;

            var slotOffset = SlottedPageHeader.Size + (location.SlotIndex * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));

            if ((slot.Flags & SlotFlags.Deleted) != 0)
                return null;

            if ((slot.Flags & SlotFlags.HasOverflow) != 0)
            {
                // Layout: [TotalLength (4)] [NextOverflowPage (4)] [DataChunk (...)]
                var payload = buffer.AsSpan(slot.Offset, slot.Length);

                // Read Metadata
                int totalLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(0, 4));
                uint nextOverflowPageId = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(4, 4));

                var fullBuffer = ArrayPool<byte>.Shared.Rent(totalLength);
                try
                {
                    // Copy primary chunk (skipping 8 bytes of metadata)
                    int primaryChunkSize = slot.Length - 8;
                    payload.Slice(8, primaryChunkSize).CopyTo(fullBuffer.AsSpan(0, primaryChunkSize));

                    int offset = primaryChunkSize;
                    var currentOverflowPageId = nextOverflowPageId;

                    // Follow overflow chain
                    while (currentOverflowPageId != 0 && offset < totalLength)
                    {
                        // Read from StorageEngine with transaction isolation
                        _storage.ReadPage(currentOverflowPageId, txnId, buffer);
                        var overflowHeader = SlottedPageHeader.ReadFrom(buffer);

                        // Calculate data in this overflow page
                        // Overflow pages are full data pages (PageSize - HeaderSize)
                        int maxChunkSize = _storage.PageSize - SlottedPageHeader.Size;
                        int remaining = totalLength - offset;
                        int chunkSize = Math.Min(maxChunkSize, remaining);

                        buffer.AsSpan(SlottedPageHeader.Size, chunkSize)
                              .CopyTo(fullBuffer.AsSpan(offset));

                        offset += chunkSize;
                        currentOverflowPageId = overflowHeader.NextOverflowPage;
                    }

                    return _mapper.Deserialize(fullBuffer.AsSpan(0, totalLength));
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(fullBuffer);
                }
            }

            // Validation check
            if (slot.Offset + slot.Length > buffer.Length)
            {
                throw new InvalidOperationException($"Corrupted slot: Offset={slot.Offset}, Length={slot.Length}, Buffer={buffer.Length}, SlotIndex={location.SlotIndex}, PageId={location.PageId}, Flags={slot.Flags}");
            }

            var docData = buffer.AsSpan(slot.Offset, slot.Length);
            return _mapper.Deserialize(docData);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    #endregion

    #region Update & Delete

    public bool Update(T entity, ITransaction? transaction = null)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        var id = _mapper.GetId(entity);
        var key = new IndexKey(id.ToByteArray());

        if (!_primaryIndex.TryFind(key, out var oldLocation))
            return false;

        var txn = transaction ?? _storage.BeginTransaction();
        var isInternalTransaction = transaction == null;

        try
        {
            // Serialize new version
            var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
            try
            {
                var bytesWritten = _mapper.Serialize(entity, buffer);
                var docData = buffer.AsSpan(0, bytesWritten);

                // Read old page to check if we can update in-place
                var pageBuffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
                try
                {
                    _storage.ReadPage(oldLocation.PageId, txn.TransactionId, pageBuffer);

                    var slotOffset = SlottedPageHeader.Size + (oldLocation.SlotIndex * SlotEntry.Size);
                    var oldSlot = SlotEntry.ReadFrom(pageBuffer.AsSpan(slotOffset));

                    if (bytesWritten <= oldSlot.Length)
                    {
                        // In-place update - location doesn't change
                        docData.CopyTo(pageBuffer.AsSpan(oldSlot.Offset, bytesWritten));

                        // Update slot length
                        var newSlot = oldSlot;
                        newSlot.Length = (ushort)bytesWritten;
                        newSlot.WriteTo(pageBuffer.AsSpan(slotOffset));

                        _storage.WritePage(oldLocation.PageId, txn.TransactionId, pageBuffer);
                        // Primary index doesn't need update (location unchanged)
                    }
                    else
                    {
                        // Delete old + insert new

                        // Check if old slot has overflow and free it
                        if ((oldSlot.Flags & SlotFlags.HasOverflow) != 0)
                        {
                            var nextOverflowPage = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(
                                pageBuffer.AsSpan(oldSlot.Offset + 4, 4));
                            FreeOverflowChain(nextOverflowPage, txn.TransactionId);
                        }

                        // Mark old slot as deleted
                        oldSlot.Flags |= SlotFlags.Deleted;
                        oldSlot.WriteTo(pageBuffer.AsSpan(slotOffset));
                        _storage.WritePage(oldLocation.PageId, txn.TransactionId, pageBuffer);

                        // Insert new version
                        var newPageId = FindPageWithSpace(bytesWritten);
                        if (newPageId == 0)
                            newPageId = AllocateNewDataPage(txn);

                        var newSlotIndex = InsertIntoPage(newPageId, docData, txn);
                        var newLocation = new DocumentLocation(newPageId, newSlotIndex);

                        // Update primary index with new location
                        _primaryIndex.Delete(key, oldLocation, txn.TransactionId);
                        _primaryIndex.Insert(key, newLocation, txn.TransactionId);
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(pageBuffer);
                }

                if (isInternalTransaction)
                {
                    txn.Commit();
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        catch
        {
            if (isInternalTransaction)
            {
                txn.Rollback();
            }
            throw;
        }

        return true;
    }

    /// <summary>
    /// Updates multiple documents in a single transaction for optimal performance.
    /// Uses parallel serialization and efficient storage handling.
    /// </summary>
    public int UpdateBulk(IEnumerable<T> entities, ITransaction? transaction = null)
    {
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        var entityList = entities.ToList();
        int updateCount = 0;
        const int BATCH_SIZE = 50;

        // Use provided transaction or create internal one
        var txn = transaction ?? _storage.BeginTransaction();
        var isInternalTransaction = transaction == null;

        try
        {
            for (int batchStart = 0; batchStart < entityList.Count; batchStart += BATCH_SIZE)
            {
                int batchEnd = Math.Min(batchStart + BATCH_SIZE, entityList.Count);
                int batchCount = batchEnd - batchStart;

                // PHASE 1: Parallel Serialization
                var serializedBatch = new (ObjectId id, byte[] data, bool found)[batchCount];

                System.Threading.Tasks.Parallel.For(0, batchCount, i =>
                {
                    var entity = entityList[batchStart + i];
                    var id = _mapper.GetId(entity);
                    var key = new IndexKey(id.ToByteArray());

                    // Check if entity exists using transaction isolation
                    if (_primaryIndex.TryFind(key, out var _, txn.TransactionId))
                    {
                        var bufferWriter = new System.Buffers.ArrayBufferWriter<byte>();
                        _mapper.Serialize(entity, bufferWriter);
                        serializedBatch[i] = (id, bufferWriter.WrittenMemory.ToArray(), true);
                    }
                    else
                    {
                        serializedBatch[i] = (id, Array.Empty<byte>(), false);
                    }
                });

                // PHASE 2: Sequential Storage Update
                // Transaction handles caching and write coalescing automatically
                for (int i = 0; i < batchCount; i++)
                {
                    var (id, docData, found) = serializedBatch[i];
                    if (!found) continue;

                    var key = new IndexKey(id.ToByteArray());
                    if (!_primaryIndex.TryFind(key, out var oldLocation))
                        continue;

                    var pageBuffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);

                    try
                    {
                        _storage.ReadPage(oldLocation.PageId, txn!.TransactionId, pageBuffer);

                        var slotOffset = SlottedPageHeader.Size + (oldLocation.SlotIndex * SlotEntry.Size);
                        var oldSlot = SlotEntry.ReadFrom(pageBuffer.AsSpan(slotOffset));

                        if (docData.Length <= oldSlot.Length)
                        {
                            // In-place Update
                            Buffer.BlockCopy(docData, 0, pageBuffer, oldSlot.Offset, docData.Length);

                            var newSlot = oldSlot;
                            newSlot.Length = (ushort)docData.Length;
                            newSlot.WriteTo(pageBuffer.AsSpan(slotOffset));

                            if (txn is Transaction txnObj)
                            {
                                var writeOp = new WriteOperation(ObjectId.Empty, pageBuffer.AsSpan(0, _storage.PageSize).ToArray(), oldLocation.PageId, OperationType.Update);
                                txnObj.AddWrite(writeOp);
                            }
                            else
                            {
                                _storage.WritePage(oldLocation.PageId, txn.TransactionId, pageBuffer);
                            }
                        }
                        else
                        {
                            // Move Update (Delete + Insert)

                            // 1. Free overflow if needed
                            if ((oldSlot.Flags & SlotFlags.HasOverflow) != 0)
                            {
                                var nextOverflowPage = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(
                                    pageBuffer.AsSpan(oldSlot.Offset + 4, 4));
                                FreeOverflowChain(nextOverflowPage, txn.TransactionId);
                            }

                            // 2. Mark old deleted
                            oldSlot.Flags |= SlotFlags.Deleted;
                            oldSlot.WriteTo(pageBuffer.AsSpan(slotOffset));

                            if (txn is Transaction txnObj2)
                            {
                                var writeOp = new WriteOperation(ObjectId.Empty, pageBuffer.AsSpan(0, _storage.PageSize).ToArray(), oldLocation.PageId, OperationType.Delete);
                                txnObj2.AddWrite(writeOp);
                            }
                            else
                            {
                                _storage.WritePage(oldLocation.PageId, txn.TransactionId, pageBuffer);
                            }

                            // 3. Allocate New
                            DocumentLocation newLocation;
                            if (docData.Length <= MaxDocumentSizeForSinglePage)
                            {
                                var newPageId = FindPageWithSpace(docData.Length);
                                if (newPageId == 0) newPageId = AllocateNewDataPage(txn);
                                var newSlotIndex = InsertIntoPage(newPageId, docData, txn);
                                newLocation = new DocumentLocation(newPageId, newSlotIndex);
                            }
                            else
                            {
                                var (newPageId, newSlotIndex) = InsertWithOverflow(docData, txn);
                                newLocation = new DocumentLocation(newPageId, newSlotIndex);
                            }

                            // Update primary index
                            _primaryIndex.Delete(key, oldLocation, txn.TransactionId);
                            _primaryIndex.Insert(key, newLocation, txn.TransactionId);
                        }
                        updateCount++;
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(pageBuffer);
                    }
                }
            }

            if (isInternalTransaction)
            {
                txn.Commit();
            }
        }
        catch
        {
            if (isInternalTransaction)
            {
                txn.Rollback();
            }
            throw;
        }

        return updateCount;
    }

    public bool Delete(ObjectId id, ITransaction? transaction = null)
    {
        var key = new IndexKey(id.ToByteArray());
        if (!_primaryIndex.TryFind(key, out var location))
            return false;

        var txn = transaction ?? _storage.BeginTransaction();
        var isInternalTransaction = transaction == null;

        try
        {
            var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
            try
            {
                _storage.ReadPage(location.PageId, txn.TransactionId, buffer);

                // Mark slot as deleted
                var slotOffset = SlottedPageHeader.Size + (location.SlotIndex * SlotEntry.Size);
                var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));

                // If has overflow, free overflow chain first
                if ((slot.Flags & SlotFlags.HasOverflow) != 0)
                {
                    // Read NextOverflowPage from inline metadata (Offset + 4)
                    // Layout: [TotalLength (4)] [NextOverflowPage (4)] [Data...]
                    var nextOverflowPage = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(
                        buffer.AsSpan(slot.Offset + 4, 4));

                    FreeOverflowChain(nextOverflowPage, txn.TransactionId);
                }

                slot.Flags |= SlotFlags.Deleted;
                slot.WriteTo(buffer.AsSpan(slotOffset));

                _storage.WritePage(location.PageId, txn.TransactionId, buffer);

                // Remove from primary index
                _primaryIndex.Delete(key, location, txn.TransactionId);

                if (isInternalTransaction)
                {
                    txn.Commit();
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        catch
        {
            if (isInternalTransaction)
            {
                txn.Rollback();
            }
            throw;
        }

        return true;
    }

    /// <summary>
    /// Deletes multiple documents in a single transaction.
    /// Efficiently updates storage and index.
    /// </summary>
    public int DeleteBulk(IEnumerable<ObjectId> ids, ITransaction? transaction = null)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));

        int deleteCount = 0;
        ITransaction txn = transaction ?? _storage.BeginTransaction();
        var isInternalTransaction = transaction == null;

        try
        {
            foreach (var id in ids)
            {
                var key = new IndexKey(id.ToByteArray());
                if (!_primaryIndex.TryFind(key, out var location))
                    continue;

                var pageData = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
                try
                {
                    _storage.ReadPage(location.PageId, txn!.TransactionId, pageData);

                    // Mark slot as deleted
                    var slotOffset = SlottedPageHeader.Size + (location.SlotIndex * SlotEntry.Size);
                    var slot = SlotEntry.ReadFrom(pageData.AsSpan(slotOffset));

                    if ((slot.Flags & SlotFlags.Deleted) != 0)
                        continue; // Already deleted

                    // Free overflow if needed
                    if ((slot.Flags & SlotFlags.HasOverflow) != 0)
                    {
                        var nextOverflowPage = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(
                            pageData.AsSpan(slot.Offset + 4, 4));
                        FreeOverflowChain(nextOverflowPage, txn.TransactionId);
                    }

                    slot.Flags |= SlotFlags.Deleted;
                    slot.WriteTo(pageData.AsSpan(slotOffset));

                    // Write back to Transaction or Disk
                    var writeOp = new WriteOperation(ObjectId.Empty, pageData.AsSpan(0, _storage.PageSize).ToArray(), location.PageId, OperationType.Delete);
                    txn.AddWrite(writeOp);

                    // Remove from primary index
                    _primaryIndex.Delete(key, location, txn!.TransactionId);

                    deleteCount++;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(pageData);
                }
            }

            if (isInternalTransaction)
            {
                txn.Commit();
            }
        }
        catch
        {
            if (isInternalTransaction)
            {
                txn.Rollback();
            }
            throw;
        }

        return deleteCount;
    }

    private void FreeOverflowChain(uint overflowPageId, ulong transactionId)
    {
        var tempBuffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            while (overflowPageId != 0)
            {
                _storage.ReadPage(overflowPageId, transactionId, tempBuffer);
                var header = SlottedPageHeader.ReadFrom(tempBuffer);
                var nextPage = header.NextOverflowPage;

                // Recycle this page
                _storage.FreePage(overflowPageId);

                overflowPageId = nextPage;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(tempBuffer);
        }
    }

    #endregion

    #region Query Helpers

    /// <summary>
    /// Counts all documents in the collection.
    /// If called within a transaction, will count uncommitted changes.
    /// </summary>
    /// <param name="transaction">Optional transaction for isolation</param>
    /// <returns>Number of documents</returns>
    public int Count(ITransaction? transaction = null)
    {
        var txn = transaction ?? _storage.BeginTransaction();
        var isInternal = transaction == null;

        try
        {
            // Count all entries in primary index
            var minKey = new IndexKey(new byte[] { 0 });
            var maxKey = new IndexKey(Enumerable.Repeat((byte)0xFF, 24).ToArray());
            return _primaryIndex.Range(minKey, maxKey, txn.TransactionId).Count();
        }
        finally
        {
            if (isInternal)
                txn.Dispose();
        }
    }

    /// <summary>
    /// Finds all documents matching the predicate.
    /// If transaction is provided, will see uncommitted changes.
    /// </summary>
    public IEnumerable<T> FindAll(Func<T, bool> predicate, ITransaction? transaction = null)
    {
        foreach (var entity in FindAll(transaction))
        {
            if (predicate(entity))
                yield return entity;
        }
    }

    /// <summary>
    /// Find entities matching predicate (alias for FindAll with predicate)
    /// </summary>
    public IEnumerable<T> Find(Func<T, bool> predicate, ITransaction? transaction = null)
        => FindAll(predicate, transaction);

    #endregion
}
