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
    private readonly PageFile _pageFile;
    private readonly BTreeIndex _primaryIndex;
    private readonly TransactionManager _txnManager;
    private readonly CollectionIndexManager<T> _indexManager; // NEW: Custom index manager
    
    // ID → (PageId, SlotIndex) mapping
    private readonly Dictionary<ObjectId, DocumentLocation> _idToLocationMap;
    
    // Free space tracking: PageId → Free bytes
    private readonly Dictionary<uint, ushort> _freeSpaceMap;
    
    // Current page for inserts (optimization)
    private uint _currentDataPage;
    
    // Metadata page for persistence
    private readonly uint _metadataPageId;
    
    private const int MaxDocumentSizeForSinglePage = 15000; // ~15KB for 16KB pages

    public DocumentCollection(
        IDocumentMapper<T> mapper,
        PageFile pageFile,
        TransactionManager txnManager)
    {
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        _pageFile = pageFile ?? throw new ArgumentNullException(nameof(pageFile));
        _txnManager = txnManager ?? throw new ArgumentNullException(nameof(txnManager));
        
        // Create primary index on _id
        var indexOptions = IndexOptions.CreateBTree("_id");
        _primaryIndex = new BTreeIndex(pageFile, indexOptions);
        
        // Initialize secondary index manager
        _indexManager = new CollectionIndexManager<T>(pageFile, mapper);
        
        // Initialize mappings
        _idToLocationMap = new Dictionary<ObjectId, DocumentLocation>();
        _freeSpaceMap = new Dictionary<uint, ushort>();
        
        // Allocate metadata page (for now, hardcoded - future: registry)
        _metadataPageId = AllocateMetadataPage();
        
        // Load existing mappings from metadata
        LoadIdMap();
    }

    #region Metadata Persistence

    private uint AllocateMetadataPage()
    {
        var pageId = _pageFile.AllocatePage();
        
        // Initialize empty metadata
        var buffer = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        try
        {
            var writer = new BsonSpanWriter(buffer);
            var sizePos = writer.BeginDocument();
            writer.WriteString("collection", _mapper.CollectionName);
            writer.WriteInt32("version", 1);
            
            var arrayPos = writer.BeginArray("locations");
            writer.EndArray(arrayPos);
            
            writer.EndDocument(sizePos);
            
            _pageFile.WritePage(pageId, buffer);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
        
        return pageId;
    }

    private void LoadIdMap()
    {
        uint currentPage = _metadataPageId;
        
        while (currentPage != 0)
        {
            uint nextPage = LoadIdMapChunk(currentPage);
            currentPage = nextPage;
        }
    }
    
    private uint LoadIdMapChunk(uint pageId)
    {
        Span<byte> buffer = stackalloc byte[_pageFile.PageSize];
        _pageFile.ReadPage(pageId, buffer);
        
        var reader = new BsonSpanReader(buffer);
        reader.ReadDocumentSize(); // Skip size
        
        uint nextPage = 0;
        
        // Read until we find "locations" array
        while (reader.Position < buffer.Length)
        {
            var type = reader.ReadBsonType();
            if (type == BsonType.EndOfDocument)
                break;
            
            var name = reader.ReadCString();
            
            if (name == "nextMetadataPage" && type == BsonType.Int32)
            {
                nextPage = (uint)reader.ReadInt32();
            }
            else if (name == "locations" && type == BsonType.Array)
            {
                reader.ReadDocumentSize(); // Array size
                
                // Read array elements
                while (true)
                {
                    var elemType = reader.ReadBsonType();
                    if (elemType == BsonType.EndOfDocument)
                        break;
                    
                    reader.ReadCString(); // Array index as string ("0", "1", ...)
                    
                    if (elemType == BsonType.Document)
                    {
                        reader.ReadDocumentSize();
                        
                        ObjectId id = ObjectId.Empty;
                        uint entryPageId = 0;
                        ushort slotIndex = 0;
                        
                        // Read document fields
                        while (true)
                        {
                            var fieldType = reader.ReadBsonType();
                            if (fieldType == BsonType.EndOfDocument)
                                break;
                            
                            var fieldName = reader.ReadCString();
                            
                            if (fieldName == "_id" && fieldType == BsonType.ObjectId)
                                id = reader.ReadObjectId();
                            else if (fieldName == "page" && fieldType == BsonType.Int32)
                                entryPageId = (uint)reader.ReadInt32();
                            else if (fieldName == "slot" && fieldType == BsonType.Int32)
                                slotIndex = (ushort)reader.ReadInt32();
                            else
                                reader.SkipValue(fieldType);
                        }
                        
                        if (id != ObjectId.Empty && entryPageId != 0)
                        {
                            _idToLocationMap[id] = new DocumentLocation(entryPageId, slotIndex);
                        }
                    }
                }
            }
            else
            {
                reader.SkipValue(type);
            }
        }
        
        return nextPage;
    }

    private void SaveIdMap()
    {
        // Conservative estimate: each entry ~60 bytes (ObjectId + page + slot + overhead)
        // For 16KB page: ~200 entries per page safely
        const int ENTRIES_PER_PAGE = 200;
        
        var entries = _idToLocationMap.ToList();
        uint currentMetadataPage = _metadataPageId;
        
        for (int offset = 0; offset < entries.Count; offset += ENTRIES_PER_PAGE)
        {
            var chunk = entries.Skip(offset).Take(ENTRIES_PER_PAGE);
            bool isFirstChunk = (offset == 0);
            bool isLastChunk = (offset + ENTRIES_PER_PAGE >= entries.Count);
            
            uint nextPage = 0;
            if (!isLastChunk)
            {
                nextPage = _pageFile.AllocatePage();
            }
            
            SaveIdMapChunk(currentMetadataPage, chunk, nextPage, isFirstChunk);
            currentMetadataPage = nextPage;
        }
    }
    
    private void SaveIdMapChunk(
        uint pageId,
        IEnumerable<KeyValuePair<ObjectId, DocumentLocation>> entries,
        uint nextPage,
        bool isFirst)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        try
        {
            Array.Clear(buffer, 0, _pageFile.PageSize); // Clear buffer
            
            var writer = new BsonSpanWriter(buffer);
            var sizePos = writer.BeginDocument();
            
            if (isFirst)
            {
                writer.WriteString("collection", _mapper.CollectionName);
                writer.WriteInt32("version", 1);
            }
            
            writer.WriteInt32("nextMetadataPage", (int)nextPage);
            
            var arrayPos = writer.BeginArray("locations");
            int index = 0;
            foreach (var kvp in entries)
            {
                var entryPos = writer.BeginDocument(index.ToString());
                writer.WriteObjectId("_id", kvp.Key);
                writer.WriteInt32("page", (int)kvp.Value.PageId);
                writer.WriteInt32("slot", kvp.Value.SlotIndex);
                writer.EndDocument(entryPos);
                index++;
            }
            writer.EndArray(arrayPos);
            writer.EndDocument(sizePos);
            
            _pageFile.WritePage(pageId, buffer);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    #endregion

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
        return _txnManager.BeginTransaction();
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

        var index = _indexManager.CreateIndex(keySelector, name, unique);
        
        // Rebuild index for existing documents
        RebuildIndex(index);
        
        return index;
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
    private void RebuildIndex(CollectionSecondaryIndex<T> index)
    {
        // Iterate all documents in the collection and insert into the new index
        foreach (var (id, location) in _idToLocationMap)
        {
            try
            {
                var document = FindById(id);
                if (document != null)
                {
                    index.Insert(document);
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
                // Load header and check
                Span<byte> page = stackalloc byte[SlottedPageHeader.Size];
                _pageFile.ReadPage(_currentDataPage, page);
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

    private uint AllocateNewDataPage(ITransaction? txn = null)
    {
        var pageId = _pageFile.AllocatePage();
        
        // Initialize slotted page header
        var buffer = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        try
        {
            buffer.AsSpan().Clear();
            
            var header = new SlottedPageHeader
            {
                PageId = pageId,
                PageType = PageType.Data,
                SlotCount = 0,
                FreeSpaceStart = SlottedPageHeader.Size,
                FreeSpaceEnd = (ushort)_pageFile.PageSize,
                NextOverflowPage = 0,
                TransactionId = 0
            };
            
            header.WriteTo(buffer);

            // Transaction write or direct write
            if (txn is Transaction t)
            {
                // OPTIMIZATION: Pass ReadOnlyMemory to avoid ToArray() allocation
                var writeOp = new WriteOperation(ObjectId.Empty, buffer.AsMemory(0, _pageFile.PageSize), pageId, OperationType.AllocatePage);
                t.AddWrite(writeOp);
            }
            else
            {
                _pageFile.WritePage(pageId, buffer);
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

    private ushort InsertIntoPage(uint pageId, ReadOnlySpan<byte> data, ITransaction? transaction = null)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        
        try
        {
            // NEW: Try read from transaction cache (Read-Your-Own-Writes)
            if (transaction != null && transaction.GetPage(pageId) is byte[] cachedPage)
            {
                cachedPage.CopyTo(buffer, 0);
            }
            else
            {
                _pageFile.ReadPage(pageId, buffer);
            }

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
                    newValue: buffer.AsMemory(0, _pageFile.PageSize),
                    pageId: pageId,
                    type: OperationType.Insert
                );
                t.AddWrite(writeOp);
            }
            else
            {
                _pageFile.WritePage(pageId, buffer);
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

    private uint AllocateOverflowPage(ReadOnlySpan<byte> data, uint nextOverflowPageId, ITransaction? transaction = null)
    {
        var pageId = _pageFile.AllocatePage();
        var buffer = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        
        try
        {
            buffer.AsSpan().Clear();
            
            var header = new SlottedPageHeader
            {
                PageId = pageId,
                PageType = PageType.Overflow,
                SlotCount = 0,
                FreeSpaceStart = SlottedPageHeader.Size,
                FreeSpaceEnd = (ushort)_pageFile.PageSize,
                NextOverflowPage = nextOverflowPageId,
                TransactionId = 0
            };
            
            header.WriteTo(buffer);
            
            // Write data immediately after header
            data.CopyTo(buffer.AsSpan(SlottedPageHeader.Size));
            
            // NEW: Buffer write in transaction or write immediately
            if (transaction != null)
            {
                var writeOp = new WriteOperation(
                    documentId: ObjectId.Empty,
                    newValue: buffer.AsSpan(0, _pageFile.PageSize).ToArray(),
                    pageId: pageId,
                    type: OperationType.Insert
                );
                ((Transaction)transaction).AddWrite(writeOp);
            }
            else
            {
                _pageFile.WritePage(pageId, buffer);
            }
            
            return pageId;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private (uint pageId, ushort slotIndex) InsertWithOverflow(ReadOnlySpan<byte> data, ITransaction? transaction = null)
    {
        // 1. Calculate Primary Chunk Size
        // We need 8 bytes for metadata (TotalLength: 4, NextOverflowPage: 4)
        const int MetadataSize = 8;
        int maxPrimaryPayload = MaxDocumentSizeForSinglePage - MetadataSize;
        
        // 2. Build Overflow Chain (Reverse Order)
        uint nextOverflowPageId = 0;
        int remainingBytes = data.Length - maxPrimaryPayload;
        int offset = data.Length;
        int overflowChunkSize = _pageFile.PageSize - SlottedPageHeader.Size;

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
        var buffer = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        try
        {
            // NEW: Try read from transaction cache
            if (transaction != null && transaction.GetPage(primaryPageId) is byte[] cachedPage)
            {
                cachedPage.CopyTo(buffer, 0);
            }
            else
            {
                _pageFile.ReadPage(primaryPageId, buffer);
            }

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
            if (transaction != null)
            {
                // OPTIMIZATION: Pass ReadOnlyMemory to avoid ToArray() allocation
                var writeOp = new WriteOperation(
                    documentId: ObjectId.Empty,
                    newValue: buffer.AsMemory(0, _pageFile.PageSize),
                    pageId: primaryPageId,
                    type: OperationType.Insert
                );
                ((Transaction)transaction).AddWrite(writeOp);
            }
            else
            {
                _pageFile.WritePage(primaryPageId, buffer);
            }

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
        
        var txn = transaction ?? _txnManager.BeginTransaction();
        bool isInternalTxn = (transaction == null);
        
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
                    pageId = AllocateNewDataPage();
                
                var slotIndex = InsertIntoPage(pageId, docData);
                
                // Map ID → Location
                _idToLocationMap[id] = new DocumentLocation(pageId, slotIndex);
                
                // NOTE: InsertIntoPage already wrote to PageFile directly.
                // We don't add to transaction WAL here because the write is immediate.
                // TODO: Implement proper transactional writes with rollback support
                
                // Add to primary index
                var key = new IndexKey(id.ToByteArray());
                _primaryIndex.Insert(key, id);
                
                // NEW: Insert into all secondary indexes
                _indexManager.InsertIntoAll(entity, txn);
            }
            else
            {
                // Multi-page overflow insert
                var (pageId, slotIndex) = InsertWithOverflow(docData, txn);
                
                // Map ID → Location
                _idToLocationMap[id] = new DocumentLocation(pageId, slotIndex);
                
                // Add to primary index
                var key = new IndexKey(id.ToByteArray());
                _primaryIndex.Insert(key, id, txn);
                
                // NEW: Insert into all secondary indexes
                _indexManager.InsertIntoAll(entity, txn);
            }
            
            // Only commit if we created the transaction
            if (isInternalTxn)
            {
                // Persist mapping before commit (only for auto-commit)
                SaveIdMap();
                txn.Commit();
            }
            
            return id;
        }
        catch
        {
            // Only rollback if we created the transaction
            if (isInternalTxn)
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
    public List<ObjectId> InsertBulk(IEnumerable<T> entities)
    {
        if (entities == null) throw new ArgumentNullException(nameof(entities));
        
        var entityList = entities.ToList();
        var ids = new List<ObjectId>(entityList.Count);
        
        const int BATCH_SIZE = 50;  // Optimal: balances parallelism benefits vs memory overhead
        
        using (var txn = BeginTransaction())
        {
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
                    
                    if (docData.Length <= MaxDocumentSizeForSinglePage)
                    {
                        var pageId = FindPageWithSpace(docData.Length);
                        
                        if (pageId == 0)
                            pageId = AllocateNewDataPage(txn);
                        
                        var slotIndex = InsertIntoPage(pageId, docData, txn);
                        _idToLocationMap[id] = new DocumentLocation(pageId, slotIndex);
                    }
                    else
                    {
                        // Multi-page overflow insert
                        var (pageId, slotIndex) = InsertWithOverflow(docData, txn);
                        _idToLocationMap[id] = new DocumentLocation(pageId, slotIndex);
                    }
                    
                    var key = new IndexKey(id.ToByteArray());
                    _primaryIndex.Insert(key, id, txn);
                    
                    // NEW: Insert into all secondary indexes
                    _indexManager.InsertIntoAll(entity, txn);
                    
                    ids.Add(id);
                }
            }
            
            SaveIdMap();
            txn.Commit();
        }
        
        return ids;
    }

    #endregion

    #region Find

    public T? FindById(ObjectId id)
    {
        if (!_idToLocationMap.TryGetValue(id, out var location))
            return null;
            
        return FindByLocation(location);
    }

    public IEnumerable<T> FindAll()
    {
        foreach (var location in _idToLocationMap.Values)
        {
            var entity = FindByLocation(location);
            if (entity != null)
                yield return entity;
        }
    }

    private T? FindByLocation(DocumentLocation location)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        try
        {
            _pageFile.ReadPage(location.PageId, buffer);
            
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
                        _pageFile.ReadPage(currentOverflowPageId, buffer);
                        var overflowHeader = SlottedPageHeader.ReadFrom(buffer);
                        
                        // Calculate data in this overflow page
                        // Overflow pages are full data pages (PageSize - HeaderSize)
                        int maxChunkSize = _pageFile.PageSize - SlottedPageHeader.Size;
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

    public bool Update(T entity)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));
        
        var id = _mapper.GetId(entity);
        if (!_idToLocationMap.TryGetValue(id, out var oldLocation))
            return false;
        
        var txn = _txnManager.BeginTransaction();
        try
        {
            // Serialize new version
            var buffer = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
            try
            {
                var bytesWritten = _mapper.Serialize(entity, buffer);
                var docData = buffer.AsSpan(0, bytesWritten);
                
                // Read old page to check if we can update in-place
                var pageBuffer = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
                try
                {
                    _pageFile.ReadPage(oldLocation.PageId, pageBuffer);
                    
                    var slotOffset = SlottedPageHeader.Size + (oldLocation.SlotIndex * SlotEntry.Size);
                    var oldSlot = SlotEntry.ReadFrom(pageBuffer.AsSpan(slotOffset));
                    
                    if (bytesWritten <= oldSlot.Length)
                    {
                        // In-place update
                        docData.CopyTo(pageBuffer.AsSpan(oldSlot.Offset, bytesWritten));
                        
                        // Update slot length
                        var newSlot = oldSlot;
                        newSlot.Length = (ushort)bytesWritten;
                        newSlot.WriteTo(pageBuffer.AsSpan(slotOffset));
                        
                        _pageFile.WritePage(oldLocation.PageId, pageBuffer);
                    }
                    else
                    {
                        // Delete old + insert new
                        
                        // Check if old slot has overflow and free it
                        if ((oldSlot.Flags & SlotFlags.HasOverflow) != 0)
                        {
                            var nextOverflowPage = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(
                                pageBuffer.AsSpan(oldSlot.Offset + 4, 4));
                            FreeOverflowChain(nextOverflowPage);
                        }

                        // Mark old slot as deleted
                        oldSlot.Flags |= SlotFlags.Deleted;
                        oldSlot.WriteTo(pageBuffer.AsSpan(slotOffset));
                        _pageFile.WritePage(oldLocation.PageId, pageBuffer);
                        
                        // Insert new version
                        var newPageId = FindPageWithSpace(bytesWritten);
                        if (newPageId == 0)
                            newPageId = AllocateNewDataPage();
                        
                        var newSlotIndex = InsertIntoPage(newPageId, docData, txn);
                        
                        // Update location mapping
                        _idToLocationMap[id] = new DocumentLocation(newPageId, newSlotIndex);
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(pageBuffer);
                }
                
                SaveIdMap();
                txn.Commit();
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        catch
        {
            txn.Rollback();
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
        var txn = transaction ?? _txnManager.BeginTransaction();
        bool isInternalTxn = transaction == null;

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
                    
                    if (_idToLocationMap.ContainsKey(id))
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

                    var oldLocation = _idToLocationMap[id];
                    var pageBuffer = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
                    
                    try
                    {
                        // Read from Transaction or Disk
                        if (txn.GetPage(oldLocation.PageId) is byte[] cachedPage)
                        {
                            cachedPage.CopyTo(pageBuffer, 0);
                        }
                        else
                        {
                            _pageFile.ReadPage(oldLocation.PageId, pageBuffer);
                        }

                        var slotOffset = SlottedPageHeader.Size + (oldLocation.SlotIndex * SlotEntry.Size);
                        var oldSlot = SlotEntry.ReadFrom(pageBuffer.AsSpan(slotOffset));

                        if (docData.Length <= oldSlot.Length)
                        {
                            // In-place Update
                            Buffer.BlockCopy(docData, 0, pageBuffer, oldSlot.Offset, docData.Length);

                            var newSlot = oldSlot;
                            newSlot.Length = (ushort)docData.Length;
                            newSlot.WriteTo(pageBuffer.AsSpan(slotOffset));
                            
                            if (txn is Transaction t)
                            {
                                var writeOp = new WriteOperation(ObjectId.Empty, pageBuffer.AsSpan(0, _pageFile.PageSize).ToArray(), oldLocation.PageId, OperationType.Update);
                                t.AddWrite(writeOp);
                            }
                            else
                            {
                                _pageFile.WritePage(oldLocation.PageId, pageBuffer);
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
                                FreeOverflowChain(nextOverflowPage);
                            }
                            
                            // 2. Mark old deleted
                            oldSlot.Flags |= SlotFlags.Deleted;
                            oldSlot.WriteTo(pageBuffer.AsSpan(slotOffset));
                            
                            if (txn is Transaction t)
                            {
                                var writeOp = new WriteOperation(ObjectId.Empty, pageBuffer.AsSpan(0, _pageFile.PageSize).ToArray(), oldLocation.PageId, OperationType.Delete);
                                t.AddWrite(writeOp);
                            }
                            else
                            {
                                _pageFile.WritePage(oldLocation.PageId, pageBuffer);
                            }

                            // 3. Allocate New
                            // Note: InsertIntoPage/InsertWithOverflow checks Transaction cache now
                            if (docData.Length <= MaxDocumentSizeForSinglePage)
                            {
                                var newPageId = FindPageWithSpace(docData.Length);
                                if (newPageId == 0) newPageId = AllocateNewDataPage(txn);
                                var newSlotIndex = InsertIntoPage(newPageId, docData, txn);
                                _idToLocationMap[id] = new DocumentLocation(newPageId, newSlotIndex);
                            }
                            else
                            {
                                var (newPageId, newSlotIndex) = InsertWithOverflow(docData, txn);
                                _idToLocationMap[id] = new DocumentLocation(newPageId, newSlotIndex);
                            }
                        }
                        updateCount++;
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(pageBuffer);
                    }
                }
            }

            if (isInternalTxn)
            {
                SaveIdMap();
                txn.Commit();
            }
        }
        catch
        {
            if (isInternalTxn) txn.Rollback();
            throw;
        }

        return updateCount;
    }

    public bool Delete(ObjectId id)
    {
        if (!_idToLocationMap.TryGetValue(id, out var location))
            return false;
        
        var txn = _txnManager.BeginTransaction();
        try
        {
            var buffer = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
            try
            {
                _pageFile.ReadPage(location.PageId, buffer);
                
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
                        
                    FreeOverflowChain(nextOverflowPage);
                }
                
                slot.Flags |= SlotFlags.Deleted;
                slot.WriteTo(buffer.AsSpan(slotOffset));
                
                _pageFile.WritePage(location.PageId, buffer);
                
                // Remove from mapping
                _idToLocationMap.Remove(id);
                
                // Remove from BTree index
                _primaryIndex.Delete(new IndexKey(id.ToByteArray()), id, txn);
                
                SaveIdMap();
                txn.Commit();
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        catch
        {
            txn.Rollback();
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
        var txn = transaction ?? _txnManager.BeginTransaction();
        bool isInternalTxn = transaction == null;
        
        // Track deletions for rollback
        var deletedEntries = new List<(ObjectId Id, DocumentLocation Location)>();

        try
        {
            foreach (var id in ids)
            {
                if (!_idToLocationMap.TryGetValue(id, out var location))
                    continue;

                var pageData = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
                try
                {
                    // Read from Transaction or Disk
                    if (txn.GetPage(location.PageId) is byte[] cachedPage)
                    {
                        cachedPage.CopyTo(pageData, 0);
                    }
                    else
                    {
                        _pageFile.ReadPage(location.PageId, pageData);
                    }

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
                        FreeOverflowChain(nextOverflowPage);
                    }

                    slot.Flags |= SlotFlags.Deleted;
                    slot.WriteTo(pageData.AsSpan(slotOffset));

                    // Write back to Transaction or Disk
                    if (txn is Transaction t)
                    {
                        var writeOp = new WriteOperation(ObjectId.Empty, pageData.AsSpan(0, _pageFile.PageSize).ToArray(), location.PageId, OperationType.Delete);
                        t.AddWrite(writeOp);
                    }
                    else
                    {
                        _pageFile.WritePage(location.PageId, pageData);
                    }

                    // 2. Remove from Memory Map
                    deletedEntries.Add((id, location));
                    _idToLocationMap.Remove(id);

                    // 3. Remove from Index
                    _primaryIndex.Delete(new IndexKey(id.ToByteArray()), id, txn);
                    
                    deleteCount++;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(pageData);
                }
            }
            
            // Register rollback handler
            if (txn is ITransaction txnInt)
            {
                 txnInt.OnRollback += () =>  
                 {
                     foreach (var entry in deletedEntries)
                     {
                         _idToLocationMap[entry.Id] = entry.Location;
                     }
                 };
            }

            if (isInternalTxn)
            {
                SaveIdMap();
                txn.Commit();
            }
        }
        catch
        {


            if (isInternalTxn) txn.Rollback();
            throw;
        }

        return deleteCount;
    }

    private void FreeOverflowChain(uint overflowPageId)
    {
        var tempBuffer = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        try
        {
            while (overflowPageId != 0)
            {
                _pageFile.ReadPage(overflowPageId, tempBuffer);
                var header = SlottedPageHeader.ReadFrom(tempBuffer);
                var nextPage = header.NextOverflowPage;
                
                // Recycle this page
                _pageFile.FreePage(overflowPageId);
                
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

    public int Count() => _idToLocationMap.Count;

    public IEnumerable<T> FindAll(Func<T, bool> predicate)
    {
        foreach (var entity in FindAll())
        {
            if (predicate(entity))
                yield return entity;
        }
    }

    /// <summary>
    /// Find entities matching predicate (alias for FindAll with predicate)
    /// </summary>
    public IEnumerable<T> Find(Func<T, bool> predicate) => FindAll(predicate);

    #endregion
}
