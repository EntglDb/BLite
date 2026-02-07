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
        Span<byte> buffer = stackalloc byte[_pageFile.PageSize];
        _pageFile.ReadPage(_metadataPageId, buffer);
        
        var reader = new BsonSpanReader(buffer);
        reader.ReadDocumentSize(); // Skip size
        
        // Read until we find "locations" array
        while (reader.Position < buffer.Length)
        {
            var type = reader.ReadBsonType();
            if (type == BsonType.EndOfDocument)
                break;
            
            var name = reader.ReadCString();
            
            if (name == "locations" && type == BsonType.Array)
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
                        uint pageId = 0;
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
                                pageId = (uint)reader.ReadInt32();
                            else if (fieldName == "slot" && fieldType == BsonType.Int32)
                                slotIndex = (ushort)reader.ReadInt32();
                            else
                                reader.SkipValue(fieldType);
                        }
                        
                        if (id != ObjectId.Empty && pageId != 0)
                        {
                            _idToLocationMap[id] = new DocumentLocation(pageId, slotIndex);
                        }
                    }
                }
                break;
            }
            else
            {
                reader.SkipValue(type);
            }
        }
    }

    private void SaveIdMap()
    {
        var buffer = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        try
        {
            var writer = new BsonSpanWriter(buffer);
            var sizePos = writer.BeginDocument();
            writer.WriteString("collection", _mapper.CollectionName);
            writer.WriteInt32("version", 1);
            
            var arrayPos = writer.BeginArray("locations");
            int index = 0;
            foreach (var kvp in _idToLocationMap)
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
            
            _pageFile.WritePage(_metadataPageId, buffer);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    #endregion

    #region Slotted Page Operations

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

    private uint AllocateNewDataPage()
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
            _pageFile.WritePage(pageId, buffer);
            
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

    private ushort InsertIntoPage(uint pageId, ReadOnlySpan<byte> data)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        try
        {
            _pageFile.ReadPage(pageId, buffer);
            
            var header = SlottedPageHeader.ReadFrom(buffer);
            
            // Check free space
            var freeSpace = header.AvailableFreeSpace;
            var requiredSpace = data.Length + SlotEntry.Size;
            
            if (freeSpace < requiredSpace)
                throw new InvalidOperationException($"Not enough space: need {requiredSpace}, have {freeSpace}");
            
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
            
            _pageFile.WritePage(pageId, buffer);
            
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

    private uint AllocateOverflowPage(ReadOnlySpan<byte> data, uint nextOverflowPageId)
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
            
            _pageFile.WritePage(pageId, buffer);
            return pageId;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private (uint pageId, ushort slotIndex) InsertWithOverflow(ReadOnlySpan<byte> data)
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
                nextOverflowPageId
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
            primaryPageId = AllocateNewDataPage();

        // 4. Write to Primary Page
        var buffer = ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        try
        {
            _pageFile.ReadPage(primaryPageId, buffer);
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

            _pageFile.WritePage(primaryPageId, buffer);

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

    public ObjectId Insert(T entity)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));
        
        var txn = _txnManager.BeginTransaction();
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
            }
            else
            {
                // Multi-page overflow insert
                var (pageId, slotIndex) = InsertWithOverflow(docData);
                
                // Map ID → Location
                _idToLocationMap[id] = new DocumentLocation(pageId, slotIndex);
                
                // Add to primary index
                var key = new IndexKey(id.ToByteArray());
                _primaryIndex.Insert(key, id);
            }
            
            // Persist mapping
            SaveIdMap();
            
            txn.Commit();
            return id;
        }
        catch
        {
            txn.Rollback();
            throw;
        }
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
                        
                        var newSlotIndex = InsertIntoPage(newPageId, docData);
                        
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
                
                // TODO: Remove from BTree index
                // _primaryIndex.Delete(new IndexKey(id.ToByteArray()));
                
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

    /// <summary>
    /// Insert multiple entities in bulk and return count
    /// </summary>
    public int InsertBulk(IEnumerable<T> entities)
    {
        int count = 0;
        foreach (var entity in entities)
        {
            Insert(entity);
            count++;
        }
        return count;
    }

    #endregion
}
