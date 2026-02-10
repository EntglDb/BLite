using System.Buffers;
using System.Buffers.Binary;
using DocumentDb.Bson;
using DocumentDb.Core.Indexing;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Transactions;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;

[assembly: InternalsVisibleTo("DocumentDb.Tests")]

namespace DocumentDb.Core.Collections;

public class DocumentCollection<T> : DocumentCollection<ObjectId, T> where T : class
{
    public DocumentCollection(StorageEngine storage, IDocumentMapper<T> mapper, string? collectionName = null)
        : base(storage, mapper, collectionName)
    {
    }
}

/// <summary>
/// Production-ready document collection with slotted page architecture.
/// Supports multiple documents per page, overflow chains, and efficient space utilization.
/// <summary>
/// Represents a collection of documents of type T with an ID of type TId.
/// </summary>
/// <typeparam name="TId">Type of the primary key</typeparam>
/// <typeparam name="T">Type of the entity</typeparam>
public class DocumentCollection<TId, T> where T : class
{
    private readonly StorageEngine _storage;
    private readonly IDocumentMapper<TId, T> _mapper;
    private readonly BTreeIndex _primaryIndex;
    private readonly CollectionIndexManager<TId, T> _indexManager;
    private readonly string _collectionName;

    // Free space tracking: PageId → Free bytes
    private readonly Dictionary<uint, ushort> _freeSpaceMap;

    // Current page for inserts (optimization)
    private uint _currentDataPage;

    private const int MaxDocumentSizeForSinglePage = 15000; // ~15KB for 16KB pages

    public DocumentCollection(StorageEngine storage, IDocumentMapper<TId, T> mapper, string? collectionName = null)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        _collectionName = collectionName ?? _mapper.CollectionName;

        // Initialize secondary index manager first (loads metadata including Primary Root Page ID)
        _indexManager = new CollectionIndexManager<TId, T>(_storage, _mapper, _collectionName);
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
    public CollectionSecondaryIndex<TId, T> CreateIndex<TKey>(
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
    public CollectionSecondaryIndex<TId, T> EnsureIndex<TKey>(
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
    /// Scans the entire collection using a raw BSON predicate.
    /// This avoids deserializing documents that don't match the criteria.
    /// </summary>
    /// <param name="predicate">Function to evaluate raw BSON data</param>
    /// <param name="transaction">Optional transaction for isolation</param>
    /// <returns>Matching documents</returns>
    public IEnumerable<T> Scan(Func<BsonSpanReader, bool> predicate, ITransaction? transaction = null)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        var txnId = transaction?.TransactionId ?? 0;
        var pageCount = _storage.PageCount;
        var buffer = new byte[_storage.PageSize];
        var pageResults = new List<T>();

        for (uint pageId = 0; pageId < pageCount; pageId++)
        {
            pageResults.Clear();
            ScanPage(pageId, txnId, buffer, predicate, pageResults, transaction);
            
            foreach (var doc in pageResults)
            {
                yield return doc;
            }
        }
    }

    /// <summary>
    /// Scans the collection in parallel using multiple threads.
    /// Useful for large collections on multi-core machines.
    /// </summary>
    /// <param name="predicate">Function to evaluate raw BSON data</param>
    /// <param name="transaction">Optional transaction for isolation</param>
    /// <param name="degreeOfParallelism">Number of threads to use (default: -1 = ProcessorCount)</param>
    public IEnumerable<T> ParallelScan(Func<BsonSpanReader, bool> predicate, ITransaction? transaction = null, int degreeOfParallelism = -1)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        var txnId = transaction?.TransactionId ?? 0;
        var pageCount = (int)_storage.PageCount;
        
        if (degreeOfParallelism <= 0)
            degreeOfParallelism = Environment.ProcessorCount;

        return Partitioner.Create(0, pageCount)
            .AsParallel()
            .WithDegreeOfParallelism(degreeOfParallelism)
            .SelectMany(range => 
            {
                var localBuffer = new byte[_storage.PageSize];
                var localResults = new List<T>();
                
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    ScanPage((uint)i, txnId, localBuffer, predicate, localResults, transaction);
                }
                return localResults; 
                // Wait: SelectMany iterates the returned IEnumerable. 
                // If I return localResults here, it means for each range, I return a LIST of all results in that range.
                // But localResults is cleared in the loop!
                // ERROR: I am clearing localResults in the loop. I need to accumulate results for the range.
                // Correct logic: Accumulate for the whole range, then return.
            });
    }

    private void ScanPage(uint pageId, ulong txnId, byte[] buffer, Func<BsonSpanReader, bool> predicate, List<T> results, ITransaction? transaction)
    {
        _storage.ReadPage(pageId, txnId, buffer);
        var header = SlottedPageHeader.ReadFrom(buffer);

        // Only scan Data pages
        if (header.PageType != PageType.Data)
            return;

        // Iterate slots
        var slots = MemoryMarshal.Cast<byte, SlotEntry>(
            buffer.AsSpan(SlottedPageHeader.Size, header.SlotCount * SlotEntry.Size));

        for (int i = 0; i < header.SlotCount; i++)
        {
            var slot = slots[i];

            if (slot.Flags.HasFlag(SlotFlags.Deleted))
                continue;

            // For now, skip overflow documents in scan optimization 
            var data = buffer.AsSpan(slot.Offset, slot.Length);
            var reader = new BsonSpanReader(data);

            if (predicate(reader))
            {
                var doc = FindByLocation(new DocumentLocation(pageId, (ushort)i), transaction);
                if (doc != null)
                    results.Add(doc);
            }
        }
    }

    /// <summary>
    /// Gets a specific secondary index by name for advanced querying.
    /// Returns null if the index doesn't exist.
    /// </summary>
    /// <param name="name">Index name</param>
    /// <returns>The secondary index, or null if not found</returns>
    public CollectionSecondaryIndex<TId, T>? GetIndex(string name)
    {
        return _indexManager.GetIndex(name);
    }

    /// <summary>
    /// Rebuilds an index by scanning all existing documents and re-inserting them.
    /// Called automatically when creating a new index.
    /// </summary>
    private void RebuildIndex(CollectionSecondaryIndex<TId, T> index, ITransaction txn)
    {
        // Iterate all documents in the collection via primary index
        var minKey = new IndexKey(Array.Empty<byte>());
        var maxKey = new IndexKey(Enumerable.Repeat((byte)0xFF, 32).ToArray());

        foreach (var entry in _primaryIndex.Range(minKey, maxKey, IndexDirection.Forward, txn.TransactionId))
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
        // We must ensure that pages closer to Primary are FULL (PageSize-Header),
        // and only the last page (tail) is partial. This matches FindByLocation greedy reading.
        
        uint nextOverflowPageId = 0;
        int overflowChunkSize = _storage.PageSize - SlottedPageHeader.Size;
        int totalOverflowBytes = data.Length - maxPrimaryPayload;
        
        if (totalOverflowBytes > 0) 
        {
            int tailSize = totalOverflowBytes % overflowChunkSize;
            int fullPages = totalOverflowBytes / overflowChunkSize;

            // 2a. Handle Tail (if any) - This is the highest offset
            if (tailSize > 0)
            {
                int tailOffset = maxPrimaryPayload + (fullPages * overflowChunkSize);
                var overflowPageId = AllocateOverflowPage(
                    data.Slice(tailOffset, tailSize),
                    nextOverflowPageId, // Points to 0 (or previous tail if we had one? No, 0)
                    transaction
                );
                nextOverflowPageId = overflowPageId;
            }
            else if (fullPages > 0)
            {
                 // If no tail, nextId starts at 0.
            }

            // 2b. Handle Full Pages (Reverse order)
            // Iterate from last full page down to first full page
            for (int i = fullPages - 1; i >= 0; i--)
            {
                int chunkOffset = maxPrimaryPayload + (i * overflowChunkSize);
                var overflowPageId = AllocateOverflowPage(
                    data.Slice(chunkOffset, overflowChunkSize),
                    nextOverflowPageId,
                    transaction
                );
                nextOverflowPageId = overflowPageId;
            }
        }

        // 3. Prepare Primary Page Payload
        // Layout: [TotalLength (4)] [NextOverflowPage (4)] [DataChunk (...)]
        // Since we are in InsertWithOverflow, we know data.Length > maxPrimaryPayload
        int primaryPayloadSize = maxPrimaryPayload;
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
    public TId Insert(T entity, ITransaction? transaction = null)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        ITransaction txn = transaction ?? _storage.BeginTransaction();
        var isInternalTransaction = transaction == null;

        try
        {
            // Get or generate ID
            var id = _mapper.GetId(entity);
            if (EqualityComparer<TId>.Default.Equals(id, default))
            {
                if (typeof(TId) == typeof(ObjectId))
                {
                    id = (TId)(object)ObjectId.NewObjectId();
                    _mapper.SetId(entity, id);
                }
                else if (typeof(TId) == typeof(Guid))
                {
                    id = (TId)(object)Guid.NewGuid();
                    _mapper.SetId(entity, id);
                }
            }

            // Serialize
            // var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
            // var bytesWritten = _mapper.Serialize(entity, buffer);
            var bytesWritten = SerializeWithRetry(entity, out var buffer);
            var docData = buffer.AsSpan(0, bytesWritten);

            DocumentLocation location;
            try
            {
                if (docData.Length <= MaxDocumentSizeForSinglePage)
                {
                    var pageId = FindPageWithSpace(docData.Length);
                    if (pageId == 0) pageId = AllocateNewDataPage(txn);
                    var slotIndex = InsertIntoPage(pageId, docData, txn);
                    location = new DocumentLocation(pageId, slotIndex);
                }
                else
                {
                    var (pageId, slotIndex) = InsertWithOverflow(docData, txn);
                    location = new DocumentLocation(pageId, slotIndex);
                }

                // Primary index update using IndexKey from TId
                var key = _mapper.ToIndexKey(id);
                _primaryIndex.Insert(key, location, txn.TransactionId);

                // Notify secondary indexes
                _indexManager.InsertIntoAll(entity, location, txn);

                if (isInternalTransaction) txn.Commit();
                return id;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        catch
        {
            if (isInternalTransaction) txn.Rollback();
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
    public List<TId> InsertBulk(IEnumerable<T> entities, ITransaction? transaction = null)
    {
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        var entityList = entities.ToList();
        var ids = new List<TId>(entityList.Count);

        var txn = transaction ?? _storage.BeginTransaction();
        var isInternalTransaction = transaction == null;

        const int BATCH_SIZE = 50;

        for (int batchStart = 0; batchStart < entityList.Count; batchStart += BATCH_SIZE)
        {
            int batchEnd = Math.Min(batchStart + BATCH_SIZE, entityList.Count);
            int batchCount = batchEnd - batchStart;

            // PHASE 1: Parallel serialize this batch
            var serializedBatch = new (TId id, byte[] data, int length)[batchCount];

            System.Threading.Tasks.Parallel.For(0, batchCount, i =>
            {
                var entity = entityList[batchStart + i];
                var id = _mapper.GetId(entity);
                if (EqualityComparer<TId>.Default.Equals(id, default))
                {
                    if (typeof(TId) == typeof(ObjectId)) id = (TId)(object)ObjectId.NewObjectId();
                    else if (typeof(TId) == typeof(Guid)) id = (TId)(object)Guid.NewGuid();
                    _mapper.SetId(entity, id);
                }

                // var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
                // var length = _mapper.Serialize(entity, buffer);
                // serializedBatch[i] = (id, buffer, length);
                // Use SerializeWithRetry
                var length = SerializeWithRetry(entity, out var buffer);
                serializedBatch[i] = (id, buffer, length);
            });

            // PHASE 2: Sequential insert this batch
            for (int i = 0; i < batchCount; i++)
            {
                var (id, buffer, length) = serializedBatch[i];
                var docData = buffer.AsSpan(0, length);
                var entity = entityList[batchStart + i];

                try
                {
                    DocumentLocation location;
                    if (docData.Length <= MaxDocumentSizeForSinglePage)
                    {
                        var pageId = FindPageWithSpace(docData.Length);
                        if (pageId == 0) pageId = AllocateNewDataPage(txn);
                        var slotIndex = InsertIntoPage(pageId, docData, txn);
                        location = new DocumentLocation(pageId, slotIndex);
                    }
                    else
                    {
                        var (pageId, slotIndex) = InsertWithOverflow(docData, txn);
                        location = new DocumentLocation(pageId, slotIndex);
                    }

                    var key = _mapper.ToIndexKey(id);
                    _primaryIndex.Insert(key, location, txn.TransactionId);
                    _indexManager.InsertIntoAll(entity, location, txn);
                    ids.Add(id);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
        }

        if (isInternalTransaction) txn.Commit();
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
    public T? FindById(TId id, ITransaction? transaction = null)
    {
        using var txn = transaction ?? _storage.BeginTransaction();
        var key = _mapper.ToIndexKey(id);

        if (!_primaryIndex.TryFind(key, out var location, txn.TransactionId))
            return null;

        return FindByLocation(location, txn);
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
        var txnId = transaction?.TransactionId ?? 0;
        var minKey = new IndexKey(Array.Empty<byte>());
        var maxKey = new IndexKey(Enumerable.Repeat((byte)0xFF, 32).ToArray());

        foreach (var entry in _primaryIndex.Range(minKey, maxKey, IndexDirection.Forward, txnId))
        {
            var entity = FindByLocation(entry.Location, transaction);
            if (entity != null)
                yield return entity;
        }
    }

    internal T? FindByLocation(DocumentLocation location, ITransaction? transaction)
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
        var key = _mapper.ToIndexKey(id);

        using var txn = transaction ?? _storage.BeginTransaction();
        try
        {
            if (!_primaryIndex.TryFind(key, out var oldLocation, txn.TransactionId))
                return false;

            // Retrieve old version for index updates
            var oldEntity = FindByLocation(oldLocation, txn);
            if (oldEntity == null) return false; // Concurrently deleted?

            // Serialize new version
            // var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
            // try {
            //     var bytesWritten = _mapper.Serialize(entity, buffer);
            var bytesWritten = SerializeWithRetry(entity, out var buffer);
            try 
            {
                var docData = buffer.AsSpan(0, bytesWritten);

                // Read old page to check if we can update in-place
                var pageBuffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
                try
                {
                    _storage.ReadPage(oldLocation.PageId, txn.TransactionId, pageBuffer);

                    var slotOffset = SlottedPageHeader.Size + (oldLocation.SlotIndex * SlotEntry.Size);
                    var oldSlot = SlotEntry.ReadFrom(pageBuffer.AsSpan(slotOffset));
                    var hasOverflow = (oldSlot.Flags & SlotFlags.HasOverflow) != 0;

                    if (bytesWritten <= oldSlot.Length && !hasOverflow)
                    {
                        // In-place update - location doesn't change
                        docData.CopyTo(pageBuffer.AsSpan(oldSlot.Offset, bytesWritten));

                        // Update slot length
                        var newSlot = oldSlot;
                        newSlot.Length = (ushort)bytesWritten;
                        newSlot.WriteTo(pageBuffer.AsSpan(slotOffset));

                        _storage.WritePage(oldLocation.PageId, txn.TransactionId, pageBuffer);
                        
                        // Notify secondary indexes (primary index unchanged)
                        _indexManager.UpdateInAll(oldEntity, entity, oldLocation, oldLocation, txn);
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
                        DocumentLocation newLocation;
                        if (bytesWritten <= MaxDocumentSizeForSinglePage)
                        {
                            var newPageId = FindPageWithSpace(bytesWritten);
                            if (newPageId == 0)
                                newPageId = AllocateNewDataPage(txn);

                            var newSlotIndex = InsertIntoPage(newPageId, docData, txn);
                            newLocation = new DocumentLocation(newPageId, newSlotIndex);
                        }
                        else
                        {
                            var (newPageId, newSlotIndex) = InsertWithOverflow(docData, txn);
                            newLocation = new DocumentLocation(newPageId, newSlotIndex);
                        }

                        // Update primary index with new location
                        _primaryIndex.Delete(key, oldLocation, txn.TransactionId);
                        _primaryIndex.Insert(key, newLocation, txn.TransactionId);

                        // Notify secondary indexes
                        _indexManager.UpdateInAll(oldEntity, entity, oldLocation, newLocation, txn);
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(pageBuffer);
                }

                if (transaction == null) txn.Commit();
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        catch
        {
            if (transaction == null) txn.Rollback();
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

        using var txn = transaction ?? _storage.BeginTransaction();
        try
        {
            for (int batchStart = 0; batchStart < entityList.Count; batchStart += BATCH_SIZE)
            {
                int batchEnd = Math.Min(batchStart + BATCH_SIZE, entityList.Count);
                int batchCount = batchEnd - batchStart;

                // PHASE 1: Parallel Serialization
                var serializedBatch = new (TId id, byte[] data, bool found)[batchCount];

                System.Threading.Tasks.Parallel.For(0, batchCount, i =>
                {
                    var entity = entityList[batchStart + i];
                    var id = _mapper.GetId(entity);
                    var key = _mapper.ToIndexKey(id);

                    // Check if entity exists
                    if (_primaryIndex.TryFind(key, out var _, txn.TransactionId))
                    {
                        // var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
                        // _mapper.Serialize(entity, buffer);
                        // serializedBatch[i] = (id, buffer, true);
                        var length = SerializeWithRetry(entity, out var buffer);
                        serializedBatch[i] = (id, buffer, true);
                    }
                    else
                    {
                        serializedBatch[i] = (default!, null!, false);
                    }
                });

                // PHASE 2: Sequential Update
                for (int i = 0; i < batchCount; i++)
                {
                    var (id, docData, found) = serializedBatch[i];
                    if (!found) continue;

                    var entity = entityList[batchStart + i];
                    var key = _mapper.ToIndexKey(id);

                    if (_primaryIndex.TryFind(key, out var oldLocation, txn.TransactionId))
                    {
                         // Re-use logic from Update but optimized for batch
                         // For simplicity in this PR, we'll just call a private UpdateInternal
                         if (UpdateInternal(id, entity, docData.AsSpan(), oldLocation, txn))
                             updateCount++;
                    }
                    ArrayPool<byte>.Shared.Return(docData);
                }
            }

            if (transaction == null) txn.Commit();
            return updateCount;
        }
        catch
        {
            if (transaction == null) txn.Rollback();
            throw;
        }
    }

    private bool UpdateInternal(TId id, T entity, ReadOnlySpan<byte> docData, DocumentLocation oldLocation, ITransaction txn)
    {
        var key = _mapper.ToIndexKey(id);
        var bytesWritten = docData.Length;

        // Retrieve old version for index updates
        var oldEntity = FindByLocation(oldLocation, txn);
        if (oldEntity == null) return false;
        
        // Read old page
        var pageBuffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            _storage.ReadPage(oldLocation.PageId, txn.TransactionId, pageBuffer);

            var slotOffset = SlottedPageHeader.Size + (oldLocation.SlotIndex * SlotEntry.Size);
            var oldSlot = SlotEntry.ReadFrom(pageBuffer.AsSpan(slotOffset));

            if (bytesWritten <= oldSlot.Length && (oldSlot.Flags & SlotFlags.HasOverflow) == 0)
            {
                // In-place update
                docData.CopyTo(pageBuffer.AsSpan(oldSlot.Offset, bytesWritten));
                var newSlot = oldSlot;
                newSlot.Length = (ushort)bytesWritten;
                newSlot.WriteTo(pageBuffer.AsSpan(slotOffset));
                _storage.WritePage(oldLocation.PageId, txn.TransactionId, pageBuffer);

                // Notify secondary indexes (primary index unchanged)
                _indexManager.UpdateInAll(oldEntity, entity, oldLocation, oldLocation, txn);
                return true;
            }
            else
            {
                // Delete old + insert new
                DeleteInternal(id, oldLocation, txn); // Be careful: DeleteInternal might commit if not careful? No, txn passed.
                
                DocumentLocation newLocation;
                if (bytesWritten <= MaxDocumentSizeForSinglePage)
                {
                    var newPageId = FindPageWithSpace(bytesWritten);
                    if (newPageId == 0) newPageId = AllocateNewDataPage(txn);
                    var newSlotIndex = InsertIntoPage(newPageId, docData, txn);
                    newLocation = new DocumentLocation(newPageId, newSlotIndex);
                }
                else
                {
                    var (newPageId, newSlotIndex) = InsertWithOverflow(docData, txn);
                    newLocation = new DocumentLocation(newPageId, newSlotIndex);
                }

                _primaryIndex.Insert(key, newLocation, txn.TransactionId);
                _indexManager.UpdateInAll(oldEntity, entity, oldLocation, newLocation, txn);
                return true;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    public bool Delete(TId id, ITransaction? transaction = null)
    {
        using var txn = transaction ?? _storage.BeginTransaction();
        try
        {
            var key = _mapper.ToIndexKey(id);
            if (!_primaryIndex.TryFind(key, out var location, txn.TransactionId))
                return false;

            var result = DeleteInternal(id, location, txn);
            if (result && transaction == null) txn.Commit();
            return result;
        }
        catch
        {
            if (transaction == null) txn.Rollback();
            throw;
        }
    }

    private bool DeleteInternal(TId id, DocumentLocation location, ITransaction txn)
    {
        var key = _mapper.ToIndexKey(id);

        // Notify secondary indexes BEFORE deleting document from storage
        // (so indexes can still read properties if needed)
        var entity = FindByLocation(location, txn);
        if (entity != null)
        {
            _indexManager.DeleteFromAll(entity, location, txn);
        }
        else
        {
             // Entity might have been deleted concurrently or index is stale
        }

        // Read page
        var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            _storage.ReadPage(location.PageId, txn.TransactionId, buffer);

            var slotOffset = SlottedPageHeader.Size + (location.SlotIndex * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));

            // Check if slot has overflow and free it
            if ((slot.Flags & SlotFlags.HasOverflow) != 0)
            {
                var nextOverflowPage = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(
                    buffer.AsSpan(slot.Offset + 4, 4));
                FreeOverflowChain(nextOverflowPage, txn.TransactionId);
            }

            // Mark slot as deleted
            var newSlot = slot;
            newSlot.Flags |= SlotFlags.Deleted;
            newSlot.WriteTo(buffer.AsSpan(slotOffset));

            _storage.WritePage(location.PageId, txn.TransactionId, buffer);

            // Remove from primary index
            _primaryIndex.Delete(key, location, txn.TransactionId);

            return true;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Deletes multiple documents in a single transaction.
    /// Efficiently updates storage and index.
    /// </summary>
    public int DeleteBulk(IEnumerable<TId> ids, ITransaction? transaction = null)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));

        int deleteCount = 0;
        using var txn = transaction ?? _storage.BeginTransaction();
        try
        {
            foreach (var id in ids)
            {
                var key = _mapper.ToIndexKey(id);
                if (_primaryIndex.TryFind(key, out var location, txn.TransactionId))
                {
                    if (DeleteInternal(id, location, txn))
                        deleteCount++;
                }
            }

            if (transaction == null) txn.Commit();
            return deleteCount;
        }
        catch
        {
            if (transaction == null) txn.Rollback();
            throw;
        }
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
            // Use generic min/max keys for the index
            var minKey = IndexKey.MinKey;
            var maxKey = IndexKey.MaxKey;
            return _primaryIndex.Range(minKey, maxKey, IndexDirection.Forward, txn.TransactionId).Count();
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

    /// <summary>
    /// Serializes an entity with adaptive buffer sizing (Stepped Retry).
    /// Strategies:
    /// 1. 64KB (Covers 99% of docs, small overhead)
    /// 2. 2MB (Covers large docs)
    /// 3. 16MB (Max limit)
    /// </summary>
    private int SerializeWithRetry(T entity, out byte[] rentedBuffer)
    {
        // 64KB, 2MB, 16MB
        int[] steps = { 65536, 2097152, 16777216 };

        for (int i = 0; i < steps.Length; i++)
        {
            int size = steps[i];
            
            // Ensure we at least cover PageSize (unlikely to be > 64KB but safe)
            if (size < _storage.PageSize) size = _storage.PageSize;

            var buffer = ArrayPool<byte>.Shared.Rent(size);
            try
            {
                int bytesWritten = _mapper.Serialize(entity, buffer);
                rentedBuffer = buffer;
                return bytesWritten;
            }
            catch (Exception ex) when (ex is ArgumentException || ex is IndexOutOfRangeException || ex is ArgumentOutOfRangeException)
            {
                ArrayPool<byte>.Shared.Return(buffer);
                // Continue to next step
            }
            catch
            {
                ArrayPool<byte>.Shared.Return(buffer);
                throw;
            }
        }

        rentedBuffer = null!; // specific compiler satisfaction, though we throw
        throw new InvalidOperationException($"Document too large. Maximum size allowed is 16MB.");
    }
}
