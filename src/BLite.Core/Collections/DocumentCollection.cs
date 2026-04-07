using BLite.Core.CDC;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using BLite.Bson;
using BLite.Core.Indexing;
using BLite.Core.Metadata;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Linq.Expressions;
using BLite.Core.Query;

[assembly: InternalsVisibleTo("BLite.Tests")]

namespace BLite.Core.Collections;

public class DocumentCollection<T> : DocumentCollection<ObjectId, T>, IDocumentCollection<T> where T : class
{
    [RequiresDynamicCode("DocumentCollection uses CollectionIndexManager which compiles index key selectors via Expression.Compile().")]
    [RequiresUnreferencedCode("Index creation uses reflection (Expression.PropertyOrField) to access type members. Ensure all entity types and their members are preserved.")]
    public DocumentCollection(StorageEngine storage, ITransactionHolder transactionHolder, IDocumentMapper<T> mapper, string? collectionName = null)
        : base(storage, transactionHolder, mapper, collectionName)
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
public class DocumentCollection<TId, T> : IDocumentCollection<TId, T>, IDisposable where T : class
{
    private readonly ITransactionHolder _transactionHolder;
    private readonly StorageEngine _storage;
    private readonly IDocumentMapper<TId, T> _mapper;
    internal readonly BTreeIndex _primaryIndex;
    private readonly CollectionIndexManager<TId, T> _indexManager;
    private readonly string _collectionName;

    // Free space tracking: PageId → Free bytes
    private readonly Dictionary<uint, ushort> _freeSpaceMap;
    private bool _isTimeSeries;
    private string? _ttlFieldName;

    // Current page for inserts (optimization)
    private uint _currentDataPage;

    public SchemaVersion? CurrentSchemaVersion { get; private set; }

    // Concurrency control for write operations (B-Tree and Page modifications)
    private readonly SemaphoreSlim _collectionLock = new(1, 1);
    private int WriteLockTimeoutMs => _storage.LockTimeout.WriteTimeoutMs;

    private readonly int _maxDocumentSizeForSinglePage;

    // Tracks the last successful serialized size (+25% margin) to skip retry steps that are
    // known to be too small. Volatile: benign race in Parallel.For — worst case a step is
    // not skipped once, never a correctness issue.
    private volatile int _lastSerializedSize = 65536;

    // Value converters registered via OnModelCreating (e.g. ValueObject Id converters).
    // Exposed to BTreeQueryProvider so the query engine can convert ValueObjects to BSON
    // primitives at query-plan time, enabling index lookups on ValueObject-keyed collections.
    internal ValueConverterRegistry ConverterRegistry { get; private set; } = ValueConverterRegistry.Empty;

    internal void SetConverterRegistry(ValueConverterRegistry registry) =>
        ConverterRegistry = registry ?? ValueConverterRegistry.Empty;

    /// <summary>
    /// Exposes the storage key map (field name → field ID) so that query components
    /// in the same assembly (e.g. <see cref="BTreeQueryProvider{TId,T}"/>) can pass
    /// it to projection/predicate compilers for offset-table fast paths.
    /// </summary>
    internal IReadOnlyDictionary<string, ushort> GetKeyMap() => _storage.GetKeyMap();

    /// <summary>
    /// Configures this collection as a TimeSeries with automatic TTL-based pruning.
    /// Must be called before the first insert. Persists configuration to storage metadata.
    /// </summary>
    internal void SetTimeSeries(string ttlFieldName, TimeSpan retention)
    {
        var meta = _indexManager.GetMetadata();
        meta.IsTimeSeries = true;
        meta.TtlFieldName = ttlFieldName;
        meta.RetentionPolicyMs = (long)retention.TotalMilliseconds;
        meta.LastPruningTimestamp = DateTime.UtcNow.Ticks;
        _storage.SaveCollectionMetadata(meta);
        _isTimeSeries = true;
        _ttlFieldName = ttlFieldName;
    }

    /// <summary>
    /// Forces immediate pruning of expired TimeSeries documents, regardless of automatic thresholds.
    /// Primarily intended for testing; in production, pruning is triggered automatically on insert.
    /// </summary>
    public async Task ForcePruneAsync()
    {
        if (!_isTimeSeries)
            throw new InvalidOperationException("ForcePrune is only valid on TimeSeries collections.");

        // Read fresh metadata from storage: InsertTimeSeries updates TimeSeriesHeadPageId
        // in StorageEngine's own metadata store, which is separate from _indexManager._metadata.
        var meta = _storage.GetCollectionMetadata(_collectionName);
        if (meta == null || meta.RetentionPolicyMs <= 0) return;

        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        _storage.PruneTimeSeries(meta, transaction);
        meta.InsertedSinceLastPruning = 0;
        meta.LastPruningTimestamp = DateTime.UtcNow.Ticks;
        _storage.SaveCollectionMetadata(meta);
    }

    [RequiresDynamicCode("DocumentCollection uses CollectionIndexManager which compiles index key selectors via Expression.Compile().")]
    [RequiresUnreferencedCode("Index creation uses reflection (Expression.PropertyOrField) to access type members. Ensure all entity types and their members are preserved.")]
    public DocumentCollection(StorageEngine storage, ITransactionHolder transactionHolder, IDocumentMapper<TId, T> mapper, string? collectionName = null)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _transactionHolder = transactionHolder ?? throw new ArgumentNullException(nameof(transactionHolder));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        _collectionName = collectionName ?? _mapper.CollectionName;

        // Initialize secondary index manager first (loads metadata including Primary Root Page ID)
        _indexManager = new CollectionIndexManager<TId, T>(_storage, _mapper, _collectionName);
        _freeSpaceMap = new Dictionary<uint, ushort>();

        // Calculate max document size dynamically based on page size
        // Reserve space for PageHeader (24) and some safety margin
        _maxDocumentSizeForSinglePage = _storage.PageSize - 128;

        // Ensure schema is persisted and versioned
        EnsureSchema();

        // Restore TimeSeries flag from persisted metadata
        var tsMeta = _indexManager.GetMetadata();
        if (tsMeta.IsTimeSeries)
        {
            _isTimeSeries = true;
            _ttlFieldName = tsMeta.TtlFieldName;
        }

        // Create primary index on _id (stores ObjectId → DocumentLocation mapping)
        // Use persisted root page ID if available
        var indexOptions = IndexOptions.CreateUnique("_id");
        _primaryIndex = new BTreeIndex(_storage, indexOptions, _indexManager.PrimaryRootPageId,
            onRootChanged: newRoot => _indexManager.SetPrimaryRootPageId(newRoot));

        // If a new root page was allocated, persist it
        if (_indexManager.PrimaryRootPageId != _primaryIndex.RootPageId)
        {
            _indexManager.SetPrimaryRootPageId(_primaryIndex.RootPageId);
        }

        // Register keys used by the mapper to ensure they are available for compression
        _storage.RegisterKeys(_mapper.UsedKeys);
    }

    [RequiresUnreferencedCode("Schema management uses reflection to discover entity properties. Ensure all entity types and their members are preserved.")]
    private void EnsureSchema()
    {
        var currentSchema = _mapper.GetSchema();
        var metadata = _indexManager.GetMetadata();

        var persistedSchemas = _storage.GetSchemas(metadata.SchemaRootPageId);
        var latestPersisted = persistedSchemas.Count > 0 ? persistedSchemas[persistedSchemas.Count - 1] : null;

        if (latestPersisted == null || !currentSchema.Equals(latestPersisted))
        {
            // Assign next version number
            int nextVersion = persistedSchemas.Count + 1;
            currentSchema.Version = nextVersion;

            var newRootId = _storage.AppendSchema(metadata.SchemaRootPageId, currentSchema);
            if (newRootId != metadata.SchemaRootPageId)
            {
                metadata.SchemaRootPageId = newRootId;
                _storage.SaveCollectionMetadata(metadata);
            }

            CurrentSchemaVersion = new SchemaVersion(nextVersion, currentSchema.GetHash());
        }
        else
        {
            // Latest persisted is same as current structure
            int resolvedVersion = latestPersisted.Version ?? persistedSchemas.Count;
            CurrentSchemaVersion = new SchemaVersion(resolvedVersion, latestPersisted.GetHash());
        }
    }

    #region Index Management API

    /// <summary>
    /// Asynchronously creates a secondary index on the specified property.
    /// </summary>
    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    public async Task<ICollectionIndex<TId, T>> CreateIndexAsync<TKey>(
        System.Linq.Expressions.Expression<Func<T, TKey>> keySelector,
        string? name = null,
        bool unique = false,
        CancellationToken ct = default)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));

        using (var txn = _storage.BeginTransaction(IsolationLevel.ReadCommitted))
        {
            var index = _indexManager.CreateIndex(keySelector, name, unique);

            // Rebuild index for existing documents
            await RebuildIndexAsync(index, ct);

            await txn.CommitAsync();

            return await BindReaderAndReturn(index);
        }
    }

    /// <summary>
    /// Asynchronously creates a vector (HNSW) index for similarity search.
    /// </summary>
    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    public async Task<ICollectionIndex<TId, T>> CreateVectorIndexAsync<TKey>(
        System.Linq.Expressions.Expression<Func<T, TKey>> keySelector,
        int dimensions,
        VectorMetric metric = VectorMetric.Cosine,
        string? name = null,
        CancellationToken ct = default)
    {
        if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));

        using (var txn = _storage.BeginTransaction(IsolationLevel.ReadCommitted))
        {
            var index = _indexManager.CreateVectorIndex(keySelector, dimensions, metric, name);
            await RebuildIndexAsync(index, ct);
            await txn.CommitAsync();
            return await BindReaderAndReturn(index);
        }
    }

    /// <summary>
    /// Asynchronously ensures that an index exists on the specified property.
    /// If the index already exists, it is returned without modification (idempotent).
    /// If it doesn't exist, it is created and populated.
    /// </summary>
    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    public async Task<ICollectionIndex<TId, T>> EnsureIndexAsync<TKey>(
        System.Linq.Expressions.Expression<Func<T, TKey>> keySelector,
        string? name = null,
        bool unique = false,
        CancellationToken ct = default)
    {
        if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));

        // 1. Check if index already exists (fast path)
        var propertyPaths = ExpressionAnalyzer.ExtractPropertyPaths(keySelector);
        var indexName = name ?? $"idx_{string.Join("_", propertyPaths)}";

        var existingIndex = await GetIndexAsync(indexName);
        if (existingIndex != null)
        {
            return existingIndex;
        }

        // 2. Create if missing (slow path: rebuilds index)
        return await CreateIndexAsync(keySelector, name, unique, ct);
    }

    /// <summary>
    /// Asynchronously drops (removes) an existing secondary index by name.
    /// The primary index (_id) cannot be dropped.
    /// </summary>
    /// <param name="name">Name of the index to drop</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>True if the index was found and dropped, false otherwise</returns>
    public Task<bool> DropIndexAsync(string name, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Index name cannot be empty", nameof(name));

        // Prevent dropping primary index
        if (name.Equals("_id", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Cannot drop primary index");

        // DropIndex is synchronous (no heavy I/O), wrap in Task
        return Task.FromResult(_indexManager.DropIndex(name));
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

    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    internal void ApplyIndexBuilder(Metadata.IndexBuilder<T> builder)
    {
        // Use the IndexManager directly to ensure the index exists
        // We need to convert the LambdaExpression to a typed expression if possible, 
        // or add an untyped CreateIndexAsync to IndexManager.

        // For now, let's use a dynamic approach or cast if we know it's Func<T, object>
        if (builder.Type == IndexType.Vector)
        {
            _indexManager.CreateVectorIndexUntyped(builder.KeySelector, builder.Dimensions, builder.Metric, builder.Name);
        }
        else if (builder.Type == IndexType.Spatial)
        {
            _indexManager.CreateSpatialIndexUntyped(builder.KeySelector, builder.Name);
        }
        else if (builder.KeySelector is System.Linq.Expressions.Expression<Func<T, object>> selector)
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
    /// Asynchronously scans the entire collection using a raw BSON predicate.
    /// This avoids deserializing documents that don't match the criteria.
    /// </summary>
    /// <param name="predicate">Function to evaluate raw BSON data</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>Async enumerable of matching documents</returns>
    public async IAsyncEnumerable<T> ScanAsync(
        BsonReaderPredicate predicate,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var txnId = transaction.TransactionId;
        var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);

        try
        {
            foreach (var pageId in _storage.GetCollectionPageIds(_collectionName))
            {
                ct.ThrowIfCancellationRequested();
                await _storage.ReadPageAsync(pageId, txnId, buffer.AsMemory(0, _storage.PageSize), ct);

                var header = SlottedPageHeader.ReadFrom(buffer);
                if (header.PageType != PageType.Data) continue;

                // Collect matching locations first (no Span across yield)
                var matchingLocations = new List<(uint pageId, ushort slotIndex)>();
                {
                    var slots = MemoryMarshal.Cast<byte, SlotEntry>(
                        buffer.AsSpan(SlottedPageHeader.Size, header.SlotCount * SlotEntry.Size));

                    for (int i = 0; i < header.SlotCount; i++)
                    {
                        var slot = slots[i];
                        if (slot.Flags.HasFlag(SlotFlags.Deleted)) continue;

                        var data = buffer.AsSpan(slot.Offset, slot.Length);
                        var reader = new BsonSpanReader(data, _storage.GetKeyReverseMap());

                        if (predicate(reader))
                        {
                            matchingLocations.Add((pageId, (ushort)i));
                        }
                    }
                }

                // Yield matching documents. The buffer already holds this page — pass it
                // to FindByLocationAsync so the page is not read a second time.
                // A data page may contain slots from multiple collections; if the mapper
                // fails for a foreign-collection document, silently skip it.
                foreach (var (pid, idx) in matchingLocations)
                {
                    T? doc;
                    try
                    {
                        doc = await FindByLocationAsync(new DocumentLocation(pid, idx), txnId, buffer, ct);
                    }
                    catch
                    {
                        continue; // foreign-collection document — skip
                    }
                    if (doc != null) yield return doc;
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Scans all documents returning only two flat fields per document as a
    /// <c>(TKey, TValue)</c> pair, without deserialising the full entity
    /// <typeparamref name="T"/>.
    /// </summary>
    /// <remarks>
    /// When both selectors access a single flat, scalar property
    /// (supported by <see cref="BsonProjectionCompiler"/>) a single-pass BSON
    /// projector is compiled and the two fields are read in one sweep.
    /// Falls back to full deserialization + LINQ projection otherwise.
    /// </remarks>
    public async IAsyncEnumerable<(TKey Key, TValue Value)> ScanPairsAsync<TKey, TValue>(
        Expression<Func<T, TKey>> keySelector,
        Expression<Func<T, TValue>> valueSelector)
    {
        if (keySelector is null) throw new ArgumentNullException(nameof(keySelector));
        if (valueSelector is null) throw new ArgumentNullException(nameof(valueSelector));

        var projector = BsonProjectionCompiler.TryCompilePair<T, TKey, TValue>(
            keySelector, valueSelector, _storage.GetKeyMap());

        if (projector is not null)
        {
            await foreach (var t in ScanAsync(projector)) { yield return t; }
            yield break;
        }

        // Fallback: full deserialization.
        var keyFunc = keySelector.Compile();
        var valFunc = valueSelector.Compile();
        await foreach (var item in FindAllAsync())
        {
            yield return (keyFunc(item), valFunc(item));
        }
    }

    /// <summary>
    /// Asynchronously scans the entire collection applying a <paramref name="projector"/> directly
    /// to raw BSON bytes. Only the fields accessed by the projector are read.
    /// </summary>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <param name="projector">
    ///   A delegate that receives a <see cref="BsonSpanReader"/> positioned at the start of each
    ///   document and returns the projected value, or <c>null</c> to skip the document.
    /// </param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>Async enumerable of projected results</returns>
    public async IAsyncEnumerable<TResult> ScanAsync<TResult>(
        BsonReaderProjector<TResult> projector,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (projector == null) throw new ArgumentNullException(nameof(projector));

        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var txnId = transaction.TransactionId;
        var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);

        try
        {
            foreach (var pageId in _storage.GetCollectionPageIds(_collectionName))
            {
                ct.ThrowIfCancellationRequested();
                await _storage.ReadPageAsync(pageId, txnId, buffer.AsMemory(0, _storage.PageSize), ct);

                var header = SlottedPageHeader.ReadFrom(buffer);
                if (header.PageType != PageType.Data) continue;

                // Process all slots and collect results (no Span across yield)
                var pageResults = new List<TResult>();
                {
                    var slots = MemoryMarshal.Cast<byte, SlotEntry>(
                        buffer.AsSpan(SlottedPageHeader.Size, header.SlotCount * SlotEntry.Size));

                    var keyMap = _storage.GetKeyReverseMap();
                    for (int i = 0; i < header.SlotCount; i++)
                    {
                        var slot = slots[i];
                        if (slot.Flags.HasFlag(SlotFlags.Deleted)) continue;

                        var data = buffer.AsSpan(slot.Offset, slot.Length);
                        var reader = new BsonSpanReader(data, keyMap);
                        var result = projector(reader);
                        if (result is not null) pageResults.Add(result);
                    }
                }

                // Yield results after Span is out of scope
                foreach (var result in pageResults)
                {
                    yield return result;
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Counts documents matching <paramref name="predicate"/> by evaluating the predicate
    /// directly on raw BSON bytes — no CLR <typeparamref name="T"/> instances are ever created.
    /// </summary>
    /// <remarks>
    /// The predicate is executed once per non-deleted, non-overflow slot on every data page.
    /// Overflow-flagged primary slots are skipped: the BSON data in such slots starts after
    /// an 8-byte overflow header that a normal BSON predicate cannot parse correctly, so
    /// those documents are not counted (same behaviour as the existing <see cref="ScanAsync(BsonReaderPredicate,CancellationToken)"/>
    /// for overflow documents).
    /// </remarks>
    internal async Task<int> CountScanAsync(
        BsonReaderPredicate predicate,
        CancellationToken ct = default)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var txnId = transaction.TransactionId;
        var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        int count = 0;

        try
        {
            foreach (var pageId in _storage.GetCollectionPageIds(_collectionName))
            {
                ct.ThrowIfCancellationRequested();
                await _storage.ReadPageAsync(pageId, txnId, buffer.AsMemory(0, _storage.PageSize), ct);

                var header = SlottedPageHeader.ReadFrom(buffer);
                if (header.PageType != PageType.Data) continue;

                var slots = MemoryMarshal.Cast<byte, SlotEntry>(
                    buffer.AsSpan(SlottedPageHeader.Size, header.SlotCount * SlotEntry.Size));
                var keyMap = _storage.GetKeyReverseMap();

                for (int i = 0; i < header.SlotCount; i++)
                {
                    var slot = slots[i];
                    if ((slot.Flags & SlotFlags.Deleted) != 0) continue;
                    // Skip overflow continuation slots: the primary slot's raw data starts
                    // with an 8-byte overflow header (totalLength + nextPageId), not BSON.
                    if ((slot.Flags & SlotFlags.HasOverflow) != 0) continue;

                    var data = buffer.AsSpan(slot.Offset, slot.Length);
                    var reader = new BsonSpanReader(data, keyMap);
                    try
                    {
                        if (predicate(reader)) count++;
                    }
                    catch
                    {
                        // Malformed BSON or foreign-collection slot — skip silently.
                    }
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        return count;
    }

    private void ScanPageProjected<TResult>(
        uint pageId,
        ulong txnId,
        byte[] buffer,
        BsonReaderProjector<TResult> projector,
        List<TResult> results)
    {
        _storage.ReadPage(pageId, txnId, buffer);
        var header = SlottedPageHeader.ReadFrom(buffer);

        if (header.PageType != PageType.Data)
            return;

        var slots = MemoryMarshal.Cast<byte, SlotEntry>(
            buffer.AsSpan(SlottedPageHeader.Size, header.SlotCount * SlotEntry.Size));

        var keyMap = _storage.GetKeyReverseMap();

        for (int i = 0; i < header.SlotCount; i++)
        {
            var slot = slots[i];
            if (slot.Flags.HasFlag(SlotFlags.Deleted)) continue;

            var data = buffer.AsSpan(slot.Offset, slot.Length);
            var reader = new BsonSpanReader(data, keyMap);
            var result = projector(reader);
            if (result is not null) results.Add(result);
        }
    }

    /// <summary>
    /// Asynchronously scans the collection in parallel using multiple tasks.
    /// Useful for large collections on multi-core machines.
    /// </summary>
    /// <param name="predicate">Function to evaluate raw BSON data</param>
    /// <param name="degreeOfParallelism">Number of concurrent tasks (default: -1 = ProcessorCount)</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>Async enumerable of matching documents</returns>
    public async IAsyncEnumerable<T> ParallelScanAsync(
        BsonReaderPredicate predicate,
        int degreeOfParallelism = -1,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var txnId = transaction.TransactionId;
        var allPageIds = _storage.GetCollectionPageIds(_collectionName).ToArray();
        var pageCount = allPageIds.Length;

        if (degreeOfParallelism <= 0)
            degreeOfParallelism = Environment.ProcessorCount;

        var semaphore = new SemaphoreSlim(degreeOfParallelism);
        var tasks = new List<Task<List<T>>>();

        for (int pageIdx = 0; pageIdx < pageCount; pageIdx++)
        {
            await semaphore.WaitAsync(ct);
            var localPageId = allPageIds[pageIdx];

            var task = Task.Run(async () =>
            {
                try
                {
                    var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
                    var results = new List<T>();

                    try
                    {
                        await _storage.ReadPageAsync(localPageId, txnId, buffer.AsMemory(0, _storage.PageSize), ct);

                        var header = SlottedPageHeader.ReadFrom(buffer);
                        if (header.PageType != PageType.Data) return results;

                        // First pass: collect matching locations (Span scope)
                        var matchingIndices = new List<ushort>();
                        {
                            var slots = MemoryMarshal.Cast<byte, SlotEntry>(
                                buffer.AsSpan(SlottedPageHeader.Size, header.SlotCount * SlotEntry.Size));

                            for (int i = 0; i < header.SlotCount; i++)
                            {
                                var slot = slots[i];
                                if (slot.Flags.HasFlag(SlotFlags.Deleted)) continue;

                                var data = buffer.AsSpan(slot.Offset, slot.Length);
                                var reader = new BsonSpanReader(data, _storage.GetKeyReverseMap());

                                if (predicate(reader))
                                {
                                    matchingIndices.Add((ushort)i);
                                }
                            }
                        }

                        // Second pass: fetch documents (no Span, safe to await)
                        foreach (var idx in matchingIndices)
                        {
                            var doc = await FindByLocationAsync(new DocumentLocation(localPageId, idx), txnId, ct);
                            if (doc != null) results.Add(doc);
                        }
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(buffer);
                    }

                    return results;
                }
                finally
                {
                    semaphore.Release();
                }
            }, ct);

            tasks.Add(task);
        }

        // Yield results as tasks complete
        while (tasks.Count > 0)
        {
            var completedTask = await Task.WhenAny(tasks);
            tasks.Remove(completedTask);

            var results = await completedTask;
            foreach (var doc in results)
            {
                yield return doc;
            }
        }
    }

    /// <summary>
    /// Gets a queryable interface for this collection.
    /// Supports LINQ queries that are translated to optimized BTree scans or index lookups.
    /// </summary>
    [RequiresDynamicCode("LINQ queries over BLite collections use Expression.Compile() and MakeGenericMethod which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ queries over BLite collections use reflection to resolve methods at runtime. Ensure all entity types and their members are preserved.")]
    public IBLiteQueryable<T> AsQueryable()
    {
        return new BTreeQueryable<T>(new BTreeQueryProvider<TId, T>(this, ConverterRegistry));
    }

    /// <summary>
    /// Gets a specific secondary index by name for advanced querying.
    /// Returns null if the index doesn't exist.
    /// </summary>
    public async Task<ICollectionIndex<TId, T>?> GetIndexAsync(string name)
    {
        var idx = _indexManager.GetIndex(name);
        return idx is null ? null : await BindReaderAndReturn(idx);
    }

    private async Task<ICollectionIndex<TId, T>> BindReaderAndReturn(CollectionSecondaryIndex<TId, T> index)
    {
        index.SetDocumentReader(
            async: (loc, ct) => new ValueTask<T?>(FindByLocation(loc)));
        return index;
    }

    /// <summary>
    /// Asynchronously queries a specific index for a range of values.
    /// Returns matching documents using the index for efficient retrieval.
    /// </summary>
    /// <param name="indexName">Name of the index to query</param>
    /// <param name="minKey">Minimum key value (inclusive)</param>
    /// <param name="maxKey">Maximum key value (inclusive)</param>
    /// <param name="ascending">True for ascending order, false for descending</param>
    /// <param name="skip">Number of index entries to skip before yielding documents.
    /// Skipping is performed at the index-entry level so documents are never read for
    /// skipped positions, enabling efficient pagination without full materialization.</param>
    /// <param name="take">Maximum number of documents to yield. Defaults to all.</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>Async enumerable of matching documents</returns>
    public async IAsyncEnumerable<T> QueryIndexAsync(
        string indexName,
        object? minKey,
        object? maxKey,
        bool ascending = true,
        int skip = 0,
        int take = int.MaxValue,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var index = _indexManager.GetIndex(indexName);
        if (index == null) throw new ArgumentException($"Index {indexName} not found");

        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var txnId = transaction.TransactionId;
        var direction = ascending ? IndexDirection.Forward : IndexDirection.Backward;

        // Per-query page cache: when multiple index entries point to the same data page
        // (common for small documents), each page is loaded only once.
        // For a collection of 200-byte docs on 16 KB pages (~80 docs/page), a query
        // returning 250 docs touches ~3 unique pages — without this cache we'd do 250
        // ReadPage calls instead of 3.
        var pageCache = new Dictionary<uint, byte[]>();
        try
        {
            int skipped = 0;
            int taken = 0;
            foreach (var location in index.Range(minKey, maxKey, direction, transaction))
            {
                // Skip index entries without reading documents — O(skip) index key reads,
                // zero document deserializations for skipped positions.
                if (skip > 0 && skipped < skip) { skipped++; continue; }

                ct.ThrowIfCancellationRequested();

                if (!pageCache.TryGetValue(location.PageId, out var cachedBuffer))
                {
                    cachedBuffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
                    _storage.ReadPage(location.PageId, txnId, cachedBuffer);
                    pageCache[location.PageId] = cachedBuffer;
                }

                var doc = await FindByLocationAsync(location, txnId, cachedBuffer, ct);
                if (doc != null)
                {
                    yield return doc;
                    if (++taken >= take) break;
                }
            }
        }
        finally
        {
            foreach (var buf in pageCache.Values)
                ArrayPool<byte>.Shared.Return(buf);
        }
    }

    private async Task RebuildIndexAsync(CollectionSecondaryIndex<TId, T> index, CancellationToken ct = default)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        // Iterate all documents in the collection via primary index
        foreach (var entry in _primaryIndex.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, transaction.TransactionId))
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                var document = await FindByLocationAsync(entry.Location, transaction.TransactionId, ct);
                if (document != null)
                {
                    index.Insert(document, entry.Location, transaction);
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

    private async Task<uint> FindPageWithSpace(int requiredBytes)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var txnId = transaction.TransactionId;

        // Try current page first
        if (_currentDataPage != 0)
        {
            if (_freeSpaceMap.TryGetValue(_currentDataPage, out var freeBytes))
            {
                if (freeBytes >= requiredBytes && !_storage.IsPageLocked(_currentDataPage, txnId))
                {
                    return _currentDataPage;
                }
            }
            else
            {
                // Load header and check - use StorageEngine
                Span<byte> page = stackalloc byte[SlottedPageHeader.Size];
                _storage.ReadPage(_currentDataPage, null, page);
                var header = SlottedPageHeader.ReadFrom(page);

                if (header.AvailableFreeSpace >= requiredBytes)
                {
                    _freeSpaceMap[_currentDataPage] = (ushort)header.AvailableFreeSpace;
                    if (!_storage.IsPageLocked(_currentDataPage, txnId))
                        return _currentDataPage;
                }
            }
        }

        // Search free space map
        foreach (var (pageId, freeBytes) in _freeSpaceMap)
        {
            if (freeBytes >= requiredBytes)
            {
                if (!_storage.IsPageLocked(pageId, txnId))
                {
                    return pageId;
                }
            }
        }

        return 0; // No suitable page
    }

    private async Task<uint> AllocateNewDataPage()
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();

        var pageId = _storage.AllocateCollectionPage(_collectionName);

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
            if (transaction is Transaction t)
            {
                // OPTIMIZATION: Pass ReadOnlyMemory to avoid ToArray() allocation
                var writeOp = new WriteOperation(ObjectId.Empty, buffer.AsMemory(0, _storage.PageSize), pageId, OperationType.AllocatePage);
                t.AddWrite(writeOp);
            }
            else
            {
                _storage.WritePage(pageId, transaction.TransactionId, buffer);
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

    private async Task<ushort> InsertIntoPage(uint pageId, byte[] data)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);

        try
        {
            _storage.ReadPage(pageId, transaction.TransactionId, buffer);

            var header = SlottedPageHeader.ReadFrom(buffer);

            // ROLLBACK RECOVERY: If the page is completely zeroed (e.g., from a rolled-back allocation)
            // we re-initialize the header for the current transaction.
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

            // UpdateAsync header
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

            // UpdateAsync free space map
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
        var pageId = _storage.AllocateCollectionPage(_collectionName);
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

    private async Task<(uint pageId, ushort slotIndex)> InsertWithOverflow(byte[] data)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        // 1. Calculate Primary Chunk Size
        // We need 8 bytes for metadata (TotalLength: 4, NextOverflowPage: 4)
        const int MetadataSize = 8;
        int maxPrimaryPayload = _maxDocumentSizeForSinglePage - MetadataSize;

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
                    data.AsSpan(tailOffset, tailSize),
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
                    data.AsSpan(chunkOffset, overflowChunkSize),
                    nextOverflowPageId,
                    transaction
                );
                nextOverflowPageId = overflowPageId;
            }
        }

        // 3. PrepareAsync Primary Page Payload
        // Layout: [TotalLength (4)] [NextOverflowPage (4)] [DataChunk (...)]
        // Since we are in InsertWithOverflow, we know data.Length > maxPrimaryPayload
        int primaryPayloadSize = maxPrimaryPayload;
        int totalSlotSize = MetadataSize + primaryPayloadSize;

        // Allocate primary page
        var primaryPageId = await FindPageWithSpace(totalSlotSize + SlotEntry.Size);
        if (primaryPageId == 0)
            primaryPageId = await AllocateNewDataPage();

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
            data.AsSpan(0, primaryPayloadSize).CopyTo(payloadSpan.Slice(8));

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

            // UpdateAsync header
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

            // UpdateAsync free space map
            _freeSpaceMap[primaryPageId] = (ushort)header.AvailableFreeSpace;

            return (primaryPageId, slotIndex);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Asynchronously inserts a new document into the collection
    /// </summary>
    public async Task<TId> InsertAsync(T entity, CancellationToken ct = default)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring collection lock (Insert).");
        try
        {
            try
            {
                var id = await InsertCore(entity);
                return id;
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    /// <summary>
    /// Asynchronously inserts multiple documents in a single transaction.
    /// </summary>
    public async Task<List<TId>> InsertBulkAsync(IEnumerable<T> entities, CancellationToken ct = default)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        var entityList = entities.ToList();
        var ids = new List<TId>(entityList.Count);

        if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring collection lock (InsertBulk).");
        try
        {
            try
            {
                await InsertBulkInternal(entityList, ids);
                return ids;
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    private async Task InsertBulkInternal(List<T> entityList, List<TId> ids)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();

        // Scale batch size with available cores: more parallelism on multi-core machines.
        // Clamped to [32, 200] to avoid pathological cases on very high-core-count machines.
        int BATCH_SIZE = Math.Max(32, Math.Min(Environment.ProcessorCount * 4, 200));

        for (int batchStart = 0; batchStart < entityList.Count; batchStart += BATCH_SIZE)
        {
            int batchEnd = Math.Min(batchStart + BATCH_SIZE, entityList.Count);
            int batchCount = batchEnd - batchStart;

            // PHASE 1: Parallel serialize this batch
            var serializedBatch = new (TId id, byte[] data, int length)[batchCount];

            System.Threading.Tasks.Parallel.For(0, batchCount, i =>
            {
                var entity = entityList[batchStart + i];
                var id = EnsureId(entity);
                var length = SerializeWithRetry(entity, out var buffer);
                serializedBatch[i] = (id, buffer, length);
            });

            // PHASE 2: Sequential insert this batch
            for (int i = 0; i < batchCount; i++)
            {
                var (id, buffer, length) = serializedBatch[i];
                var entity = entityList[batchStart + i];

                try
                {
                    await InsertDataCore(id, entity, buffer, length);
                    ids.Add(id);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
        }
    }

    private TId EnsureId(T entity)
    {
        var id = _mapper.GetId(entity);
        if (EqualityComparer<TId>.Default.Equals(id, default!))
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
            else if (typeof(TId) == typeof(int))
            {
                id = (TId)(object)(int)_storage.GetNextSequence(_collectionName);
                _mapper.SetId(entity, id);
            }
            else if (typeof(TId) == typeof(long))
            {
                id = (TId)(object)_storage.GetNextSequence(_collectionName);
                _mapper.SetId(entity, id);
            }
            else if (typeof(TId) == typeof(string))
            {
                id = (TId)(object)Ulid.NewUlid().ToString();
                _mapper.SetId(entity, id);
            }
        }
        return id;
    }

    private async Task<TId> InsertCore(T entity)
    {
        var id = EnsureId(entity);
        var length = SerializeWithRetry(entity, out var buffer);
        try
        {
            await InsertDataCore(id, entity, buffer, length);
            return id;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private async Task InsertDataCore(TId id, T entity, byte[] docData, int docLength = -1)
    {
        if (docLength >= 0 && docLength < docData.Length)
            docData = docData[..docLength]; // trim to actual serialized size
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        DocumentLocation location;
        if (_isTimeSeries)
        {
            // Wrap the already-serialized BSON bytes in a BsonDocument so InsertTimeSeries
            // can read the timestamp field and manage TimeSeriesPage allocation/pruning.
            var bsonDoc = new BsonDocument(docData.ToArray(), _storage.GetKeyReverseMap(), _storage.GetKeyMap());
            var loc = _storage.InsertTimeSeries(_collectionName, bsonDoc, transaction);
            location = new DocumentLocation(loc.PageId, (ushort)loc.SlotIndex);
        }
        else if (docData.Length + SlotEntry.Size <= _maxDocumentSizeForSinglePage)
        {
            var pageId = await FindPageWithSpace(docData.Length + SlotEntry.Size);
            if (pageId == 0) pageId = await AllocateNewDataPage();
            var slotIndex = await InsertIntoPage(pageId, docData);
            location = new DocumentLocation(pageId, slotIndex);
        }
        else
        {
            var (pageId, slotIndex) = await InsertWithOverflow(docData);
            location = new DocumentLocation(pageId, slotIndex);
        }

        var key = _mapper.ToIndexKey(id);
        _primaryIndex.Insert(key, location, transaction.TransactionId);
        _indexManager.InsertIntoAll(entity, location, transaction);

        // Notify CDC
        await NotifyCdc(OperationType.Insert, id, docData);
    }

    /// <summary>
    /// Compacts the data area of a slotted page by reclaiming bytes occupied by deleted slots.
    /// Live documents are packed contiguously toward the top of the page and each slot's Offset
    /// field is updated accordingly. Slot indices are never renumbered, so the primary index
    /// remains valid. <see cref="SlottedPageHeader.FreeSpaceEnd"/> is advanced to reflect the
    /// reclaimed space.
    /// </summary>
    /// <param name="buffer">Full page buffer (exactly <c>PageSize</c> bytes) to compact in-place.</param>
    /// <returns><c>true</c> if at least one deleted slot was found and space was reclaimed; <c>false</c> if the page was already compact.</returns>
    private bool CompactPage(Span<byte> buffer)
    {
        var header = SlottedPageHeader.ReadFrom(buffer);
        if (header.SlotCount == 0) return false;

        // Gather live (non-deleted) slots
        var liveSlots = new List<(ushort index, SlotEntry slot)>(header.SlotCount);
        for (ushort i = 0; i < header.SlotCount; i++)
        {
            var entryOffset = SlottedPageHeader.Size + (i * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(buffer.Slice(entryOffset, SlotEntry.Size));
            if ((slot.Flags & SlotFlags.Deleted) == 0)
                liveSlots.Add((i, slot));
        }

        if (liveSlots.Count == header.SlotCount)
            return false; // No deleted slots — nothing to compact

        // Pack live document data from top of page downward using a temp copy
        // of the data area to avoid read-after-write corruption.
        var temp = ArrayPool<byte>.Shared.Rent(buffer.Length);
        try
        {
            buffer.CopyTo(temp);
            ushort newEnd = (ushort)buffer.Length;
            foreach (var (idx, slot) in liveSlots)
            {
                newEnd -= slot.Length;
                // Copy original bytes from temp into the new (higher) position in buffer
                temp.AsSpan(slot.Offset, slot.Length).CopyTo(buffer.Slice(newEnd, slot.Length));
                // UpdateAsync slot entry to point at the new offset
                var updatedSlot = slot;
                updatedSlot.Offset = newEnd;
                var entryOffset = SlottedPageHeader.Size + (idx * SlotEntry.Size);
                updatedSlot.WriteTo(buffer.Slice(entryOffset, SlotEntry.Size));
            }

            header.FreeSpaceEnd = newEnd;
            header.WriteTo(buffer);
            return true;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(temp);
        }
    }

    #endregion

    #region Find

    /// <summary>
    /// Async exact-match lookup. Uses <see cref="BTreeIndex.TryFindAsync"/> for the index
    /// traversal and <see cref="FindByLocationAsync"/> for the page read.
    /// </summary>
    public async ValueTask<T?> FindByIdAsync(TId id, CancellationToken ct = default)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var key = _mapper.ToIndexKey(id);
        var (found, location) = await _primaryIndex.TryFindAsync(key, transaction.TransactionId, ct).ConfigureAwait(false);
        if (!found) return default;
        return await FindByLocationAsync(location, transaction.TransactionId, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Async full-collection scan. Uses <see cref="BTreeIndex.RangeAsync"/> for leaf chaining
    /// and <see cref="FindByLocationAsync"/> for each page read.
    /// </summary>
    public async IAsyncEnumerable<T> FindAllAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var txnId = transaction?.TransactionId ?? 0;

        var pageCache = new Dictionary<uint, byte[]>();
        try
        {
            await foreach (var entry in _primaryIndex
                .RangeAsync(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txnId, ct)
                .ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();

                if (!pageCache.TryGetValue(entry.Location.PageId, out var cachedBuffer))
                {
                    cachedBuffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
                    _storage.ReadPage(entry.Location.PageId, txnId, cachedBuffer);
                    pageCache[entry.Location.PageId] = cachedBuffer;
                }

                var entity = await FindByLocationAsync(entry.Location, txnId, cachedBuffer, ct).ConfigureAwait(false);
                if (entity != null)
                    yield return entity;
            }
        }
        finally
        {
            foreach (var buf in pageCache.Values)
                ArrayPool<byte>.Shared.Return(buf);
        }
    }

    internal async Task<T?> FindByLocation(DocumentLocation location)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var txnId = transaction?.TransactionId ?? 0;
        var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            // Read from StorageEngine with transaction isolation
            _storage.ReadPage(location.PageId, txnId, buffer);

            var pageType = (PageType)buffer[4];
            if (pageType == PageType.Free || pageType == PageType.Empty)
                return null;

            if (pageType == PageType.TimeSeries)
            {
                int tsOffset = location.SlotIndex;
                if (tsOffset < TimeSeriesPage.DataOffset || tsOffset + 4 > buffer.Length)
                    return null;
                int tsSize = BitConverter.ToInt32(buffer.AsSpan(tsOffset, 4));
                if (tsSize <= 0 || tsOffset + tsSize > buffer.Length)
                    return null;
                return _mapper.Deserialize(new BsonSpanReader(buffer.AsSpan(tsOffset, tsSize), _storage.GetKeyReverseMap()));
            }

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

                    return _mapper.Deserialize(new BsonSpanReader(fullBuffer.AsSpan(0, totalLength), _storage.GetKeyReverseMap()));
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
            return _mapper.Deserialize(new BsonSpanReader(docData, _storage.GetKeyReverseMap()));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Async version of <see cref="FindByLocation"/>.
    /// All <see cref="Span{T}"/> operations are performed synchronously on the in-memory buffer.
    /// No Span variable survives across an <c>await</c> boundary.
    /// </summary>
    internal ValueTask<T?> FindByLocationAsync(DocumentLocation location, ulong txnId, CancellationToken ct = default)
        => FindByLocationCoreAsync(location, txnId, null, ct);

    // Called when the primary page for this location is already in memory (page-cache fast path).
    // The preloadedPage buffer skips the initial ReadPageAsync; overflow chain pages are still read
    // from storage as needed.
    internal ValueTask<T?> FindByLocationAsync(DocumentLocation location, ulong txnId, byte[] preloadedPage, CancellationToken ct = default)
        => FindByLocationCoreAsync(location, txnId, preloadedPage, ct);

    private async ValueTask<T?> FindByLocationCoreAsync(DocumentLocation location, ulong txnId, byte[]? preloadedPage, CancellationToken ct)
    {
        byte[]? ownedBuffer = null;
        byte[] buffer;

        if (preloadedPage is not null)
        {
            buffer = preloadedPage;
        }
        else
        {
            ownedBuffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
            await _storage.ReadPageAsync(location.PageId, txnId, ownedBuffer.AsMemory(0, _storage.PageSize), ct).ConfigureAwait(false);
            buffer = ownedBuffer;
        }

        try
        {
            // --- all Span work is sync after the first read ---
            var pageType = (PageType)buffer[4];
            if (pageType == PageType.Free || pageType == PageType.Empty)
                return default;

            if (pageType == PageType.TimeSeries)
            {
                int tsOffset = location.SlotIndex;
                if (tsOffset < TimeSeriesPage.DataOffset || tsOffset + 4 > buffer.Length)
                    return default;
                int tsSize = BitConverter.ToInt32(buffer.AsSpan(tsOffset, 4));
                if (tsSize <= 0 || tsOffset + tsSize > buffer.Length)
                    return default;
                return _mapper.Deserialize(new BsonSpanReader(buffer.AsSpan(tsOffset, tsSize), _storage.GetKeyReverseMap()));
            }

            var header = SlottedPageHeader.ReadFrom(buffer);
            if (location.SlotIndex >= header.SlotCount) return default;

            var slotOffset = SlottedPageHeader.Size + (location.SlotIndex * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));
            if ((slot.Flags & SlotFlags.Deleted) != 0) return default;

            if ((slot.Flags & SlotFlags.HasOverflow) != 0)
            {
                // Extract scalar values from the buffer before the first await in the loop
                int totalLength        = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(buffer.AsSpan(slot.Offset, 4));
                uint nextOverflowPage  = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(buffer.AsSpan(slot.Offset + 4, 4));
                int primaryChunkSize   = slot.Length - 8;

                var fullBuffer = ArrayPool<byte>.Shared.Rent(totalLength);
                // Use a dedicated overflow-chain buffer so the primary page buffer (which may be a
                // shared cached entry from FindAllAsync) is never overwritten during the chain walk.
                var overflowBuffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
                try
                {
                    // Copy primary chunk synchronously (no await here)
                    buffer.AsSpan(slot.Offset + 8, primaryChunkSize).CopyTo(fullBuffer.AsSpan(0, primaryChunkSize));

                    int bytesCopied = primaryChunkSize;
                    uint currentPage = nextOverflowPage;

                    while (currentPage != 0 && bytesCopied < totalLength)
                    {
                        ct.ThrowIfCancellationRequested();
                        await _storage.ReadPageAsync(currentPage, txnId, overflowBuffer.AsMemory(0, _storage.PageSize), ct).ConfigureAwait(false);

                        // Recreate SlottedPageHeader span after each await — safe because overflowBuffer is byte[]
                        uint nextPage   = SlottedPageHeader.ReadFrom(overflowBuffer).NextOverflowPage;
                        int chunkSize   = Math.Min(_storage.PageSize - SlottedPageHeader.Size, totalLength - bytesCopied);
                        overflowBuffer.AsSpan(SlottedPageHeader.Size, chunkSize).CopyTo(fullBuffer.AsSpan(bytesCopied));
                        bytesCopied    += chunkSize;
                        currentPage     = nextPage;
                    }

                    return _mapper.Deserialize(new BsonSpanReader(fullBuffer.AsSpan(0, totalLength), _storage.GetKeyReverseMap()));
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(overflowBuffer);
                    ArrayPool<byte>.Shared.Return(fullBuffer);
                }
            }

            if (slot.Offset + slot.Length > buffer.Length)
                throw new InvalidOperationException($"Corrupted slot: Offset={slot.Offset}, Length={slot.Length}, PageId={location.PageId}");

            // Inline deserialization — Span created after the only await, safe
            return _mapper.Deserialize(new BsonSpanReader(buffer.AsSpan(slot.Offset, slot.Length), _storage.GetKeyReverseMap()));
        }
        finally
        {
            // Only return the buffer to the pool when we own it; caller-provided pages
            // are managed by the caller (e.g. the per-query page cache in QueryIndexAsync).
            if (ownedBuffer is not null)
                ArrayPool<byte>.Shared.Return(ownedBuffer);
        }
    }

    #endregion

    #region Update & Delete

    /// <summary>
    /// Asynchronously updates an existing document in the collection
    /// </summary>
    public async Task<bool> UpdateAsync(T entity, CancellationToken ct = default)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring collection lock (Update).");
        try
        {
            var result = await UpdateCore(entity);
            return result;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    /// <summary>
    /// Asynchronously updates multiple documents in a single transaction.
    /// </summary>
    public async Task<int> UpdateBulkAsync(IEnumerable<T> entities, CancellationToken ct = default)
    {
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        var entityList = entities.ToList();
        int updateCount = 0;

        if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring collection lock (UpdateBulk).");
        try
        {
            updateCount = await UpdateBulkInternal(entityList);
            return updateCount;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    private async Task<int> UpdateBulkInternal(List<T> entityList)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        int updateCount = 0;
        const int BATCH_SIZE = 50;

        for (int batchStart = 0; batchStart < entityList.Count; batchStart += BATCH_SIZE)
        {
            int batchEnd = Math.Min(batchStart + BATCH_SIZE, entityList.Count);
            int batchCount = batchEnd - batchStart;

            // PHASE 1: Parallel Serialization
            var serializedBatch = new (TId id, byte[] data, int length, bool found)[batchCount];

            for (int i = 0; i < batchCount; i++)
            {
                var entity = entityList[batchStart + i];
                var id = _mapper.GetId(entity);
                var key = _mapper.ToIndexKey(id);

                // Check if entity exists
                // We do this sequentially to avoid ThreadPool exhaustion or IO-related deadlocks
                if (_primaryIndex.TryFind(key, out var _, transaction.TransactionId))
                {
                    var length = SerializeWithRetry(entity, out var buffer);
                    serializedBatch[i] = (id, buffer, length, true);
                }
                else
                {
                    serializedBatch[i] = (default!, null!, 0, false);
                }
            }

            // PHASE 2: Sequential UpdateAsync
            for (int i = 0; i < batchCount; i++)
            {
                var (id, docData, length, found) = serializedBatch[i];
                if (!found) continue;

                var entity = entityList[batchStart + i];
                try
                {
                    if (await UpdateDataCore(id, entity, docData, length))
                        updateCount++;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(docData);
                }
            }
        }
        return updateCount;
    }

    private async Task<bool> UpdateCore(T entity)
    {
        var id = _mapper.GetId(entity);
        var length = SerializeWithRetry(entity, out var buffer);
        try
        {
            return await UpdateDataCore(id, entity, buffer, length);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private async Task<bool> UpdateDataCore(TId id, T entity, byte[] docData, int docLength = -1)
    {
        if (docLength >= 0 && docLength < docData.Length)
            docData = docData[..docLength]; // trim to actual serialized size
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var key = _mapper.ToIndexKey(id);
        var bytesWritten = docData.Length;

        if (!_primaryIndex.TryFind(key, out var oldLocation, transaction.TransactionId))
            return false;

        // Retrieve old version for index updates
        var oldEntity = await FindByLocation(oldLocation);
        if (oldEntity == null) return false;

        // Read old page
        var pageBuffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            _storage.ReadPage(oldLocation.PageId, transaction.TransactionId, pageBuffer);

            var slotOffset = SlottedPageHeader.Size + (oldLocation.SlotIndex * SlotEntry.Size);
            var oldSlot = SlotEntry.ReadFrom(pageBuffer.AsSpan(slotOffset));

            if (bytesWritten <= oldSlot.Length && (oldSlot.Flags & SlotFlags.HasOverflow) == 0)
            {
                // In-place update
                docData.CopyTo(pageBuffer.AsSpan(oldSlot.Offset, bytesWritten));
                var newSlot = oldSlot;
                newSlot.Length = (ushort)bytesWritten;
                newSlot.WriteTo(pageBuffer.AsSpan(slotOffset));
                _storage.WritePage(oldLocation.PageId, transaction.TransactionId, pageBuffer);

                // Notify secondary indexes (primary index unchanged)
                _indexManager.UpdateInAll(oldEntity, entity, oldLocation, oldLocation, transaction);

                // Notify CDC
                await NotifyCdc(OperationType.Update, id, docData);
                return true;
            }
            else
            {
                // Delete old + insert new
                await DeleteCore(id, notifyCdc: false);

                DocumentLocation newLocation;
                if (bytesWritten + SlotEntry.Size <= _maxDocumentSizeForSinglePage)
                {
                    var newPageId = await FindPageWithSpace(bytesWritten + SlotEntry.Size);
                    if (newPageId == 0) newPageId = await AllocateNewDataPage();
                    var newSlotIndex = await InsertIntoPage(newPageId, docData);
                    newLocation = new DocumentLocation(newPageId, newSlotIndex);
                }
                else
                {
                    var (newPageId, newSlotIndex) = await InsertWithOverflow(docData);
                    newLocation = new DocumentLocation(newPageId, newSlotIndex);
                }

                _primaryIndex.Insert(key, newLocation, transaction.TransactionId);
                _indexManager.UpdateInAll(oldEntity, entity, oldLocation, newLocation, transaction);

                // Notify CDC
                await NotifyCdc(OperationType.Update, id, docData);
                return true;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    /// <summary>
    /// Asynchronously deletes a document by its primary key.
    /// </summary>
    public async Task<bool> DeleteAsync(TId id, CancellationToken ct = default)
    {
        if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring collection lock (Delete).");
        try
        {
            var result = await DeleteCore(id);
            return result;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    /// <summary>
    /// Asynchronously deletes multiple documents in a single transaction.
    /// </summary>
    public async Task<int> DeleteBulkAsync(IEnumerable<TId> ids, CancellationToken ct = default)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));

        int deleteCount = 0;
        if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring collection lock (DeleteBulk).");
        try
        {
            deleteCount = await DeleteBulkInternal(ids);
            return deleteCount;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    private async Task<int> DeleteBulkInternal(IEnumerable<TId> ids)
    {
        int deleteCount = 0;
        foreach (var id in ids)
        {
            if (await DeleteCore(id))
                deleteCount++;
        }
        return deleteCount;
    }

    private async Task<bool> DeleteCore(TId id, bool notifyCdc = true)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var key = _mapper.ToIndexKey(id);
        if (!_primaryIndex.TryFind(key, out var location, transaction.TransactionId))
            return false;

        // Notify secondary indexes BEFORE deleting document from storage
        var entity = await FindByLocation(location);
        if (entity != null)
        {
            _indexManager.DeleteFromAll(entity, location, transaction);
        }

        // Read page
        var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            _storage.ReadPage(location.PageId, transaction.TransactionId, buffer);

            var slotOffset = SlottedPageHeader.Size + (location.SlotIndex * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));

            // Check if slot has overflow and free it
            if ((slot.Flags & SlotFlags.HasOverflow) != 0)
            {
                var nextOverflowPage = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(
                    buffer.AsSpan(slot.Offset + 4, 4));
                await FreeOverflowChain(nextOverflowPage);
            }

            // Mark slot as deleted
            var newSlot = slot;
            newSlot.Flags |= SlotFlags.Deleted;
            newSlot.WriteTo(buffer.AsSpan(slotOffset));

            // Compact the page: reclaim the bytes that belonged to the deleted document
            // and advance FreeSpaceEnd so FindPageWithSpace can see the freed space.
            CompactPage(buffer.AsSpan(0, _storage.PageSize));

            _storage.WritePage(location.PageId, transaction.TransactionId, buffer);

            // UpdateAsync free space map with post-compaction free bytes
            var compactedHeader = SlottedPageHeader.ReadFrom(buffer.AsSpan(0, SlottedPageHeader.Size));
            _freeSpaceMap[location.PageId] = (ushort)compactedHeader.AvailableFreeSpace;

            // Remove from primary index
            _primaryIndex.Delete(key, location, transaction.TransactionId);

            // Notify CDC
            if (notifyCdc) 
                await NotifyCdc(OperationType.Delete, id);

            return true;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private async Task FreeOverflowChain(uint overflowPageId)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var tempBuffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            while (overflowPageId != 0)
            {
                _storage.ReadPage(overflowPageId, transaction.TransactionId, tempBuffer);
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
    /// Asynchronously counts all documents in the collection.
    /// If called within a transaction, will count uncommitted changes.
    /// </summary>
    /// <param name="ct">Cancellation token</param>
    /// <returns>Number of documents</returns>
    public async Task<int> CountAsync(CancellationToken ct = default)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        // Count all entries in primary index
        // Use generic min/max keys for the index
        var minKey = IndexKey.MinKey;
        var maxKey = IndexKey.MaxKey;
        return _primaryIndex.Range(minKey, maxKey, IndexDirection.Forward, transaction.TransactionId).Count();
    }

    /// <summary>
    /// Counts documents matching <paramref name="whereClause"/> without fully materializing
    /// the result set in memory.
    /// <list type="number">
    ///   <item>If the predicate targets an indexed field with an exact-covering filter,
    ///         the count is derived from an index key-only leaf scan — zero data-page reads.</item>
    ///   <item>Otherwise the documents are streamed through <see cref="FetchAsync"/> (index /
    ///         BSON / full-scan strategies) and counted in a tight loop without accumulating
    ///         a <c>List&lt;T&gt;</c>.</item>
    /// </list>
    /// </summary>
    [RequiresDynamicCode("Count-by-predicate uses index optimization and Expression.Compile() which require dynamic code generation.")]
    [RequiresUnreferencedCode("Count-by-predicate uses reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    internal async Task<int> CountByPredicateAsync(
        System.Linq.Expressions.LambdaExpression whereClause,
        CancellationToken ct = default)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();

        // Strategy 1: Index key-only scan — no data-page reads at all.
        // Applicable whenever the predicate targets an indexed field AND the index fully
        // covers the predicate (HasResiduePredicate=false).  Strict operators (> / <) set
        // IsExactFilter=false but HasResiduePredicate=false, so we still handle them here
        // by passing the correct start/end inclusivity to CountRange.
        // When HasResiduePredicate=true (compound AND with a non-indexed clause), we must
        // fall through to FetchAsync so the residue predicate is applied per document.
        var indexOpt = Query.IndexOptimizer.TryOptimize<T>(whereClause, GetIndexes(), ConverterRegistry);
        if (indexOpt != null
            && !indexOpt.IsVectorSearch
            && !indexOpt.IsSpatialSearch
            && !indexOpt.HasResiduePredicate)
        {
            var index = _indexManager.GetIndex(indexOpt.IndexName);
            if (index != null)
            {
                // Use the per-bound inclusivity flags from OptimizationResult.
                // These are set correctly for every operator (==, >=, >, <=, <) and
                // propagated through AND-merges, so compound predicates like
                // x.Price > 50 && x.Price < 90 get both boundaries exclusive.
                return index.CountRange(indexOpt.MinValue, indexOpt.MaxValue,
                    indexOpt.StartInclusive, indexOpt.EndInclusive, transaction);
            }
        }

        // Strategy 2: BSON-level count — evaluates the predicate on raw BSON bytes without
        // deserialising T.  Combined with the Phase 1 widening of BsonExpressionEvaluator
        // this covers the vast majority of real-world WHERE predicates.
        if (BsonExpressionEvaluator.TryCompile<T>(whereClause, ConverterRegistry, _storage.GetKeyMap()) is { } bsonPred)
            return await CountScanAsync(bsonPred, ct).ConfigureAwait(false);

        // Strategy 3: Stream matching documents via FetchAsync and count without
        // keeping them in a List<T>, relying on FetchAsync's own BSON-scan / full-scan
        // strategies to minimise allocations.
        int count = 0;
        await foreach (var _ in FetchAsync(whereClause, int.MaxValue, ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            count++;
        }
        return count;
    }

    /// <summary>
    /// Queries the collection using an expression tree predicate, enabling index optimisation.
    /// When the predicate references an indexed field the query engine performs a BTree lookup
    /// instead of a full collection scan.
    /// The returned <see cref="IBLiteQueryable{T}"/> preserves the expression tree so that
    /// terminal operators (<c>FirstOrDefaultAsync</c>, <c>ToListAsync</c>, etc.) can inject
    /// limits before materialisation.
    /// </summary>
    [RequiresDynamicCode("LINQ-style find operations use Expression.Compile() and index optimization which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ-style find operations use reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    public IAsyncEnumerable<T> FindAsync(
        System.Linq.Expressions.Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
        => FetchAsync(predicate, int.MaxValue, ct);

    /// <summary>
    /// Returns the first document matching <paramref name="predicate"/>, or <c>null</c> if none.
    /// Stops reading from the storage engine as soon as one document is found (fetchLimit = 1).
    /// </summary>
    [RequiresDynamicCode("LINQ-style find operations use Expression.Compile() and index optimization which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ-style find operations use reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    public async Task<T?> FindOneAsync(
        System.Linq.Expressions.Expression<Func<T, bool>> predicate,
        CancellationToken ct = default)
    {
        // Fast path: equality query on an indexed field.
        // Bypasses FetchAsync / QueryIndexAsync / page-cache Dictionary entirely.
        // Path: IndexOptimizer → CollectionSecondaryIndex.Seek (TryFindFirst) → FindByLocationAsync.
        var indexOpt = Query.IndexOptimizer.TryOptimize<T>(predicate, GetIndexes(), ConverterRegistry);
        if (indexOpt != null && !indexOpt.IsRange && !indexOpt.IsVectorSearch && !indexOpt.IsSpatialSearch
            && indexOpt.MinValue != null)
        {
            var index = _indexManager.GetIndex(indexOpt.IndexName);
            if (index != null)
            {
                var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync().ConfigureAwait(false);
                var location = index.Seek(indexOpt.MinValue, transaction);
                if (location == null) return default;
                return await FindByLocationAsync(location.Value, transaction.TransactionId, ct).ConfigureAwait(false);
            }
        }

        // General path: range / vector / spatial / no index.
        await foreach (var item in FetchAsync(predicate, 1, ct).ConfigureAwait(false))
            return item;
        return default;
    }

    /// <summary>
    /// Core data-fetching primitive used by both <see cref="FindAsync"/> and
    /// <see cref="BLite.Core.Query.BTreeQueryProvider{TId,T}"/>.
    /// Selects the cheapest access strategy in order:
    /// <list type="number">
    ///   <item>BTree index scan (narrows candidates to the index range) + in-memory WHERE filter</item>
    ///   <item>BSON-level predicate scan (compiled once, no full deserialization per-document)</item>
    ///   <item>Full collection scan + in-memory predicate filter</item>
    /// </list>
    /// All three strategies guarantee the returned stream is fully filtered: callers
    /// never need to re-apply <paramref name="whereClause"/> after consuming this enumerable.
    /// </summary>
    /// <param name="whereClause">
    /// Optional filter.  Must be an <c>Expression&lt;Func&lt;T, bool&gt;&gt;</c> at runtime.
    /// Pass <c>null</c> to enumerate all documents.
    /// </param>
    /// <param name="fetchLimit">
    /// Maximum number of documents to yield.  Pass <see cref="int.MaxValue"/> for no limit.
    /// </param>
    [RequiresDynamicCode("LINQ-style fetch operations use Expression.Compile() and index optimization which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ-style fetch operations use reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    internal async IAsyncEnumerable<T> FetchAsync(
        System.Linq.Expressions.LambdaExpression? whereClause,
        int fetchLimit,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        int yielded = 0;

        if (whereClause != null)
        {
            // Lazy compile: Expression.Compile() is expensive (~1 ms) — pay it at most once
            // per FetchAsync call, only if we actually need the compiled delegate.
            Func<T, bool>? compiledWhere = null;
            Func<T, bool> GetCompiled() =>
                compiledWhere ??= ((System.Linq.Expressions.Expression<Func<T, bool>>)whereClause).Compile();

            // ── Strategy 1: BTree index ───────────────────────────────────────
            // FilterCompleteness.Exact      → index range == full WHERE result; no post-filter.
            // FilterCompleteness.StrictBoundary / .PartialAnd
            //   → index only narrows candidates; reuse the lazily-compiled Func<T,bool> as post-filter.
            var indexOpt = Query.IndexOptimizer.TryOptimize<T>(whereClause, GetIndexes(), ConverterRegistry);
            if (indexOpt != null)
            {
                if (indexOpt.IsVectorSearch)
                {
                    await foreach (var item in VectorSearchAsync(indexOpt.IndexName, indexOpt.VectorQuery!, indexOpt.K, ct: ct))
                        if (indexOpt.FilterCompleteness == Query.IndexOptimizer.FilterCompleteness.Exact || GetCompiled()(item)) { yield return item; if (++yielded >= fetchLimit) yield break; }
                }
                else if (indexOpt.IsSpatialSearch)
                {
                    var spatialSeq = indexOpt.SpatialType == Query.IndexOptimizer.SpatialQueryType.Near
                        ? NearAsync(indexOpt.IndexName, indexOpt.SpatialPoint, indexOpt.RadiusKm, ct)
                        : WithinAsync(indexOpt.IndexName, indexOpt.SpatialMin, indexOpt.SpatialMax, ct);
                    await foreach (var item in spatialSeq)
                        if (indexOpt.FilterCompleteness == Query.IndexOptimizer.FilterCompleteness.Exact || GetCompiled()(item)) { yield return item; if (++yielded >= fetchLimit) yield break; }
                }
                else
                {
                    await foreach (var item in QueryIndexAsync(indexOpt.IndexName, indexOpt.MinValue, indexOpt.MaxValue, ct: ct))
                        if (indexOpt.FilterCompleteness == Query.IndexOptimizer.FilterCompleteness.Exact || GetCompiled()(item)) { yield return item; if (++yielded >= fetchLimit) yield break; }
                }
                yield break;
            }

            // ── Strategy 2: BSON-level predicate scan ─────────────────────────
            // Filters at raw-BSON level before deserializing — no compiled Func<T,bool> needed.
            if (BsonExpressionEvaluator.TryCompile<T>(whereClause, ConverterRegistry, _storage.GetKeyMap()) is { } bsonPred)
            {
                await foreach (var item in ScanAsync(bsonPred, ct))
                {
                    yield return item;
                    if (++yielded >= fetchLimit) yield break;
                }
                yield break;
            }

            // ── Strategy 3: full scan + in-memory filter ──────────────────────
            // Reuses compiledWhere if Strategy 1 already triggered it; compiles otherwise.
            await foreach (var item in FindAllAsync(ct))
                if (GetCompiled()(item)) { yield return item; if (++yielded >= fetchLimit) yield break; }
        }
        else
        {
            // ── No WHERE: plain full scan with optional limit ─────────────────
            await foreach (var item in FindAllAsync(ct))
            {
                yield return item;
                if (++yielded >= fetchLimit) yield break;
            }
        }
    }

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
        // Cache once — avoids repeated volatile reads of the FrozenDictionary snapshot inside the loop.
        var keyMap = _storage.GetFrozenKeyMap();

        // Stored as compiler-constant data segment — no heap allocation.
        ReadOnlySpan<int> steps = [65536, 524288, 2097152, 16777216]; // 64KB, 512KB, 2MB, 16MB

        int minSize = _lastSerializedSize;

        foreach (var size in steps)
        {
            int bufSize = size < _storage.PageSize ? _storage.PageSize : size;

            // Skip steps that are smaller than the last known serialized size.
            // This avoids predictable failures and the associated exception overhead.
            if (bufSize < minSize) continue;

            var buffer = ArrayPool<byte>.Shared.Rent(bufSize);
            try
            {
                int bytesWritten = _mapper.Serialize(entity, new BsonSpanWriter(buffer, keyMap));

                // Inject schema version if available
                if (CurrentSchemaVersion != null)
                {
                    if (bytesWritten + 8 > buffer.Length)
                    {
                        throw new IndexOutOfRangeException("Not enough space for version field");
                    }
                    AppendVersionField(buffer, ref bytesWritten);
                }

                // Update warm-start hint: last size + 25% margin so the next document of
                // similar size starts at the right step without needing a retry.
                _lastSerializedSize = Math.Max(_lastSerializedSize, bytesWritten + (bytesWritten >> 2));

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

    /// <summary>
    /// Appends a version field to the specified BSON buffer if a current schema version is set.
    /// </summary>
    /// <remarks>The version field is only appended if a current schema version is available. The method
    /// updates the BSON document's size and ensures the buffer remains in a valid BSON format.</remarks>
    /// <param name="buffer">The byte array buffer to which the version field is appended. Must be large enough to accommodate the additional
    /// bytes.</param>
    /// <param name="bytesWritten">A reference to the number of bytes written to the buffer. Updated to reflect the new total after the version
    /// field is appended.</param>
    private void AppendVersionField(byte[] buffer, ref int bytesWritten)
    {
        if (CurrentSchemaVersion == null) return;

        int version = CurrentSchemaVersion.Value.Version;

        // BSON element for _v (Int32) with Compressed Key:
        // Type (1 byte: 0x10)
        // Key ID (2 bytes, little-endian)
        // Value (4 bytes: int32)
        // Total = 7 bytes

        int pos = bytesWritten - 1; // Position of old 0x00 terminator
        buffer[pos++] = 0x10; // Int32

        ushort versionKeyId = _storage.GetOrAddDictionaryEntry("_v");
        BinaryPrimitives.WriteUInt16LittleEndian(buffer.AsSpan(pos, 2), versionKeyId);
        pos += 2;

        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(pos, 4), version);
        pos += 4;

        buffer[pos++] = 0x00; // new document terminator

        bytesWritten = pos;

        // UpdateAsync total size (first 4 bytes)
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(0, 4), bytesWritten);
    }

    /// <summary>
    /// Asynchronously performs a vector similarity search on the specified index and returns up to
    /// the top-k matching documents.
    /// </summary>
    /// <param name="indexName">The name of the index to search</param>
    /// <param name="query">The query vector</param>
    /// <param name="k">Maximum number of nearest neighbors to return</param>
    /// <param name="efSearch">Size of the dynamic candidate list during search</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>Async enumerable of matching documents</returns>
    public async IAsyncEnumerable<T> VectorSearchAsync(
        string indexName,
        float[] query,
        int k,
        int efSearch = 100,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var index = _indexManager.GetIndex(indexName);
        if (index == null)
            throw new ArgumentException($"Index '{indexName}' not found.", nameof(indexName));

        foreach (var result in index.VectorSearch(query, k, efSearch, transaction))
        {
            ct.ThrowIfCancellationRequested();
            var doc = await FindByLocationAsync(result.Location, transaction.TransactionId, ct);
            if (doc != null) yield return doc;
        }
    }

    /// <summary>
    /// Asynchronously finds all documents located within a specified radius of a geographic center
    /// point using a spatial index.
    /// </summary>
    /// <param name="indexName">The name of the spatial index to use for the search</param>
    /// <param name="center">Latitude and longitude of the center point</param>
    /// <param name="radiusKm">The search radius in kilometers</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>Async enumerable of matching documents</returns>
    public async IAsyncEnumerable<T> NearAsync(
        string indexName,
        (double Latitude, double Longitude) center,
        double radiusKm,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var index = _indexManager.GetIndex(indexName);
        if (index == null)
            throw new ArgumentException($"Index '{indexName}' not found.", nameof(indexName));

        foreach (var loc in index.Near(center, radiusKm, transaction))
        {
            ct.ThrowIfCancellationRequested();
            var doc = await FindByLocationAsync(loc, transaction.TransactionId, ct);
            if (doc != null) yield return doc;
        }
    }

    /// <summary>
    /// Asynchronously returns all documents within the specified rectangular geographic area from
    /// the given spatial index.
    /// </summary>
    /// <param name="indexName">The name of the spatial index to search within</param>
    /// <param name="min">Minimum latitude and longitude coordinates</param>
    /// <param name="max">Maximum latitude and longitude coordinates</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>Async enumerable of matching documents</returns>
    public async IAsyncEnumerable<T> WithinAsync(
        string indexName,
        (double Latitude, double Longitude) min,
        (double Latitude, double Longitude) max,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        var index = _indexManager.GetIndex(indexName);
        if (index == null)
            throw new ArgumentException($"Index '{indexName}' not found.", nameof(indexName));

        foreach (var loc in index.Within(min, max, transaction))
        {
            ct.ThrowIfCancellationRequested();
            var doc = await FindByLocationAsync(loc, transaction.TransactionId, ct);
            if (doc != null) yield return doc;
        }
    }

    /// <summary>
    /// Subscribes to a change stream that notifies observers of changes to the collection.
    /// </summary>
    /// <remarks>The returned observable emits events as changes are detected in the collection. Observers can
    /// subscribe to receive real-time updates. The behavior of the event payload depends on the value of the
    /// capturePayload parameter.</remarks>
    /// <param name="capturePayload">true to include the full payload of changed documents in each event; otherwise, false to include only metadata
    /// about the change. The default is false.</param>
    /// <returns>An observable sequence of change stream events for the collection. Subscribers receive notifications as changes
    /// occur.</returns>
    /// <exception cref="InvalidOperationException">Thrown if change data capture (CDC) is not initialized for the storage.</exception>
    public IObservable<ChangeStreamEvent<TId, T>> Watch(bool capturePayload = false)
    {
        if (_storage.Cdc == null) throw new InvalidOperationException("CDC is not initialized.");

        return new ChangeStreamObservable<TId, T>(_storage.Cdc, _collectionName, capturePayload, _mapper, _storage.GetKeyReverseMap());
    }

    private async Task NotifyCdc(OperationType type, TId id, ReadOnlyMemory<byte> docData = default)
    {
        var transaction = await _transactionHolder.GetCurrentTransactionOrStartAsync();
        if (_storage.Cdc == null) return;

        // Early exit if no watchers for this collection - avoid allocations
        if (!_storage.Cdc.HasAnyWatchers(_collectionName)) return;

        ReadOnlyMemory<byte>? payload = null;
        if (!docData.IsEmpty && _storage.Cdc.HasPayloadWatchers(_collectionName))
        {
            payload = docData.ToArray();
        }

        var idBytes = _mapper.ToIndexKey(id).Data.ToArray();

        if (transaction is Transaction t)
        {
            t.AddChange(new InternalChangeEvent
            {
                Timestamp = DateTime.UtcNow.Ticks,
                TransactionId = transaction.TransactionId,
                CollectionName = _collectionName,
                Type = type,
                IdBytes = idBytes,
                PayloadBytes = payload
            });
        }
    }

    /// <summary>
    /// Releases all resources used by the current instance of the class.
    /// </summary>
    /// <remarks>Call this method when you are finished using the object to free unmanaged resources
    /// immediately. After calling Dispose, the object should not be used.</remarks>
    public void Dispose()
    {
        _indexManager.Dispose();
        GC.SuppressFinalize(this);
    }
}

