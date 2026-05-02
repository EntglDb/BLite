using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BLite.Bson;
using BLite.Core.CDC;
using BLite.Core.Collections;
using BLite.Core.Indexing;
using BLite.Core.Indexing.Internal;
using BLite.Core.Retention;
using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace BLite.Core;

/// <summary>Typed descriptor for a secondary index on a DynamicCollection.</summary>
public sealed record DynamicIndexDescriptor(
    string Name,
    IndexType Type,
    string FieldPath,
    int Dimensions,
    VectorMetric Metric,
    bool IsUnique = false)
{
    public bool IsVector => Type == IndexType.Vector;
    public bool IsSpatial => Type == IndexType.Spatial;
    public bool IsStandard => Type == IndexType.BTree;
}

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
    private int WriteLockTimeoutMs => _storage.LockTimeout.WriteTimeoutMs;
    private readonly FreeSpaceIndex _fsi;
    // Cached delegate — avoids per-call closure allocation when passed to _fsi.FindPage.
    private readonly Func<uint, ulong, bool> _isPageLocked;
    private readonly int _maxDocumentSizeForSinglePage;
    private uint _currentDataPage;
    private bool _isTimeSeries;

    // ── Generalized Retention Policy ─────────────────────────────────────────
    private RetentionPolicy? _retentionPolicy;
    private Timer? _retentionTimer;
    private int _retentionRunning; // 0=idle, 1=running (Interlocked flag)

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

    // ── Internal access for extension methods ─────────────────────────────
    internal Metrics.MetricsDispatcher? MetricsDispatcher => _storage.MetricsDispatcher;

    /// <summary>
    /// Creates or opens a dynamic collection.
    /// </summary>
    /// <param name="storage">The storage engine instance</param>
    /// <param name="transactionHolder">Transaction holder for ACID operations</param>
    /// <param name="collectionName">Name of the collection</param>
    /// <param name="idType">The BSON type used for the _id field (default: ObjectId)</param>
    public DynamicCollection(StorageEngine storage, ITransactionHolder transactionHolder, string collectionName, BsonIdType idType = BsonIdType.ObjectId)
        : this(storage, transactionHolder, collectionName, idType, null)
    {
    }

    internal DynamicCollection(StorageEngine storage, ITransactionHolder transactionHolder, string collectionName, BsonIdType idType, FreeSpaceIndex? freeSpaceIndex)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _transactionHolder = transactionHolder ?? throw new ArgumentNullException(nameof(transactionHolder));
        _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
        _idType = idType;
        _maxDocumentSizeForSinglePage = _storage.PageSize - 128;
        _fsi = freeSpaceIndex ?? new FreeSpaceIndex(_storage.PageSize);
        _isPageLocked = _storage.IsPageLocked;

        // Load or create collection metadata
        var metadata = _storage.GetCollectionMetadata(_collectionName);
        uint primaryRootPageId = 0;

        if (metadata != null)
        {
            primaryRootPageId = metadata.PrimaryRootPageId;
            _isTimeSeries = metadata.IsTimeSeries;

            // Restore generalized retention policy and set up the scheduled timer if needed.
            if (metadata.GeneralRetentionPolicy != null)
                ApplyRetentionPolicyConfig(metadata.GeneralRetentionPolicy);

            // Restore secondary indexes from metadata
            foreach (var idxMeta in metadata.Indexes)
            {
                if (idxMeta.PropertyPaths.Length == 0) continue;
                var fieldPath = idxMeta.PropertyPaths[0];
                var indexName = idxMeta.Name; // capture for closure

                // Root page ID tracking is deferred to PersistIndexMetadata() at commit time.
                // BTreeIndex already updates its internal _rootPageId before invoking this callback,
                // so no action is needed here. Writing to disk during Insert was both unnecessary
                // contention and a crash-safety bug (uncommitted root IDs on disk).
                Action<uint> makeRootCallback = _ => { };

                switch (idxMeta.Type)
                {
                    case IndexType.BTree:
                    {
                        var opts = idxMeta.IsUnique
                            ? IndexOptions.CreateUnique(idxMeta.PropertyPaths)
                            : IndexOptions.CreateBTree(idxMeta.PropertyPaths);
                        var btree = new BTreeIndex(_storage, opts, idxMeta.RootPageId, makeRootCallback);
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

        var indexOptions = IndexOptions.CreateUnique("_id");
        // Root page ID tracking is deferred to PersistIndexMetadata() at commit time.
        // BTreeIndex already updates its internal _rootPageId before invoking this callback.
        _primaryIndex = new BTreeIndex(_storage, indexOptions, primaryRootPageId,
            onRootChanged: _ => { });

        // Persist root page if newly allocated
        if (metadata.PrimaryRootPageId != _primaryIndex.RootPageId)
        {
            metadata.PrimaryRootPageId = _primaryIndex.RootPageId;
            _storage.SaveCollectionMetadata(metadata);
        }

        // Rebuild the free-space index from existing page headers on cold start.
        RebuildFreeSpaceIndex();
    }

    private void RebuildFreeSpaceIndex()
    {
        // Read only the fixed-size page header (24 bytes) rather than renting a full-page
        // buffer.  Using a stack-allocated span avoids any heap allocation and reduces
        // memory traffic: we copy 24 bytes per page instead of the full page size.
        Span<byte> hdrBuf = stackalloc byte[SlottedPageHeader.Size];
        uint lastDataPage = 0;

        foreach (var pageId in _storage.GetCollectionPageIds(_collectionName))
        {
            _storage.ReadPageHeader(pageId, null, hdrBuf);
            var hdr = SlottedPageHeader.ReadFrom(hdrBuf);

            if (hdr.PageType == PageType.Data)
            {
                _fsi.Update(pageId, hdr.AvailableFreeSpace);
                lastDataPage = pageId;
            }
        }

        if (lastDataPage != 0)
            _currentDataPage = lastDataPage;
    }

    /// <summary>The collection name.</summary>
    public string Name => _collectionName;

    /// <summary>The ID type used by this collection.</summary>
    public BsonIdType IdType => _idType;

    /// <summary>Whether this collection is configured as a TimeSeries collection.</summary>
    public bool IsTimeSeries => _isTimeSeries;

    /// <summary>
    /// Returns the current TimeSeries configuration (retention policy and TTL field name).
    /// Returns <c>(0, null)</c> when the collection is not a TimeSeries.
    /// </summary>
    public (long RetentionPolicyMs, string? TtlFieldName) GetTimeSeriesConfig()
    {
        var meta = _storage.GetCollectionMetadata(_collectionName);
        if (meta == null || !meta.IsTimeSeries) return (0, null);
        return (meta.RetentionPolicyMs, meta.TtlFieldName);
    }

    /// <summary>
    /// Configures the collection as a TimeSeries with a retention policy (TTL).
    /// </summary>
    public void SetTimeSeries(string ttlFieldName, TimeSpan retentionPolicy)
    {
        var meta = _storage.GetCollectionMetadata(_collectionName);
        if (meta == null) meta = new CollectionMetadata { Name = _collectionName };
        meta.IsTimeSeries = true;
        meta.TtlFieldName = ttlFieldName;
        meta.RetentionPolicyMs = (long)retentionPolicy.TotalMilliseconds;
        meta.LastPruningTimestamp = DateTime.UtcNow.Ticks;
        _storage.SaveCollectionMetadata(meta);
        _isTimeSeries = true; // UpdateAsync in-memory flag so subsequent inserts route to TS path
    }

    /// <summary>
    /// Forces pruning of expired TimeSeries documents immediately, regardless of insert counters.
    /// Primarily intended for testing; in production, pruning is triggered automatically on insert.
    /// NOTE (v1 known limitation): the primary BTree index retains stale entries for pruned pages.
    /// FindAll will silently skip null results from freed pages, so reads remain safe.
    /// </summary>
    public async Task ForcePruneAsync()
    {
        if (!_isTimeSeries)
            throw new InvalidOperationException("ForcePrune is only valid on TimeSeries collections.");

        var meta = _storage.GetCollectionMetadata(_collectionName);
        if (meta == null || meta.RetentionPolicyMs <= 0) return;

        var transaction = _storage.BeginTransaction(IsolationLevel.ReadCommitted);
        _storage.PruneTimeSeries(meta, transaction);
        meta.InsertedSinceLastPruning = 0;
        meta.LastPruningTimestamp = DateTime.UtcNow.Ticks;
        _storage.SaveCollectionMetadata(meta);
        await transaction.CommitAsync();
    }

    #region Retention Policy

    private static readonly TimeSpan DefaultScheduledInterval = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Configures a generalized retention policy for this collection.
    /// The policy is persisted in collection metadata and survives engine restarts.
    /// </summary>
    /// <param name="configure">Action that configures the retention policy via the fluent builder.</param>
    public void SetRetentionPolicy(Action<RetentionPolicyBuilder> configure)
    {
        if (configure == null) throw new ArgumentNullException(nameof(configure));
        var builder = new RetentionPolicyBuilder();
        configure(builder);
        var policy = builder.Build();

        var meta = _storage.GetCollectionMetadata(_collectionName)
                   ?? new CollectionMetadata { Name = _collectionName };
        meta.GeneralRetentionPolicy = policy;
        _storage.SaveCollectionMetadata(meta);

        ApplyRetentionPolicyConfig(policy);
    }

    /// <summary>
    /// Immediately runs the retention policy regardless of any triggers.
    /// Primarily intended for testing; in production, retention is triggered automatically.
    /// </summary>
    public async Task ForceApplyRetentionPolicyAsync(CancellationToken ct = default)
    {
        // Load from persisted metadata if not yet set in memory.
        if (_retentionPolicy == null)
        {
            var metaPolicy = _storage.GetCollectionMetadata(_collectionName)?.GeneralRetentionPolicy;
            if (metaPolicy == null) return;
            ApplyRetentionPolicyConfig(metaPolicy);
        }

        if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring collection lock (ForceApplyRetentionPolicy).");
        try
        {
            await ApplyRetentionPolicyCoreAsync(ct);
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    /// <summary>
    /// Sets up the in-memory retention policy reference and starts/stops the scheduled timer.
    /// Called from the constructor (when restoring persisted policy) and from SetRetentionPolicy.
    /// </summary>
    private void ApplyRetentionPolicyConfig(RetentionPolicy policy)
    {
        _retentionPolicy = policy;

        // Dispose any existing timer before (re)configuring.
        _retentionTimer?.Dispose();
        _retentionTimer = null;

        if ((policy.Triggers & RetentionTrigger.Scheduled) != 0)
        {
            var interval = policy.ScheduledIntervalMs > 0
                ? TimeSpan.FromMilliseconds(policy.ScheduledIntervalMs)
                : DefaultScheduledInterval;

            _retentionTimer = new Timer(
                callback: _ => _ = RunScheduledRetentionAsync(),
                state: null,
                dueTime: interval,
                period: interval);
        }
    }

    private async Task RunScheduledRetentionAsync()
    {
        // Non-overlapping: skip this run if a previous one is still in progress.
        if (Interlocked.CompareExchange(ref _retentionRunning, 1, 0) != 0)
            return;
        try
        {
            if (_retentionPolicy == null) return;

            if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs))
                return; // skip if lock is not available quickly
            try
            {
                await ApplyRetentionPolicyCoreAsync(CancellationToken.None);
            }
            finally
            {
                _collectionLock.Release();
            }
        }
        catch (OperationCanceledException)
        {
            // Ignore cancellation in background retention execution.
        }
        catch (Exception ex) when (!IsFatalException(ex))
        {
            // Swallow non-fatal exceptions (including ObjectDisposedException during Dispose)
            // so background retention can never destabilise the process.
        }
        finally
        {
            Interlocked.Exchange(ref _retentionRunning, 0);
        }
    }

    private static bool IsFatalException(Exception ex) =>
        ex is OutOfMemoryException
        or StackOverflowException
        or AccessViolationException
        or AppDomainUnloadedException
        or BadImageFormatException
        or CannotUnloadAppDomainException
        or InvalidProgramException
        or ThreadAbortException;

    /// <summary>
    /// Core retention enforcement logic. Must be called with the collection lock held.
    /// </summary>
    private async Task ApplyRetentionPolicyCoreAsync(CancellationToken ct)
    {
        var policy = _retentionPolicy!;

        // ── 1. Collect all document IDs and (optionally) timestamps ──────────
        var entries = new List<(BsonId Id, long TimestampTicks)>();
        long nowTicks = DateTime.UtcNow.Ticks;

        foreach (var entry in _primaryIndex.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, 0UL))
        {
            ct.ThrowIfCancellationRequested();
            BsonId id;
            long ticks = 0;

            if (policy.TimestampField != null || policy.MaxAgeMs > 0)
            {
                var doc = ReadDocumentAt(entry.Location, 0UL);
                if (doc == null) continue;

                if (!TryExtractId(doc, out id))
                {
                    // IndexKey.Data includes a 1-byte prefix; strip it to get the raw key bytes.
                    var rawKeyBytes = entry.Key.Data.Length > 1
                        ? entry.Key.Data.Slice(1)
                        : entry.Key.Data;
                    id = BsonId.FromBytes(rawKeyBytes, _idType);
                }

                if (policy.TimestampField != null && TryGetNestedValue(doc, policy.TimestampField, out var tsVal))
                {
                    ticks = tsVal.Type switch
                    {
                        BsonType.DateTime => tsVal.AsDateTime.Ticks,
                        BsonType.Int64    => tsVal.AsInt64,
                        _                 => 0
                    };
                }
            }
            else
            {
                // IndexKey.Data includes a 1-byte prefix; strip it to get the raw key bytes.
                var rawKeyBytes = entry.Key.Data.Length > 1
                    ? entry.Key.Data.Slice(1)
                    : entry.Key.Data;
                id = BsonId.FromBytes(rawKeyBytes, _idType);
            }

            entries.Add((id, ticks));
        }

        var toDelete = new HashSet<BsonId>();

        // ── 2. MaxAge — delete documents older than the cutoff ───────────────
        if (policy.MaxAgeMs > 0 && policy.TimestampField != null)
        {
            long cutoff = nowTicks - (policy.MaxAgeMs * TimeSpan.TicksPerMillisecond);
            foreach (var (id, ticks) in entries)
            {
                ct.ThrowIfCancellationRequested();
                // ticks == 0 means the timestamp field was absent or unreadable → exempt
                if (ticks > 0 && ticks < cutoff)
                    toDelete.Add(id);
            }
        }

        // ── 3. MaxDocumentCount — delete oldest documents over the limit ─────
        if (policy.MaxDocumentCount > 0 && entries.Count > policy.MaxDocumentCount)
        {
            var excess = (int)(entries.Count - policy.MaxDocumentCount);
            // Sort by timestamp ascending (oldest first). If no timestamp, keep primary-index
            // order which is equivalent to insertion order for ObjectId-keyed collections.
            var ordered = policy.TimestampField != null
                ? entries.OrderBy(e => e.TimestampTicks == 0 ? long.MaxValue : e.TimestampTicks)
                : entries.AsEnumerable();

            foreach (var (id, _) in ordered.Take(excess))
            {
                ct.ThrowIfCancellationRequested();
                toDelete.Add(id);
            }
        }

        // ── 4. MaxSizeBytes — delete oldest documents until size is within limit
        if (policy.MaxSizeBytes > 0)
        {
            long estimatedBytes = EstimateCollectionSizeBytes();
            if (estimatedBytes > policy.MaxSizeBytes)
            {
                var ordered = policy.TimestampField != null
                    ? entries.OrderBy(e => e.TimestampTicks == 0 ? long.MaxValue : e.TimestampTicks)
                    : entries.AsEnumerable();

                foreach (var (id, _) in ordered)
                {
                    ct.ThrowIfCancellationRequested();
                    if (estimatedBytes <= policy.MaxSizeBytes) break;
                    toDelete.Add(id);
                    // Rough estimate: assume average document size reduces pages linearly
                    estimatedBytes -= _storage.PageSize / Math.Max(entries.Count, 1);
                }
            }
        }

        if (toDelete.Count == 0) return;

        // ── 5. Delete the collected IDs in a single transaction ──────────────
        var transaction = _storage.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            foreach (var id in toDelete)
            {
                ct.ThrowIfCancellationRequested();
                var key = new IndexKey(id.ToBytes());
                if (!_primaryIndex.TryFind(key, out var loc, transaction.TransactionId))
                    continue;

                var doc = ReadDocumentAt(loc, transaction.TransactionId);
                _primaryIndex.Delete(key, loc, transaction.TransactionId);

                if (doc != null)
                {
                    foreach (var (_, idx) in _secondaryIndexes)
                        IndexDelete(idx, doc, loc, transaction);
                }

                DeleteSlot(loc, transaction);
                await NotifyCdcAsync(OperationType.Delete, id, transaction);
            }
            await transaction.CommitAsync(ct);
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    /// <summary>
    /// Tries to extract the primary key <see cref="BsonId"/> from a document's "_id" field.
    /// Returns <see langword="false"/> if the field is absent or empty.
    /// </summary>
    private static bool TryExtractId(BsonDocument doc, out BsonId id)
    {
        return doc.TryGetId(out id) && !id.IsEmpty;
    }

    /// <summary>
    /// Estimates the total size of this collection's data pages in bytes.
    /// </summary>
    private long EstimateCollectionSizeBytes()
    {
        long count = 0;
        var hdrBuf = new byte[SlottedPageHeader.Size];
        foreach (var pageId in _storage.GetCollectionPageIds(_collectionName))
        {
            _storage.ReadPageHeader(pageId, null, hdrBuf);
            var h = SlottedPageHeader.ReadFrom(hdrBuf);
            if (h.PageType == PageType.Data)
                count++;
        }
        return count * _storage.PageSize;
    }

    #endregion

    /// <summary>
    /// Applies a BLQL projection to a document using the database-level key maps.
    /// </summary>
    internal BsonDocument ProjectDocument(BsonDocument source, Query.Blql.BlqlProjection projection)
        => projection.Apply(source, _storage.GetKeyMap(), _storage.GetKeyReverseMap());

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
        return BsonDocument.Create(_storage.GetFrozenKeyMap(), _storage.GetKeyReverseMap(), buildAction);
    }

    #region Insert

    private async Task<BsonId> InsertCore(BsonDocument document, ITransaction transaction)
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
        DocumentLocation location = default;

        if (_isTimeSeries)
        {
            var loc = _storage.InsertTimeSeries(_collectionName, document, transaction);
            location = new DocumentLocation(loc.PageId, (ushort)loc.SlotIndex);
        }
        else if (docData.Length + SlotEntry.Size <= _maxDocumentSizeForSinglePage)
        {
            var pageId = FindPageWithSpace(docData.Length + SlotEntry.Size, transaction.TransactionId);
            if (pageId == 0) pageId = AllocateNewDataPage(transaction);
            ushort slotIndex;
            try
            {
                slotIndex = InsertIntoPage(pageId, docData, transaction);
            }
            catch (InvalidOperationException ex) when (ex.Message.StartsWith("Not enough space in page", StringComparison.Ordinal))
            {
                // The FSI entry was stale (e.g., a recycled page from a previously dropped
                // collection still appeared to have free space).  InsertIntoPage already
                // corrected the FSI; fall back to a freshly allocated page.
                pageId = AllocateNewDataPage(transaction);
                slotIndex = InsertIntoPage(pageId, docData, transaction);
            }
            location = new DocumentLocation(pageId, slotIndex);
        }
        else
        {
            throw new InvalidOperationException($"Document size {docData.Length} exceeds maximum single page size. Overflow not yet supported in DynamicCollection.");
        }

        // Index the _id
        var key = new IndexKey(id.ToBytes());
        _primaryIndex.Insert(key, location, transaction.TransactionId);

        // UpdateAsync secondary indexes
        foreach (var (_, idx) in _secondaryIndexes)
            IndexInsert(idx, document, location, transaction);

        await NotifyCdcAsync(OperationType.Insert, id, transaction, document.RawData);
        return id;
    }

    private BsonDocument PrependId(BsonDocument document, BsonId id)
    {
        // Register _id key if not already present
        _storage.RegisterKeys(new[] { "_id" });

        // Estimate size: existing doc + id field overhead
        var estimatedSize = document.RawData.Length + 64;
        var buffer = new byte[estimatedSize];
        var writer = new BsonSpanWriter(buffer, _storage.GetFrozenKeyMap());

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

        return new BsonDocument(buffer[..writer.Position], _storage.GetKeyReverseMap(), _storage.GetKeyMap());
    }

    public ValueTask<BsonId> InsertAsync(BsonDocument document, CancellationToken ct = default)
    {
        return InsertAsync(document, null, ct);
    }
    /// <summary>
    /// Inserts a BsonDocument into the collection asynchronously.
    /// If the document has no _id field, one is auto-generated.
    /// Returns the BsonId of the inserted document.
    /// </summary>
    public async ValueTask<BsonId> InsertAsync(BsonDocument document, ITransaction? transaction, CancellationToken ct = default)
    {
        if (document == null) throw new ArgumentNullException(nameof(document));

        var sw = _storage.MetricsDispatcher != null ? Metrics.ValueStopwatch.StartNew() : default;
        bool success = false;
        bool autoCommit = transaction == null;

        if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring collection lock (Insert).");

        transaction ??= _storage.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var result = await InsertCore(document, transaction);
            if (autoCommit)
            {
                await transaction.CommitAsync(ct);
                // ── OnInsert retention trigger — runs AFTER commit within the lock ──
                if (_retentionPolicy != null && (_retentionPolicy.Triggers & RetentionTrigger.OnInsert) != 0)
                    await ApplyRetentionPolicyCoreAsync(ct);
            }
            else if (_retentionPolicy != null && (_retentionPolicy.Triggers & RetentionTrigger.OnInsert) != 0
                     && transaction is Transaction concreteTx)
            {
                // For caller-managed transactions, fire retention in the background after commit.
                concreteTx.OnCommit += () => _ = RunScheduledRetentionAsync();
            }
            success = true;
            return result;
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
        finally
        {
            _collectionLock.Release();
            if (sw.IsActive)
                _storage.MetricsDispatcher?.Publish(new Metrics.MetricEvent
                {
                    Timestamp      = sw.StartTimestamp,
                    Type           = Metrics.MetricEventType.CollectionInsert,
                    ElapsedMicros  = sw.GetElapsedMicros(),
                    CollectionName = _collectionName,
                    Success        = success,
                });
        }
    }

    public ValueTask<List<BsonId>> InsertBulkAsync(IEnumerable<BsonDocument> documents, CancellationToken ct = default)
    {
        return InsertBulkAsync(documents, null, ct);
    }

    /// <summary>
    /// Inserts multiple BsonDocuments asynchronously in a single transaction.
    /// Returns the list of generated/existing BsonIds in insertion order.
    /// </summary>
    public async ValueTask<List<BsonId>> InsertBulkAsync(IEnumerable<BsonDocument> documents, ITransaction? transaction, CancellationToken ct = default)
    {
        if (documents == null) throw new ArgumentNullException(nameof(documents));
        bool autoCommit = transaction == null;

        if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring collection lock (InsertBulk).");

        transaction ??= _storage.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var ids = new List<BsonId>();
            foreach (var doc in documents)
            {
                ct.ThrowIfCancellationRequested();
                ids.Add(await InsertCore(doc, transaction));
            }
            if (autoCommit)
            {
                await transaction.CommitAsync(ct);
                // ── OnInsert retention trigger — runs AFTER commit within the lock ──
                if (_retentionPolicy != null && (_retentionPolicy.Triggers & RetentionTrigger.OnInsert) != 0)
                    await ApplyRetentionPolicyCoreAsync(ct);
            }
            else if (_retentionPolicy != null && (_retentionPolicy.Triggers & RetentionTrigger.OnInsert) != 0
                     && transaction is Transaction concreteTx)
            {
                // For caller-managed transactions, fire retention in the background after commit.
                concreteTx.OnCommit += () => _ = RunScheduledRetentionAsync();
            }
            return ids;
        }
        catch
        {
            await transaction.RollbackAsync();
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
    /// Scans all documents applying a predicate at the BSON level (no deserialization to T).
    /// The predicate receives a BsonSpanReader positioned at the start of each document.
    /// </summary>
    public async IAsyncEnumerable<BsonDocument> ScanAsync(BsonReaderPredicate predicate)
    {
        var txnId = 0UL;

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
    public async IAsyncEnumerable<BsonDocument> QueryIndexAsync(string indexName, object? minValue, object? maxValue, bool ascending = true)
    {
        if (!_secondaryIndexes.TryGetValue(indexName, out var entry))
            throw new ArgumentException($"Index '{indexName}' not found on collection '{_collectionName}'");
        if (entry.Kind != DynamicIndexKind.BTree || entry.BTree == null)
            throw new InvalidOperationException($"Index '{indexName}' is not a BTree index. Use VectorSearch/Near/Within for vector/spatial indexes.");

        var txnId = 0UL;

        // (null, null) → unbounded full-index scan: include every entry (nulls and non-nulls alike).
        // (null, upper) → open lower bound: use NullSentinelNext to skip null entries.
        // (lower, null) → open upper bound: scan from lower to MaxKey.
        // DBNull → explicit null equality: use NullSentinel to target exactly the null bucket.
        IndexKey minKey;
        IndexKey maxKey;

        if (minValue == null && maxValue == null)
        {
            minKey = IndexKey.MinKey;
            maxKey = IndexKey.MaxKey;
        }
        else
        {
            if (minValue == null)
                minKey = IndexKey.NullSentinelNext;    // skip null entries in range scans
            else if (minValue is DBNull)
                minKey = IndexKey.NullSentinel;        // explicit null equality
            else
                minKey = CreateIndexKeyFromObject(minValue);

            if (maxValue == null)
                maxKey = IndexKey.MaxKey;
            else if (maxValue is DBNull)
                maxKey = IndexKey.NullSentinel;        // explicit null equality
            else
                maxKey = CreateIndexKeyFromObject(maxValue);
        }

        foreach (var indexEntry in entry.BTree.Range(minKey, maxKey, ascending ? IndexDirection.Forward : IndexDirection.Backward, txnId))
        {
            var doc = ReadDocumentAt(indexEntry.Location, txnId);
            if (doc != null) yield return doc;
        }
    }

    /// <summary>
    /// Returns the name of the first BTree secondary index whose field path matches
    /// <paramref name="fieldPath"/>, or <c>null</c> if none exists.
    /// Used by the BLQL query optimizer to route filters through indexed scans.
    /// </summary>
    internal string? FindBTreeIndexForField(string fieldPath)
    {
        foreach (var (name, entry) in _secondaryIndexes)
        {
            if (entry.Kind == DynamicIndexKind.BTree
                && string.Equals(entry.FieldPath, fieldPath, StringComparison.OrdinalIgnoreCase))
                return name;
        }
        return null;
    }

    /// <summary>
    /// Performs a vector similarity search using the named vector index.
    /// The field must have been indexed via <see cref="CreateVectorIndexAsync"/>.
    /// </summary>
    /// <param name="indexName">Name of the vector index.</param>
    /// <param name="query">Query vector (must match the index dimensionality).</param>
    /// <param name="k">Maximum number of nearest neighbours to return.</param>
    /// <param name="efSearch">HNSW efSearch parameter (higher = more recall, slower). Default 100.</param>
    public async IAsyncEnumerable<BsonDocument> VectorSearchAsync(string indexName, float[] query, int k, int efSearch = 100)
    {
        if (!_secondaryIndexes.TryGetValue(indexName, out var entry) || entry.Kind != DynamicIndexKind.Vector || entry.Vector == null)
            throw new ArgumentException($"Vector index '{indexName}' not found on collection '{_collectionName}'");

        foreach (var result in entry.Vector.Search(query, k, efSearch, null))
        {
            var doc = ReadDocumentAt(result.Location, 0UL);
            if (doc != null) yield return doc;
        }
    }

    /// <summary>
    /// Returns documents within a radius (km) of a geographic centre point.
    /// The field must have been indexed via <see cref="CreateSpatialIndexAsync"/>.
    /// </summary>
    public async IAsyncEnumerable<BsonDocument> NearAsync(string indexName, (double Latitude, double Longitude) center, double radiusKm)
    {
        if (!_secondaryIndexes.TryGetValue(indexName, out var entry) || entry.Kind != DynamicIndexKind.Spatial || entry.Spatial == null)
            throw new ArgumentException($"Spatial index '{indexName}' not found on collection '{_collectionName}'");

        var queryBox = SpatialMath.BoundingBox(center.Latitude, center.Longitude, radiusKm);
        foreach (var loc in entry.Spatial.Search(queryBox, null))
        {
            var doc = ReadDocumentAt(loc, 0UL);
            if (doc != null) yield return doc;
        }
    }

    /// <summary>
    /// Returns documents within a rectangular geographic area.
    /// The field must have been indexed via <see cref="CreateSpatialIndexAsync"/>.
    /// </summary>
    public async IAsyncEnumerable<BsonDocument> WithinAsync(string indexName, (double Latitude, double Longitude) min, (double Latitude, double Longitude) max)
    {
        if (!_secondaryIndexes.TryGetValue(indexName, out var entry) || entry.Kind != DynamicIndexKind.Spatial || entry.Spatial == null)
            throw new ArgumentException($"Spatial index '{indexName}' not found on collection '{_collectionName}'");

        var area = new GeoBox(min.Latitude, min.Longitude, max.Latitude, max.Longitude);
        foreach (var loc in entry.Spatial.Search(area, null))
        {
            var doc = ReadDocumentAt(loc, 0UL);
            if (doc != null) yield return doc;
        }
    }

    /// <summary>
    /// Asynchronously yields documents matching the specified predicate.
    /// </summary>
    public async IAsyncEnumerable<BsonDocument> FindAsync(Func<BsonDocument, bool> predicate, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        await foreach (var doc in FindAllAsync(ct))
        {
            ct.ThrowIfCancellationRequested();
            if (predicate(doc)) yield return doc;
        }
    }

    #endregion

    #region Update
    public ValueTask<bool> UpdateAsync(BsonId id, BsonDocument newDocument, CancellationToken ct = default)
    {
        return UpdateAsync(id, newDocument, null, ct);
    }
    /// <summary>
    /// Updates a document by its BsonId asynchronously. Replaces the entire document.
    /// </summary>
    public async ValueTask<bool> UpdateAsync(BsonId id, BsonDocument newDocument, ITransaction? transaction, CancellationToken ct = default)
    {
        if (newDocument == null) throw new ArgumentNullException(nameof(newDocument));

        var sw = _storage.MetricsDispatcher != null ? Metrics.ValueStopwatch.StartNew() : default;
        bool success = false;
        bool autoCommit = transaction == null;

        if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring collection lock (Update).");

        transaction ??= _storage.BeginTransaction(IsolationLevel.ReadCommitted);
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
            DocumentLocation newLocation = default;
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

            await NotifyCdcAsync(OperationType.Update, id, transaction, newDocument.RawData);
            if (autoCommit) await transaction.CommitAsync(ct);
            success = true;
            return true;
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
        finally
        {
            _collectionLock.Release();
            if (sw.IsActive)
                _storage.MetricsDispatcher?.Publish(new Metrics.MetricEvent
                {
                    Timestamp      = sw.StartTimestamp,
                    Type           = Metrics.MetricEventType.CollectionUpdate,
                    ElapsedMicros  = sw.GetElapsedMicros(),
                    CollectionName = _collectionName,
                    Success        = success,
                });
        }
    }

    public ValueTask<int> UpdateBulkAsync(IEnumerable<(BsonId Id, BsonDocument Document)> updates, CancellationToken ct = default)
    {
        return UpdateBulkAsync(updates, null, ct);
    }
    /// <summary>
    /// Updates multiple documents asynchronously in a single transaction.
    /// Returns the number of documents successfully updated.
    /// </summary>
    public async ValueTask<int> UpdateBulkAsync(IEnumerable<(BsonId Id, BsonDocument Document)> updates, ITransaction? transaction, CancellationToken ct = default)
    {
        if (updates == null) throw new ArgumentNullException(nameof(updates));
        bool autoCommit = transaction == null;

        if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring collection lock (UpdateBulk).");

        transaction ??= _storage.BeginTransaction(IsolationLevel.ReadCommitted);
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

                await NotifyCdcAsync(OperationType.Update, id, transaction, newDoc.RawData);
                count++;
            }
            if (autoCommit) await transaction.CommitAsync(ct);
            return count;
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    #endregion

    #region Delete

    public ValueTask<bool> DeleteAsync(BsonId id, CancellationToken ct = default)
    {
        return DeleteAsync(id, null, ct);
    }

    /// <summary>
    /// Deletes a document by its BsonId asynchronously.
    /// </summary>
    public async ValueTask<bool> DeleteAsync(BsonId id, ITransaction? transaction, CancellationToken ct = default)
    {
        var sw = _storage.MetricsDispatcher != null ? Metrics.ValueStopwatch.StartNew() : default;
        bool success = false;
        bool autoCommit = transaction == null;

        if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring collection lock (Delete).");

        transaction ??= _storage.BeginTransaction(IsolationLevel.ReadCommitted);
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
            await NotifyCdcAsync(OperationType.Delete, id, transaction);
            if (autoCommit) await transaction.CommitAsync(ct);
            success = true;
            return true;
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
        finally
        {
            _collectionLock.Release();
            if (sw.IsActive)
                _storage.MetricsDispatcher?.Publish(new Metrics.MetricEvent
                {
                    Timestamp      = sw.StartTimestamp,
                    Type           = Metrics.MetricEventType.CollectionDelete,
                    ElapsedMicros  = sw.GetElapsedMicros(),
                    CollectionName = _collectionName,
                    Success        = success,
                });
        }
    }

    public async ValueTask<int> DeleteBulkAsync(IEnumerable<BsonId> ids, CancellationToken ct = default)
    {
        return await DeleteBulkAsync(ids, null, ct);
    }
    /// <summary>
    /// Deletes multiple documents asynchronously in a single transaction.
    /// Returns the number of documents successfully deleted.
    /// </summary>
    public async ValueTask<int> DeleteBulkAsync(IEnumerable<BsonId> ids, ITransaction? transaction, CancellationToken ct = default)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));
        bool autoCommit = transaction == null;

        if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring collection lock (DeleteBulk).");

        transaction ??= _storage.BeginTransaction(IsolationLevel.ReadCommitted);
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
                await NotifyCdcAsync(OperationType.Delete, id, transaction);
                count++;
            }
            if (autoCommit) await transaction.CommitAsync(ct);
            return count;
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
        finally
        {
            _collectionLock.Release();
        }
    }

    /// <summary>
    /// Deletes all documents in the collection.
    /// The collection structure (indexes, metadata) is preserved.
    /// </summary>
    /// <returns>Number of documents deleted.</returns>
    public async Task<int> TruncateAsync(CancellationToken ct = default)
    {
        if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs, ct).ConfigureAwait(false))
            throw new TimeoutException("Timed out acquiring collection lock (TruncateAsync).");

        var transaction = _storage.BeginTransaction(IsolationLevel.ReadCommitted);
        int deleted = 0;
        try
        {
            // Collect all keys first to avoid modifying the index while iterating.
            var keys = new List<IndexKey>();
            foreach (var entry in _primaryIndex.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, 0UL))
            {
                ct.ThrowIfCancellationRequested();
                keys.Add(entry.Key);
            }

            foreach (var key in keys)
            {
                ct.ThrowIfCancellationRequested();
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
                deleted++;
            }

            await transaction.CommitAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            await transaction.RollbackAsync().ConfigureAwait(false);
            throw;
        }
        finally
        {
            _collectionLock.Release();
        }

        return deleted;
    }

    #endregion

    #region Vacuum / Secure Erase

    /// <summary>
    /// Compacts all data pages in this collection, securely erasing freed byte ranges
    /// by zero-filling the free space area of every page after compaction.
    /// <para>
    /// Acquires the collection write lock for the duration of the operation —
    /// no concurrent writes to this collection are allowed while VACUUM runs.
    /// </para>
    /// </summary>
    public async Task VacuumAsync(VacuumOptions? options = null, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!await _collectionLock.WaitAsync(WriteLockTimeoutMs, ct).ConfigureAwait(false))
            throw new TimeoutException("Timed out acquiring collection write lock (VacuumAsync).");

        var sw = _storage.MetricsDispatcher != null ? Metrics.ValueStopwatch.StartNew() : default;
        long bytesFreed = 0;
        bool success = false;
        try
        {
            var buffer = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
            var scratch = ArrayPool<byte>.Shared.Rent(_storage.PageSize);
            try
            {
                var transaction = _storage.BeginTransaction(IsolationLevel.ReadCommitted);
                try
                {
                    bool secureErase = options?.SecureErase ?? true;
                    foreach (var pageId in _storage.GetCollectionPageIds(_collectionName))
                    {
                        ct.ThrowIfCancellationRequested();

                        _storage.ReadPage(pageId, transaction.TransactionId, buffer);
                        var header = SlottedPageHeader.ReadFrom(buffer.AsSpan(0, SlottedPageHeader.Size));

                        if (header.PageType != PageType.Data)
                            continue;

                        // Compact the page. Pass the reusable scratch buffer to avoid
                        // a per-page ArrayPool rent inside CompactAndErase.
                        // Zero-filling (secure erase) is conditional on the option.
                        SlottedPageUtils.CompactAndErase(
                            buffer.AsSpan(0, _storage.PageSize), scratch, secureErase);

                        var compactedHdr = SlottedPageHeader.ReadFrom(
                            buffer.AsSpan(0, SlottedPageHeader.Size));
                        int freeBytes = compactedHdr.FreeSpaceEnd - compactedHdr.FreeSpaceStart;
                        if (freeBytes > 0)
                        {
                            bytesFreed += freeBytes;
                            _storage.WritePage(pageId, transaction.TransactionId,
                                buffer.AsSpan(0, _storage.PageSize));
                            SnapshotFsiForTransaction(transaction, pageId);
                            _fsi.Update(pageId, compactedHdr.AvailableFreeSpace);
                        }
                    }

                    await transaction.CommitAsync(ct).ConfigureAwait(false);
                }
                catch
                {
                    await transaction.RollbackAsync().ConfigureAwait(false);
                    throw;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(scratch);
                ArrayPool<byte>.Shared.Return(buffer);
            }

            // Optionally rebuild all secondary indexes from scratch.
            if (options?.RebuildIndexes == true && _secondaryIndexes.Count > 0)
            {
                // Replace every secondary index with a fresh, empty instance.
                var rebuildEntries = new Dictionary<string, DynamicSecondaryIndex>(_secondaryIndexes.Count, StringComparer.OrdinalIgnoreCase);
                foreach (var (name, entry) in _secondaryIndexes)
                {
                    DynamicSecondaryIndex freshEntry = entry.Kind switch
                    {
                        DynamicIndexKind.BTree    => new DynamicSecondaryIndex(new BTreeIndex(_storage, entry.Options), entry.FieldPath, entry.Options),
                        DynamicIndexKind.Vector   => new DynamicSecondaryIndex(new VectorSearchIndex(_storage, entry.Options), entry.FieldPath, entry.Options),
                        DynamicIndexKind.Spatial  => new DynamicSecondaryIndex(new RTreeIndex(_storage, entry.Options, 0), entry.FieldPath, entry.Options),
                        _                         => entry
                    };
                    rebuildEntries[name] = freshEntry;
                }

                var rebuildTxn = _storage.BeginTransaction(IsolationLevel.ReadCommitted);
                try
                {
                    foreach (var e in _primaryIndex.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, rebuildTxn.TransactionId))
                    {
                        ct.ThrowIfCancellationRequested();
                        var doc = ReadDocumentAt(e.Location, rebuildTxn.TransactionId);
                        if (doc != null)
                        {
                            foreach (var (_, idx) in rebuildEntries)
                                IndexInsert(idx, doc, e.Location, rebuildTxn);
                        }
                    }
                    await rebuildTxn.CommitAsync(ct).ConfigureAwait(false);
                }
                catch
                {
                    await rebuildTxn.RollbackAsync().ConfigureAwait(false);
                    throw;
                }

                // Swap in the rebuilt indexes and persist their new root page IDs.
                foreach (var (name, entry) in rebuildEntries)
                    _secondaryIndexes[name] = entry;
                PersistIndexMetadata();
            }

            success = true;
        }
        finally
        {
            _collectionLock.Release();
            if (sw.IsActive)
            {
                _storage.MetricsDispatcher?.Publish(new Metrics.MetricEvent
                {
                    Timestamp     = sw.StartTimestamp,
                    Type          = Metrics.MetricEventType.Vacuum,
                    ElapsedMicros = sw.GetElapsedMicros(),
                    BytesFreed    = bytesFreed,
                    Success       = success,
                });
            }
        }
    }

    #endregion

    #region Index Management

    /// <summary>
    /// Creates a secondary B-Tree index on a field path.
    /// Supports nested properties using dot-notation (e.g., "address.city.name").
    /// </summary>
    public async Task CreateIndexAsync(string fieldPath, string? name = null, bool unique = false)
    {
        name ??= $"idx_{fieldPath.ToLowerInvariant()}";
        fieldPath = fieldPath.ToLowerInvariant();

        if (_secondaryIndexes.ContainsKey(name))
            throw new InvalidOperationException($"Index '{name}' already exists");

        // Register all key components for nested paths (e.g., "address.city.name" → ["address", "city", "name"])
        RegisterNestedPathKeys(fieldPath);

        var opts = unique ? IndexOptions.CreateUnique(fieldPath) : IndexOptions.CreateBTree(fieldPath);
        var btree = new BTreeIndex(_storage, opts);
        var entry = new DynamicSecondaryIndex(btree, fieldPath, opts);
        _secondaryIndexes[name] = entry;

        var transaction = _storage.BeginTransaction(IsolationLevel.ReadCommitted);
        foreach (var e in _primaryIndex.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, transaction.TransactionId))
        {
            var doc = ReadDocumentAt(e.Location, transaction.TransactionId);
            if (doc != null) IndexInsert(entry, doc, e.Location, transaction);
        }
        await transaction.CommitAsync();

        PersistIndexMetadata();
    }

    /// <summary>
    /// Creates a vector (HNSW) index for similarity search on a float-array field.
    /// The field must be stored as a BSON Array of numeric values.
    /// Supports nested properties using dot-notation.
    /// </summary>
    public async Task CreateVectorIndexAsync(string fieldPath, int dimensions, VectorMetric metric = VectorMetric.Cosine, string? name = null)
    {
        name ??= $"idx_vector_{fieldPath.ToLowerInvariant()}";
        fieldPath = fieldPath.ToLowerInvariant();

        if (_secondaryIndexes.ContainsKey(name))
            throw new InvalidOperationException($"Index '{name}' already exists");

        // Register all key components for nested paths
        RegisterNestedPathKeys(fieldPath);

        var opts = IndexOptions.CreateVector(dimensions, metric, 16, 200, fieldPath);
        var vector = new VectorSearchIndex(_storage, opts);
        var entry = new DynamicSecondaryIndex(vector, fieldPath, opts);
        _secondaryIndexes[name] = entry;

        var transaction = _storage.BeginTransaction(IsolationLevel.ReadCommitted);
        foreach (var e in _primaryIndex.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, transaction.TransactionId))
        {
            var doc = ReadDocumentAt(e.Location, transaction.TransactionId);
            if (doc != null) IndexInsert(entry, doc, e.Location, transaction);
        }
        await transaction.CommitAsync();

        PersistIndexMetadata();
    }

    /// <summary>
    /// Creates a geospatial (R-Tree) index for <c>Near</c> and <c>Within</c> queries.
    /// The field must be stored as a BSON coordinates array <c>[lat, lon]</c>.
    /// Supports nested properties using dot-notation.
    /// </summary>
    public async Task CreateSpatialIndexAsync(string fieldPath, string? name = null)
    {
        name ??= $"idx_spatial_{fieldPath.ToLowerInvariant()}";
        fieldPath = fieldPath.ToLowerInvariant();

        if (_secondaryIndexes.ContainsKey(name))
            throw new InvalidOperationException($"Index '{name}' already exists");

        // Register all key components for nested paths
        RegisterNestedPathKeys(fieldPath);

        var opts = IndexOptions.CreateSpatial(fieldPath);
        var spatial = new RTreeIndex(_storage, opts, 0);
        var entry = new DynamicSecondaryIndex(spatial, fieldPath, opts);
        _secondaryIndexes[name] = entry;

        var transaction = _storage.BeginTransaction(IsolationLevel.ReadCommitted);
        foreach (var e in _primaryIndex.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, transaction.TransactionId))
        {
            var doc = ReadDocumentAt(e.Location, transaction.TransactionId);
            if (doc != null) IndexInsert(entry, doc, e.Location, transaction);
        }
        await transaction.CommitAsync();

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

    /// <summary>Gets all persisted schema versions for this collection.</summary>
    public IReadOnlyList<BsonSchema> GetSchemas()
    {
        var metadata = _storage.GetCollectionMetadata(_collectionName);
        if (metadata == null || metadata.SchemaRootPageId == 0)
            return Array.Empty<BsonSchema>();

        return _storage.GetSchemas(metadata.SchemaRootPageId);
    }

    /// <summary>Sets a new schema version for this collection and persists it.</summary>
    public void SetSchema(BsonSchema schema)
    {
        if (schema == null) throw new ArgumentNullException(nameof(schema));
        
        var metadata = _storage.GetCollectionMetadata(_collectionName) ?? new CollectionMetadata { Name = _collectionName };
        metadata.SchemaRootPageId = _storage.AppendSchema(metadata.SchemaRootPageId, schema);
        _storage.SaveCollectionMetadata(metadata);
        
        // Ensure all keys used in the schema are registered in the global key map
        _storage.RegisterKeys(schema.GetAllKeys().ToArray());
    }

    /// <summary>Gets the VectorSource configuration for this collection, or null if not configured.</summary>
    public VectorSourceConfig? GetVectorSource()
    {
        var metadata = _storage.GetCollectionMetadata(_collectionName);
        return metadata?.VectorSource;
    }

    /// <summary>Sets the VectorSource configuration for this collection and persists it.</summary>
    public void SetVectorSource(VectorSourceConfig? config)
    {
        var metadata = _storage.GetCollectionMetadata(_collectionName) ?? new CollectionMetadata { Name = _collectionName };
        metadata.VectorSource = config;
        _storage.SaveCollectionMetadata(metadata);
    }

    /// <summary>Returns typed descriptors for all secondary indexes on this collection.</summary>
    public IReadOnlyList<DynamicIndexDescriptor> GetIndexDescriptors()
    {
        return _secondaryIndexes.Select(kvp =>
        {
            var type = kvp.Value.Kind switch
            {
                DynamicIndexKind.Vector  => IndexType.Vector,
                DynamicIndexKind.Spatial => IndexType.Spatial,
                _                        => IndexType.BTree
            };
            return new DynamicIndexDescriptor(
                kvp.Key, type,
                kvp.Value.FieldPath,
                kvp.Value.Options.Dimensions,
                kvp.Value.Options.Metric,
                kvp.Value.Options.Unique);
        }).ToList();
    }

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
        // Support nested paths with dot-notation (e.g., "address.city.name")
        if (!TryGetNestedValue(document, idx.FieldPath, out var val)) return;

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
        // Support nested paths with dot-notation
        if (!TryGetNestedValue(document, idx.FieldPath, out var val)) return;

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
            var pageType = (PageType)buffer[4];

            // Freed or otherwise invalid page – entry is stale (e.g. after TS pruning).
            if (pageType == PageType.Free || pageType == PageType.Empty)
                return null;

            if (pageType == PageType.TimeSeries)
            {
                int offset = location.SlotIndex;
                if (offset < TimeSeriesPage.DataOffset || offset + 4 > buffer.Length)
                    return null;

                int size = BitConverter.ToInt32(buffer.AsSpan(offset, 4));
                if (size <= 0 || offset + size > buffer.Length)
                    return null;

                var data = buffer.AsSpan(offset, size).ToArray();
                return new BsonDocument(data, _storage.GetKeyReverseMap(), _storage.GetKeyMap());
            }

            var header = SlottedPageHeader.ReadFrom(buffer);

            if (location.SlotIndex >= header.SlotCount)
                return null;

            var slotOffset = SlottedPageHeader.Size + (location.SlotIndex * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));

            if (slot.Flags.HasFlag(SlotFlags.Deleted))
                return null;

            // Copy document data (buffer is pooled, data must outlive it)
            var docData = buffer.AsSpan(slot.Offset, slot.Length).ToArray();
            return new BsonDocument(docData, _storage.GetKeyReverseMap(), _storage.GetKeyMap());
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
            await _storage.ReadPageAsync(location.PageId, txnId, buffer.AsMemory(0, _storage.PageSize), ct);

            var pageType = (PageType)buffer[4];

            if (pageType == PageType.Free || pageType == PageType.Empty)
                return null;

            if (pageType == PageType.TimeSeries)
            {
                int offset = location.SlotIndex;
                if (offset < TimeSeriesPage.DataOffset || offset + 4 > buffer.Length)
                    return null;

                int size = BitConverter.ToInt32(buffer.AsSpan(offset, 4));
                if (size <= 0 || offset + size > buffer.Length)
                    return null;

                // ToArray() copies before buffer is returned to the pool
                var tsData = buffer.AsSpan(offset, size).ToArray();
                return new BsonDocument(tsData, _storage.GetKeyReverseMap(), _storage.GetKeyMap());
            }

            var header = SlottedPageHeader.ReadFrom(buffer);
            if (location.SlotIndex >= header.SlotCount) return null;

            var slotOffset = SlottedPageHeader.Size + (location.SlotIndex * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));
            if (slot.Flags.HasFlag(SlotFlags.Deleted)) return null;

            // Span created after the only await — safe; ToArray() copies before buffer is returned
            var docData = buffer.AsSpan(slot.Offset, slot.Length).ToArray();
            return new BsonDocument(docData, _storage.GetKeyReverseMap(), _storage.GetKeyMap());
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>Async exact-match lookup.</summary>
    public async ValueTask<BsonDocument?> FindByIdAsync(BsonId id, CancellationToken ct = default)
    {
        var key = new IndexKey(id.ToBytes());
        var (found, location) = await _primaryIndex.TryFindAsync(key, (ulong?)null, ct);
        if (!found) return null;
        return await ReadDocumentAtAsync(location, 0UL, ct);
    }

    /// <summary>Async full-collection scan.</summary>
    public async IAsyncEnumerable<BsonDocument> FindAllAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var txnId = 0UL;

        await foreach (var entry in _primaryIndex
            .RangeAsync(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txnId, ct)
            )
        {
            ct.ThrowIfCancellationRequested();
            var doc = await ReadDocumentAtAsync(entry.Location, txnId, ct);
            if (doc != null) yield return doc;
        }
    }

    /// <summary>Returns the number of documents in this collection.</summary>
    public async Task<int> CountAsync(CancellationToken ct = default)
    {
        var count = 0;
        await foreach (var _ in FindAllAsync(ct))
            count++;
        return count;
    }

    private uint FindPageWithSpace(int requiredBytes, ulong txnId)
    {
        if (_currentDataPage != 0)
        {
            if (_fsi.TryGetFreeBytes(_currentDataPage, out var freeBytes) && freeBytes >= requiredBytes && !_storage.IsPageLocked(_currentDataPage, txnId))
                return _currentDataPage;
        }

        // Use the cached _isPageLocked delegate to avoid per-call closure allocation.
        return _fsi.FindPage(requiredBytes, txnId, _isPageLocked);
    }

    private uint AllocateNewDataPage(ITransaction transaction)
    {
        var pageId = _storage.AllocateCollectionPage(_collectionName);
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
            SnapshotFsiForTransaction(transaction, pageId);
            _fsi.Update(pageId, header.AvailableFreeSpace);
            _currentDataPage = pageId;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
        return pageId;
    }

    private ushort InsertIntoPage(uint pageId, ReadOnlyMemory<byte> data, ITransaction transaction)
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
            {
                // Correct the FSI to match the physical page state so that the next call to
                // FindPageWithSpace does not route back to this page (breaks the poisoned-cache loop
                // that can arise when a transaction that updated the FSI was later rolled back).
                _fsi.Update(pageId, header.AvailableFreeSpace);
                throw new InvalidOperationException($"Not enough space in page {pageId}: need {requiredSpace}, have {header.AvailableFreeSpace}");
            }

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
            data.Span.CopyTo(buffer.AsSpan(docOffset, data.Length));

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

            _storage.WritePage(pageId, transaction.TransactionId, buffer.AsSpan(0, _storage.PageSize));
            SnapshotFsiForTransaction(transaction, pageId);
            _fsi.Update(pageId, header.AvailableFreeSpace);

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

                // Compact the page and securely erase freed bytes (zero-fill free space).
                SlottedPageUtils.CompactAndErase(buffer.AsSpan(0, _storage.PageSize));

                SnapshotFsiForTransaction(transaction, location.PageId);
                var compactedHdr = SlottedPageHeader.ReadFrom(buffer.AsSpan(0, SlottedPageHeader.Size));
                _fsi.Update(location.PageId, compactedHdr.AvailableFreeSpace);

                _storage.WritePage(location.PageId, transaction.TransactionId, buffer.AsSpan(0, _storage.PageSize));
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private void SnapshotFsiForTransaction(ITransaction transaction, uint pageId)
    {
        if (_fsi.SnapshotForTransaction(transaction.TransactionId, pageId))
        {
            var txnId = transaction.TransactionId;
            transaction.OnRollback += () => _fsi.RollbackTransaction(txnId);

            if (transaction is Transaction concreteTxn)
                concreteTxn.OnCommit += () => _fsi.CommitTransaction(txnId);
        }
    }

    private static IndexKey? BsonValueToIndexKey(BsonValue value)
    {
        return value.Type switch
        {
            BsonType.Null     => IndexKey.NullSentinel, // Null values are indexed with a dedicated sentinel key
            BsonType.Int32    => new IndexKey(value.AsInt32),
            BsonType.Int64    => new IndexKey(value.AsInt64),
            BsonType.String   => new IndexKey(value.AsString),
            BsonType.ObjectId => new IndexKey(value.AsObjectId),
            BsonType.Double   => new IndexKey(BitConverter.GetBytes(value.AsDouble)),
            // DateTime is stored as BitConverter.Int64BitsToDouble(unixMs); key on the raw long
            // to get the same ordering used by ToIndexObject in BlqlFilter.
            BsonType.DateTime => new IndexKey(value.AsDateTimeOffset.ToUnixTimeMilliseconds()),
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

    /// <summary>
    /// Tries to get a value from a BsonDocument using a dot-notation path (e.g., "address.city.name").
    /// Returns false if any intermediate property is null or not a document.
    /// </summary>
    private static bool TryGetNestedValue(BsonDocument document, string path, out BsonValue value)
    {
        value = BsonValue.Null;
        
        // Simple case: no dots, direct field access
        if (!path.Contains('.'))
        {
            return document.TryGetValue(path, out value);
        }

        // Nested case: traverse the path
        var parts = path.Split('.');
        BsonDocument current = document;
        
        for (int i = 0; i < parts.Length - 1; i++)
        {
            if (!current.TryGetValue(parts[i], out var intermediate))
                return false; // Intermediate field not found
            
            if (intermediate.Type != BsonType.Document)
                return false; // Intermediate value is not a document (can't traverse further)
            
            current = intermediate.AsDocument;
        }
        
        // Get the final value
        return current.TryGetValue(parts[^1], out value);
    }

    /// <summary>
    /// Registers all key components of a nested path for BSON dictionary caching.
    /// Example: "address.city.name" → ["address", "city", "name"]
    /// </summary>
    private void RegisterNestedPathKeys(string path)
    {
        if (!path.Contains('.'))
        {
            _storage.RegisterKeys(new[] { path });
            return;
        }

        var parts = path.Split('.');
        _storage.RegisterKeys(parts);
    }

    private static IndexKey CreateIndexKeyFromObject(object value) => value switch
    {
        DBNull  => IndexKey.NullSentinel, // explicit null equality query
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

    // ── Change Data Capture ───────────────────────────────────────────────────

    /// <summary>
    /// Subscribes to a live stream of changes on this collection.
    /// Calling this method initializes CDC for the underlying storage engine.
    /// </summary>
    /// <param name="capturePayload">
    /// When <c>true</c>, each event includes the full BSON payload of the changed document.
    /// When <c>false</c>, only metadata (ID, operation type, timestamps) is included.
    /// </param>
    public IObservable<BsonChangeEvent> Watch(bool capturePayload = false) =>
        new DynamicChangeStreamObservable(
            _storage.EnsureCdc(), _collectionName, capturePayload, _storage.GetKeyReverseMap(), _storage.GetKeyMap());

    private Task NotifyCdcAsync(OperationType type, BsonId id, ITransaction transaction, ReadOnlyMemory<byte> docData = default)
    {
        if (_storage.Cdc == null) return Task.CompletedTask;
        if (!_storage.Cdc.HasAnyWatchers(_collectionName)) return Task.CompletedTask;

        ReadOnlyMemory<byte>? payload = null;
        if (!docData.IsEmpty && _storage.Cdc.HasPayloadWatchers(_collectionName))
            payload = docData.ToArray();

        if (transaction is Transaction t)
        {
            t.AddChange(new InternalChangeEvent
            {
                Timestamp = DateTime.UtcNow.Ticks,
                TransactionId = transaction.TransactionId,
                CollectionName = _collectionName,
                Type = type,
                IdType = _idType,
                IdBytes = id.ToBytes(),
                PayloadBytes = payload
            });
        }

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _retentionTimer?.Dispose();
        _retentionTimer = null;
        _collectionLock.Dispose();
    }
}
