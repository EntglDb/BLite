using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using BLite.Core.Indexing;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    // -----------------------------------------------------------------------
    // Multi-file pageId encoding
    //
    //  Main file page  — bit 31 = 0, bits [30:0] = physical page number
    //  Index file page — bits [31:30] = 10, bits [29:0] = local page number
    //  Collection page — bits [31:30] = 11, bits [29:24] = slot (0-63), bits [23:0] = local page number
    //
    // In single-file (embedded) mode the encoding is never applied; all 32 bits
    // are the physical page number, preserving full backward compatibility.
    // -----------------------------------------------------------------------
    private const uint IndexPageMarker      = 0x8000_0000u; // bit 31=1, bit 30=0
    private const uint CollectionPageMarker = 0xC000_0000u; // bits 31-30 = 11
    private const uint SubFileTypeMask      = 0xC000_0000u; // selects bits 31-30
    private const uint IndexLocalMask       = 0x3FFF_FFFFu; // bits 29:0
    private const uint CollLocalMask        = 0x00FF_FFFFu; // bits 23:0
    private const uint CollSlotMask         = 0x3F00_0000u; // bits 29:24
    private const int  CollSlotShift        = 24;

    /// <summary>Maximum number of named per-collection files in multi-file mode.</summary>
    public const int MaxCollections = 64;

    // -----------------------------------------------------------------------
    // Page allocation
    // -----------------------------------------------------------------------

    /// <summary>Allocates a new page in the main data file.</summary>
    public uint AllocatePage() => _pageFile.AllocatePage();

    /// <summary>
    /// Allocates a new index page. When <see cref="PageFileConfig.IndexFilePath"/> is configured the
    /// page is created in the index file and its ID is encoded with the index-file marker bits
    /// (<c>0x80000000 | localId</c>). Falls back to the main data file otherwise.
    /// </summary>
    public uint AllocateIndexPage()
    {
        if (_indexFile == null)
            return _pageFile.AllocatePage();

        var localId = _indexFile.AllocatePage();
        return IndexPageMarker | (localId & IndexLocalMask);
    }

    /// <summary>
    /// Allocates a new data page for the specified collection.
    /// When <see cref="PageFileConfig.CollectionDataDirectory"/> is configured the page is created in
    /// the collection's dedicated file and its ID is encoded with the collection-file marker bits.
    /// Falls back to the main data file otherwise.
    /// </summary>
    public uint AllocateCollectionPage(string collectionName)
    {
        if (_collectionFiles == null)
            return _pageFile.AllocatePage();

        ValidateCollectionName(collectionName);
        var slot = GetOrAssignCollectionSlot(collectionName);
        var file = GetOrCreateCollectionFile(collectionName);
        var localId = file.AllocatePage();
        return CollectionPageMarker | ((uint)(slot & 0x3F) << CollSlotShift) | (localId & CollLocalMask);
    }

    // Pages checkpointed out of _walIndex whose records are still in the WAL file, i.e. since
    // the last truncation. See FreePage.
    private readonly ConcurrentDictionary<uint, byte> _checkpointedWalPages = new();

    // Freed pages that had a WAL image, held off the free list until the next truncation.
    private readonly object _pendingFreeLock = new();
    private HashSet<uint> _pendingFreePages = new();

    /// <summary>
    /// Frees a page, routing to the correct sub-file via the encoded page ID.
    /// <para>
    /// A page that still has a committed image in the WAL is not reusable right away:
    /// <see cref="ReadPage"/> prefers that image over the page file and recovery replays it over
    /// the page file, so a page handed out again would read back, or be overwritten with, its
    /// old contents. Such a page is queued instead and returned to its file's free list right
    /// after the next WAL truncation (checkpoint, backup or recovery); until then
    /// <see cref="GetCollectionPageIds"/> skips it. A queued page is leaked, never reused
    /// unsafely, if the process dies first. In multi-process mode every page is queued, since
    /// another process may have written it.
    /// </para>
    /// <para>
    /// Nothing truncates the WAL while it stays under <c>MaxWalSize</c>, and a workload that frees
    /// pages faster than it fills the WAL (a loop of index creates and drops: #209) would find the
    /// free list empty at every allocation and grow the file by one tree per cycle. So every
    /// <c>MaxPendingFreeBytes</c> worth of queued pages requests a checkpoint, which truncates the
    /// WAL and releases them; the request is asynchronous and never blocks the caller.
    /// </para>
    /// </summary>
    public void FreePage(uint pageId)
    {
        if (pageId == 0) throw new InvalidOperationException("Cannot free header page 0");

        if (_shm == null && !HasWalImage(pageId))
        {
            GetPageFile(pageId, out var physId).FreePage(physId);
            return;
        }

        int queued;
        lock (_pendingFreeLock)
        {
            if (!_pendingFreePages.Add(pageId)) return;
            queued = _pendingFreePages.Count;
        }

        // At the threshold, then every eighth of it past the threshold, not at every free: the
        // truncation is skipped when a commit holds the lock at that instant, and pages an open
        // transaction uses stay queued, so a retry is needed, but not a checkpoint per free.
        var threshold = Math.Max(8, (int)(MaxPendingFreeBytes / PageSize));
        if (queued >= threshold && (queued - threshold) % (threshold / 8) == 0)
            RequestPendingFreeCheckpoint();
    }

    private int _pendingFreeCheckpointQueued;
    private Task? _pendingFreeCheckpointTask;

    /// <summary>
    /// Starts a checkpoint on the thread pool to release the pages queued by <see cref="FreePage"/>,
    /// unless one requested here is still running. Like the checkpoint a commit requests when the
    /// WAL exceeds <c>MaxWalSize</c>, it is fire-and-forget; a failure leaves the pages queued for
    /// the next request or truncation.
    /// </summary>
    private void RequestPendingFreeCheckpoint()
    {
        if (_disposed || Interlocked.CompareExchange(ref _pendingFreeCheckpointQueued, 1, 0) != 0)
            return;

        _pendingFreeCheckpointTask = Task.Run(async () =>
        {
            try { await CheckpointAsync().ConfigureAwait(false); }
            catch { /* best-effort: the pages stay queued */ }
            finally { Interlocked.Exchange(ref _pendingFreeCheckpointQueued, 0); }
        });
    }

    /// <summary>
    /// Completes when the checkpoint last requested by <see cref="RequestPendingFreeCheckpoint"/>
    /// has finished (at once if none is running). For tests, which would otherwise race it.
    /// </summary>
    internal Task PendingFreeCheckpointCompletion => Volatile.Read(ref _pendingFreeCheckpointTask) ?? Task.CompletedTask;

    // Pages each open transaction stops using, freed only once it commits (#233). See FreePageOnCommit.
    private readonly ConcurrentDictionary<ulong, HashSet<uint>> _freeOnCommit = new();

    /// <summary>
    /// Frees a page that <paramref name="transactionId"/> no longer uses, once that transaction
    /// commits (#233).
    /// <para>
    /// <see cref="FreePage"/> acts outside any transaction: the page goes back to the free list
    /// at once, while the committed state, which a rollback returns to, may still use it. A
    /// B-Tree merge that unlinks a node is one such case: freed on the spot, a rolled-back merge
    /// would leave the committed tree pointing at a free page, which the next allocation hands
    /// out again. A page queued here stays allocated until the transaction's commit is durable
    /// in the WAL, and is then freed through <see cref="FreePage"/>, which still holds it off the
    /// free list while the WAL has an image of it. A rollback drops the queue, so the page stays
    /// in use. The transaction itself cannot be handed the page again either, since it is not
    /// free until the transaction ends.
    /// </para>
    /// <para>
    /// If the process dies between the commit and the free, or the commit fails, the page is
    /// leaked: allocated but no longer referenced. It is never freed twice nor freed while in use.
    /// </para>
    /// </summary>
    public void FreePageOnCommit(uint pageId, ulong transactionId)
    {
        if (pageId == 0) throw new InvalidOperationException("Cannot free header page 0");
        if (transactionId == 0)
            throw new InvalidOperationException("Cannot defer a free without a transaction (transactionId=0 is reserved)");

        var pages = _freeOnCommit.GetOrAdd(transactionId, _ => new HashSet<uint>());
        lock (pages)
            pages.Add(pageId);
    }

    /// <summary>
    /// Frees the pages queued by <see cref="FreePageOnCommit"/> for a transaction whose commit is
    /// now durable and whose pages have been moved to <c>_walIndex</c>. Call under
    /// <c>_commitLock</c>. A page that cannot be freed is leaked rather than failing a commit
    /// that already happened.
    /// </summary>
    private void FreePagesOfCommittedTransaction(ulong transactionId)
    {
        if (!_freeOnCommit.TryRemove(transactionId, out var pages))
            return;

        lock (pages)
        {
            foreach (var pageId in pages)
            {
                try { FreePage(pageId); }
                catch { /* best-effort: the page is leaked, never reused while in use */ }
            }
        }
    }

    /// <summary>Drops the frees queued by a transaction that rolled back: its pages stay in use.</summary>
    private void DiscardFreesOnCommit(ulong transactionId) => _freeOnCommit.TryRemove(transactionId, out _);

    /// <summary>
    /// Whether the WAL holds, or an open transaction is about to commit, an image of the page.
    /// </summary>
    private bool HasWalImage(uint pageId)
    {
        return _walIndex.ContainsKey(pageId)
            || _checkpointedWalPages.ContainsKey(pageId)
            || IsInOpenTransaction(pageId);
    }

    /// <summary>
    /// Picks the pages queued by <see cref="FreePage"/> that the truncation just made releasable.
    /// Call only right after the WAL was truncated, under <c>_commitLock</c>, so no commit can
    /// append to it. A page that an open transaction has written stays queued: its image reaches
    /// the WAL only when that transaction commits, so it is released after the truncation that
    /// follows. The returned pages are freed by <see cref="ReleaseTruncatedPages"/> once the
    /// locks are released: freeing thousands of them, each a header read and two page writes,
    /// kept <c>_commitLock</c> for longer than a commit may wait (#209).
    /// </summary>
    private List<uint>? OnWalTruncated()
    {
        _checkpointedWalPages.Clear();

        List<uint>? released = null;
        lock (_pendingFreeLock)
        {
            foreach (var pageId in _pendingFreePages)
                if (!IsInOpenTransaction(pageId))
                    (released ??= new List<uint>()).Add(pageId);
        }
        return released;
    }

    /// <summary>
    /// Returns pages picked by <see cref="OnWalTruncated"/> to their file's free list, outside
    /// <c>_commitLock</c>. That is safe: their free was requested before the truncation, so no
    /// committed structure refers to them, no transaction can write them, and the truncation
    /// removed their last WAL image; a commit that runs meanwhile cannot give them a new one.
    /// Each page stays queued until it is actually freed, so scans keep skipping it, a repeated
    /// free request is a no-op, and a page dropped from the queue meanwhile (its collection file
    /// was deleted: <see cref="DiscardPendingFreePages"/>) is left alone. A page whose release
    /// fails stays queued and is retried at the next truncation.
    /// </summary>
    private void ReleaseTruncatedPages(List<uint> pages)
    {
        foreach (var pageId in pages)
        {
            lock (_pendingFreeLock)
                if (!_pendingFreePages.Contains(pageId)) continue;

            try { GetPageFile(pageId, out var physId).FreePage(physId); }
            catch { continue; }

            lock (_pendingFreeLock)
                _pendingFreePages.Remove(pageId);
        }
    }

    private bool IsInOpenTransaction(uint pageId)
    {
        foreach (var txnPages in _walCache.Values)
            if (txnPages.ContainsKey(pageId))
                return true;
        return false;
    }

    /// <summary>Snapshot of the queued frees, or <c>null</c> when there are none.</summary>
    private HashSet<uint>? SnapshotPendingFreePages()
    {
        lock (_pendingFreeLock)
            return _pendingFreePages.Count == 0 ? null : new HashSet<uint>(_pendingFreePages);
    }

    /// <summary>
    /// Drops queued frees of pages in the given collection file, which is being deleted: the
    /// slot keeps its number, so releasing them later would free pages of a recreated file.
    /// </summary>
    private void DiscardPendingFreePages(int collectionSlot)
    {
        var slotBits = CollectionPageMarker | ((uint)(collectionSlot & 0x3F) << CollSlotShift);
        lock (_pendingFreeLock)
            _pendingFreePages.RemoveWhere(p => (p & (SubFileTypeMask | CollSlotMask)) == slotBits);
    }

    private bool HasPendingFreePages
    {
        get { lock (_pendingFreeLock) return _pendingFreePages.Count > 0; }
    }

    // -----------------------------------------------------------------------
    // Internal routing
    // -----------------------------------------------------------------------

    /// <summary>
    /// Returns the <see cref="IPageStorage"/> that owns the given (possibly encoded) page ID
    /// and outputs the physical (file-local) page number to pass to that storage backend.
    /// Routing is determined entirely from the high bits of <paramref name="pageId"/>:
    /// no in-memory dictionaries are required and routing survives engine restarts.
    /// </summary>
    private IPageStorage GetPageFile(uint pageId, out uint physicalPageId)
    {
        var fileTag = pageId & SubFileTypeMask;

        if (fileTag == IndexPageMarker)
        {
            physicalPageId = pageId & IndexLocalMask;
            return _indexFile ?? _pageFile;
        }

        if (fileTag == CollectionPageMarker)
        {
            var slot = (int)((pageId & CollSlotMask) >> CollSlotShift);
            physicalPageId = pageId & CollLocalMask;
            var collName = GetCollectionNameBySlot(slot);
            if (collName != null)
                return GetOrCreateCollectionFile(collName);
            // Slot not found (e.g. file was dropped) — fall through to main file
        }

        physicalPageId = pageId;
        return _pageFile;
    }

    private string? GetCollectionNameBySlot(int slot)
        => (_collectionSlotToName != null && _collectionSlotToName.TryGetValue(slot, out var name))
            ? name : null;

    private IPageStorage GetOrCreateCollectionFile(string collectionName)
    {
        if (_collectionFiles == null)
            return _pageFile;

        return _collectionFiles.GetOrAdd(collectionName, name =>
            new Lazy<IPageStorage>(() =>
            {
                var filePath = CollectionFilePath(name);
                var collConfig = AsStandaloneConfig(_config);

                // When encryption is configured, derive a per-collection provider keyed on
                // the slot index so that collection files never share a key with each other
                // or with the main/index file even when they contain the same page IDs.
                if (_config.CryptoProvider != null &&
                    _collectionNameToSlot != null &&
                    _collectionNameToSlot.TryGetValue(name, out var slot))
                {
                    collConfig = collConfig with
                    {
                        CryptoProvider = _config.CryptoProvider.CreateSiblingProvider(1, (ushort)slot)
                    };
                }

                var pf = new PageFile(filePath, collConfig);
                // Creating a collection file races with other processes just like the main
                // file does (#216).
                using (EnterOpenLock())
                    pf.Open();
                return pf;
            })).Value;
    }

    // -----------------------------------------------------------------------
    // Collection file helpers
    // -----------------------------------------------------------------------

    /// <summary>
    /// Returns the absolute path for a collection's dedicated data file.
    /// The filename is always lowercased to avoid case-sensitive file-system ambiguity.
    /// </summary>
    private string CollectionFilePath(string collectionName)
        => Path.Combine(_config.CollectionDataDirectory!, collectionName.ToLowerInvariant() + ".db");

    /// <summary>
    /// Drops a collection's dedicated file, reclaiming disk space immediately.
    /// No-op in single-file (embedded) mode.
    /// </summary>
    public void DropCollectionFile(string collectionName)
    {
        if (_collectionFiles == null) return;

        if (_collectionNameToSlot != null && _collectionNameToSlot.TryGetValue(collectionName, out var slot))
            DiscardPendingFreePages(slot);

        if (_collectionFiles.TryRemove(collectionName, out var lazy))
        {
            if (lazy.IsValueCreated) lazy.Value.Dispose();
            var filePath = CollectionFilePath(collectionName);
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }

    /// <summary>
    /// Marks all pages owned by the named collection as free, making them immediately
    /// available for reuse by the page allocator.
    /// <list type="bullet">
    ///   <item><b>Single-file mode:</b> frees data pages, overflow pages, all index pages
    ///   (B-tree, vector, spatial), TimeSeries page chains, and schema pages.</item>
    ///   <item><b>Multi-file mode:</b> data and TimeSeries pages reside in the dedicated
    ///   collection file, which the caller frees via <see cref="DropCollectionFile"/>.
    ///   This method frees the index pages and schema pages that are stored in the shared
    ///   main/index file.</item>
    /// </list>
    /// </summary>
    /// <remarks>
    /// This does not compact the file — a VACUUM pass is required to reclaim physical disk space.
    /// Must be called BEFORE <see cref="DeleteCollectionMetadata"/> so that metadata is still available.
    /// </remarks>
    public void FreeCollectionPages(string collectionName) => FreeCollectionPagesCore(collectionName);

    /// <summary>
    /// Same as <see cref="FreeCollectionPages"/>, but returns the freed page IDs.
    /// Kept separate so the public <c>void</c> signature stays binary-compatible.
    /// </summary>
    /// <param name="beforeFree">Invoked with the collected page IDs before any page is returned
    /// to the free list, so callers can invalidate cached views of them (e.g. the shared
    /// free-space index) before they can be re-allocated. This does not lock against concurrent
    /// inserts into other collections, so a page may still be advertised between the slot count
    /// and this callback; drops are expected to run without concurrent writers.</param>
    /// <returns>The page IDs that were freed.</returns>
    internal IReadOnlyCollection<uint> FreeCollectionPagesCore(string collectionName,
        Action<IReadOnlyCollection<uint>>? beforeFree = null)
    {
        var metadata = GetCollectionMetadata(collectionName);
        if (metadata == null) return Array.Empty<uint>();

        // The pages of a feature this version lacks (e.g. an index type it does not know) cannot be
        // found here; refuse instead of leaking them and dropping the rest.
        CollectionFeatures.EnsureCanWrite(metadata);

        // Collect page IDs in a set first, then free them all — avoids modifying storage
        // while iterating over B-tree nodes (which also read from the same storage).
        var toFree = new HashSet<uint>();

        // ── 1. Data pages (single-file mode only; multi-file: deleted via DropCollectionFile) ──
        if (_collectionFiles == null && metadata.PrimaryRootPageId != 0)
        {
            var primaryIndex = new BTreeIndex(this, IndexOptions.CreateUnique("_id"), metadata.PrimaryRootPageId);

            // Collect all B-tree node pages first.
            foreach (var pageId in primaryIndex.CollectAllPages())
                toFree.Add(pageId);

            // For each data page, only free it if ALL non-deleted slots belong to this
            // collection.  In single-file mode multiple collections can share a data page;
            // freeing a shared page would corrupt the other collection's data.
            // We track (pageId → owned-slot-count from this collection) and compare against
            // the total live-slot count read from the page header.  The logic is correct
            // because every non-deleted slot on a page is either owned by this collection
            // (tracked in ownedSlotCount) or by another collection; if liveSlots == ownedSlotCount
            // then there are exactly zero slots from any other collection on the page.
            var pageSlotCounts = new Dictionary<uint, int>(); // pageId → owned-slot-count
            var pageBuffer = ArrayPool<byte>.Shared.Rent(PageSize);
            try
            {
                foreach (var entry in primaryIndex.Range(IndexKey.MinKey, IndexKey.MaxKey))
                {
                    var dataPageId = entry.Location.PageId;
                    if (dataPageId == 0) continue;
                    pageSlotCounts.TryGetValue(dataPageId, out var prev);
                    pageSlotCounts[dataPageId] = prev + 1;
                }

                foreach (var (dataPageId, ownedSlotCount) in pageSlotCounts)
                {
                    ReadPage(dataPageId, null, pageBuffer);
                    var hdr = SlottedPageHeader.ReadFrom(pageBuffer.AsSpan(0, SlottedPageHeader.Size));
                    if (hdr.PageType != PageType.Data) continue;

                    // Count non-deleted slots on this page.
                    int liveSlots = 0;
                    for (ushort s = 0; s < hdr.SlotCount; s++)
                    {
                        var slotOff = SlottedPageHeader.Size + (s * SlotEntry.Size);
                        var slot = SlotEntry.ReadFrom(pageBuffer.AsSpan(slotOff));
                        if ((slot.Flags & SlotFlags.Deleted) == 0)
                            liveSlots++;
                    }

                    if (liveSlots != ownedSlotCount)
                        // Page is shared with another collection — leave it; VACUUM will compact.
                        continue;

                    toFree.Add(dataPageId);

                    // The page is exclusively ours — also chase any overflow chains.
                    for (ushort s = 0; s < hdr.SlotCount; s++)
                    {
                        var slotOff = SlottedPageHeader.Size + (s * SlotEntry.Size);
                        var slot = SlotEntry.ReadFrom(pageBuffer.AsSpan(slotOff));
                        if ((slot.Flags & SlotFlags.HasOverflow) == 0) continue;
                        if (slot.Offset + 8 > pageBuffer.Length) continue;
                        var overflowPageId = BinaryPrimitives.ReadUInt32LittleEndian(
                            pageBuffer.AsSpan(slot.Offset + 4, 4));
                        while (overflowPageId != 0)
                        {
                            toFree.Add(overflowPageId);
                            ReadPage(overflowPageId, null, pageBuffer);
                            var overflowHdr = SlottedPageHeader.ReadFrom(pageBuffer.AsSpan(0, SlottedPageHeader.Size));
                            overflowPageId = overflowHdr.NextOverflowPage;
                        }
                    }
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(pageBuffer);
            }
        }
        else if (_collectionFiles != null && metadata.PrimaryRootPageId != 0)
        {
            // Multi-file: B-tree index pages for the primary index live in the shared index file.
            // Collect them here; the data pages are freed by deleting the collection file.
            var primaryIndex = new BTreeIndex(this, IndexOptions.CreateUnique("_id"), metadata.PrimaryRootPageId);
            foreach (var pageId in primaryIndex.CollectAllPages())
                toFree.Add(pageId);
        }

        // ── 2. Secondary index pages (all modes — these live in the shared index/main file) ──
        foreach (var idx in metadata.Indexes)
        {
            if (idx.RootPageId == 0) continue;

            switch (idx.Type)
            {
                case IndexType.BTree:
                case IndexType.Unique:
                {
                    var opts = idx.IsUnique
                        ? IndexOptions.CreateUnique(idx.PropertyPaths)
                        : IndexOptions.CreateBTree(idx.PropertyPaths);
                    var secIndex = new BTreeIndex(this, opts, idx.RootPageId);
                    foreach (var pageId in secIndex.CollectAllPages())
                        toFree.Add(pageId);
                    break;
                }
                case IndexType.Vector:
                {
                    var opts = IndexOptions.CreateVector(idx.Dimensions, idx.Metric, fields: idx.PropertyPaths);
                    var vecIndex = new VectorSearchIndex(this, opts, idx.RootPageId);
                    foreach (var pageId in vecIndex.CollectAllPages())
                        toFree.Add(pageId);
                    break;
                }
                case IndexType.FullText:
                {
                    var ftsIndex = new FullTextIndex(this, IndexOptions.CreateBTree(idx.PropertyPaths), idx.RootPageId);
                    foreach (var pageId in ftsIndex.CollectAllPages())
                        toFree.Add(pageId);
                    break;
                }
                case IndexType.Spatial:
                {
                    var opts = IndexOptions.CreateSpatial(idx.PropertyPaths);
                    var rtreeIndex = new RTreeIndex(this, opts, idx.RootPageId);
                    foreach (var pageId in rtreeIndex.CollectAllPages())
                        toFree.Add(pageId);
                    break;
                }
            }
        }

        // ── 3. TimeSeries page chain (single-file mode only; multi-file: in collection file) ──
        if (_collectionFiles == null && metadata.IsTimeSeries && metadata.TimeSeriesHeadPageId != 0)
        {
            var tsBuf = ArrayPool<byte>.Shared.Rent(PageSize);
            try
            {
                var tsPageId = metadata.TimeSeriesHeadPageId;
                while (tsPageId != 0)
                {
                    toFree.Add(tsPageId);
                    ReadPage(tsPageId, null, tsBuf);
                    var tsHdr = PageHeader.ReadFrom(tsBuf.AsSpan(0, 32));
                    tsPageId = tsHdr.NextPageId;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(tsBuf);
            }
        }

        // ── 4. Schema chain pages (all modes — these live in the main file) ──
        if (metadata.SchemaRootPageId != 0)
        {
            var schemaBuf = ArrayPool<byte>.Shared.Rent(PageSize);
            try
            {
                var schemaPageId = metadata.SchemaRootPageId;
                while (schemaPageId != 0)
                {
                    toFree.Add(schemaPageId);
                    ReadPage(schemaPageId, null, schemaBuf);
                    var sHdr = PageHeader.ReadFrom(schemaBuf.AsSpan(0, 32));
                    schemaPageId = sHdr.NextPageId;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(schemaBuf);
            }
        }

        beforeFree?.Invoke(toFree);

        // FreePage defers reuse until the WAL holds no stale image of these pages.
        foreach (var pageId in toFree)
            FreePage(pageId);

        return toFree;
    }

    /// <summary>
    /// Returns an enumerable of all (encoded) page IDs for the specified collection.
    /// <list type="bullet">
    ///   <item>Single-file mode: enumerates all pages 0..N from the main file
    ///   (DocumentCollection filters by <see cref="PageType.Data"/>).</item>
    ///   <item>Multi-file mode: enumerates only the pages in the collection's dedicated file,
    ///   using encoded IDs that survive engine restarts.</item>
    /// </list>
    /// </summary>
    public IEnumerable<uint> GetCollectionPageIds(string collectionName)
    {
        // Freed pages waiting for a WAL truncation still carry their old contents on disk.
        var pending = SnapshotPendingFreePages();

        if (_collectionFiles == null)
        {
            var mainCount = _pageFile.NextPageId;
            for (uint i = 0; i < mainCount; i++)
                if (pending == null || !pending.Contains(i))
                    yield return i;
            yield break;
        }

        if (!_collectionNameToSlot!.TryGetValue(collectionName, out var slot))
            yield break; // No pages allocated for this collection yet

        var file = GetOrCreateCollectionFile(collectionName);
        var localCount = file.NextPageId;
        var encBase = CollectionPageMarker | ((uint)(slot & 0x3F) << CollSlotShift);
        for (uint localId = 0; localId < localCount; localId++)
        {
            var pageId = encBase | (localId & CollLocalMask);
            if (pending == null || !pending.Contains(pageId))
                yield return pageId;
        }
    }

    // -----------------------------------------------------------------------
    // Collection slot management
    // -----------------------------------------------------------------------

    private int GetOrAssignCollectionSlot(string collectionName)
    {
        if (_collectionNameToSlot!.TryGetValue(collectionName, out var existing))
            return existing;

        lock (_collectionSlotLock)
        {
            if (_collectionNameToSlot.TryGetValue(collectionName, out existing))
                return existing;

            if (_nextSlotIndex >= MaxCollections)
                throw new InvalidOperationException(
                    $"Maximum number of per-collection files ({MaxCollections}) reached. "
                  + $"Use single-file mode (no CollectionDataDirectory) for more than {MaxCollections} collections.");

            var newSlot = _nextSlotIndex++;
            _collectionNameToSlot[collectionName] = newSlot;
            _collectionSlotToName![newSlot] = collectionName;
            SaveCollectionSlots();
            return newSlot;
        }
    }

    private void LoadCollectionSlots()
    {
        if (_slotsFilePath == null || !File.Exists(_slotsFilePath))
            return;

        foreach (var line in File.ReadAllLines(_slotsFilePath))
        {
            var sep = line.IndexOf(',');
            if (sep < 0) continue;
#if NET5_0_OR_GREATER
            if (!int.TryParse(line.AsSpan(0, sep), out var slot)) continue;
#else
            if (!int.TryParse(line.Substring(0, sep), out var slot)) continue;
#endif
            var name = line.Substring(sep + 1);
            if (string.IsNullOrEmpty(name)) continue;
            _collectionNameToSlot![name] = slot;
            _collectionSlotToName![slot] = name;
            if (slot >= _nextSlotIndex)
                _nextSlotIndex = slot + 1;
        }
    }

    private void SaveCollectionSlots()
    {
        if (_slotsFilePath == null) return;

        string[] lines;
        lock (_collectionSlotLock)
        {
            lines = _collectionSlotToName!
                .OrderBy(kvp => kvp.Key)
                .Select(kvp => $"{kvp.Key},{kvp.Value}")
                .ToArray();
        }
        File.WriteAllLines(_slotsFilePath, lines);
    }

    /// <summary>Validates that a collection name can safely be used as a filename component.</summary>
    private static void ValidateCollectionName(string collectionName)
    {
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Collection name must not be null or whitespace.", nameof(collectionName));

        if (Path.IsPathRooted(collectionName) ||
            collectionName.Contains("..") ||
            collectionName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            throw new ArgumentException(
                $"Collection name '{collectionName}' contains characters that are invalid in a filename.",
                nameof(collectionName));
    }
}
