using System.Collections.Concurrent;
using System.Threading.Channels;
using BLite.Core.Encryption;
using BLite.Core.Transactions;

namespace BLite.Core.Storage;

/// <summary>
/// Central storage engine managing page-based storage with WAL for durability.
/// 
/// Architecture (WAL-based like SQLite/PostgreSQL):
/// - PageFile: Committed baseline (persistent on disk)
/// - WAL Cache: Uncommitted transaction writes (in-memory)
/// - Read: PageFile + WAL cache overlay (for Read Your Own Writes)
/// - Commit: Flush to WAL, clear cache
/// - CheckpointAsync: Merge WAL ? PageFile periodically
/// </summary>
public sealed partial class StorageEngine : IDisposable
{
    private readonly IPageStorage _pageFile;                // data: Data, Overflow, Collection, KV, Dictionary, TimeSeries, Metadata
    private readonly IPageStorage? _indexFile;              // indices: Index, Vector, Spatial (null = uses _pageFile)
    private readonly IWriteAheadLog _wal;

    // Cross-process WAL coordination sidecar (null in single-process mode).
    // Owned by this engine; created in the constructor when
    // PageFileConfig.AllowMultiProcessAccess is true and disposed in Dispose.
    // See roadmap/v5/MULTI_PROCESS_WAL.md for the full design.
    private readonly Transactions.WalSharedMemory? _shm;

    // Holds of the SHM open lock by this engine (#216, see EnterOpenLock). The engine takes the
    // OS-level lock on the first hold and releases it with the last one, so nested or concurrent
    // holds from its own threads never wait for each other. Guarded by _openLockGate.
    private readonly object _openLockGate = new();
    private int _openLockHolds;
    private CDC.ChangeStreamDispatcher? _cdc;
    private volatile Metrics.MetricsDispatcher? _metrics;
    
    // WAL cache: TransactionId → (PageId → PageData)
    // Stores uncommitted writes for "Read Your Own Writes" isolation
    private readonly ConcurrentDictionary<ulong, ConcurrentDictionary<uint, byte[]>> _walCache;
    
    // WAL index cache: PageId → PageData (from latest committed transaction)
    // Lazily populated on first read after commit
    private readonly ConcurrentDictionary<uint, byte[]> _walIndex;

    // Tracks the WAL byte offset (record start) of the most recent committed Write
    // record for each page. Populated alongside _walIndex in the group-commit path
    // so that CheckpointAsync can bound its work to WAL entries ≤ the minimum reader
    // offset (Phase 6). Only populated when _shm != null (multi-process mode).
    private readonly ConcurrentDictionary<uint, long>? _walOffsets;

    // Last WAL end-offset this engine has replayed into its local _walIndex.
    // Compared against _shm.ReadWalEndOffset() on BeginTransaction to detect
    // cross-process commits and trigger incremental WAL replay (Phase 7).
    private long _lastKnownWalEndOffset;

    // Multi-process: the generation of the shared WAL (WalSharedMemory.ReadWalGeneration) that the
    // pages this engine replayed or read from it belong to, and how often it has discarded them.
    // Both change only under _replayLock, which also guards caching a page read from the WAL.
    private long _knownWalGeneration;
    private long _replayEpoch;
    private readonly object _replayLock = new();

    // Multi-process: catch-ups in a row that could not replay the shared WAL (under _commitLock),
    // and the published WAL end the last of them failed at (-1 if none). A read does not retry
    // the replay until the WAL has moved past it (LocalImageIsCurrent).
    private int _failedCatchUps;
    private long _catchUpFailedAtEnd = -1;
    
    // Collection-per-file: collectionName → IPageStorage dedicated
    // Null if CollectionDataDirectory not configured (embedded mode, single file)
    // Lazy<IPageStorage> ensures the file-open factory runs exactly once per collection,
    // even when ConcurrentDictionary.GetOrAdd is called concurrently for the same key.
    private readonly ConcurrentDictionary<string, Lazy<IPageStorage>>? _collectionFiles;

    // Collection slot registry — only populated in multi-file mode
    // Maps collection name → slot index (0-63) and slot → name
    private readonly ConcurrentDictionary<string, int>? _collectionNameToSlot;
    private readonly Dictionary<int, string>? _collectionSlotToName;
    private int _nextSlotIndex;
    private readonly string? _slotsFilePath;
    private readonly object _collectionSlotLock = new();

    // Stored config for use by multi-file helpers
    private readonly PageFileConfig _config;
    
    // Global lock for commit/checkpoint synchronization.
    // Held only by the group commit writer (and sync commit / checkpoint paths).
    private readonly SemaphoreSlim _commitLock = new(1, 1);

    // Serialises the multi-step read-modify-write on the Collection catalog pages
    // (page 1 and its overflow chain).  Prevents concurrent SaveCollectionMetadata /
    // DeleteCollectionMetadata calls from corrupting the slotted-page structure.
    private readonly SemaphoreSlim _metadataLock = new(1, 1);

    // Guard to prevent multiple concurrent checkpoint attempts.
    // Only one checkpoint runs at a time; additional callers skip.
    private int _checkpointRunning;

    // Admission gate: limits how many threads can simultaneously enter the commit path.
    // Prevents deep queues on internal WAL/commit locks that cause latency spikes.
    // null when MaxConcurrentWriters == 0 (admission control disabled).
    private readonly SemaphoreSlim? _writerGate;

    // Serialises data-page writes across every collection backed by this engine's file.
    // A DocumentCollection locks its own writes, but page placement depends on state shared by
    // the whole file: the free-space index (FreeSpaceIndexProvider hands out one instance in
    // single-file mode) and the page allocator. FindPageWithSpace and InsertIntoPage are two
    // separate calls, so without a file-wide lock two collections can be handed the same page,
    // and since a page is written back as a whole buffer the loser's slots are dropped.
    // Null in collection-per-file mode, where nothing is shared and per-collection locks suffice.
    private readonly SemaphoreSlim? _dataWriteLock;

    // Group commit writer infrastructure.
    private readonly Channel<PendingCommit> _commitChannel;
    private readonly CancellationTokenSource _writerCts = new();
    private readonly Task _writerTask;

    // Transaction Management
    private readonly ConcurrentDictionary<ulong, Transaction> _activeTransactions;
    // Stored as long so Interlocked.Increment works on all target frameworks.
    private long _nextTransactionId;

    private const long MaxWalSize = 16 * 1024 * 1024; // 16MB

    // Freed pages waiting for a WAL truncation (see FreePage) are file space that cannot be reused.
    // Past this many bytes of them a checkpoint is requested, so a workload that frees pages faster
    // than it fills the WAL, such as a loop of index creates and drops, does not grow the file by
    // one tree per cycle until the WAL reaches MaxWalSize (#209).
    private const long MaxPendingFreeBytes = 4 * 1024 * 1024; // 4MB

    private volatile bool _disposed;

    // ── Audit ─────────────────────────────────────────────────────────────────
    private volatile Audit.BLiteAuditOptions? _auditOptions;

    /// <summary>
    /// The lock timeout configuration for this engine, as specified in the <see cref="PageFileConfig"/>.
    /// Exposed so that higher-level components (collections, engine, sessions) can read the same settings.
    /// </summary>
    internal LockTimeout LockTimeout => _config.LockTimeout;
    internal bool UsesSeparateCollectionFiles => _collectionFiles != null;

    /// <summary>
    /// Whether the database was opened read-only (<see cref="PageFileConfig.Access"/> is
    /// <see cref="System.IO.MemoryMappedFiles.MemoryMappedFileAccess.Read"/>). Collections that
    /// require features this version cannot maintain may still be opened for reading then
    /// (see <see cref="CollectionFeatures"/>).
    /// </summary>
    internal bool IsReadOnly => _config.Access == System.IO.MemoryMappedFiles.MemoryMappedFileAccess.Read;

    /// <summary>
    /// Returns the semaphore a <see cref="Collections.DocumentCollection{TId,T}"/> must hold for
    /// write operations. In single-file mode every collection of this engine gets the same
    /// instance, because they share the free-space index and the page allocator
    /// (see <see cref="_dataWriteLock"/>). In collection-per-file mode each collection gets its
    /// own, preserving write concurrency between collections.
    /// </summary>
    internal SemaphoreSlim CreateCollectionWriteLock() => _dataWriteLock ?? new SemaphoreSlim(1, 1);

    /// <summary>
    /// Multi-process WAL coordination sidecar; <c>null</c> when
    /// <see cref="PageFileConfig.AllowMultiProcessAccess"/> was not set on the config.
    /// Exposed to internals (notably <c>BLite.Tests</c>) so cross-process behaviour can be
    /// asserted from integration tests.
    /// </summary>
    internal Transactions.WalSharedMemory? SharedMemory => _shm;

    // ── Cross-process open lock (multi-process mode, #216) ───────────────────

    /// <summary>
    /// Holds the SHM open lock (<see cref="Transactions.WalSharedMemory.AcquireOpenLock"/>) until
    /// the returned scope is disposed; a no-op in single-process mode. Besides the constructor,
    /// everything that creates or changes shared structures directly in the page file rather than
    /// through the WAL takes it: creating collections (catalog entries, schema pages, index
    /// roots) and adding dictionary keys. Otherwise two processes creating the same collections
    /// read each other's half-written catalog and allocate the same page ids.
    /// <para>
    /// Holds by this engine's own threads share one OS-level lock (taken on the first hold,
    /// released with the last), so they nest and never wait for each other. When the OS-level
    /// lock is taken, the page files first pick up what other processes did meanwhile
    /// (<see cref="PageFile.RefreshAllocationState"/>).
    /// </para>
    /// </summary>
    internal OpenLockScope EnterOpenLock()
    {
        if (_shm == null) return default;
        EnterOpenLockCore();
        return new OpenLockScope(this);
    }

    private void EnterOpenLockCore()
    {
        lock (_openLockGate)
        {
            if (_openLockHolds == 0)
            {
                _shm!.AcquireOpenLock();
                try
                {
                    // Null while the constructor opens them: they are then read fresh anyway.
                    (_pageFile as PageFile)?.RefreshAllocationState();
                    (_indexFile as PageFile)?.RefreshAllocationState();
                    if (_collectionFiles != null)
                        foreach (var lazy in _collectionFiles.Values)
                            if (lazy.IsValueCreated && lazy.Value is PageFile collectionFile)
                                collectionFile.RefreshAllocationState();
                }
                catch
                {
                    _shm.ReleaseOpenLock();
                    throw;
                }
            }
            _openLockHolds++;
        }
    }

    private void ExitOpenLock()
    {
        lock (_openLockGate)
        {
            if (--_openLockHolds == 0)
                _shm!.ReleaseOpenLock();
        }
    }

    /// <summary>Scope returned by <see cref="EnterOpenLock"/>; disposing it ends the hold.</summary>
    internal readonly struct OpenLockScope : IDisposable
    {
        private readonly StorageEngine? _engine;
        internal OpenLockScope(StorageEngine engine) => _engine = engine;
        public void Dispose() => _engine?.ExitOpenLock();
    }

    public StorageEngine(string databasePath, PageFileConfig config)
    {
        config = config.Normalize();
        _config = config;
        ICryptoProvider? walCryptoProvider = null;
        bool holdsOpenLock = false;
        try
        {
            // Use WalPath if specified, otherwise derive from databasePath (default behavior unchanged)
            var walPath = config.WalPath ?? Path.ChangeExtension(databasePath, ".wal");

            // Ensure WAL parent directory exists
            var walDirectory = Path.GetDirectoryName(walPath);
            if (!string.IsNullOrWhiteSpace(walDirectory))
                Directory.CreateDirectory(walDirectory);

            // ── Multi-process WAL coordination (opt-in) ─────────────────────────
            // When AllowMultiProcessAccess is true, open the .wal-shm sidecar that
            // coordinates cross-process writers / readers (see roadmap/v5/MULTI_PROCESS_WAL.md).
            // The SHM file lives next to the WAL so both have the same lifetime.
            if (config.AllowMultiProcessAccess)
            {
                var shmPath = walPath + "-shm";
                _shm = Transactions.WalSharedMemory.Open(shmPath, config.PageSize);
                _walOffsets = new ConcurrentDictionary<uint, long>();
                _knownWalGeneration = _shm.ReadWalGeneration();

                // If a previous writer crashed without releasing the writer lock, the
                // recorded PID will not be alive — clear it so this process (or another)
                // can re-acquire the lock. The OS-level mutex / OFD lock has already been
                // auto-released by the kernel; this only clears our PID stamp.
                _shm.ForceClearStaleWriter();

                // Everything from here to the end of the constructor creates or reads the
                // database files' initial state: the page file (created if missing, header),
                // the index file, WAL recovery, and the dictionary / KV roots. Without the
                // open lock, a second process opening a new database at the same moment reads
                // a header that is not written yet, or a page that the first process wrote past
                // the end of its own memory map (#216). Released at the end of the constructor.
                // A process that waits here then finds the files initialised and opens them
                // normally. Single-process mode needs no lock: FileShare.None excludes others.
                EnterOpenLockCore();
                holdsOpenLock = true;
            }

            // Initialize storage infrastructure
            _pageFile = new PageFile(databasePath, config);

            _pageFile.Open();
            // NOTE: If CryptoProvider is a coordinator-managed provider, opening the main file
            // primes the coordinator's database salt, making CreateSiblingProvider available
            // for index, WAL, and per-collection files.

            // Phase 3: open separate index file if configured
            if (config.IndexFilePath != null)
            {
                var idxDirectory = Path.GetDirectoryName(config.IndexFilePath);
                if (!string.IsNullOrWhiteSpace(idxDirectory))
                    Directory.CreateDirectory(idxDirectory);

                // Derive a dedicated index-file provider so that index and data pages
                // encrypted with the same pageId never share a key.
                var idxConfig = config.CryptoProvider != null
                    ? AsStandaloneConfig(config) with { CryptoProvider = config.CryptoProvider.CreateSiblingProvider(2, 0) }
                    : AsStandaloneConfig(config);

                _indexFile = new PageFile(config.IndexFilePath, idxConfig);
                _indexFile.Open();
            }

            // Phase 4: initialize collection-per-file structures if configured
            if (config.CollectionDataDirectory != null)
            {
                Directory.CreateDirectory(config.CollectionDataDirectory);
                _collectionFiles = new ConcurrentDictionary<string, Lazy<IPageStorage>>(StringComparer.OrdinalIgnoreCase);
                _collectionNameToSlot = new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                _collectionSlotToName = new Dictionary<int, string>();
                _slotsFilePath = Path.Combine(config.CollectionDataDirectory, ".slots");
                LoadCollectionSlots();
            }

            // When encryption is configured, derive a dedicated WAL provider from the main-file
            // provider so that WAL records are always encrypted alongside the rest of the database.
            walCryptoProvider = config.CryptoProvider?.CreateSiblingProvider(3, 0);

            _wal = new WriteAheadLog(walPath, walCryptoProvider, config.LockTimeout.WriteTimeoutMs, config.AllowMultiProcessAccess,
                syncOnFlush: config.WalSync != WalSyncMode.Normal);
            walCryptoProvider = null;
            _walCache = new ConcurrentDictionary<ulong, ConcurrentDictionary<uint, byte[]>>();
            _walIndex = new ConcurrentDictionary<uint, byte[]>();
            _activeTransactions = new ConcurrentDictionary<ulong, Transaction>();
            _nextTransactionId = 0; // Interlocked.Increment pre-increments, so first txnId == 1.

            // Admission gate: limits concurrent commit pressure on WAL/commit locks.
            _writerGate = config.LockTimeout.MaxConcurrentWriters > 0
                ? new SemaphoreSlim(config.LockTimeout.MaxConcurrentWriters, config.LockTimeout.MaxConcurrentWriters)
                : null;

            _dataWriteLock = _collectionFiles == null ? new SemaphoreSlim(1, 1) : null;

            // Start the group commit writer.
            _commitChannel = Channel.CreateBounded<PendingCommit>(new BoundedChannelOptions(4096)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false
            });
            _writerTask = Task.Run(() => GroupCommitWriterAsync(_writerCts.Token));
            
            // RecoverAsync from WAL if exists (crash recovery or resume after close)
            // This replays any committed transactions not yet checkpointed
            if (_wal.GetCurrentSize() > 0)
            {
                RecoverAsync().GetAwaiter().GetResult();
            }
            
            InitializeDictionary();
            InitializeKv();

            if (holdsOpenLock)
            {
                holdsOpenLock = false;
                ExitOpenLock();
            }
            
            // Create and start checkpoint manager
            // _checkpointManager = new Transactions.CheckpointManager(this);
            // _checkpointManager.StartAutoCheckpoint();
        }
        catch
        {
            if (holdsOpenLock)
                try { ExitOpenLock(); } catch { /* best-effort */ }
            try { _writerCts.Cancel(); } catch { /* best-effort */ }
            try { _writerTask?.Wait(TimeSpan.FromSeconds(5)); } catch { /* best-effort */ }
            try { _wal?.Dispose(); } catch { /* best-effort */ }
            try { _pageFile?.Dispose(); } catch { /* best-effort */ }
            try { _indexFile?.Dispose(); } catch { /* best-effort */ }
            try { _writerGate?.Dispose(); } catch { /* best-effort */ }
            try { _shm?.Dispose(); } catch { /* best-effort */ }
            if (walCryptoProvider is IDisposable disposableWalCrypto)
                try { disposableWalCrypto.Dispose(); } catch { /* best-effort */ }
            try { _writerCts.Dispose(); } catch { /* best-effort */ }
            throw;
        }
    }

    /// <summary>
    /// Creates a storage engine backed by pre-built <see cref="IPageStorage"/> and
    /// <see cref="IWriteAheadLog"/> instances. Use this constructor when you need a
    /// non-file-system backend, such as <see cref="MemoryPageStorage"/> for in-memory
    /// or WASM use cases.
    /// <para>
    /// The supplied <paramref name="pageStorage"/> must already be opened
    /// (i.e. <c>Open()</c> called) before being passed here.
    /// </para>
    /// <para>
    /// Multi-file routing (separate index file, per-collection files) is not available
    /// in this mode — all pages share the single <paramref name="pageStorage"/> instance.
    /// </para>
    /// </summary>
    /// <param name="pageStorage">Page storage backend (already opened).</param>
    /// <param name="wal">Write-ahead log implementation.</param>
    public StorageEngine(IPageStorage pageStorage, IWriteAheadLog wal)
    {
        _config = PageFileConfig.Default;
        _pageFile = pageStorage ?? throw new ArgumentNullException(nameof(pageStorage));
        _wal = wal ?? throw new ArgumentNullException(nameof(wal));

        _walCache = new ConcurrentDictionary<ulong, ConcurrentDictionary<uint, byte[]>>();
        _walIndex = new ConcurrentDictionary<uint, byte[]>();
        _activeTransactions = new ConcurrentDictionary<ulong, Transaction>();
        _nextTransactionId = 0;

        _writerGate = null; // No admission control needed for single-backend mode.
        _dataWriteLock = new SemaphoreSlim(1, 1);

        _commitChannel = Channel.CreateBounded<PendingCommit>(new BoundedChannelOptions(4096)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
        _writerTask = Task.Run(() => GroupCommitWriterAsync(_writerCts.Token));

        // No WAL recovery: the caller provides a fresh backend.
        // For MemoryWriteAheadLog this is always correct; for a custom persistent WAL
        // the caller is responsible for replaying WAL before constructing the engine.

        InitializeDictionary();
        InitializeKv();
    }

    /// <summary>
    /// Page size for this storage engine
    /// </summary>
    public int PageSize => _pageFile.PageSize;

    /// <summary>
    /// Checks if a page is currently being modified by another active transaction.
    /// This is used to implement pessimistic locking for page allocation/selection.
    /// </summary>
    public bool IsPageLocked(uint pageId, ulong excludingTxId)
    {
        foreach (var kvp in _walCache)
        {
            var txId = kvp.Key;
            if (txId == excludingTxId) continue;
            
            var txnPages = kvp.Value;
            if (txnPages.ContainsKey(pageId))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Disposes the storage engine and closes WAL.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // 1. Stop accepting new commits and let the group commit writer drain.
        _commitChannel?.Writer.TryComplete();
        _writerCts?.Cancel();
        try { _writerTask?.Wait(TimeSpan.FromSeconds(5)); } catch { /* best-effort */ }
        _writerCts?.Dispose();

        // 2. RollbackAsync any active transactions.
        if (_activeTransactions != null)
        {
            foreach (var txn in _activeTransactions.Values)
            {
                try
                {
                    RollbackTransactionAsync(txn.TransactionId).GetAwaiter().GetResult();
                }
                catch { /* Ignore errors during dispose */ }
            }
            _activeTransactions.Clear();
        }

        // 2b. Pages freed since the last WAL truncation only reach the free list after the next
        // one (see FreePage); checkpoint now so they are not leaked when the engine closes. A
        // checkpoint the queued frees requested may still be running: the one below would return
        // at once while it holds _checkpointRunning, and closing the files under it would make
        // its truncation and frees fail, leaking the pages for good. Wait for it to finish (#209).
        //
        // The wait lasts as long as that checkpoint, not a lock timeout: on a loaded CI runner one
        // took longer than WriteTimeoutMs (500 ms) and was cut short while releasing the pages. It
        // cannot hang: every lock the checkpoint takes (commit lock, cross-process writer lock,
        // page file locks) is waited for with its own timeout, its I/O is finite (the WAL images,
        // one flush, the truncation, one free per queued page), it waits for nothing Dispose
        // holds, and its task swallows failures, so it completes on its own. The cap is only for
        // I/O that never returns (a hung file system), where closing the files beats never
        // returning: 10 × WriteTimeoutMs, and at least 30 s, is far beyond the few lock timeouts a
        // checkpoint can spend and any checkpoint that makes progress.
        var pendingFreeWait = TimeSpan.FromMilliseconds(Math.Min(int.MaxValue, Math.Max(30_000L, 10L * _config.LockTimeout.WriteTimeoutMs)));
        try { PendingFreeCheckpointCompletion.Wait(pendingFreeWait); } catch { /* best-effort */ }
        if (HasPendingFreePages)
        {
            try { Task.Run(() => CheckpointAsync()).GetAwaiter().GetResult(); } catch { /* best-effort: the pages stay leaked */ }
        }

        // 3. Close WAL and PageFile.
        try { _wal?.Dispose(); } catch { /* best-effort */ }
        try { _pageFile?.Dispose(); } catch { /* best-effort */ }
        try { _indexFile?.Dispose(); } catch { /* best-effort */ }

        // 4. Close per-collection PageFiles (Phase 4)
        if (_collectionFiles != null)
        {
            foreach (var lazy in _collectionFiles.Values)
            {
                try { if (lazy.IsValueCreated) lazy.Value.Dispose(); } catch { /* best-effort */ }
            }
            _collectionFiles.Clear();
        }

        try { _commitLock?.Dispose(); } catch { /* best-effort */ }
        try { _metadataLock?.Dispose(); } catch { /* best-effort */ }
        try { _writerGate?.Dispose(); } catch { /* best-effort */ }
        try { _metrics?.Dispose(); } catch { /* best-effort */ }
        try { _shm?.Dispose(); } catch { /* best-effort */ }
    }

    internal void RegisterCdc(CDC.ChangeStreamDispatcher cdc)
    {
        _cdc = cdc;
    }

    internal CDC.ChangeStreamDispatcher? Cdc => _cdc;

    /// <summary>
    /// Ensures the CDC dispatcher is initialized. No-op if already active.
    /// Called by <see cref="BLiteEngine.SubscribeToChanges"/> before the first subscription.
    /// </summary>
    internal CDC.ChangeStreamDispatcher EnsureCdc()
    {
        return _cdc ??= new CDC.ChangeStreamDispatcher();
    }

    /// <summary>
    /// Releases a cross-process reader slot back to the SHM sidecar.
    /// Called by <see cref="Transactions.Transaction.Dispose"/> after the transaction ends.
    /// No-op when <see cref="_shm"/> is <c>null</c> or the slot index is invalid.
    /// </summary>
    internal void ReleaseReaderSlot(int slotIndex)
    {
        _shm?.ReleaseReaderSlot(slotIndex);
    }

    // ── Metrics ──────────────────────────────────────────────────────────────

    internal void RegisterMetrics(Metrics.MetricsDispatcher metrics)
    {
        _metrics = metrics;
    }

    internal Metrics.MetricsDispatcher? MetricsDispatcher => _metrics;

    /// <summary>
    /// Ensures the metrics dispatcher is initialized. No-op if already active.
    /// Called by <c>BLiteEngine.EnableMetrics()</c> before the first metric is published.
    /// <para>
    /// <paramref name="enableDiagnosticSource"/> is only honored on the <em>first</em> call;
    /// subsequent calls (including from <c>EnableMetrics</c> after metrics are already running)
    /// are no-ops and will not change the diagnostic-source configuration of the existing dispatcher.
    /// </para>
    /// </summary>
    internal Metrics.MetricsDispatcher EnsureMetrics(bool enableDiagnosticSource = false)
    {
        return _metrics ??= new Metrics.MetricsDispatcher(enableDiagnosticSource);
    }

    /// <summary>
    /// Returns a copy of <paramref name="config"/> with all multi-file routing fields cleared,
    /// suitable for use when opening a standalone sub-file (index file or per-collection file)
    /// that should not itself spawn further sub-files.
    /// </summary>
    private static PageFileConfig AsStandaloneConfig(PageFileConfig config)
        => config with { WalPath = null, IndexFilePath = null, CollectionDataDirectory = null };

    // ── Audit ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Configures the audit subsystem. Called by <c>BLiteEngine.ConfigureAudit()</c>
    /// and <c>DocumentDbContext.ConfigureAudit()</c> after construction.
    /// </summary>
    internal void ConfigureAudit(Audit.BLiteAuditOptions options)
    {
        _auditOptions = options ?? throw new ArgumentNullException(nameof(options));
        _auditMetrics = options.EnableMetrics ? new Audit.BLiteMetrics() : null;
    }

    private volatile Audit.BLiteMetrics? _auditMetrics;

    /// <summary>In-process audit metrics. Non-null only when <see cref="Audit.BLiteAuditOptions.EnableMetrics"/> is <see langword="true"/>.</summary>
    internal Audit.BLiteMetrics? AuditMetrics => _auditMetrics;

    /// <summary>The configured audit sink, or <see langword="null"/> when audit is not configured.</summary>
    internal Audit.IBLiteAuditSink? AuditSink => _auditOptions?.Sink;

    /// <summary>The full audit options object, or <see langword="null"/> when audit is not configured.</summary>
    internal Audit.BLiteAuditOptions? AuditOptions => _auditOptions;
}
