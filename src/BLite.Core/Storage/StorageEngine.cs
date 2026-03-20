using System.Collections.Concurrent;
using System.Threading.Channels;
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
/// - Checkpoint: Merge WAL ? PageFile periodically
/// </summary>
public sealed partial class StorageEngine : IDisposable
{
    private readonly PageFile _pageFile;                // data: Data, Overflow, Collection, KV, Dictionary, TimeSeries, Metadata
    private readonly PageFile? _indexFile;              // indices: Index, Vector, Spatial (null = uses _pageFile)
    private readonly WriteAheadLog _wal;
    private CDC.ChangeStreamDispatcher? _cdc;
    
    // WAL cache: TransactionId → (PageId → PageData)
    // Stores uncommitted writes for "Read Your Own Writes" isolation
    private readonly ConcurrentDictionary<ulong, ConcurrentDictionary<uint, byte[]>> _walCache;
    
    // WAL index cache: PageId → PageData (from latest committed transaction)
    // Lazily populated on first read after commit
    private readonly ConcurrentDictionary<uint, byte[]> _walIndex;
    
    // Tracks which pageIds belong to _indexFile (only populated when _indexFile != null)
    private readonly ConcurrentDictionary<uint, byte> _indexPageIds;

    // Collection-per-file: collectionName → PageFile dedicated
    // Null if CollectionDataDirectory not configured (embedded mode, single file)
    private readonly ConcurrentDictionary<string, PageFile>? _collectionFiles;

    // pageId → collectionName (only populated in multi-file mode)
    private readonly ConcurrentDictionary<uint, string>? _collectionPageMap;

    // Stored config for use by multi-file helpers
    private readonly PageFileConfig _config;
    
    // Global lock for commit/checkpoint synchronization.
    // Held only by the group commit writer (and sync commit / checkpoint paths).
    private readonly SemaphoreSlim _commitLock = new(1, 1);

    // Group commit writer infrastructure.
    private readonly Channel<PendingCommit> _commitChannel;
    private readonly CancellationTokenSource _writerCts = new();
    private readonly Task _writerTask;

    // Transaction Management
    private readonly ConcurrentDictionary<ulong, Transaction> _activeTransactions;
    // Stored as long so Interlocked.Increment works on all target frameworks.
    private long _nextTransactionId;

    private const long MaxWalSize = 4 * 1024 * 1024; // 4MB

    private volatile bool _disposed;

    public StorageEngine(string databasePath, PageFileConfig config)
    {
        _config = config;

        // Use WalPath if specified, otherwise derive from databasePath (default behavior unchanged)
        var walPath = config.WalPath ?? Path.ChangeExtension(databasePath, ".wal");

        // Initialize storage infrastructure
        _pageFile = new PageFile(databasePath, config);
        _pageFile.Open();

        // Phase 3: open separate index file if configured
        if (config.IndexFilePath != null)
        {
            _indexFile = new PageFile(config.IndexFilePath, AsStandaloneConfig(config));
            _indexFile.Open();
        }

        // Phase 4: initialize collection-per-file structures if configured
        if (config.CollectionDataDirectory != null)
        {
            Directory.CreateDirectory(config.CollectionDataDirectory);
            _collectionFiles = new ConcurrentDictionary<string, PageFile>(StringComparer.OrdinalIgnoreCase);
            _collectionPageMap = new ConcurrentDictionary<uint, string>();
        }

        _wal = new WriteAheadLog(walPath);
        _walCache = new ConcurrentDictionary<ulong, ConcurrentDictionary<uint, byte[]>>();
        _walIndex = new ConcurrentDictionary<uint, byte[]>();
        _indexPageIds = new ConcurrentDictionary<uint, byte>();
        _activeTransactions = new ConcurrentDictionary<ulong, Transaction>();
        _nextTransactionId = 0; // Interlocked.Increment pre-increments, so first txnId == 1.

        // Start the group commit writer.
        _commitChannel = Channel.CreateBounded<PendingCommit>(new BoundedChannelOptions(4096)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
        _writerTask = Task.Run(() => GroupCommitWriterAsync(_writerCts.Token));
        
        // Recover from WAL if exists (crash recovery or resume after close)
        // This replays any committed transactions not yet checkpointed
        if (_wal.GetCurrentSize() > 0)
        {
            Recover();
        }
        
        InitializeDictionary();
        InitializeKv();
        
        // Create and start checkpoint manager
        // _checkpointManager = new Transactions.CheckpointManager(this);
        // _checkpointManager.StartAutoCheckpoint();
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

        // 2. Rollback any active transactions.
        if (_activeTransactions != null)
        {
            foreach (var txn in _activeTransactions.Values)
            {
                try
                {
                    RollbackTransaction(txn.TransactionId);
                }
                catch { /* Ignore errors during dispose */ }
            }
            _activeTransactions.Clear();
        }

        // 3. Close WAL and PageFile.
        _wal?.Dispose();
        _pageFile?.Dispose();
        _indexFile?.Dispose();

        // 4. Close per-collection PageFiles (Phase 4)
        if (_collectionFiles != null)
        {
            foreach (var pf in _collectionFiles.Values)
            {
                try { pf.Dispose(); } catch { /* best-effort */ }
            }
            _collectionFiles.Clear();
        }

        _commitLock?.Dispose();
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
    /// Returns a copy of <paramref name="config"/> with all multi-file routing fields cleared,
    /// suitable for use when opening a standalone sub-file (index file or per-collection file)
    /// that should not itself spawn further sub-files.
    /// </summary>
    private static PageFileConfig AsStandaloneConfig(PageFileConfig config)
        => config with { WalPath = null, IndexFilePath = null, CollectionDataDirectory = null };
}
