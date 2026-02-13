using System.Collections.Concurrent;
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
    private readonly PageFile _pageFile;
    private readonly WriteAheadLog _wal;
    private CDC.ChangeStreamDispatcher? _cdc;
    
    // WAL cache: TransactionId → (PageId → PageData)
    // Stores uncommitted writes for "Read Your Own Writes" isolation
    private readonly ConcurrentDictionary<ulong, ConcurrentDictionary<uint, byte[]>> _walCache;
    
    // WAL index cache: PageId → PageData (from latest committed transaction)
    // Lazily populated on first read after commit
    private readonly ConcurrentDictionary<uint, byte[]> _walIndex;
    
    // Global lock for commit/checkpoint synchronization
    private readonly SemaphoreSlim _commitLock = new(1, 1);
    
    // Transaction Management
    private readonly ConcurrentDictionary<ulong, Transaction> _activeTransactions;
    private ulong _nextTransactionId;

    private const long MaxWalSize = 4 * 1024 * 1024; // 4MB

    public StorageEngine(string databasePath, PageFileConfig config)
    {

        // Auto-derive WAL path
        var walPath = Path.ChangeExtension(databasePath, ".wal");

        // Initialize storage infrastructure
        _pageFile = new PageFile(databasePath, config);
        _pageFile.Open();

        _wal = new WriteAheadLog(walPath);
        _walCache = new ConcurrentDictionary<ulong, ConcurrentDictionary<uint, byte[]>>();
        _walIndex = new ConcurrentDictionary<uint, byte[]>();
        _activeTransactions = new ConcurrentDictionary<ulong, Transaction>();
        _nextTransactionId = 1;
        
        // Recover from WAL if exists (crash recovery or resume after close)
        // This replays any committed transactions not yet checkpointed
        if (_wal.GetCurrentSize() > 0)
        {
            Recover();
        }
        
        InitializeDictionary();
        
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
        // 1. Rollback any active transactions
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

        // 2. Close WAL and PageFile
        _wal?.Dispose();
        _pageFile?.Dispose();
        _commitLock?.Dispose();
    }

    internal void RegisterCdc(CDC.ChangeStreamDispatcher cdc)
    {
        _cdc = cdc;
    }

    internal CDC.ChangeStreamDispatcher? Cdc => _cdc;
}
