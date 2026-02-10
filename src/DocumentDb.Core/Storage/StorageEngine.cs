using System.Collections.Concurrent;
using DocumentDb.Core.Transactions;

namespace DocumentDb.Core.Storage;

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
public sealed class StorageEngine : IDisposable
{
    private readonly PageFile _pageFile;
    private readonly WriteAheadLog _wal;
    
    // WAL cache: TransactionId → (PageId → PageData)
    // Stores uncommitted writes for "Read Your Own Writes" isolation
    private readonly ConcurrentDictionary<ulong, ConcurrentDictionary<uint, byte[]>> _walCache;
    
    // WAL index cache: PageId → PageData (from latest committed transaction)
    // Lazily populated on first read after commit
    private readonly ConcurrentDictionary<uint, byte[]> _walIndex;
    
    // Global lock for commit/checkpoint synchronization
    private readonly object _commitLock = new();
    
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
        
        // Create and start checkpoint manager
        // _checkpointManager = new Transactions.CheckpointManager(this);
        // _checkpointManager.StartAutoCheckpoint();
    }

    /// <summary>
    /// Page size for this storage engine
    /// </summary>
    public int PageSize => _pageFile.PageSize;

    #region Transaction Management

    public Transaction BeginTransaction(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        lock (_commitLock)
        {
            var txnId = _nextTransactionId++;
            var transaction = new Transaction(txnId, this, isolationLevel);
            _activeTransactions[txnId] = transaction;
            return transaction;
        }
    }

    public void CommitTransaction(Transaction transaction)
    {
        lock (_commitLock)
        {
            if (!_activeTransactions.ContainsKey(transaction.TransactionId))
                throw new InvalidOperationException($"Transaction {transaction.TransactionId} is not active.");

            // 1. Prepare (Write to WAL)
            // In a fuller 2PC, this would be separate. Here we do it as part of commit.
            if (!PrepareTransaction(transaction.TransactionId))
                throw new IOException("Failed to write transaction to WAL");

            // 2. Commit (Write commit record, flush, move to cache)
            CommitTransaction(transaction.TransactionId);
            
            _activeTransactions.TryRemove(transaction.TransactionId, out _);
        }
    }

    public void RollbackTransaction(Transaction transaction)
    {
        RollbackTransaction(transaction.TransactionId);
        _activeTransactions.TryRemove(transaction.TransactionId, out _);
    }

    #endregion


    /// <summary>
    /// Reads a page with transaction isolation.
    /// 1. Check WAL cache for uncommitted writes (Read Your Own Writes)
    /// 2. Check WAL index for committed writes (lazy replay)
    /// 3. Read from PageFile (committed baseline)
    /// </summary>
    /// <param name="pageId">Page to read</param>
    /// <param name="transactionId">Optional transaction ID for isolation</param>
    /// <param name="destination">Buffer to write page data</param>
    public void ReadPage(uint pageId, ulong? transactionId, Span<byte> destination)
    {
        // 1. Check transaction-local WAL cache (Read Your Own Writes)
        // transactionId=0 or null means "no active transaction, read committed only"
        if (transactionId.HasValue && 
            transactionId.Value != 0 &&
            _walCache.TryGetValue(transactionId.Value, out var txnPages) &&
            txnPages.TryGetValue(pageId, out var uncommittedData))
        {
            var length = Math.Min(uncommittedData.Length, destination.Length);
            uncommittedData.AsSpan(0, length).CopyTo(destination);
            return;
        }
        
        // 2. Check WAL index (committed but not checkpointed)
        if (_walIndex.TryGetValue(pageId, out var committedData))
        {
            var length = Math.Min(committedData.Length, destination.Length);
            committedData.AsSpan(0, length).CopyTo(destination);
            return;
        }
        
        // 3. Read committed baseline from PageFile
        _pageFile.ReadPage(pageId, destination);
    }

    /// <summary>
    /// Writes a page within a transaction.
    /// Data goes to WAL cache immediately and becomes visible to that transaction only.
    /// Will be written to WAL on commit.
    /// </summary>
    /// <param name="pageId">Page to write</param>
    /// <param name="transactionId">Transaction ID owning this write</param>
    /// <param name="data">Page data</param>
    public void WritePage(uint pageId, ulong transactionId, ReadOnlySpan<byte> data)
    {
        if (transactionId == 0)
            throw new InvalidOperationException("Cannot write without a transaction (transactionId=0 is reserved)");
        
        // Get or create transaction-local cache
        var txnPages = _walCache.GetOrAdd(transactionId, 
            _ => new ConcurrentDictionary<uint, byte[]>());
        
        // Store defensive copy
        var copy = data.ToArray();
        txnPages[pageId] = copy;
    }


    /// <summary>
    /// Writes a page immediately to disk (non-transactional).
    /// Used for initialization and metadata updates outside of transactions.
    /// </summary>
    /// <param name="pageId">Page to write</param>
    /// <param name="data">Page data</param>
    public void WritePageImmediate(uint pageId, ReadOnlySpan<byte> data)
    {
        _pageFile.WritePage(pageId, data);
    }

    /// <summary>
    /// Prepares a transaction: writes all changes to WAL but doesn't commit yet.
    /// Part of 2-Phase Commit protocol.
    /// </summary>
    /// <param name="transactionId">Transaction ID</param>
    /// <param name="writeSet">All writes to record in WAL</param>
    /// <returns>True if preparation succeeded</returns>
    public bool PrepareTransaction(ulong transactionId)
    {
        try
        {
            _wal.WriteBeginRecord(transactionId);

            foreach (var walEntry in _walCache[transactionId])
            {
                _wal.WriteDataRecord(transactionId, walEntry.Key, walEntry.Value);
            }
            
            _wal.Flush(); // Ensure WAL is persisted
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Commits a transaction:
    /// 1. Writes all changes to WAL (for durability)
    /// 2. Writes commit record
    /// 3. Flushes WAL to disk
    /// 4. Moves pages from cache to WAL index (for future reads)
    /// 5. Clears WAL cache
    /// </summary>
    /// <param name="transactionId">Transaction to commit</param>
    /// <param name="writeSet">All writes performed in this transaction (unused, kept for compatibility)</param>
    public void CommitTransaction(ulong transactionId)
    {
        lock (_commitLock)
        {
            // Get ALL pages from WAL cache (includes both data and index pages)
            if (!_walCache.TryGetValue(transactionId, out var pages))
            {
                // No writes for this transaction, just write commit record
                _wal.WriteCommitRecord(transactionId);
                _wal.Flush();
                return;
            }
            
            // 1. Write all changes to WAL (from cache, not writeSet!)
            _wal.WriteBeginRecord(transactionId);
            
            foreach (var (pageId, data) in pages)
            {
                _wal.WriteDataRecord(transactionId, pageId, data);
            }
            
            // 2. Write commit record and flush
            _wal.WriteCommitRecord(transactionId);
            _wal.Flush(); // Durability: ensure WAL is on disk
            
            // 3. Move pages from cache to WAL index (for reads)
            _walCache.TryRemove(transactionId, out _);
            foreach (var kvp in pages)
            {
                _walIndex[kvp.Key] = kvp.Value;
            }

            // Auto-checkpoint if WAL grows too large
            if (_wal.GetCurrentSize() > MaxWalSize)
            {
                Checkpoint();
            }
        }
    }
    
    /// <summary>
    /// Marks a transaction as committed after WAL writes.
    /// Used for 2PC: after Prepare() writes to WAL, this finalizes the commit.
    /// </summary>
    /// <param name="transactionId">Transaction to mark committed</param>
    public void MarkTransactionCommitted(ulong transactionId)
    {
        lock (_commitLock)
        {
            _wal.WriteCommitRecord(transactionId);
            _wal.Flush();
            
            // Move from cache to WAL index
            if (_walCache.TryRemove(transactionId, out var pages))
            {
                foreach (var kvp in pages)
                {
                    _walIndex[kvp.Key] = kvp.Value;
                }
            }

            // Auto-checkpoint if WAL grows too large
            if (_wal.GetCurrentSize() > MaxWalSize)
            {
                Checkpoint();
            }
        }
    }

    /// <summary>
    /// Rolls back a transaction: discards all uncommitted changes.
    /// </summary>
    /// <param name="transactionId">Transaction to rollback</param>
    public void RollbackTransaction(ulong transactionId)
    {
        _walCache.TryRemove(transactionId, out _);
        _wal.WriteAbortRecord(transactionId);
    }

    /// <summary>
    /// Allocates a new page.
    /// </summary>
    /// <returns>Page ID of the allocated page</returns>
    public uint AllocatePage()
    {
        return _pageFile.AllocatePage();
    }

    /// <summary>
    /// Frees a page.
    /// </summary>
    /// <param name="pageId">Page to free</param>
    public void FreePage(uint pageId)
    {
        _pageFile.FreePage(pageId);
    }

    /// <summary>
    /// Gets the number of active transactions (diagnostics).
    /// </summary>
    public int ActiveTransactionCount => _walCache.Count;

    /// <summary>
    /// Gets the number of pages currently allocated in the page file.
    /// Useful for full database scans.
    /// </summary>
    public uint PageCount => _pageFile.NextPageId;

    /// <summary>
    /// Gets the current size of the WAL file.
    /// </summary>
    public long GetWalSize()
    {
        return _wal.GetCurrentSize();
    }
    
    /// <summary>
    /// Truncates the WAL file.
    /// Should only be called after a successful checkpoint.
    /// </summary>
    public void TruncateWal()
    {
        _wal.Truncate();
    }
    
    /// <summary>
    /// Flushes the WAL to disk.
    /// </summary>
    public void FlushWal()
    {
        _wal.Flush();
    }
    
    /// <summary>
    /// Performs a checkpoint: merges WAL into PageFile.
    /// Reads all committed transactions from WAL and applies them to PageFile.
    /// Then truncates the WAL.
    /// </summary>
    /// <summary>
    /// Performs a checkpoint: merges WAL into PageFile.
    /// Uses in-memory WAL index for efficiency and consistency.
    /// </summary>
    public void Checkpoint()
    {
        lock (_commitLock)
        {
            if (_walIndex.IsEmpty)
                return;

            // 1. Write all committed pages from index to PageFile
            foreach (var kvp in _walIndex)
            {
                _pageFile.WritePage(kvp.Key, kvp.Value);
            }
            
            // 2. Flush PageFile to ensure durability
            _pageFile.Flush();
            
            // 3. Clear in-memory WAL index (now persisted)
            _walIndex.Clear();
            
            // 4. Truncate WAL (all changes now in PageFile)
            _wal.Truncate();
        }
    }
    
    /// <summary>
    /// Recovers from crash by replaying WAL.
    /// Applies all committed transactions to PageFile, then truncates WAL.
    /// </summary>
    public void Recover()
    {
        lock (_commitLock)
        {
            // 1. Read WAL and identify committed transactions
            var records = _wal.ReadAll();
            var committedTxns = new HashSet<ulong>();
            var txnWrites = new Dictionary<ulong, List<(uint pageId, byte[] data)>>();
            
            foreach (var record in records)
            {
                if (record.Type == WalRecordType.Commit)
                    committedTxns.Add(record.TransactionId);
                else if (record.Type == WalRecordType.Write)
                {
                    if (!txnWrites.ContainsKey(record.TransactionId))
                        txnWrites[record.TransactionId] = new List<(uint, byte[])>();
                    
                    if (record.AfterImage != null)
                    {
                        txnWrites[record.TransactionId].Add((record.PageId, record.AfterImage));
                    }
                }
            }
            
            // 2. Apply committed transactions to PageFile
            foreach (var txnId in committedTxns)
            {
                if (!txnWrites.ContainsKey(txnId))
                    continue;
                    
                foreach (var (pageId, data) in txnWrites[txnId])
                {
                    _pageFile.WritePage(pageId, data);
                }
            }
            
            // 3. Flush PageFile to ensure durability
            _pageFile.Flush();
            
            // 4. Clear in-memory WAL index (redundant since we just recovered)
            _walIndex.Clear();
            
            // 5. Truncate WAL (all changes now in PageFile)
            _wal.Truncate();
        }
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
    }

    internal void WriteAbortRecord(ulong transactionId)
    {
        _wal.WriteAbortRecord(transactionId);
    }
}
