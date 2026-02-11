using BLite.Core.Transactions;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    #region Transaction Management

    public Transaction BeginTransaction(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        _commitLock.Wait();
        try
        {
            var txnId = _nextTransactionId++;
            var transaction = new Transaction(txnId, this, isolationLevel);
            _activeTransactions[txnId] = transaction;
            return transaction;
        }
        finally
        {
            _commitLock.Release();
        }
    }

    public async Task<Transaction> BeginTransactionAsync(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted, CancellationToken ct = default)
    {
        await _commitLock.WaitAsync(ct);
        try
        {
            var txnId = _nextTransactionId++;
            var transaction = new Transaction(txnId, this, isolationLevel);
            _activeTransactions[txnId] = transaction;
            return transaction;
        }
        finally
        {
            _commitLock.Release();
        }
    }

    public void CommitTransaction(Transaction transaction)
    {
        _commitLock.Wait();
        try
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
        finally
        {
            _commitLock.Release();
        }
    }

    public async Task CommitTransactionAsync(Transaction transaction, CancellationToken ct = default)
    {
        await _commitLock.WaitAsync(ct);
        try
        {
            if (!_activeTransactions.ContainsKey(transaction.TransactionId))
                throw new InvalidOperationException($"Transaction {transaction.TransactionId} is not active.");

            // 1. Prepare (Write to WAL)
            if (!await PrepareTransactionAsync(transaction.TransactionId, ct))
                throw new IOException("Failed to write transaction to WAL");

            // 2. Commit (Write commit record, flush, move to cache)
            await CommitTransactionAsync(transaction.TransactionId, ct);
            
            _activeTransactions.TryRemove(transaction.TransactionId, out _);
        }
        finally
        {
            _commitLock.Release();
        }
    }

    public void RollbackTransaction(Transaction transaction)
    {
        RollbackTransaction(transaction.TransactionId);
        _activeTransactions.TryRemove(transaction.TransactionId, out _);
    }
    
    // Rollback doesn't usually require async logic unless logging abort record is async, 
    // but for consistency we might consider it. For now, sync is fine as it's not the happy path bottleneck.

    #endregion

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
            // TODO: Log error?
            return false;
        }
    }

    public async Task<bool> PrepareTransactionAsync(ulong transactionId, CancellationToken ct = default)
    {
        try
        {
            await _wal.WriteBeginRecordAsync(transactionId, ct);

            if (_walCache.TryGetValue(transactionId, out var changes))
            {
                foreach (var walEntry in changes)
                {
                    await _wal.WriteDataRecordAsync(transactionId, walEntry.Key, walEntry.Value, ct);
                }
            }
            
            await _wal.FlushAsync(ct); // Ensure WAL is persisted
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
        _commitLock.Wait();
        try
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
                CheckpointInternal();
            }
        }
        finally
        {
            _commitLock.Release();
        }
    }

    public async Task CommitTransactionAsync(ulong transactionId, CancellationToken ct = default)
    {
        await _commitLock.WaitAsync(ct);
        try
        {
            // Get ALL pages from WAL cache (includes both data and index pages)
            if (!_walCache.TryGetValue(transactionId, out var pages))
            {
                // No writes for this transaction, just write commit record
                await _wal.WriteCommitRecordAsync(transactionId, ct);
                await _wal.FlushAsync(ct);
                return;
            }
            
            // 1. Write all changes to WAL (from cache, not writeSet!)
            await _wal.WriteBeginRecordAsync(transactionId, ct);
            
            foreach (var (pageId, data) in pages)
            {
                await _wal.WriteDataRecordAsync(transactionId, pageId, data, ct);
            }
            
            // 2. Write commit record and flush
            await _wal.WriteCommitRecordAsync(transactionId, ct);
            await _wal.FlushAsync(ct); // Durability: ensure WAL is on disk
            
            // 3. Move pages from cache to WAL index (for reads)
            _walCache.TryRemove(transactionId, out _);
            foreach (var kvp in pages)
            {
                _walIndex[kvp.Key] = kvp.Value;
            }

            // Auto-checkpoint if WAL grows too large
            if (_wal.GetCurrentSize() > MaxWalSize)
            {
                // Checkpoint might be sync or async. For now sync inside the lock is "safe" but blocking.
                // Ideally this should be async too.
                CheckpointInternal(); 
            }
        }
        finally
        {
            _commitLock.Release();
        }
    }
    
    /// <summary>
    /// Marks a transaction as committed after WAL writes.
    /// Used for 2PC: after Prepare() writes to WAL, this finalizes the commit.
    /// </summary>
    /// <param name="transactionId">Transaction to mark committed</param>
    public void MarkTransactionCommitted(ulong transactionId)
    {
        _commitLock.Wait();
        try
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
                CheckpointInternal();
            }
        }
        finally
        {
            _commitLock.Release();
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

    internal void WriteAbortRecord(ulong transactionId)
    {
        _wal.WriteAbortRecord(transactionId);
    }

    /// <summary>
    /// Gets the number of active transactions (diagnostics).
    /// </summary>
    public int ActiveTransactionCount => _walCache.Count;
}
