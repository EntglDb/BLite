using BLite.Core.Transactions;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    #region Transaction Management

    public Transaction BeginTransaction(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        var txnId = (ulong)Interlocked.Increment(ref _nextTransactionId);
        var transaction = new Transaction(txnId, this, isolationLevel);
        _activeTransactions[txnId] = transaction;
        return transaction;
    }

    public async Task CommitTransactionAsync(Transaction transaction, CancellationToken ct = default)
    {
        // Same reasoning as CommitTransactionAsync(Transaction) above: no outer lock,
        // no separate PrepareTransactionAsync call.

        if (!_activeTransactions.ContainsKey(transaction.TransactionId))
            throw new InvalidOperationException($"Transaction {transaction.TransactionId} is not active.");

        await CommitTransactionAsync(transaction.TransactionId, ct);

        _activeTransactions.TryRemove(transaction.TransactionId, out _);
    }

    public async Task RollbackTransactionAsync(Transaction transaction)
    {
        await RollbackTransactionAsync(transaction.TransactionId);
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
    public async Task<bool> PrepareTransactionAsync(ulong transactionId)
    {
        try
        {
            await _wal.WriteBeginRecordAsync(transactionId);

            foreach (var walEntry in _walCache[transactionId])
            {
                await _wal.WriteDataRecordAsync(transactionId, walEntry.Key, walEntry.Value);
            }

            await _wal.FlushAsync(); // Ensure WAL is persisted
            return true;
        }
        catch (Exception ex)
        {
            System.Diagnostics.Debug.WriteLine($"[BLite] PrepareTransaction({transactionId}) failed: {ex}");
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
    public async Task CommitTransactionAsync(ulong transactionId)
    {
        bool needsCheckpoint = false;

        await _commitLock.WaitAsync();
        try
        {
            // Get ALL pages from WAL cache (includes both data and index pages)
            if (!_walCache.TryGetValue(transactionId, out var pages))
            {
                // No writes for this transaction, just write commit record
                await _wal.WriteCommitRecordAsync(transactionId);
                await _wal.FlushAsync();
                return;
            }

            // 1. Write all changes to WAL (from cache, not writeSet!)
            await _wal.WriteBeginRecordAsync(transactionId);
            
            foreach (var (pageId, data) in pages)
            {
                await _wal.WriteDataRecordAsync(transactionId, pageId, data);
            }

            // 2. Write commit record and flush
            await _wal.WriteCommitRecordAsync(transactionId);
            await _wal.FlushAsync(); // Durability: ensure WAL is on disk
            
            // 3. Move pages from cache to WAL index (for reads)
            _walCache.TryRemove(transactionId, out _);
            foreach (var kvp in pages)
            {
                _walIndex[kvp.Key] = kvp.Value;
            }

            // Check if checkpoint is needed, but defer it until after releasing the lock
            needsCheckpoint = _wal.GetCurrentSize() > MaxWalSize;
        }
        finally
        {
            _commitLock.Release();
        }

        // Run checkpoint outside _commitLock so other commits aren't blocked.
        // CheckpointAsync() acquires _commitLock internally for the actual I/O.
        if (needsCheckpoint)
        {
            await CheckpointAsync();
        }
    }

    public async Task CommitTransactionAsync(ulong transactionId, CancellationToken ct = default)
    {
        // Group commit path: post to the background writer and await its TCS.
        // The writer batches this commit with any other pending ones, issues one
        // WAL flush for the entire batch, then signals all waiters.
        _walCache.TryGetValue(transactionId, out var pages);
        var pending = new PendingCommit(transactionId, pages);
        await _commitChannel.Writer.WriteAsync(pending, ct).ConfigureAwait(false);
        await pending.Completion.Task.ConfigureAwait(false);
    }
    
    /// <summary>
    /// Marks a transaction as committed after WAL writes.
    /// Used for 2PC: after PrepareAsync() writes to WAL, this finalizes the commit.
    /// </summary>
    /// <param name="transactionId">Transaction to mark committed</param>
    public async Task MarkTransactionCommittedAsync(ulong transactionId)
    {
        bool needsCheckpoint = false;

        _commitLock.Wait();
        try
        {
            await _wal.WriteCommitRecordAsync(transactionId);
            await _wal.FlushAsync();
            
            // Move from cache to WAL index
            if (_walCache.TryRemove(transactionId, out var pages))
            {
                foreach (var kvp in pages)
                {
                    _walIndex[kvp.Key] = kvp.Value;
                }
            }

            // Check if checkpoint is needed, but defer it until after releasing the lock
            needsCheckpoint = _wal.GetCurrentSize() > MaxWalSize;
        }
        finally
        {
            _commitLock.Release();
        }

        if (needsCheckpoint)
        {
            await CheckpointAsync();
        }
    }

    /// <summary>
    /// Rolls back a transaction: discards all uncommitted changes.
    /// </summary>
    /// <param name="transactionId">Transaction to rollback</param>
    public async Task RollbackTransactionAsync(ulong transactionId)
    {
        _walCache.TryRemove(transactionId, out _);
        await _wal.WriteAbortRecordAsync(transactionId);
    }

    /// <summary>
    /// Gets the number of active transactions (diagnostics).
    /// </summary>
    public int ActiveTransactionCount => _walCache.Count;
}
