using DocumentDb.Core.Storage;

namespace DocumentDb.Core.Transactions;

/// <summary>
/// Transaction manager coordinating ACID transactions.
/// Manages transaction lifecycle, WAL, and concurrency control.
/// </summary>
public sealed class TransactionManager : IDisposable
{
    private ulong _nextTransactionId;
    private readonly object _lock = new();
    private readonly Dictionary<ulong, Transaction> _activeTransactions;
    private readonly StorageEngine _storage;
    private bool _disposed;

    public TransactionManager(StorageEngine storage)
    {
        _nextTransactionId = 1;
        _activeTransactions = new Dictionary<ulong, Transaction>();
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

    /// <summary>
    /// Gets the storage engine
    /// </summary>
    public StorageEngine Storage => _storage;

    /// <summary>
    /// Begins a new transaction
    /// </summary>
    public Transaction BeginTransaction(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        lock (_lock)
        {
            var txnId = _nextTransactionId++;
            var transaction = new Transaction(txnId, _storage, isolationLevel);
            _activeTransactions[txnId] = transaction;
            return transaction;
        }
    }

    /// <summary>
    /// Commits a transaction using 2PC (Two-Phase Commit).
    /// Writes only to WAL for performance. CheckpointManager handles PageFile updates asynchronously.
    /// </summary>
    public void CommitTransaction(Transaction transaction)
    {
        lock (_lock)
        {
            // Phase 1: Prepare
            if (!transaction.Prepare())
                throw new InvalidOperationException("Transaction prepare failed");

            // Phase 2: Mark as committed (StorageEngine handles WAL commit record)
            // The transaction is now durable in WAL.
            // CheckpointManager will apply changes to PageFile asynchronously.
            transaction.MarkCommitted();

            _activeTransactions.Remove(transaction.TransactionId);
            
            // Note: CheckpointManager runs in background or can be triggered manually
            // No immediate I/O to PageFile = SQLite-style performance!
        }
    }

    /// <summary>
    /// Rolls back a transaction
    /// </summary>
    public void RollbackTransaction(Transaction transaction)
    {
        lock (_lock)
        {
            transaction.Rollback();
            _storage.WriteAbortRecord(transaction.TransactionId);
            _activeTransactions.Remove(transaction.TransactionId);
        }
    }

    /// <summary>
    /// Recovers from crash by replaying WAL
    /// </summary>
    public void Recover()
    {
        _storage.Recover();
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        lock (_lock)
        {
            // Rollback only ACTIVE transactions (not already committed/aborted)
            foreach (var txn in _activeTransactions.Values.ToList())
            {
                try 
                { 
                    // Only rollback if still active
                    if (txn.State == TransactionState.Active || 
                        txn.State == TransactionState.Preparing)
                    {
                        txn.Rollback();
                    }
                } 
                catch { /* Best effort */ }
            }
            _activeTransactions.Clear();
            
            _disposed = true;
        }

        GC.SuppressFinalize(this);
    }
}
