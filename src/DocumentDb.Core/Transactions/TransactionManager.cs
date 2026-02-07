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
    private readonly WriteAheadLog _wal;
    private readonly PageFile _pageFile;
    private bool _disposed;

    public TransactionManager(string walPath, PageFile pageFile)
    {
        _nextTransactionId = 1;
        _activeTransactions = new Dictionary<ulong, Transaction>();
        _wal = new WriteAheadLog(walPath);
        _pageFile = pageFile ?? throw new ArgumentNullException(nameof(pageFile));
    }

    /// <summary>
    /// Begins a new transaction
    /// </summary>
    public Transaction BeginTransaction(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        lock (_lock)
        {
            var txnId = _nextTransactionId++;
            var transaction = new Transaction(txnId, _pageFile, _wal, isolationLevel);
            _activeTransactions[txnId] = transaction;
            return transaction;
        }
    }

    /// <summary>
    /// Commits a transaction using 2PC (Two-Phase Commit)
    /// </summary>
    public void CommitTransaction(Transaction transaction)
    {
        lock (_lock)
        {
            // Phase 1: Prepare
            if (!transaction.Prepare())
                throw new InvalidOperationException("Transaction prepare failed");

            // Write to WAL before committing
            _wal.WriteCommitRecord(transaction.TransactionId);
            _wal.Flush();

            // Phase 2: Commit
            transaction.Commit();

            _activeTransactions.Remove(transaction.TransactionId);
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
            _wal.WriteAbortRecord(transaction.TransactionId);
            _activeTransactions.Remove(transaction.TransactionId);
        }
    }

    /// <summary>
    /// Recovers from crash by replaying WAL
    /// </summary>
    public void Recover()
    {
        var records = _wal.ReadAll();
        var committedTxns = new HashSet<ulong>();
        var txnWrites = new Dictionary<ulong, List<WalRecord>>();
        
        // First pass: identify committed transactions and collect writes
        foreach (var record in records)
        {
            if (record.Type == WalRecordType.Commit)
                committedTxns.Add(record.TransactionId);
            else if (record.Type == WalRecordType.Write)
            {
                if (!txnWrites.ContainsKey(record.TransactionId))
                    txnWrites[record.TransactionId] = new List<WalRecord>();
                txnWrites[record.TransactionId].Add(record);
            }
        }
        
        // Second pass: redo committed transactions
        foreach (var txnId in committedTxns)
        {
            if (!txnWrites.ContainsKey(txnId))
                continue;
                
            foreach (var write in txnWrites[txnId])
            {
                // Apply after-image to page
                if (write.AfterImage != null)
                    _pageFile.WritePage(write.PageId, write.AfterImage);
            }
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        lock (_lock)
        {
            // Rollback any active transactions
            foreach (var txn in _activeTransactions.Values.ToList())
            {
                try { txn.Rollback(); } catch { }
            }
            _activeTransactions.Clear();

            _wal.Dispose();
            _disposed = true;
        }

        GC.SuppressFinalize(this);
    }
}
