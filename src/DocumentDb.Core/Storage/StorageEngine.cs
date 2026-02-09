using DocumentDb.Core.Transactions;

namespace DocumentDb.Core.Storage;

/// <summary>
/// Central storage engine that manages page reads/writes.
/// Acts as an intermediary between components (BTreeIndex, DocumentCollection)
/// and the underlying storage layers (BufferManager, WAL, PageFile).
/// 
/// Provides transparent caching and transaction isolation without exposing
/// WAL or PageFile details to upper layers.
/// </summary>
public sealed class StorageEngine : IDisposable
{
    private readonly PageFile _pageFile;
    private readonly WriteAheadLog _wal;
    private readonly BufferManager _bufferManager;

    public StorageEngine(PageFile pageFile, WriteAheadLog wal)
    {
        _pageFile = pageFile ?? throw new ArgumentNullException(nameof(pageFile));
        _wal = wal ?? throw new ArgumentNullException(nameof(wal));
        _bufferManager = new BufferManager(_pageFile);
    }

    /// <summary>
    /// Page size for this storage engine
    /// </summary>
    public int PageSize => _pageFile.PageSize;

    /// <summary>
    /// Reads a page with transaction isolation.
    /// Automatically checks transaction-local changes, committed buffer, then disk.
    /// </summary>
    /// <param name="pageId">Page to read</param>
    /// <param name="transactionId">Optional transaction ID for isolation</param>
    /// <param name="destination">Buffer to write page data</param>
    public void ReadPage(uint pageId, ulong? transactionId, Span<byte> destination)
    {
        // BufferManager handles the hierarchy:
        // 1. Transaction-local (if txnId provided)
        // 2. Committed buffer
        // 3. PageFile
        _bufferManager.ReadPage(pageId, transactionId, destination);
    }

    /// <summary>
    /// Writes a page within a transaction.
    /// Data is buffered in-memory and not visible to other transactions until commit.
    /// </summary>
    /// <param name="pageId">Page to write</param>
    /// <param name="transactionId">Transaction ID owning this write</param>
    /// <param name="data">Page data</param>
    public void WritePage(uint pageId, ulong transactionId, ReadOnlySpan<byte> data)
    {
        _bufferManager.WritePage(pageId, transactionId, data);
    }

    /// <summary>
    /// Writes a page immediately to disk (non-transactional).
    /// Used for initialization and metadata updates.
    /// </summary>
    /// <param name="pageId">Page to write</param>
    /// <param name="data">Page data</param>
    public void WritePageImmediate(uint pageId, ReadOnlySpan<byte> data)
    {
        _pageFile.WritePage(pageId, data);
    }

    /// <summary>
    /// Prepares a transaction: writes to WAL but doesn't commit yet.
    /// Returns true if preparation was successful.
    /// </summary>
    /// <param name="transactionId">Transaction ID</param>
    /// <param name="writeSet">All writes to record in WAL</param>
    /// <returns>True if preparation succeeded</returns>
    public bool PrepareTransaction(ulong transactionId, IEnumerable<(uint pageId, ReadOnlyMemory<byte> data)> writeSet)
    {
        try
        {
            // Write Begin record to WAL
            _wal.WriteBeginRecord(transactionId);
            
            // Write all data modifications to WAL
            foreach (var (pageId, data) in writeSet)
            {
                _wal.WriteDataRecord(transactionId, pageId, data.Span);
            }
            
            _wal.Flush(); // Ensure WAL is on disk
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Commits a transaction.
    /// Writes commit record to WAL and moves data to committed buffer.
    /// </summary>
    /// <param name="transactionId">Transaction to commit</param>
    /// <param name="writeSet">All writes performed in this transaction</param>
    public void CommitTransaction(ulong transactionId, IEnumerable<(uint pageId, ReadOnlyMemory<byte> data)> writeSet)
    {
        // 1. Write to WAL for durability
        _wal.WriteBeginRecord(transactionId);
        
        foreach (var (pageId, data) in writeSet)
        {
            _wal.WriteDataRecord(transactionId, pageId, data.Span);
        }
        
        _wal.WriteCommitRecord(transactionId);
        _wal.Flush(); // Ensure durability

        // 2. Move from transaction-local to committed buffer
        // Data is now visible to all, but not yet on disk
        _bufferManager.CommitTransaction(transactionId);
    }
    
    /// <summary>
    /// Marks a transaction as committed after WAL writes.
    /// Used for 2PC: after Prepare() writes to WAL, this finalizes the commit.
    /// </summary>
    /// <param name="transactionId">Transaction to mark committed</param>
    public void MarkTransactionCommitted(ulong transactionId)
    {
        // Write commit record to WAL
        _wal.WriteCommitRecord(transactionId);
        _wal.Flush();
        
        // Move from transaction-local to committed buffer
        _bufferManager.CommitTransaction(transactionId);
    }

    /// <summary>
    /// Rolls back a transaction, discarding all writes.
    /// </summary>
    /// <param name="transactionId">Transaction to rollback</param>
    public void RollbackTransaction(ulong transactionId)
    {
        _bufferManager.RollbackTransaction(transactionId);
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
    /// Gets the number of committed pages waiting for checkpoint (diagnostics).
    /// </summary>
    public int CommittedPagesCount => _bufferManager.CommittedPagesCount;

    /// <summary>
    /// Gets the number of active transactions (diagnostics).
    /// </summary>
    public int ActiveTransactionCount => _bufferManager.ActiveTransactionCount;

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
    /// Performs a checkpoint: writes committed pages to disk and clears committed buffer.
    /// Called by CheckpointManager.
    /// </summary>
    public void Checkpoint()
    {
        var committedPages = _bufferManager.GetCommittedPages();
        
        foreach (var kvp in committedPages)
        {
            _pageFile.WritePage(kvp.Key, kvp.Value);
            _bufferManager.ClearCommittedPage(kvp.Key);
        }
        
        _pageFile.Flush();
    }
    
    /// <summary>
    /// Recovers from crash by replaying WAL.
    /// Reads WAL records and applies committed transactions to PageFile.
    /// </summary>
    public void Recover()
    {
        var records = _wal.ReadAll();
        var committedTxns = new HashSet<ulong>();
        var txnWrites = new Dictionary<ulong, List<(uint pageId, byte[] data)>>();
        
        // First pass: identify committed transactions and collect writes
        foreach (var record in records)
        {
            if (record.Type == WalRecordType.Commit)
                committedTxns.Add(record.TransactionId);
            else if (record.Type == WalRecordType.Write)
            {
                if (!txnWrites.ContainsKey(record.TransactionId))
                    txnWrites[record.TransactionId] = new List<(uint, byte[])>();
                
                if (record.AfterImage != null) // Null check
                {
                    txnWrites[record.TransactionId].Add((record.PageId, record.AfterImage));
                }
            }
        }
        
        // Second pass: redo committed transactions
        foreach (var txnId in committedTxns)
        {
            if (!txnWrites.ContainsKey(txnId))
                continue;
                
            foreach (var (pageId, data) in txnWrites[txnId])
            {
                _pageFile.WritePage(pageId, data);
            }
        }
        
        _pageFile.Flush();
    }
    
    /// <summary>
    /// Disposes the storage engine and closes WAL.
    /// </summary>
    public void Dispose()
    {
        // Perform final checkpoint to write all committed pages
        try
        {
            Checkpoint();
        }
        catch
        {
            // Best effort
        }
        
        // Close WAL
        _wal?.Dispose();
    }

    internal void WriteAbortRecord(ulong transactionId)
    {
        _wal.WriteAbortRecord(transactionId);
    }
}
