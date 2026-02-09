using System.Collections.Concurrent;

namespace DocumentDb.Core.Storage;

/// <summary>
/// Manages in-memory page buffers for transactions.
/// Implements "Read Your Own Writes" (RYOW) isolation by maintaining
/// transaction-local page modifications until commit.
/// </summary>
public sealed class BufferManager
{
    private readonly PageFile _pageFile;
    
    // Transaction ID -> Page ID -> Page Data (uncommitted)
    private readonly ConcurrentDictionary<ulong, ConcurrentDictionary<uint, byte[]>> _transactionBuffers;
    
    // Page ID -> Page Data (committed but not yet checkpointed)
    private readonly ConcurrentDictionary<uint, byte[]> _committedBuffers;

    public BufferManager(PageFile pageFile)
    {
        _pageFile = pageFile ?? throw new ArgumentNullException(nameof(pageFile));
        _transactionBuffers = new ConcurrentDictionary<ulong, ConcurrentDictionary<uint, byte[]>>();
        _committedBuffers = new ConcurrentDictionary<uint, byte[]>();
    }

    /// <summary>
    /// Reads a page, checking transaction-local and committed buffers first.
    /// Implements "Read Your Own Writes" and "Read Committed" isolation.
    /// </summary>
    /// <param name="pageId">The page to read</param>
    /// <param name="transactionId">Optional transaction ID for isolation</param>
    /// <param name="destination">Buffer to write the page data into</param>
    public void ReadPage(uint pageId, ulong? transactionId, Span<byte> destination)
    {
        // 1. Check transaction-local buffer first (Read Your Own Writes)
        // Note: transactionId=0 or null means "no active transaction, read committed only"
        if (transactionId.HasValue && 
            transactionId.Value != 0 &&  // ? 0 means "no transaction"
            _transactionBuffers.TryGetValue(transactionId.Value, out var pageBuffer) &&
            pageBuffer.TryGetValue(pageId, out var txnData))
        {
            // Found in transaction's write set - use uncommitted data
            var length = Math.Min(txnData.Length, destination.Length);
            txnData.AsSpan(0, length).CopyTo(destination);
            return;
        }
        
        // 2. Check committed buffer (committed but not checkpointed)
        if (_committedBuffers.TryGetValue(pageId, out var committedData))
        {
            var length = Math.Min(committedData.Length, destination.Length);
            committedData.AsSpan(0, length).CopyTo(destination);
            return;
        }

        // 3. Not in any buffer - read from disk
        _pageFile.ReadPage(pageId, destination);
    }

    /// <summary>
    /// Writes a page to the transaction-local buffer.
    /// The write is not visible to other transactions until commit.
    /// </summary>
    /// <param name="pageId">The page to write</param>
    /// <param name="transactionId">Transaction ID owning this write</param>
    /// <param name="data">Page data to write</param>
    public void WritePage(uint pageId, ulong transactionId, ReadOnlySpan<byte> data)
    {
        var pageBuffer = _transactionBuffers.GetOrAdd(
            transactionId,
            _ => new ConcurrentDictionary<uint, byte[]>());

        // Make a defensive copy to prevent external modifications
        var copy = data.ToArray();
        pageBuffer[pageId] = copy;
    }

    /// <summary>
    /// Gets all modified pages for a transaction.
    /// Used during commit to apply changes to WAL.
    /// </summary>
    /// <param name="transactionId">Transaction ID</param>
    /// <returns>Dictionary of pageId -> pageData, or null if no modifications</returns>
    public IReadOnlyDictionary<uint, byte[]>? GetTransactionPages(ulong transactionId)
    {
        if (_transactionBuffers.TryGetValue(transactionId, out var pages))
        {
            return pages;
        }
        return null;
    }

    /// <summary>
    /// Moves transaction-local pages to committed buffer after commit.
    /// Pages remain in memory until checkpoint writes them to disk.
    /// </summary>
    /// <param name="transactionId">Transaction ID to commit</param>
    public void CommitTransaction(ulong transactionId)
    {
        if (_transactionBuffers.TryRemove(transactionId, out var pages))
        {
            // Move pages to committed buffer
            foreach (var kvp in pages)
            {
                _committedBuffers[kvp.Key] = kvp.Value;
            }
        }
    }

    /// <summary>
    /// Discards transaction-local buffers on rollback.
    /// </summary>
    /// <param name="transactionId">Transaction ID to roll back</param>
    public void RollbackTransaction(ulong transactionId)
    {
        _transactionBuffers.TryRemove(transactionId, out _);
    }
    
    /// <summary>
    /// Removes a page from the committed buffer after checkpoint.
    /// Called by CheckpointManager after writing to PageFile.
    /// </summary>
    /// <param name="pageId">Page that has been checkpointed</param>
    public void ClearCommittedPage(uint pageId)
    {
        _committedBuffers.TryRemove(pageId, out _);
    }
    
    /// <summary>
    /// Gets all committed pages that need checkpointing.
    /// </summary>
    public IReadOnlyDictionary<uint, byte[]> GetCommittedPages()
    {
        return _committedBuffers;
    }

    /// <summary>
    /// Gets the number of active transaction buffers (for diagnostics).
    /// </summary>
    public int ActiveTransactionCount => _transactionBuffers.Count;
    
    /// <summary>
    /// Gets the number of committed pages waiting for checkpoint (for diagnostics).
    /// </summary>
    public int CommittedPagesCount => _committedBuffers.Count;
}
