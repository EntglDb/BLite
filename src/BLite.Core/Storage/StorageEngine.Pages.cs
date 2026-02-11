using BLite.Core.Transactions;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
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
            _ => new System.Collections.Concurrent.ConcurrentDictionary<uint, byte[]>());
        
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
    /// Gets the number of pages currently allocated in the page file.
    /// Useful for full database scans.
    /// </summary>
    public uint PageCount => _pageFile.NextPageId;
}
