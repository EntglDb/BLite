using BLite.Core.Transactions;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
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
    /// Flushes pending memory-mapped (MMF) writes from the PageFile to the OS kernel buffer.
    /// Required for consistency when <see cref="WritePageImmediate"/> is followed by
    /// <see cref="ReadPageAsync"/>, which uses <see cref="System.IO.RandomAccess"/> and
    /// reads from the kernel buffer pool rather than the MMF view.
    /// </summary>
    public void FlushPageFile()
    {
        _pageFile.Flush();
    }
    
    /// <summary>
    /// Performs a checkpoint: merges WAL into PageFile.
    /// Uses in-memory WAL index for efficiency and consistency.
    /// </summary>
    /// <summary>
    /// Performs a checkpoint: merges WAL into PageFile.
    /// Uses in-memory WAL index for efficiency and consistency.
    /// </summary>
    public void Checkpoint()
    {
        _commitLock.Wait();
        try
        {
            CheckpointInternal();
        }
        finally
        {
            _commitLock.Release();
        }
    }

    private void CheckpointInternal()
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

    public async Task CheckpointAsync(CancellationToken ct = default)
    {
        await _commitLock.WaitAsync(ct);
        try
        {
            if (_walIndex.IsEmpty)
                return;

            // 1. Write all committed pages from index to PageFile
            // PageFile writes are sync (MMF), but that's fine as per plan (ValueTask strategy for MMF)
            foreach (var kvp in _walIndex)
            {
                _pageFile.WritePage(kvp.Key, kvp.Value);
            }
            
            // 2. Flush PageFile to ensure durability
            _pageFile.Flush();
            
            // 3. Clear in-memory WAL index (now persisted)
            _walIndex.Clear();
            
            // 4. Truncate WAL (all changes now in PageFile)
            // WAL truncation involves file resize and flush
            // TODO: Add TruncateAsync to WAL? For now Truncate is sync.
            _wal.Truncate();
        }
        finally
        {
            _commitLock.Release();
        }
    }
    
    /// <summary>
    /// Recovers from crash by replaying WAL.
    /// Applies all committed transactions to PageFile, then truncates WAL.
    /// </summary>
    public void Recover()
    {
        _commitLock.Wait();
        try
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
        finally
        {
            _commitLock.Release();
        }
    }
}
