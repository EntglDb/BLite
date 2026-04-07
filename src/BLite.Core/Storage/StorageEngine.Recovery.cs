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
    /// Flushes pending memory-mapped (MMF) writes from the PageFile to the OS kernel buffer.
    /// Required for consistency when <see cref="WritePageImmediate"/> is followed by
    /// <see cref="ReadPageAsync"/>, which uses <see cref="System.IO.RandomAccess"/> and
    /// reads from the kernel buffer pool rather than the MMF view.
    /// </summary>
    public void FlushPageFile()
    {
        _pageFile.Flush();
        _indexFile?.Flush();
        if (_collectionFiles != null)
        {
            foreach (var lazy in _collectionFiles.Values)
                if (lazy.IsValueCreated) lazy.Value.Flush();
        }
    }

    /// <summary>
    /// Async opportunistic checkpoint — NEVER blocks commits.
    /// See <see cref="Checkpoint"/> for the strategy.
    /// </summary>
    public async Task CheckpointAsync(CancellationToken ct = default)
    {
        if (_walIndex.IsEmpty) return;
        if (Interlocked.CompareExchange(ref _checkpointRunning, 1, 0) != 0) return;
        try
        {
            var snapshot = _walIndex.ToArray();
            if (snapshot.Length == 0) return;

            foreach (var kvp in snapshot)
                GetPageFile(kvp.Key, out var physId).WritePage(physId, kvp.Value);

            _pageFile.Flush();
            _indexFile?.Flush();
            if (_collectionFiles != null)
            {
                foreach (var lazy in _collectionFiles.Values)
                    if (lazy.IsValueCreated) lazy.Value.Flush();
            }

            // Try-acquire: if commits are flowing, skip cleanup and let WAL grow.
            if (!_commitLock.Wait(0)) return;
            try
            {
                foreach (var kvp in snapshot)
                {
                    if (_walIndex.TryGetValue(kvp.Key, out var current) && ReferenceEquals(current, kvp.Value))
                        _walIndex.TryRemove(kvp.Key, out _);
                }

                if (_walIndex.IsEmpty)
                    await _wal.TruncateAsync(ct);
            }
            finally
            {
                _commitLock.Release();
            }
        }
        finally
        {
            Interlocked.Exchange(ref _checkpointRunning, 0);
        }
    }

    /// <summary>
    /// Creates a consistent backup of this database to <paramref name="destinationDbPath"/>.
    /// <para>
    /// Strategy (fully safe under concurrent writes):
    /// <list type="number">
    ///   <item>Acquire the commit lock — no new transaction can commit while we work.</item>
    ///   <item>CheckpointAsync: merge all committed WAL entries into the PageFile and flush.</item>
    ///   <item>Copy the PageFile while holding the lock (no concurrent resize possible).</item>
    ///   <item>Release the lock — normal writes resume.</item>
    /// </list>
    /// WAL is not copied because, after the checkpoint, it contains only housekeeping
    /// data and the destination DB is consistent on its own.
    /// </para>
    /// <para>
    /// <strong>Multi-file mode limitation:</strong> when <see cref="PageFileConfig.IndexFilePath"/>
    /// or <see cref="PageFileConfig.CollectionDataDirectory"/> are configured, this method backs up
    /// only the <em>main</em> data file. Index pages and per-collection data pages reside in their
    /// own files and are not included in this backup. A consistent multi-file backup must copy all
    /// associated files while the commit lock is held; this is outside the scope of the current API.
    /// </para>
    /// </summary>
    /// <param name="destinationDbPath">
    /// Full path of the target .db file. Parent directory is created if missing.
    /// </param>
    public async Task BackupAsync(string destinationDbPath, CancellationToken ct = default)
    {
#if NET8_0_OR_GREATER
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationDbPath);
#else
        if (string.IsNullOrWhiteSpace(destinationDbPath))
            throw new ArgumentException("The value cannot be null, empty, or whitespace.", nameof(destinationDbPath));
#endif

        if (!await _commitLock.WaitAsync(_config.LockTimeout.WriteTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring commit lock (Backup).");
        try
        {
            // 1. CheckpointAsync: push committed WAL pages into the PageFile.
            await CheckpointAsync();

            // 2. Copy the PageFile. _pageFile.BackupAsync acquires PageFile._lock,
            //    which is safe because no other caller can hold _commitLock + PageFile._lock
            //    simultaneously in the reverse order.
            await _pageFile.BackupAsync(destinationDbPath, ct);
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
    public async Task RecoverAsync(CancellationToken ct = default)
    {
        if (!await _commitLock.WaitAsync(_config.LockTimeout.WriteTimeoutMs))
            throw new TimeoutException("Timed out acquiring commit lock (Recovery).");
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
            
            // 2. Apply committed transactions to the correct PageFile
            foreach (var txnId in committedTxns)
            {
                if (!txnWrites.ContainsKey(txnId))
                    continue;
                    
                foreach (var (pageId, data) in txnWrites[txnId])
                {
                    var targetFile = GetPageFile(pageId, out var physId);
                    targetFile.WritePage(physId, data);
                }
            }
            
            // 3. Flush all PageFiles to ensure durability
            await _pageFile.FlushAsync(ct);
            if (_indexFile != null)
                await _indexFile.FlushAsync(ct);
            if (_collectionFiles != null)
            {
                foreach (var lazy in _collectionFiles.Values)
                    if (lazy.IsValueCreated) lazy.Value.Flush();
            }
            
            // 4. Clear in-memory WAL index (redundant since we just recovered)
            _walIndex.Clear();
            
            // 5. Truncate WAL (all changes now in PageFile)
            await _wal.TruncateAsync(ct);
        }
        finally
        {
            _commitLock.Release();
        }
    }
}
