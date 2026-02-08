using DocumentDb.Core.Storage;

namespace DocumentDb.Core.Transactions;

/// <summary>
/// Manages checkpointing of WAL (Write-Ahead Log) to the main database file.
/// Implements SQLite-style lazy checkpointing to improve write performance.
/// </summary>
public sealed class CheckpointManager : IDisposable
{
    private readonly WriteAheadLog _wal;
    private readonly PageFile _pageFile;
    private readonly object _lock = new();
    private long _lastCheckpointPosition;
    private bool _disposed;
    private Timer? _autoCheckpointTimer;
    private readonly TimeSpan _autoCheckpointInterval;
    private readonly long _autoCheckpointThreshold; // bytes

    public CheckpointManager(
        WriteAheadLog wal, 
        PageFile pageFile,
        TimeSpan? autoCheckpointInterval = null,
        long autoCheckpointThreshold = 10 * 1024 * 1024) // 10MB default
    {
        _wal = wal ?? throw new ArgumentNullException(nameof(wal));
        _pageFile = pageFile ?? throw new ArgumentNullException(nameof(pageFile));
        _lastCheckpointPosition = 0;
        _autoCheckpointInterval = autoCheckpointInterval ?? TimeSpan.FromSeconds(30);
        _autoCheckpointThreshold = autoCheckpointThreshold;
    }

    /// <summary>
    /// Gets the current WAL file position that has been checkpointed
    /// </summary>
    public long LastCheckpointPosition
    {
        get
        {
            lock (_lock)
            {
                return _lastCheckpointPosition;
            }
        }
    }

    /// <summary>
    /// Starts automatic background checkpointing
    /// </summary>
    public void StartAutoCheckpoint()
    {
        lock (_lock)
        {
            if (_autoCheckpointTimer != null)
                return;

            _autoCheckpointTimer = new Timer(
                _ => AutoCheckpointCallback(),
                null,
                _autoCheckpointInterval,
                _autoCheckpointInterval);
        }
    }

    /// <summary>
    /// Stops automatic background checkpointing
    /// </summary>
    public void StopAutoCheckpoint()
    {
        lock (_lock)
        {
            _autoCheckpointTimer?.Dispose();
            _autoCheckpointTimer = null;
        }
    }

    private void AutoCheckpointCallback()
    {
        try
        {
            var walSize = _wal.GetCurrentSize();
            if (walSize - _lastCheckpointPosition > _autoCheckpointThreshold)
            {
                Checkpoint(CheckpointMode.Passive);
            }
        }
        catch
        {
            // Swallow exceptions in background thread
            // Could log here if logging infrastructure exists
        }
    }

    /// <summary>
    /// Performs a checkpoint from WAL to the main database file.
    /// Returns the number of pages checkpointed.
    /// </summary>
    public int Checkpoint(CheckpointMode mode = CheckpointMode.Passive)
    {
        lock (_lock)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CheckpointManager));

            // Read all committed transactions from WAL
            var records = _wal.ReadAll();
            if (records.Count == 0)
                return 0;

            // Group records by transaction
            var committedTransactions = new HashSet<ulong>();
            var transactionWrites = new Dictionary<ulong, List<WalRecord>>();
            
            // First pass: identify committed transactions
            foreach (var record in records)
            {
                if (record.Type == WalRecordType.Commit)
                {
                    committedTransactions.Add(record.TransactionId);
                }
                else if (record.Type == WalRecordType.Write)
                {
                    if (!transactionWrites.ContainsKey(record.TransactionId))
                        transactionWrites[record.TransactionId] = new List<WalRecord>();
                    
                    transactionWrites[record.TransactionId].Add(record);
                }
            }

            // Second pass: apply only committed transactions to PageFile
            int pagesCheckpointed = 0;
            var checkpointedPages = new HashSet<uint>(); // Track unique pages

            foreach (var txnId in committedTransactions)
            {
                if (!transactionWrites.ContainsKey(txnId))
                    continue;

                foreach (var write in transactionWrites[txnId])
                {
                    if (write.AfterImage != null)
                    {
                        // Apply write to PageFile
                        var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
                        try
                        {
                            // Read current page (may be needed for partial updates)
                            _pageFile.ReadPage(write.PageId, buffer);
                            
                            // Apply modification
                            var targetLength = Math.Min(write.AfterImage.Length, _pageFile.PageSize);
                            write.AfterImage.AsSpan(0, targetLength).CopyTo(buffer.AsSpan(0, targetLength));
                            
                            // Write back to PageFile
                            _pageFile.WritePage(write.PageId, buffer);
                            
                            checkpointedPages.Add(write.PageId);
                        }
                        finally
                        {
                            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
                        }
                    }
                }
            }

            pagesCheckpointed = checkpointedPages.Count;

            // Flush PageFile to ensure durability
            _pageFile.Flush();

            // Update checkpoint position
            _lastCheckpointPosition = _wal.GetCurrentSize();

            // Handle different checkpoint modes
            switch (mode)
            {
                case CheckpointMode.Truncate:
                case CheckpointMode.Restart:
                    // Truncate WAL after successful checkpoint
                    _wal.Truncate();
                    _lastCheckpointPosition = 0;
                    break;
                    
                case CheckpointMode.Full:
                    // Full mode: ensure everything is flushed
                    _wal.Flush();
                    break;
                    
                case CheckpointMode.Passive:
                    // Passive: best effort, already done
                    break;
            }

            return pagesCheckpointed;
        }
    }

    /// <summary>
    /// Forces an immediate full checkpoint and truncates the WAL.
    /// Use this for clean shutdown or after large batch operations.
    /// </summary>
    public void CheckpointAndTruncate()
    {
        Checkpoint(CheckpointMode.Truncate);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        lock (_lock)
        {
            StopAutoCheckpoint();
            
            // Final checkpoint before disposal
            try
            {
                Checkpoint(CheckpointMode.Full);
            }
            catch
            {
                // Best effort on dispose
            }
            
            _disposed = true;
        }

        GC.SuppressFinalize(this);
    }
}
