using DocumentDb.Core.Storage;
using System.Diagnostics;

namespace DocumentDb.Core.Transactions;

/// <summary>
/// Manages checkpointing of committed pages to the main database file.
/// Implements SQLite-style lazy checkpointing to improve write performance.
/// </summary>
public sealed class CheckpointManager : IDisposable
{
    private readonly StorageEngine _storage;
    private readonly object _lock = new();
    private long _lastCheckpointPosition;
    private bool _disposed;
    private Timer? _autoCheckpointTimer;
    private readonly TimeSpan _autoCheckpointInterval;
    private readonly long _autoCheckpointThreshold; // bytes

    public CheckpointManager(
        StorageEngine storage,
        TimeSpan? autoCheckpointInterval = null,
        long autoCheckpointThreshold = 10 * 1024 * 1024) // 10MB default
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
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
            var walSize = _storage.GetWalSize();
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
    /// Performs a checkpoint from committed buffer to the main database file.
    /// Returns the number of pages checkpointed.
    /// </summary>
    public int Checkpoint(CheckpointMode mode = CheckpointMode.Passive)
    {
        lock (_lock)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CheckpointManager));

            // Note: We no longer track "committed pages count" since we use WAL-only architecture
            // The checkpoint now merges WAL into PageFile instead of flushing a buffer

            // Delegate to StorageEngine to merge WAL into PageFile
            _storage.Checkpoint();

            // Update checkpoint position
            _lastCheckpointPosition = _storage.GetWalSize();

            // Handle different checkpoint modes
            switch (mode)
            {
                case CheckpointMode.Truncate:
                case CheckpointMode.Restart:
                case CheckpointMode.Full:
                    // Truncate WAL after successful checkpoint
                    // (Full mode needs this for clean shutdown)
                    _storage.TruncateWal();
                    _lastCheckpointPosition = 0;
                    break;

                case CheckpointMode.Passive:
                    // Passive: best effort, already done
                    break;
            }

            // Return 0 since we don't track page count anymore
            // (WAL-based architecture doesn't maintain a committed buffer)
            return 0;
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
            catch (Exception ex)
            {
                // Best effort on dispose
                Debug.WriteLine(ex.Message);
            }

            _disposed = true;
        }

        GC.SuppressFinalize(this);
    }
}
