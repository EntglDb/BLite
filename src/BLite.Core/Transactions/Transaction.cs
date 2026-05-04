using BLite.Bson;
using BLite.Core.Storage;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core.Transactions;

/// <summary>
/// Represents a transaction with ACID properties.
/// Uses MVCC (Multi-Version Concurrency Control) for isolation.
/// </summary>
public sealed class Transaction : ITransaction
{
    private readonly ulong _transactionId;
    private readonly IsolationLevel _isolationLevel;
    private readonly DateTime _startTime;
    private readonly StorageEngine _storage;
    private readonly List<CDC.InternalChangeEvent> _pendingChanges = new();
    private TransactionState _state;
    private bool _disposed;

    /// <summary>
    /// Index of the reader slot in the <c>.wal-shm</c> file, or <c>-1</c> when no
    /// slot was acquired (single-process mode, or all slots were full at begin time).
    /// Released automatically in <see cref="Dispose"/>.
    /// </summary>
    internal int ShmReaderSlotIndex { get; set; } = -1;

    public Transaction(ulong transactionId, 
                       StorageEngine storage,
                       IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        _transactionId = transactionId;
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _isolationLevel = isolationLevel;
        _startTime = DateTime.UtcNow;
        _state = TransactionState.Active;
    }

    internal void AddChange(CDC.InternalChangeEvent change)
    {
        _pendingChanges.Add(change);
    }

    public ulong TransactionId => _transactionId;
    public TransactionState State => _state;
    public IsolationLevel IsolationLevel => _isolationLevel;
    public DateTime StartTime => _startTime;

    /// <summary>
    /// Adds a write operation to the transaction's write set.
    /// NOTE: Makes a defensive copy of the data to ensure memory safety.
    /// This allocation is necessary because the caller may return the buffer to a pool.
    /// </summary>
    public void AddWrite(WriteOperation operation)
    {
        if (_state != TransactionState.Active)
            throw new InvalidOperationException($"Cannot add writes to transaction in state {_state}");

        // Defensive copy: necessary to prevent use-after-return if caller uses pooled buffers
        byte[] ownedCopy = operation.NewValue.ToArray();
        // StorageEngine gestisce tutte le scritture transazionali
        _storage.WritePage(operation.PageId, _transactionId, ownedCopy);
    }

    /// <summary>
    /// Prepares the transaction for commit (2PC first phase)
    /// </summary>
    public async Task<bool> PrepareAsync()
    {
        if (_state != TransactionState.Active)
            return false;

        _state = TransactionState.Preparing;
        
        // StorageEngine handles WAL writes
        return await _storage.PrepareTransactionAsync(_transactionId);
    }

    public async ValueTask CommitAsync(CancellationToken ct = default)
    {
        if (_state != TransactionState.Preparing && _state != TransactionState.Active)
            throw new InvalidOperationException($"Cannot commit transaction in state {_state}");
        
        // StorageEngine handles WAL writes and buffer management
        await _storage.CommitTransactionAsync(_transactionId, ct);

        _state = TransactionState.Committed;

        // Publish CDC events after successful commit
        if (_pendingChanges.Count > 0 && _storage.Cdc != null)
        {
            foreach (var change in _pendingChanges)
            {
                _storage.Cdc.Publish(change);
            }
        }

        InvokeOnCommitHandlersSafely();
    }

    /// <summary>
    /// Fires after a successful commit. Useful for cleaning up per-transaction in-memory state.
    /// Each handler is invoked in a best-effort manner so that a failing handler does not
    /// prevent other handlers from running or make CommitAsync appear to fail.
    /// </summary>
    public event Action? OnCommit;

    private void InvokeOnCommitHandlersSafely()
    {
        var handlers = OnCommit;
        if (handlers == null)
            return;

        foreach (Action handler in handlers.GetInvocationList())
        {
            try
            {
                handler();
            }
            catch
            {
                // Best-effort post-commit cleanup/notification should not make
                // CommitAsync appear to fail after the transaction was committed.
            }
        }
    }

    /// <summary>
    /// Rolls back the transaction (discards all writes)
    /// </summary>
    public event Action? OnRollback;

    public async ValueTask RollbackAsync()
    {
        if (_state == TransactionState.Committed)
            throw new InvalidOperationException("Cannot rollback committed transaction");

        _pendingChanges.Clear();
        await _storage.RollbackTransactionAsync(_transactionId);
        _state = TransactionState.Aborted;
        
        InvokeOnRollbackHandlersSafely();
    }

    private void InvokeOnRollbackHandlersSafely()
    {
        var handlers = OnRollback;
        if (handlers == null)
            return;

        foreach (Action handler in handlers.GetInvocationList())
        {
            try
            {
                handler();
            }
            catch
            {
                // Best-effort post-rollback cleanup/notification should not prevent
                // other handlers from running or mask the original rollback outcome.
            }
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        if (_state == TransactionState.Active || _state == TransactionState.Preparing)
        {
            // Auto-rollback if not committed
            RollbackAsync().GetAwaiter().GetResult();
        }

        // Release the cross-process reader slot (Phase 5).
        // This must happen even if rollback throws, so it is placed after the rollback
        // path. The slot release is idempotent and best-effort.
        if (ShmReaderSlotIndex >= 0)
        {
            _storage.ReleaseReaderSlot(ShmReaderSlotIndex);
            ShmReaderSlotIndex = -1;
        }

        _disposed = true;
        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// Represents a write operation in a transaction.
/// Optimized to avoid allocations by using ReadOnlyMemory instead of byte[].
/// </summary>
public struct WriteOperation
{
    public ObjectId DocumentId { get; set; }
    public ReadOnlyMemory<byte> NewValue { get; set; }
    public uint PageId { get; set; }
    public OperationType Type { get; set; }

    public WriteOperation(ObjectId documentId, ReadOnlyMemory<byte> newValue, uint pageId, OperationType type)
    {
        DocumentId = documentId;
        NewValue = newValue;
        PageId = pageId;
        Type = type;
    }
    
    // Backward compatibility constructor
    public WriteOperation(ObjectId documentId, byte[] newValue, uint pageId, OperationType type)
    {
        DocumentId = documentId;
        NewValue = newValue;
        PageId = pageId;
        Type = type;
    }
}

/// <summary>
/// Type of write operation
/// </summary>
public enum OperationType : byte
{
    Insert = 1,
    Update = 2,
    Delete = 3,
    AllocatePage = 4
}
