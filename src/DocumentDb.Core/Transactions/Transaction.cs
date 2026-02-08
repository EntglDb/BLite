using DocumentDb.Bson;
using DocumentDb.Core.Storage;

namespace DocumentDb.Core.Transactions;

/// <summary>
/// Represents a transaction with ACID properties.
/// Uses MVCC (Multi-Version Concurrency Control) for isolation.
/// </summary>
public sealed class Transaction : ITransaction
{
    private readonly ulong _transactionId;
    private readonly IsolationLevel _isolationLevel;
    private readonly DateTime _startTime;
    private readonly PageFile _pageFile;
    private readonly WriteAheadLog _wal;
    private TransactionState _state;
    private readonly Dictionary<uint, WriteOperation> _writeSet;
    private bool _disposed;

    public Transaction(ulong transactionId, 
                       PageFile pageFile, 
                       WriteAheadLog wal,
                       IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        _transactionId = transactionId;
        _pageFile = pageFile ?? throw new ArgumentNullException(nameof(pageFile));
        _wal = wal ?? throw new ArgumentNullException(nameof(wal));
        _isolationLevel = isolationLevel;
        _startTime = DateTime.UtcNow;
        _state = TransactionState.Active;
        _writeSet = new Dictionary<uint, WriteOperation>();
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
        // This is the primary remaining allocation, but it's required for correctness
        byte[] ownedCopy = operation.NewValue.ToArray();
        
        var ownedOperation = new WriteOperation(
            operation.DocumentId,
            ownedCopy,
            operation.PageId,
            operation.Type
        );

        // Coalesce writes: if we already have a write for this page, replace it
        _writeSet[operation.PageId] = ownedOperation;
    }

    /// <summary>
    /// Gets a page from the transaction's write cache (if modified) or null.
    /// Enables Read-Your-Own-Writes.
    /// </summary>
    public byte[]? GetPage(uint pageId)
    {
        if (_writeSet.TryGetValue(pageId, out var op))
        {
            // Return the underlying array if it's backed by one, otherwise null
            // This maintains backward compatibility
            if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(op.NewValue, out var segment))
            {
                if (segment.Offset == 0 && segment.Count == segment.Array!.Length)
                    return segment.Array;
                
                // If it's a slice, we need to copy (rare case)
                return op.NewValue.ToArray();
            }
        }
        return null;
    }

    /// <summary>
    /// Prepares the transaction for commit (2PC first phase)
    /// </summary>
    public bool Prepare()
    {
        if (_state != TransactionState.Active)
            return false;

        _state = TransactionState.Preparing;
        
        // Write Begin record to WAL
        _wal.WriteBeginRecord(_transactionId);
        
        // Write all data modifications to WAL
        foreach (var write in _writeSet.Values)
        {
            // Optimization: We only log the AfterImage (REDO log).
            // UNDO is handled by discarding memory state, not by WAL rollback.
            // Crash recovery only needs REDO to restore committed transactions.
            _wal.WriteDataRecord(_transactionId, write.PageId, write.NewValue.Span);
        }
        
        _wal.Flush(); // Ensure WAL is on disk
        return true;
    }

    /// <summary>
    /// Commits the transaction (marks as committed in WAL).
    /// NOTE: This no longer writes to PageFile directly!
    /// CheckpointManager handles PageFile writes asynchronously.
    /// </summary>
    public void Commit()
    {
        if (_state != TransactionState.Preparing && _state != TransactionState.Active)
            throw new InvalidOperationException($"Cannot commit transaction in state {_state}");

        // Apply all writes to actual pages
        foreach (var write in _writeSet.Values)
        {
            var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
            try
            {
                // Read current page
                _pageFile.ReadPage(write.PageId, buffer);
                
                // Apply modification (copy new data into buffer)
                var targetLength = Math.Min(write.NewValue.Length, _pageFile.PageSize);
                write.NewValue.Span.Slice(0, targetLength).CopyTo(buffer.AsSpan(0, targetLength));
                
                // Write back
                _pageFile.WritePage(write.PageId, buffer);
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        _state = TransactionState.Committed;
    }

    /// <summary>
    /// Marks the transaction as committed without writing to PageFile.
    /// Used by TransactionManager with lazy checkpointing.
    /// </summary>
    internal void MarkCommitted()
    {
        if (_state != TransactionState.Preparing && _state != TransactionState.Active)
            throw new InvalidOperationException($"Cannot commit transaction in state {_state}");

        // Simply mark as committed - no PageFile I/O!
        // The WAL already contains all changes, and CheckpointManager will apply them later.
        _state = TransactionState.Committed;
    }

    /// <summary>
    /// Rolls back the transaction (discards all writes)
    /// </summary>
    public event Action? OnRollback;

    public void Rollback()
    {
        if (_state == TransactionState.Committed)
            throw new InvalidOperationException("Cannot rollback committed transaction");

        _writeSet.Clear();
        _state = TransactionState.Aborted;
        
        OnRollback?.Invoke();
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        if (_state == TransactionState.Active || _state == TransactionState.Preparing)
        {
            // Auto-rollback if not committed
            Rollback();
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
