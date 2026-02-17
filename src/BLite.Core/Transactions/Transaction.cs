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
    public bool Prepare()
    {
        if (_state != TransactionState.Active)
            return false;

        _state = TransactionState.Preparing;
        
        // StorageEngine handles WAL writes
        return _storage.PrepareTransaction(_transactionId);
    }

    /// <summary>
    /// Commits the transaction.
    /// Writes to WAL for durability and moves data to committed buffer.
    /// Pages remain in memory until CheckpointManager writes them to disk.
    /// </summary>
    public void Commit()
    {
        if (_state != TransactionState.Preparing && _state != TransactionState.Active)
            throw new InvalidOperationException($"Cannot commit transaction in state {_state}");
        
        // StorageEngine handles WAL writes and buffer management
        _storage.CommitTransaction(_transactionId);

        _state = TransactionState.Committed;

        // Publish CDC events after successful commit
        if (_pendingChanges.Count > 0 && _storage.Cdc != null)
        {
            foreach (var change in _pendingChanges)
            {
                _storage.Cdc.Publish(change);
            }
        }
    }

    public async Task CommitAsync(CancellationToken ct = default)
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
    }

    /// <summary>
    /// Marks the transaction as committed without writing to PageFile.
    /// Used by TransactionManager with lazy checkpointing.
    /// </summary>
    internal void MarkCommitted()
    {
        if (_state != TransactionState.Preparing && _state != TransactionState.Active)
            throw new InvalidOperationException($"Cannot commit transaction in state {_state}");

        // StorageEngine marks transaction as committed and moves to committed buffer
        _storage.MarkTransactionCommitted(_transactionId);
        
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

        _pendingChanges.Clear();
        _storage.RollbackTransaction(_transactionId);
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
