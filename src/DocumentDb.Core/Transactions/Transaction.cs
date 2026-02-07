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
    private readonly List<WriteOperation> _writeSet;
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
        _writeSet = new List<WriteOperation>();
    }

    public ulong TransactionId => _transactionId;
    public TransactionState State => _state;
    public IsolationLevel IsolationLevel => _isolationLevel;
    public DateTime StartTime => _startTime;

    /// <summary>
    /// Adds a write operation to the transaction's write set
    /// </summary>
    public void AddWrite(WriteOperation operation)
    {
        if (_state != TransactionState.Active)
            throw new InvalidOperationException($"Cannot add writes to transaction in state {_state}");

        _writeSet.Add(operation);
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
        foreach (var write in _writeSet)
        {
            // Read current page state as "before image"
            var beforeImage = System.Buffers.ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
            try
            {
                _pageFile.ReadPage(write.PageId, beforeImage);
                
                // After image is the new value
                _wal.WriteDataRecord(_transactionId, write.PageId, 
                    beforeImage.AsSpan(0, _pageFile.PageSize), 
                    write.NewValue);
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(beforeImage);
            }
        }
        
        _wal.Flush(); // Ensure WAL is on disk
        return true;
    }

    /// <summary>
    /// Commits the transaction (makes all writes visible)
    /// </summary>
    public void Commit()
    {
        if (_state != TransactionState.Preparing && _state != TransactionState.Active)
            throw new InvalidOperationException($"Cannot commit transaction in state {_state}");

        // Apply all writes to actual pages
        foreach (var write in _writeSet)
        {
            var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
            try
            {
                // Read current page
                _pageFile.ReadPage(write.PageId, buffer);
                
                // Apply modification (copy new data into buffer)
                var targetLength = Math.Min(write.NewValue.Length, _pageFile.PageSize);
                write.NewValue.AsSpan(0, targetLength).CopyTo(buffer.AsSpan(0, targetLength));
                
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
    /// Rolls back the transaction (discards all writes)
    /// </summary>
    public void Rollback()
    {
        if (_state == TransactionState.Committed)
            throw new InvalidOperationException("Cannot rollback committed transaction");

        _writeSet.Clear();
        _state = TransactionState.Aborted;
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
/// Implemented as struct for efficiency.
/// </summary>
public struct WriteOperation
{
    public ObjectId DocumentId { get; set; }
    public byte[] NewValue { get; set; }
    public uint PageId { get; set; }
    public OperationType Type { get; set; }

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
    Delete = 3
}
