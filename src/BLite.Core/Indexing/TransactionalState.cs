using BLite.Core.Storage;

namespace BLite.Core.Indexing;

/// <summary>
/// Holds a piece of index state (a root page id, an HNSW entry point, ...) whose changes only
/// exist in the private page cache of the transaction that made them. Until that transaction
/// commits, only it sees the new value; every other reader keeps seeing the last committed one.
/// On commit the value is published (and <c>onCommitted</c> is invoked, e.g. to persist the new
/// root in the collection metadata); on rollback it is discarded.
/// </summary>
/// <remarks>
/// Only one transaction at a time can own a pending change. Index pages are not locked, so a
/// second transaction changing the same state supersedes the first one's pending value (concurrent
/// root splits from two transactions on the same index remain unsupported).
/// </remarks>
internal sealed class TransactionalState<T>
{
    private readonly StorageEngine _storage;
    private readonly Action<T>? _onCommitted;
    private readonly object _lock = new();
    private ulong _ownerTxnId;
    private T _committed;
    private T _pending;

    public TransactionalState(StorageEngine storage, T initial, Action<T>? onCommitted = null)
    {
        _storage = storage;
        _onCommitted = onCommitted;
        _committed = initial;
        _pending = initial;
    }

    /// <summary>The last committed value.</summary>
    public T Committed
    {
        get { lock (_lock) return _committed; }
    }

    /// <summary>The value <paramref name="transactionId"/> must use: its own pending value, else the committed one.</summary>
    public T For(ulong transactionId)
    {
        lock (_lock)
            return _ownerTxnId != 0 && _ownerTxnId == transactionId ? _pending : _committed;
    }

    /// <summary>
    /// Sets the value on behalf of <paramref name="transactionId"/>. Without an active transaction
    /// (id 0, or already finished) the value is committed immediately.
    /// </summary>
    /// <param name="onRolledBack">Optional hook run when the owning transaction rolls back.</param>
    public void Set(T value, ulong transactionId, Action? onRolledBack = null)
    {
        if (transactionId != 0)
        {
            lock (_lock)
            {
                if (_ownerTxnId == transactionId)
                {
                    _pending = value; // already the owner: hooks are registered
                    return;
                }
            }
        }

        Action commitHook = () =>
        {
            T published;
            lock (_lock)
            {
                if (_ownerTxnId != transactionId)
                    return;
                _ownerTxnId = 0;
                _committed = _pending;
                published = _committed; // captured under the lock: a later owner's value must not be published
            }
            _onCommitted?.Invoke(published);
        };
        Action rollbackHook = () =>
        {
            lock (_lock)
            {
                if (_ownerTxnId != transactionId)
                    return;
                _ownerTxnId = 0;
            }
            onRolledBack?.Invoke();
        };

        // Without an active transaction (id 0, or already finished) the value is committed immediately.
        if (!_storage.TryRegisterTransactionHooks(transactionId, commitHook, rollbackHook))
        {
            lock (_lock)
                _committed = value;
            _onCommitted?.Invoke(value);
            return;
        }

        lock (_lock)
        {
            _ownerTxnId = transactionId;
            _pending = value;
        }
    }
}
