namespace DocumentDb.Core.Transactions;

/// <summary>
/// Public interface for database transactions.
/// Allows user-controlled transaction boundaries for batch operations.
/// </summary>
/// <example>
/// using (var txn = collection.BeginTransaction())
/// {
///     collection.Insert(entity1, txn);
///     collection.Insert(entity2, txn);
///     txn.Commit();
/// }
/// </example>
public interface ITransaction : IDisposable
{
    /// <summary>
    /// Unique transaction identifier
    /// </summary>
    ulong TransactionId { get; }
    
    /// <summary>
    /// Current state of the transaction
    /// </summary>
    TransactionState State { get; }
    
    /// <summary>
    /// Commits the transaction, making all changes permanent.
    /// Must be called before Dispose() to persist changes.
    /// </summary>
    void Commit();
    
    /// <summary>
    /// Rolls back the transaction, discarding all changes.
    /// Called automatically on Dispose() if Commit() was not called.
    /// </summary>
    void Rollback();
}
