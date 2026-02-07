namespace DocumentDb.Core.Transactions;

/// <summary>
/// Transaction states
/// </summary>
public enum TransactionState : byte
{
    /// <summary>Transaction is active and can accept operations</summary>
    Active = 1,
    
    /// <summary>Transaction is preparing to commit</summary>
    Preparing = 2,
    
    /// <summary>Transaction committed successfully</summary>
    Committed = 3,
    
    /// <summary>Transaction was rolled back</summary>
    Aborted = 4
}

/// <summary>
/// Transaction isolation levels
/// </summary>
public enum IsolationLevel : byte
{
    /// <summary>Read uncommitted data</summary>
    ReadUncommitted = 0,
    
    /// <summary>Read only committed data (default)</summary>
    ReadCommitted = 1,
    
    /// <summary>Repeatable reads</summary>
    RepeatableRead = 2,
    
    /// <summary>Serializable (full isolation)</summary>
    Serializable = 3
}
