using BLite.Core.Transactions;

namespace BLite.Core;

/// <summary>
/// Defines a contract for managing and providing access to the current transaction context.
/// </summary>
/// <remarks>Implementations of this interface are responsible for tracking the current transaction and starting a
/// new one if none exists. This is typically used in scenarios where transactional consistency is required across
/// multiple operations.</remarks>
public interface ITransactionHolder
{
    /// <summary>
    /// Gets the current transaction if one exists; otherwise, starts a new transaction.
    /// </summary>
    /// <remarks>Use this method to ensure that a transaction context is available for the current operation.
    /// If a transaction is already in progress, it is returned; otherwise, a new transaction is started and returned.
    /// The caller is responsible for managing the transaction's lifetime as appropriate.</remarks>
    /// <returns>An <see cref="ITransaction"/> representing the current transaction, or a new transaction if none is active.</returns>
    ITransaction GetCurrentTransactionOrStart();

    /// <summary>
    /// Gets the current transaction if one exists; otherwise, starts a new transaction asynchronously.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation. The task result contains an <see cref="ITransaction"/>
    /// representing the current or newly started transaction.</returns>
    Task<ITransaction> GetCurrentTransactionOrStartAsync();
}
