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
    /// Gets the current transaction if one exists; otherwise, starts a new transaction asynchronously.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation. The task result contains an <see cref="ITransaction"/>
    /// representing the current or newly started transaction.</returns>
    Task<ITransaction> GetCurrentTransactionOrStartAsync();
}
