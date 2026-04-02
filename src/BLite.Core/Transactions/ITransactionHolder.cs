using System.Threading.Tasks;
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
    /// Returns a <see cref="ValueTask{T}"/> to avoid heap allocation on the hot path (existing transaction).
    /// </summary>
    ValueTask<ITransaction> GetCurrentTransactionOrStartAsync();
}
