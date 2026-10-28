using System;
using System.Threading.Tasks;

namespace BLite.Core.Transactions;

/// <summary>
/// Shared failure handling for the collection write methods (typed and dynamic).
/// </summary>
internal static class WriteFailure
{
    /// <summary>
    /// Throws if a caller-owned <paramref name="transaction"/> can no longer take writes, e.g. because
    /// an earlier failed write rolled it back. Writing into it anyway would not fail at the storage
    /// level: the pages would land in a cache entry that is never committed or released.
    /// </summary>
    internal static void EnsureWritable(ITransaction? transaction)
    {
        if (transaction != null && transaction.State != TransactionState.Active)
            throw new InvalidOperationException(
                $"Cannot write in transaction {transaction.TransactionId}: its state is {transaction.State}. " +
                "A failed write rolls the transaction back unless it threw BLiteValidationException.");
    }

    /// <summary>
    /// Called from the catch block of a write method. Rolls <paramref name="transaction"/> back unless
    /// the failure left it clean, which is only the case for a <see cref="BLiteValidationException"/>
    /// in a caller-owned transaction when the call had not written anything yet. A write that failed
    /// after its first page write may have updated the data and some indexes but not the others, so
    /// committing the transaction afterwards would leave the indexes out of step with the data.
    /// A transaction that is no longer active is left alone: it was committed (a step after the
    /// commit failed, e.g. OnInsert retention) or has already been rolled back.
    /// </summary>
    /// <param name="transaction">The transaction the write ran in.</param>
    /// <param name="ownsTransaction">True if the write method opened the transaction itself.</param>
    /// <param name="error">The exception being handled.</param>
    /// <param name="callHasWritten">True if an earlier document of a bulk call was already written.</param>
    internal static async ValueTask HandleAsync(ITransaction transaction, bool ownsTransaction, Exception error, bool callHasWritten = false)
    {
        if (transaction.State != TransactionState.Active && transaction.State != TransactionState.Preparing)
            return;

        if (!ownsTransaction && !callHasWritten && error is BLiteValidationException)
            return;

        await transaction.RollbackAsync().ConfigureAwait(false);
    }

    /// <summary>Attempts a write without a caller-owned transaction makes before a conflict reaches its caller.</summary>
    internal const int AutoCommitAttempts = 10;

    /// <summary>
    /// True if a write method that opened <paramref name="transaction"/> itself must run again after
    /// <paramref name="error"/>: its commit was rejected because a caller-owned transaction committed one
    /// of its pages first (#304). Such commits do not take the collection lock, so they can land between
    /// the write's reads and its commit. <see cref="Transaction.CommitAsync"/> has rolled the transaction
    /// back and nothing was written; the write method still holds the collection lock, so running it again
    /// in a new transaction reads that commit's changes and cannot conflict with another such write.
    /// </summary>
    internal static bool ShouldRetry(ITransaction transaction, bool ownsTransaction, Exception error, int attempt)
        => ownsTransaction
           && attempt < AutoCommitAttempts
           && error is BLiteTransactionConflictException conflict
           && conflict.TransactionId == transaction.TransactionId;
}
