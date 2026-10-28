using System;

namespace BLite.Core;

/// <summary>
/// Thrown by a commit when another transaction has committed a change to a page this transaction
/// also changed, after this transaction read that page. The first transaction to commit wins; this
/// one is rolled back (<see cref="Transactions.ITransaction.State"/> becomes
/// <see cref="Transactions.TransactionState.Aborted"/>) so that neither commit silently overwrites
/// the other (#304).
/// </summary>
/// <remarks>
/// <para>
/// Nothing of the rejected transaction is written. Run its work again in a new transaction to retry:
/// it then reads the other transaction's committed changes first.
/// </para>
/// <para>
/// Conflicts are detected per page, not per document: two transactions that change different
/// documents stored on the same data page, or whose index entries share a B-Tree leaf, conflict too.
/// A write method called without a transaction (<c>InsertAsync(document)</c>, <c>UpdateAsync</c>,
/// <c>DeleteAsync</c>, their bulk and upsert variants, <c>TruncateAsync</c> of a typed collection, and
/// the <c>BLiteSession</c> methods) commits on its own. When a transaction that committed first changed
/// one of its pages, it runs again in a new transaction while it still holds the collection lock, and
/// throws this exception only if all of its attempts (10) were rejected. Other operations that commit a
/// transaction of their own (index creation, <c>VacuumAsync</c>, retention and time-series pruning,
/// <c>DynamicCollection.TruncateAsync</c>) do not retry and can throw it; run them again.
/// </para>
/// <para>
/// In multi-process mode (<c>PageFileConfig.AllowMultiProcessAccess</c>) a commit that writes pages is
/// also rejected with this exception when the commits of other processes in the shared WAL could not
/// be read, so that it cannot be checked against them.
/// </para>
/// <para>
/// Derives from <see cref="InvalidOperationException"/>, so code that already catches that type
/// for commit failures keeps working.
/// </para>
/// </remarks>
public class BLiteTransactionConflictException : InvalidOperationException
{
    /// <summary>Creates the exception for <paramref name="transactionId"/> and the first conflicting page.</summary>
    internal BLiteTransactionConflictException(ulong transactionId, uint pageId)
        : this(transactionId, pageId,
               $"Transaction {transactionId} conflicts with a transaction that committed first: page {pageId} " +
               "was changed by both. The transaction was rolled back; run it again to retry.")
    {
    }

    private BLiteTransactionConflictException(ulong transactionId, uint pageId, string message)
        : base(message)
    {
        TransactionId = transactionId;
        PageId = pageId;
    }

    /// <summary>
    /// Creates the exception for a commit that writes <paramref name="pageId"/> (its first page) while
    /// this engine could not replay what other processes committed to the shared WAL.
    /// </summary>
    internal static BLiteTransactionConflictException SharedWalNotReplayed(ulong transactionId, uint pageId)
        => new(transactionId, pageId,
               $"Transaction {transactionId} was not committed: the commits of other processes in the shared WAL " +
               $"could not be read, so its pages (first: page {pageId}) cannot be checked against them. " +
               "The transaction was rolled back; run it again to retry.");

    /// <summary>The transaction that was rolled back.</summary>
    public ulong TransactionId { get; }

    /// <summary>The first page found that both transactions changed, or the first page of a commit that could not be checked.</summary>
    public uint PageId { get; }
}
