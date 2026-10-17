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
/// Writes made without an explicit transaction commit on their own and retry nothing; they can only
/// make a caller's open transaction fail, never be lost to it.
/// </para>
/// <para>
/// Derives from <see cref="InvalidOperationException"/>, so code that already catches that type
/// for commit failures keeps working.
/// </para>
/// </remarks>
public class BLiteTransactionConflictException : InvalidOperationException
{
    /// <summary>Creates the exception for <paramref name="transactionId"/> and the first conflicting page.</summary>
    public BLiteTransactionConflictException(ulong transactionId, uint pageId)
        : base($"Transaction {transactionId} conflicts with a transaction that committed first: page {pageId} " +
               "was changed by both. The transaction was rolled back; run it again to retry.")
    {
        TransactionId = transactionId;
        PageId = pageId;
    }

    /// <summary>The transaction that was rolled back.</summary>
    public ulong TransactionId { get; }

    /// <summary>The first page found that both transactions changed.</summary>
    public uint PageId { get; }
}
