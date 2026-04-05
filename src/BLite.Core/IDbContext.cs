using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.KeyValue;
using BLite.Core.Transactions;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core;

/// <summary>
/// Defines the contract for a document database context.
/// </summary>
public interface IDbContext : IDisposable, ITransactionHolder
{
    /// <summary>
    /// Gets the current active transaction, or <c>null</c> if no transaction is active.
    /// </summary>
    ITransaction? CurrentTransaction { get; }

    /// <summary>
    /// Provides access to the embedded Key-Value store that shares the same database file.
    /// </summary>
    IBLiteKvStore KvStore { get; }

    /// <summary>
    /// Opens a new isolated session backed by this context's storage engine.
    /// </summary>
    BLiteSession OpenSession();

    /// <summary>
    /// Creates a document collection bound to an explicit <paramref name="holder"/>.
    /// </summary>
    IDocumentCollection<TId, T> CreateSessionCollection<TId, T>(IDocumentMapper<TId, T> mapper, ITransactionHolder holder)
        where T : class;

    /// <summary>
    /// Gets the document collection for the specified entity type using an ObjectId as the key.
    /// </summary>
    IDocumentCollection<ObjectId, T> Set<T>() where T : class;

    /// <summary>
    /// Gets the document collection for the specified entity type using a custom key type.
    /// </summary>
    IDocumentCollection<TId, T> Set<TId, T>() where T : class;

    /// <summary>
    /// Begins a new transaction synchronously.
    /// </summary>
    ITransaction BeginTransaction();

    /// <summary>
    /// Begins a new transaction asynchronously.
    /// </summary>
    Task<ITransaction> BeginTransactionAsync(CancellationToken ct = default);

    /// <summary>
    /// Commits the current transaction and saves all pending changes.
    /// </summary>
    Task SaveChangesAsync(CancellationToken ct = default);
}
