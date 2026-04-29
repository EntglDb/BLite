using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.KeyValue;
using BLite.Core.Transactions;
using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core;

/// <summary>
/// Defines the contract for a document database context.
/// </summary>
public interface IDocumentDbContext : IDisposable, ITransactionHolder
{
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
    [RequiresDynamicCode("Collection creation uses Expression.Compile() via CollectionIndexManager and ValueConverterRegistry which require dynamic code generation.")]
    [RequiresUnreferencedCode("Index creation uses reflection to access type members. Ensure all entity types and their members are preserved.")]
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
    ValueTask<ITransaction> BeginTransactionAsync(CancellationToken ct = default);

    /// <summary>
    /// No-op when called without a transaction.
    /// Auto-commit operations commit immediately; this exists for API compatibility.
    /// </summary>
    ValueTask SaveChangesAsync(CancellationToken ct = default);

    /// <summary>
    /// Commits a caller-owned transaction created by <see cref="BeginTransactionAsync"/>.
    /// </summary>
    ValueTask SaveChangesAsync(ITransaction transaction, CancellationToken ct = default);

    [RequiresDynamicCode("Dropped collection proxy creation uses MakeGenericType and reflection-based property access.")]
    [RequiresUnreferencedCode("Dropped collection proxy installation inspects collection properties via reflection.")]
    Task DropCollectionAsync<T>(CancellationToken ct = default) where T : class;

    [RequiresUnreferencedCode("Collection lookup inspects collection properties via reflection.")]
    Task<int> TruncateCollectionAsync<T>(CancellationToken ct = default) where T : class;

    /// <summary>
    /// Enables the metrics subsystem. Safe to call multiple times — idempotent.
    /// </summary>
    void EnableMetrics(Metrics.MetricsOptions? options = null);

    /// <summary>
    /// Returns an immutable point-in-time snapshot of performance counters,
    /// or <c>null</c> if <see cref="EnableMetrics"/> has not been called.
    /// </summary>
    Metrics.MetricsSnapshot? GetMetrics();

    /// <summary>
    /// Returns an <see cref="IObservable{T}"/> that emits a <see cref="Metrics.MetricsSnapshot"/>
    /// at the given <paramref name="interval"/>. Enables the metrics subsystem automatically.
    /// </summary>
    IObservable<Metrics.MetricsSnapshot> WatchMetrics(TimeSpan? interval = null);
}
