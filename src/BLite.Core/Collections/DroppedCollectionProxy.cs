using BLite.Bson;
using BLite.Core.CDC;
using BLite.Core.Indexing;
using BLite.Core.Query;
using BLite.Core.Transactions;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core.Collections;

internal sealed class DroppedCollectionProxy<TId, T> : IDocumentCollection<TId, T> where T : class
{
    private readonly InvalidOperationException _exception;

    public DroppedCollectionProxy(string collectionName)
    {
        _exception = new InvalidOperationException($"Collection '{collectionName}' has been dropped.");
    }

    public SchemaVersion? CurrentSchemaVersion => throw _exception;

    private InvalidOperationException CreateException() => new(_exception.Message);

    private Task FailTask() => Task.FromException(CreateException());
    private Task<TResult> FailTask<TResult>() => Task.FromException<TResult>(CreateException());
    private ValueTask<TResult> FailValueTask<TResult>() => new(Task.FromException<TResult>(CreateException()));
    private IAsyncEnumerable<TValue> FailAsyncEnumerable<TValue>() => throw CreateException();
    private IEnumerable<TValue> FailEnumerable<TValue>() => throw CreateException();
    private IObservable<TValue> FailObservable<TValue>() => throw CreateException();

    public ValueTask<TId> InsertAsync(T entity, CancellationToken ct = default) => FailValueTask<TId>();
    public ValueTask<TId> InsertAsync(T entity, ITransaction? transaction, CancellationToken ct = default) => FailValueTask<TId>();
    public ValueTask<List<TId>> InsertBulkAsync(IEnumerable<T> entities, CancellationToken ct = default) => FailValueTask<List<TId>>();
    public ValueTask<List<TId>> InsertBulkAsync(IEnumerable<T> entities, ITransaction? transaction, CancellationToken ct = default) => FailValueTask<List<TId>>();
    public ValueTask<T?> FindByIdAsync(TId id, CancellationToken ct = default) => FailValueTask<T?>();
    public ValueTask<T?> FindByIdAsync(TId id, ITransaction? transaction, CancellationToken ct = default) => FailValueTask<T?>();
    public IAsyncEnumerable<T> FindAllAsync(CancellationToken ct = default) => FailAsyncEnumerable<T>();
    public IAsyncEnumerable<T> FindAllAsync(ITransaction? transaction, CancellationToken ct = default) => FailAsyncEnumerable<T>();
    [RequiresDynamicCode("LINQ-style find operations use Expression.Compile() and index optimization which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ-style find operations use reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    public IAsyncEnumerable<T> FindAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default) => FailAsyncEnumerable<T>();
    [RequiresDynamicCode("LINQ-style find operations use Expression.Compile() and index optimization which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ-style find operations use reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    public IAsyncEnumerable<T> FindAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction, CancellationToken ct = default) => FailAsyncEnumerable<T>();
    [RequiresDynamicCode("LINQ-style find operations use Expression.Compile() and index optimization which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ-style find operations use reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    public Task<T?> FindOneAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default) => FailTask<T?>();
    [RequiresDynamicCode("LINQ-style find operations use Expression.Compile() and index optimization which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ-style find operations use reflection to resolve members at runtime. Ensure all entity types are preserved.")]
    public Task<T?> FindOneAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction, CancellationToken ct = default) => FailTask<T?>();
    [RequiresDynamicCode("LINQ queries over BLite collections use Expression.Compile() and MakeGenericMethod which require dynamic code generation.")]
    [RequiresUnreferencedCode("LINQ queries over BLite collections use reflection to resolve methods at runtime. Ensure all entity types and their members are preserved.")]
    public IBLiteQueryable<T> AsQueryable() => throw CreateException();
    public ValueTask<bool> UpdateAsync(T entity, CancellationToken ct = default) => FailValueTask<bool>();
    public ValueTask<bool> UpdateAsync(T entity, ITransaction? transaction, CancellationToken ct = default) => FailValueTask<bool>();
    public ValueTask<int> UpdateBulkAsync(IEnumerable<T> entities, CancellationToken ct = default) => FailValueTask<int>();
    public ValueTask<int> UpdateBulkAsync(IEnumerable<T> entities, ITransaction? transaction, CancellationToken ct = default) => FailValueTask<int>();
    public ValueTask<bool> DeleteAsync(TId id, CancellationToken ct = default) => FailValueTask<bool>();
    public ValueTask<bool> DeleteAsync(TId id, ITransaction? transaction, CancellationToken ct = default) => FailValueTask<bool>();
    public ValueTask<int> DeleteBulkAsync(IEnumerable<TId> ids, CancellationToken ct = default) => FailValueTask<int>();
    public ValueTask<int> DeleteBulkAsync(IEnumerable<TId> ids, ITransaction? transaction, CancellationToken ct = default) => FailValueTask<int>();
    public Task<int> TruncateAsync(CancellationToken ct = default) => FailTask<int>();
    public Task<int> TruncateAsync(ITransaction? transaction, CancellationToken ct = default) => FailTask<int>();
    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    public Task<ICollectionIndex<TId, T>> CreateIndexAsync<TKey>(Expression<Func<T, TKey>> keySelector, string? name = null, bool unique = false, CancellationToken ct = default) => FailTask<ICollectionIndex<TId, T>>();
    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    public Task<ICollectionIndex<TId, T>> CreateVectorIndexAsync<TKey>(Expression<Func<T, TKey>> keySelector, int dimensions, VectorMetric metric = VectorMetric.Cosine, string? name = null, CancellationToken ct = default) => FailTask<ICollectionIndex<TId, T>>();
    [RequiresDynamicCode("Index creation compiles key selector expressions using Expression.Compile() which requires dynamic code generation.")]
    public Task<ICollectionIndex<TId, T>> EnsureIndexAsync<TKey>(Expression<Func<T, TKey>> keySelector, string? name = null, bool unique = false, CancellationToken ct = default) => FailTask<ICollectionIndex<TId, T>>();
    public Task<bool> DropIndexAsync(string name, CancellationToken ct = default) => FailTask<bool>();
    public IEnumerable<CollectionIndexInfo> GetIndexes() => FailEnumerable<CollectionIndexInfo>();
    public Task<ICollectionIndex<TId, T>?> GetIndexAsync(string name) => FailTask<ICollectionIndex<TId, T>?>();
    public IAsyncEnumerable<T> QueryIndexAsync(string indexName, object? minKey, object? maxKey, bool ascending = true, int skip = 0, int take = int.MaxValue, CancellationToken ct = default) => FailAsyncEnumerable<T>();
    public IAsyncEnumerable<T> QueryIndexAsync(string indexName, object? minKey, object? maxKey, bool ascending, int skip, int take, ITransaction? transaction, CancellationToken ct = default) => FailAsyncEnumerable<T>();
    public IAsyncEnumerable<T> ScanAsync(BsonReaderPredicate predicate, CancellationToken ct = default) => FailAsyncEnumerable<T>();
    public IAsyncEnumerable<T> ScanAsync(BsonReaderPredicate predicate, ITransaction? transaction, CancellationToken ct = default) => FailAsyncEnumerable<T>();
    public IAsyncEnumerable<T> ScanAsync(IndexQueryPlan plan, CancellationToken ct = default) => FailAsyncEnumerable<T>();
    public IAsyncEnumerable<TResult> ScanAsync<TResult>(BsonReaderProjector<TResult> projector, CancellationToken ct = default) => FailAsyncEnumerable<TResult>();
    public IAsyncEnumerable<T> ParallelScanAsync(BsonReaderPredicate predicate, int degreeOfParallelism = -1, CancellationToken ct = default) => FailAsyncEnumerable<T>();
    public Task ForcePruneAsync() => FailTask();
    public Task VacuumAsync(VacuumOptions? options = null, CancellationToken ct = default) => FailTask();
    public IObservable<ChangeStreamEvent<TId, T>> Watch(bool capturePayload = false) => FailObservable<ChangeStreamEvent<TId, T>>();
}
