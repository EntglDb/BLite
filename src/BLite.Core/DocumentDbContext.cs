using BLite.Core.Collections;
using BLite.Core.KeyValue;
using BLite.Core.Metadata;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using System.Threading;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BLite.Bson;

namespace BLite.Core;

/// <summary>
/// Base class for database contexts.
/// Inherit and add DocumentCollection{T} properties for your entities.
/// Use partial class for Source Generator integration.
/// </summary>
public abstract partial class DocumentDbContext : IDisposable, ITransactionHolder
{
    protected readonly StorageEngine _storage;
    internal readonly CDC.ChangeStreamDispatcher _cdc;
    private readonly BLiteKvStore _kvStore;
    protected bool _disposed;
    private readonly SemaphoreSlim _transactionLock = new SemaphoreSlim(1, 1);
    private ITransaction? _currentTransaction;
    public ITransaction? CurrentTransaction
    {
        get
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(DocumentDbContext));
            return _currentTransaction != null && (_currentTransaction.State == TransactionState.Active)
                ? _currentTransaction : null;
        }
        private set => _currentTransaction = value;
    }

    /// <summary>
    /// Creates a new database context with default configuration.
    /// Auto-detects the page size for existing database files.
    /// </summary>
    protected DocumentDbContext(string databasePath)
        : this(databasePath, PageFileConfig.DetectFromFile(databasePath) ?? PageFileConfig.Default, null)
    {
    }

    /// <summary>
    /// Creates a new database context with custom page configuration.
    /// </summary>
    protected DocumentDbContext(string databasePath, PageFileConfig config)
        : this(databasePath, config, null)
    {
    }

    /// <summary>
    /// Creates a new database context with custom Key-Value store options.
    /// </summary>
    protected DocumentDbContext(string databasePath, BLiteKvOptions kvOptions)
        : this(databasePath, PageFileConfig.Default, kvOptions)
    {
    }

    /// <summary>
    /// Creates a new database context with custom page and Key-Value store configuration.
    /// </summary>
    protected DocumentDbContext(string databasePath, PageFileConfig config, BLiteKvOptions? kvOptions)
    {
        if (string.IsNullOrWhiteSpace(databasePath))
            throw new ArgumentNullException(nameof(databasePath));

        _storage = new StorageEngine(databasePath, config);
        _cdc = new CDC.ChangeStreamDispatcher();
        _storage.RegisterCdc(_cdc);
        _kvStore = new BLiteKvStore(_storage, kvOptions);

        // Initialize model before collections
        var modelBuilder = new ModelBuilder();
        OnModelCreating(modelBuilder);
        _model = modelBuilder.GetEntityBuilders();
        InitializeCollections();
    }

    /// <summary>
    /// Provides access to the embedded Key-Value store that shares the same database file.
    /// </summary>
    public IBLiteKvStore KvStore
    {
        get
        {
            if (_disposed) throw new ObjectDisposedException(GetType().Name);
            return _kvStore;
        }
    }

    protected virtual void InitializeCollections()
    {
        // Derived classes can override to initialize collections
    }

    private readonly IReadOnlyDictionary<Type, object> _model;
    private readonly List<IDocumentMapper> _registeredMappers = new();

    /// <summary>
    /// Override to configure the model using Fluent API.
    /// 
    /// This method is used in two phases:
    /// 1. Compile-time: The Source Generator analyzes this method to extract entity configurations
    ///    (collection names, keys, converters, etc.) and embeds them into generated mappers.
    /// 2. Runtime: Configuration is used to dynamically apply indexes to collections.
    /// 
    /// Configuration priority: ModelBuilder > Attributes > Conventions
    /// 
    /// Example:
    ///   modelBuilder.Entity&lt;Order&gt;()
    ///       .ToCollection("orders")
    ///       .HasIndex(x => x.Status);
    ///   
    ///   modelBuilder.Entity&lt;Order&gt;()
    ///       .Property(x => x.Id)
    ///       .HasConversion&lt;OrderIdConverter&gt;();
    /// </summary>
    protected virtual void OnModelCreating(ModelBuilder modelBuilder)
    {
    }

    /// <summary>
    /// Helper to create a DocumentCollection instance with custom TId.
    /// Used by derived classes in InitializeCollections for typed primary keys.
    /// </summary>
    protected DocumentCollection<TId, T> CreateCollection<TId, T>(IDocumentMapper<TId, T> mapper)
        where T : class
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(DocumentDbContext));

        string? customName = null;
        EntityTypeBuilder<T>? builder = null;

        if (_model.TryGetValue(typeof(T), out var builderObj))
        {
            builder = builderObj as EntityTypeBuilder<T>;
            customName = builder?.CollectionName;
        }

        _registeredMappers.Add(mapper);
        var collection = new DocumentCollection<TId, T>(_storage, this, mapper, customName);

        // Apply configurations from ModelBuilder
        if (builder != null)
        {
            // Register value converters so the query engine can convert ValueObjects
            // to BSON primitives at query-plan time.
            if (builder.PropertyConverters.Count > 0)
                collection.SetConverterRegistry(new ValueConverterRegistry(builder.PropertyConverters));

            foreach (var indexBuilder in builder.Indexes)
            {
                collection.ApplyIndexBuilder(indexBuilder);
            }

            if (builder.TimeSeriesTtlField != null && builder.TimeSeriesRetention.HasValue)
                collection.SetTimeSeries(builder.TimeSeriesTtlField, builder.TimeSeriesRetention.Value);
        }

        _storage.RegisterMappers(_registeredMappers);

        return collection;
    }

    /// <summary>
    /// Gets the document collection for the specified entity type using an ObjectId as the key.
    /// </summary>
    /// <typeparam name="T">The type of entity to retrieve the document collection for. Must be a reference type.</typeparam>
    /// <returns>A DocumentCollection<ObjectId, T> instance for the specified entity type.</returns>
    public DocumentCollection<ObjectId, T> Set<T>() where T : class => Set<ObjectId, T>();

    /// <summary>
    /// Gets a collection for managing documents of type T, identified by keys of type TId.
    /// Override is generated automatically by the Source Generator for partial DbContext classes.
    /// </summary>
    /// <typeparam name="TId">The type of the unique identifier for documents in the collection.</typeparam>
    /// <typeparam name="T">The type of the document to be managed. Must be a reference type.</typeparam>
    /// <returns>A DocumentCollection<TId, T> instance for performing operations on documents of type T.</returns>
    public virtual DocumentCollection<TId, T> Set<TId, T>() where T : class
        => throw new InvalidOperationException($"No collection registered for entity type '{typeof(T).Name}' with key type '{typeof(TId).Name}'.");

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        _storage?.Dispose();
        _cdc?.Dispose();
        _transactionLock?.Dispose();

        GC.SuppressFinalize(this);
    }

    public ITransaction BeginTransaction()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(DocumentDbContext));
        
        _transactionLock.Wait();
        try
        {
            if (CurrentTransaction != null)
                return CurrentTransaction; // Return existing active transaction
            CurrentTransaction = _storage.BeginTransaction();
            return CurrentTransaction;
        }
        finally
        {
            _transactionLock.Release();
        }
    }

    public async Task<ITransaction> BeginTransactionAsync(CancellationToken ct = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(DocumentDbContext));
        
        bool lockAcquired = false;
        try
        {
            await _transactionLock.WaitAsync(ct);
            lockAcquired = true;
            
            if (CurrentTransaction != null)
                return CurrentTransaction; // Return existing active transaction
            CurrentTransaction = await _storage.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
            return CurrentTransaction;
        }
        finally
        {
            if (lockAcquired)
                _transactionLock.Release();
        }
    }

    public ITransaction GetCurrentTransactionOrStart()
    {
        return BeginTransaction();
    }

    public async Task<ITransaction> GetCurrentTransactionOrStartAsync()
    {
        return await BeginTransactionAsync();
    }

    public void SaveChanges()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(DocumentDbContext));
        if (CurrentTransaction != null)
        {
            try
            {
                CurrentTransaction.Commit();
            }
            finally
            {
                CurrentTransaction = null;
            }
        }
    }
    public async Task SaveChangesAsync(CancellationToken ct = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(DocumentDbContext));
        if (CurrentTransaction != null)
        {
            try
            {
                await CurrentTransaction.CommitAsync(ct);
            }
            finally
            {
                CurrentTransaction = null;
            }
        }
    }
}
