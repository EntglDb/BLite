using BLite.Core.Collections;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Core.Metadata;

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
    protected bool _disposed;
    private readonly SemaphoreSlim _transactionLock = new SemaphoreSlim(1, 1);
    public ITransaction? CurrentTransaction
    {
        get
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(DocumentDbContext));
            return field != null && (field.State == TransactionState.Active) ? field : null;
        }
        private set;
    }

    /// <summary>
    /// Creates a new database context with default configuration
    /// </summary>
    protected DocumentDbContext(string databasePath)
        : this(databasePath, PageFileConfig.Default)
    {
    }

    /// <summary>
    /// Creates a new database context with custom configuration
    /// </summary>
    protected DocumentDbContext(string databasePath, PageFileConfig config)
    {
        if (string.IsNullOrWhiteSpace(databasePath))
            throw new ArgumentNullException(nameof(databasePath));

        _storage = new StorageEngine(databasePath, config);
        _cdc = new CDC.ChangeStreamDispatcher();
        _storage.RegisterCdc(_cdc);

        // Initialize model before collections
        var modelBuilder = new ModelBuilder();
        OnModelCreating(modelBuilder);
        _model = modelBuilder.GetEntityBuilders();
        InitializeCollections();
    }

    protected virtual void InitializeCollections()
    {
        // Derived classes can override to initialize collections
    }

    private readonly IReadOnlyDictionary<Type, object> _model;
    private readonly List<IDocumentMapper> _registeredMappers = new();

    /// <summary>
    /// Override to configure the model using Fluent API.
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
            foreach (var indexBuilder in builder.Indexes)
            {
                collection.ApplyIndexBuilder(indexBuilder);
            }
        }

        _storage.RegisterMappers(_registeredMappers);

        return collection;
    }

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
