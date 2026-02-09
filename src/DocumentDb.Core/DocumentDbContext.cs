using DocumentDb.Core.Collections;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Transactions;

namespace DocumentDb.Core;

/// <summary>
/// Base class for database contexts.
/// Inherit and add DocumentCollection{T} properties for your entities.
/// Use partial class for Source Generator integration.
/// </summary>
public abstract partial class DocumentDbContext : IDisposable
{
    public readonly TransactionManager _transactionManager;
    public readonly StorageEngine _storage;
    public bool _disposed;
    
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

        _transactionManager = new TransactionManager(_storage);

        // Initialize collections - implemented by derived class or Source Generator
        InitializeCollections();
    }
    
    /// <summary>
    /// Initialize collection properties.
    /// Override in derived class to manually create collections,
    /// or let Source Generator implement this as partial method.
    /// </summary>
    protected virtual void InitializeCollections()
    {
        // Default: no-op
        // Derived classes override to initialize collection properties
        // OR Source Generator implements this via partial method
    }
    
    /// <summary>
    /// Helper to create a DocumentCollection instance.
    /// Used by derived classes in InitializeCollections.
    /// </summary>
    protected DocumentCollection<T> CreateCollection<T>(IDocumentMapper<T> mapper)
        where T : class
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(DocumentDbContext));
        
        return new DocumentCollection<T>(mapper, _storage, _transactionManager);
    }
    
    /// <summary>
    /// Exposes TransactionManager for advanced scenarios
    /// </summary>
    protected TransactionManager TransactionManager => _transactionManager;
    
    public void Dispose()
    {
        if (_disposed)
            return;
        
        _disposed = true;
        
        _transactionManager?.Dispose();
        _storage?.Dispose();
        
        GC.SuppressFinalize(this);
    }
}
