using DocumentDb.Core.Collections;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Transactions;
using DocumentDb.Core.Metadata;

namespace DocumentDb.Core;

/// <summary>
/// Base class for database contexts.
/// Inherit and add DocumentCollection{T} properties for your entities.
/// Use partial class for Source Generator integration.
/// </summary>
public abstract partial class DocumentDbContext : IDisposable
{
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

        // Initialize model before collections
        var modelBuilder = new ModelBuilder();
        OnModelCreating(modelBuilder);
        _model = modelBuilder.GetEntityBuilders();

        // Initialize collections - implemented by derived class or Source Generator
        InitializeCollections();
    }
    
    private readonly IReadOnlyDictionary<Type, object> _model;

    /// <summary>
    /// Override to configure the model using Fluent API.
    /// </summary>
    protected virtual void OnModelCreating(ModelBuilder modelBuilder)
    {
    }
    
    /// <summary>
    /// Initialize collection properties.
    /// Override in derived class to manually create collections,
    /// or let Source Generator implement this as partial method.
    /// </summary>
    protected virtual void InitializeCollections()
    {
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
        
        string? customName = null;
        EntityTypeBuilder<T>? builder = null;

        if (_model.TryGetValue(typeof(T), out var builderObj))
        {
            builder = builderObj as EntityTypeBuilder<T>;
            customName = builder?.CollectionName;
        }

        var collection = new DocumentCollection<T>(mapper, _storage, customName);

        // Apply configurations from ModelBuilder
        if (builder != null)
        {
            foreach (var indexBuilder in builder.Indexes)
            {
                collection.ApplyIndexBuilder(indexBuilder);
            }
        }

        return collection;
    }
    
    public void Dispose()
    {
        if (_disposed)
            return;
        
        _disposed = true;
        
        _storage?.Dispose();
        
        GC.SuppressFinalize(this);
    }
}
