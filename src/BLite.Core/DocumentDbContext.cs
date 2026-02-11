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
    }
    
    private readonly IReadOnlyDictionary<Type, object> _model;

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

        var collection = new DocumentCollection<TId, T>(_storage, mapper, customName);

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
