using System.Linq.Expressions;
using BLite.Core.Indexing;

namespace BLite.Core.Metadata;

public class ModelBuilder
{
    private readonly Dictionary<Type, object> _entityBuilders = new();

    public EntityTypeBuilder<T> Entity<T>() where T : class
    {
        if (!_entityBuilders.TryGetValue(typeof(T), out var builder))
        {
            builder = new EntityTypeBuilder<T>();
            _entityBuilders[typeof(T)] = builder;
        }
        return (EntityTypeBuilder<T>)builder;
    }

    public IReadOnlyDictionary<Type, object> GetEntityBuilders() => _entityBuilders;
}
