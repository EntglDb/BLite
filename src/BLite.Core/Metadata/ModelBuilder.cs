using System.Linq.Expressions;
using BLite.Core.GDPR;
using BLite.Core.Indexing;

namespace BLite.Core.Metadata;

public class ModelBuilder
{
    private readonly Dictionary<Type, object> _entityBuilders = new();

    public EntityTypeBuilder<T> Entity<T>() where T : class
    {
        if (!_entityBuilders.TryGetValue(typeof(T), out var builder))
        {
            var newBuilder = new EntityTypeBuilder<T>();

            // Seed from the [GdprMode] attribute if present; fluent HasGdprMode() wins
            // when called afterwards because it overwrites the property.
            var attr = typeof(T).GetCustomAttributes(typeof(GdprModeAttribute), inherit: true)
                                .OfType<GdprModeAttribute>()
                                .FirstOrDefault();
            if (attr != null)
                newBuilder.GdprMode = attr.Mode;

            builder = newBuilder;
            _entityBuilders[typeof(T)] = builder;
        }
        return (EntityTypeBuilder<T>)builder;
    }

    public IReadOnlyDictionary<Type, object> GetEntityBuilders() => _entityBuilders;
}
