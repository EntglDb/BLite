namespace BLite.Core.Metadata;

/// <summary>
/// Defines a bidirectional conversion between a model type (e.g. ValueObject) 
/// and a provider type supported by the storage engine (e.g. string, int, Guid, ObjectId).
/// </summary>
public abstract class ValueConverter<TModel, TProvider>
{
    /// <summary>
    /// Converts the model value to the provider value.
    /// </summary>
    public abstract TProvider ConvertToProvider(TModel model);

    /// <summary>
    /// Converts the provider value back to the model value.
    /// </summary>
    public abstract TModel ConvertFromProvider(TProvider provider);
}
