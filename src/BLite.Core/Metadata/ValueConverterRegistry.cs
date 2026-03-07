using System;
using System.Collections.Generic;
using System.Reflection;

namespace BLite.Core.Metadata;

/// <summary>
/// Holds the converter instances registered via
/// <c>modelBuilder.Entity&lt;T&gt;().Property(x => x.Prop).HasConversion&lt;TConverter&gt;()</c>
/// and provides a single entry-point to convert a model-side value (e.g. a ValueObject)
/// into the BSON-compatible provider value (e.g. a string or int).
/// </summary>
public sealed class ValueConverterRegistry
{
    // propertyName (case-insensitive) → Func<object, object> that calls ConvertToProvider
    private readonly Dictionary<string, Func<object, object?>> _toProvider;

    public static readonly ValueConverterRegistry Empty = new(new Dictionary<string, Type>());

    public ValueConverterRegistry(Dictionary<string, Type> converterTypes)
    {
        _toProvider = new Dictionary<string, Func<object, object?>>(StringComparer.OrdinalIgnoreCase);

        foreach (var (propertyName, converterType) in converterTypes)
        {
            var method = FindConvertToProvider(converterType);
            if (method == null) continue;

            // Instantiate the converter once (converters are stateless by convention)
            var instance = Activator.CreateInstance(converterType)
                ?? throw new InvalidOperationException($"Could not create converter instance of type '{converterType}'.");

            _toProvider[propertyName] = value => method.Invoke(instance, [value]);
        }
    }

    /// <summary>
    /// Tries to convert <paramref name="value"/> using the converter registered for
    /// <paramref name="propertyName"/>. Returns <c>true</c> and the converted
    /// <paramref name="providerValue"/> when a converter exists; otherwise <c>false</c>.
    /// </summary>
    public bool TryConvert(string propertyName, object? value, out object? providerValue)
    {
        if (value != null && _toProvider.TryGetValue(propertyName, out var convert))
        {
            try
            {
                providerValue = convert(value);
                return true;
            }
            catch
            {
                // Conversion failure → fall through to in-memory LINQ filter
            }
        }

        providerValue = null;
        return false;
    }

    public bool HasConverter(string propertyName) =>
        _toProvider.ContainsKey(propertyName);

    // ── Private helpers ────────────────────────────────────────────────────────

    /// <summary>
    /// Finds the concrete <c>ConvertToProvider</c> method on a
    /// <see cref="ValueConverter{TModel,TProvider}"/> subclass via reflection.
    /// </summary>
    private static MethodInfo? FindConvertToProvider(Type converterType)
    {
        // Walk the base-type chain to find ValueConverter<TModel, TProvider>
        var baseType = converterType.BaseType;
        while (baseType != null)
        {
            if (baseType.IsGenericType &&
                baseType.GetGenericTypeDefinition() == typeof(ValueConverter<,>))
            {
                return converterType.GetMethod(
                    nameof(ValueConverter<object, object>.ConvertToProvider),
                    BindingFlags.Public | BindingFlags.Instance);
            }
            baseType = baseType.BaseType;
        }
        return null;
    }
}
