using System;
using System.Collections.Generic;
using System.Linq.Expressions;
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

            // Instantiate the converter once (converters are stateless by convention).
            var instance = Activator.CreateInstance(converterType)
                ?? throw new InvalidOperationException($"Could not create converter instance of type '{converterType}'.");

            // Compile a direct Func<object, object?> delegate so that the hot query path
            // never uses MethodInfo.Invoke. Compilation is paid once per converter type.
            var modelType = GetModelType(converterType);
            var param     = Expression.Parameter(typeof(object), "v");
            var castInput = Expression.Convert(param, modelType);
            var callExpr  = Expression.Call(Expression.Constant(instance, converterType), method, castInput);
            var boxOutput = Expression.Convert(callExpr, typeof(object));
            _toProvider[propertyName] = Expression.Lambda<Func<object, object?>>(boxOutput, param).Compile();
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

    // ── Private helpers ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Finds the concrete <c>ConvertToProvider</c> method on a
    /// <see cref="ValueConverter{TModel,TProvider}"/> subclass via reflection.
    /// </summary>
    private static MethodInfo? FindConvertToProvider(Type converterType)
    {
        var baseType = FindValueConverterBase(converterType);
        return baseType is null ? null
            : converterType.GetMethod(
                nameof(ValueConverter<object, object>.ConvertToProvider),
                BindingFlags.Public | BindingFlags.Instance);
    }

    /// <summary>
    /// Extracts the model type <c>TModel</c> from the <c>ValueConverter&lt;TModel, TProvider&gt;</c>
    /// base type.  Used to build the typed input cast in the compiled delegate.
    /// </summary>
    private static Type GetModelType(Type converterType)
    {
        var baseType = FindValueConverterBase(converterType)
            ?? throw new InvalidOperationException(
                $"Cannot determine model type for converter '{converterType}': " +
                "it does not inherit ValueConverter<TModel, TProvider>.");
        return baseType.GetGenericArguments()[0];
    }

    private static Type? FindValueConverterBase(Type converterType)
    {
        var bt = converterType.BaseType;
        while (bt != null)
        {
            if (bt.IsGenericType && bt.GetGenericTypeDefinition() == typeof(ValueConverter<,>))
                return bt;
            bt = bt.BaseType;
        }
        return null;
    }
}
