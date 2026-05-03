using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json.Serialization;

namespace BLite.Core.GDPR;

/// <summary>
/// Thread-safe per-<see cref="Type"/> cache that resolves <see cref="PersonalDataField"/>
/// lists by scanning <see cref="PersonalDataAttribute"/> reflectively.
/// This is the <em>reflection fallback</em> path used when the BLite source generator
/// has not emitted static metadata for a given entity type (e.g. dynamic collections,
/// third-party POCOs registered ad-hoc).
/// </summary>
internal static class PersonalDataMetadataCache
{
    private static readonly ConcurrentDictionary<Type, IReadOnlyList<PersonalDataField>> s_cache
        = new();

    /// <summary>
    /// Returns all personal-data fields declared on <paramref name="type"/> by scanning
    /// <see cref="PersonalDataAttribute"/> on every public instance property.
    /// Results are cached; the first call for each type may incur reflection overhead.
    /// </summary>
    [RequiresUnreferencedCode("PersonalDataMetadataCache uses reflection to scan [PersonalData] attributes on entity properties.")]
    public static IReadOnlyList<PersonalDataField> Resolve(Type type)
    {
        if (s_cache.TryGetValue(type, out var cached)) return cached;
        var fields = BuildFields(type);
        s_cache.TryAdd(type, fields);
        return fields;
    }

    [RequiresUnreferencedCode("PersonalDataMetadataCache uses reflection to scan [PersonalData] attributes on entity properties.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2070",
        Justification = "Reflection fallback for dynamic entities. Source-gen path is preferred and AOT-safe.")]
    private static IReadOnlyList<PersonalDataField> BuildFields(Type type)
    {
        var list = new List<PersonalDataField>();

        try
        {
            foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                var attr = prop.GetCustomAttribute<PersonalDataAttribute>(inherit: true);
                if (attr is null) continue;

                var bsonFieldName = ResolveBsonFieldName(prop);
                list.Add(new PersonalDataField(prop.Name, attr.Sensitivity, attr.IsTimestamp, bsonFieldName));
            }
        }
        catch
        {
            // Reflection may fail in trimmed environments; return empty list.
            return Array.Empty<PersonalDataField>();
        }

        return list.Count == 0
            ? Array.Empty<PersonalDataField>()
            : list.AsReadOnly();
    }

    // Cache the ColumnAttribute.Name PropertyInfo once it's first found, so subsequent
    // calls don't repeat the GetProperty reflection lookup.
    private static PropertyInfo? s_columnNameProp;

    /// <summary>
    /// Resolves the BSON field name for a property using the same priority order as the
    /// BLite source generator: <c>[JsonPropertyName]</c> first, then <c>[Column]</c> (resolved
    /// by attribute name to avoid a hard dependency on DataAnnotations), then
    /// <c>PropertyName.ToLowerInvariant()</c>.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2075",
        Justification = "Reflection fallback for dynamic entities. GetType().GetProperty() on ColumnAttribute is intentional and cached. Source-gen path is preferred and AOT-safe.")]
    private static string ResolveBsonFieldName(PropertyInfo prop)
    {
        // [JsonPropertyName("bsonkey")] — System.Text.Json (always available)
        var jsonAttr = prop.GetCustomAttribute<JsonPropertyNameAttribute>(inherit: true);
        if (jsonAttr?.Name is { Length: > 0 } jsonName)
            return jsonName;

        // [Column("bsonkey")] — System.ComponentModel.DataAnnotations.Schema
        // Look up by type name to avoid adding a package reference.
        foreach (var customAttr in prop.GetCustomAttributes(inherit: true))
        {
            if (customAttr.GetType().FullName == "System.ComponentModel.DataAnnotations.Schema.ColumnAttribute")
            {
                // Lazily resolve and cache ColumnAttribute.Name PropertyInfo.
                s_columnNameProp ??= customAttr.GetType().GetProperty("Name", BindingFlags.Public | BindingFlags.Instance);
                if (s_columnNameProp?.GetValue(customAttr) is string colName && colName.Length > 0)
                    return colName;
                break;
            }
        }

        // BLite default: property name lowercased
        return prop.Name.ToLowerInvariant();
    }

    /// <summary>
    /// Clears the cache. Intended for testing only.
    /// </summary>
    internal static void Clear() => s_cache.Clear();
}
