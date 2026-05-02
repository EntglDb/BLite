using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

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

                list.Add(new PersonalDataField(prop.Name, attr.Sensitivity, attr.IsTimestamp));
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

    /// <summary>
    /// Clears the cache. Intended for testing only.
    /// </summary>
    internal static void Clear() => s_cache.Clear();
}
