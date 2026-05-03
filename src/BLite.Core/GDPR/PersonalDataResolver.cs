using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace BLite.Core.GDPR;

/// <summary>
/// Resolves <see cref="PersonalDataField"/> metadata for a given entity type.
/// Resolution order:
/// <list type="number">
///   <item>Static <c>PersonalDataFields</c> property on the BLite-generated mapper class
///         (compile-time path; zero-reflection at runtime).</item>
///   <item><see cref="PersonalDataMetadataCache"/> — reflection over
///         <see cref="PersonalDataAttribute"/> declarations (runtime fallback).</item>
/// </list>
/// </summary>
internal static class PersonalDataResolver
{
    // ── Per-entity-type cache ──────────────────────────────────────────────────
    private static readonly ConcurrentDictionary<Type, IReadOnlyList<PersonalDataField>>
        _typeCache = new();

    // ── Per-collection-name cache (built lazily from all scanned mappers) ──────
    private static readonly ConcurrentDictionary<string, IReadOnlyList<PersonalDataField>>
        _collectionCache = new(StringComparer.OrdinalIgnoreCase);

    // Guard to run the full-assembly scan at most once.
    // Double-checked locking: _collectionCacheBuilt flips to true only AFTER the scan
    // has fully populated _collectionCache, so concurrent callers either see false
    // (and serialize on _collectionCacheLock) or true (and observe a complete cache).
    private static volatile bool _collectionCacheBuilt;
    private static readonly object _collectionCacheLock = new();

    /// <summary>
    /// Resolves personal-data fields for <paramref name="entityType"/>.
    /// Never returns <see langword="null"/>; returns an empty list when the type has no
    /// <see cref="PersonalDataAttribute"/> annotations and no generated metadata.
    /// Results are cached per entity type after the first resolution.
    /// </summary>
    [RequiresUnreferencedCode("PersonalDataResolver uses reflection to discover generated mapper classes.")]
    public static IReadOnlyList<PersonalDataField> Resolve(Type entityType)
    {
        return _typeCache.GetOrAdd(entityType, static t =>
        {
            // 1. Try to find the generated mapper class's static PersonalDataFields property.
            var generated = TryResolveFromGeneratedMapper(t);
            if (generated is not null)
                return generated;

            // 2. Fall back to reflection-based cache.
            return PersonalDataMetadataCache.Resolve(t);
        });
    }

    /// <summary>
    /// Resolves personal-data fields by collection name (used by <c>InspectDatabase</c>).
    /// Scans loaded assemblies once for mapper types that have both
    /// <c>CollectionNameStatic</c> and <c>PersonalDataFields</c> static properties,
    /// then caches all results so subsequent calls are O(1) dictionary lookups.
    /// Returns an empty list when no generated mapper is found for the collection.
    /// </summary>
    [RequiresUnreferencedCode("PersonalDataResolver scans loaded assemblies to build the collection → fields map.")]
    public static IReadOnlyList<PersonalDataField> ResolveByCollectionName(string collectionName)
    {
        EnsureCollectionCacheBuilt();
        return _collectionCache.TryGetValue(collectionName, out var fields)
            ? fields
            : Array.Empty<PersonalDataField>();
    }

    // Runs the full-assembly scan exactly once and populates _collectionCache.
    [RequiresUnreferencedCode("PersonalDataResolver scans loaded assemblies for generated mapper classes.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Reflection fallback; source-gen path is preferred and AOT-safe.")]
    private static void EnsureCollectionCacheBuilt()
    {
        if (_collectionCacheBuilt) return;

        lock (_collectionCacheLock)
        {
            if (_collectionCacheBuilt) return;

            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                foreach (var type in TryGetTypes(assembly))
                {
                    if (type.Name is null || !type.Name.EndsWith("Mapper", StringComparison.Ordinal)) continue;
                    if (type.Namespace is null || !type.Namespace.EndsWith("_Mappers", StringComparison.Ordinal)) continue;

                    var colNameProp = type.GetProperty("CollectionNameStatic",
                        BindingFlags.Public | BindingFlags.Static);
                    if (colNameProp?.PropertyType != typeof(string)) continue;

                    var fieldsProp = type.GetProperty("PersonalDataFields",
                        BindingFlags.Public | BindingFlags.Static);
                    if (fieldsProp is null) continue;
                    if (!typeof(IReadOnlyList<PersonalDataField>).IsAssignableFrom(fieldsProp.PropertyType)) continue;

                    var colName = colNameProp.GetValue(null) as string;
                    if (string.IsNullOrEmpty(colName)) continue;

                    var fields = fieldsProp.GetValue(null) as IReadOnlyList<PersonalDataField>
                        ?? Array.Empty<PersonalDataField>();

                    _collectionCache.TryAdd(colName, fields);
                }
            }

            // Publish only after the cache is fully populated.
            _collectionCacheBuilt = true;
        }
    }

    [RequiresUnreferencedCode("PersonalDataResolver scans loaded assemblies for generated mapper classes.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Reflection fallback for dynamic entities; source-gen path is preferred and AOT-safe.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2075",
        Justification = "Reflection fallback for dynamic entities; source-gen path is preferred and AOT-safe.")]
    private static IReadOnlyList<PersonalDataField>? TryResolveFromGeneratedMapper(Type entityType)
    {
        // Convention: the generated mapper is named <EntityName>Mapper and lives in
        // a namespace ending with "_Mappers". We scan loaded assemblies for it.
        var mapperName = entityType.Name + "Mapper";

        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            // Only scan the assembly that defines the entity or assemblies that reference it.
            if (!ReferencesOrIsAssembly(assembly, entityType.Assembly))
                continue;

            foreach (var type in TryGetTypes(assembly))
            {
                if (type.Name != mapperName) continue;
                if (type.Namespace is null || !type.Namespace.EndsWith("_Mappers", StringComparison.Ordinal)) continue;

                var prop = type.GetProperty("PersonalDataFields",
                    BindingFlags.Public | BindingFlags.Static);
                if (prop?.PropertyType is null) continue;
                if (!typeof(IReadOnlyList<PersonalDataField>).IsAssignableFrom(prop.PropertyType)) continue;

                return prop.GetValue(null) as IReadOnlyList<PersonalDataField>
                    ?? Array.Empty<PersonalDataField>();
            }
        }

        return null;
    }

    private static bool ReferencesOrIsAssembly(Assembly candidate, Assembly target)
    {
        if (candidate == target) return true;
#pragma warning disable IL2026
        foreach (var r in candidate.GetReferencedAssemblies())
#pragma warning restore IL2026
        {
            if (r.Name == target.GetName().Name) return true;
        }
        return false;
    }

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Reflection fallback for dynamic entities; source-gen path is preferred and AOT-safe.")]
    private static IEnumerable<Type> TryGetTypes(Assembly assembly)
    {
        try { return assembly.GetTypes(); }
        catch { return Array.Empty<Type>(); }
    }
}
