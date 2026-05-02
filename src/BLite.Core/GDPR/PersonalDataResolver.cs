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
    /// <summary>
    /// Resolves personal-data fields for <paramref name="entityType"/>.
    /// Never returns <see langword="null"/>; returns an empty list when the type has no
    /// <see cref="PersonalDataAttribute"/> annotations and no generated metadata.
    /// </summary>
    [RequiresUnreferencedCode("PersonalDataResolver uses reflection to discover generated mapper classes.")]
    public static IReadOnlyList<PersonalDataField> Resolve(Type entityType)
    {
        // 1. Try to find the generated mapper class's static PersonalDataFields property.
        var generated = TryResolveFromGeneratedMapper(entityType);
        if (generated is not null)
            return generated;

        // 2. Fall back to reflection-based cache.
        return PersonalDataMetadataCache.Resolve(entityType);
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
