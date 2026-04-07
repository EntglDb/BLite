using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using BLite.SourceGenerators.Helpers;
using BLite.SourceGenerators.Models;

namespace BLite.SourceGenerators
{
    public static class EntityAnalyzer
    {
        public static EntityInfo Analyze(INamedTypeSymbol entityType, SemanticModel semanticModel, List<BLiteDiagnostic>? diagnostics = null)
        {
            var entityInfo = new EntityInfo
            {
                Name = entityType.Name,
                Namespace = entityType.ContainingNamespace.ToDisplayString(),
                FullTypeName = SyntaxHelper.GetFullName(entityType),
                CollectionName = entityType.Name.ToLowerInvariant() + BLiteConventions.DefaultCollectionNameSuffix
            };

            var tableAttr = AttributeHelper.GetAttribute(entityType, BLiteConventions.TableAttribute);
            if (tableAttr != null)
            {
                var tableName = tableAttr.ConstructorArguments.Length > 0 ? tableAttr.ConstructorArguments[0].Value?.ToString() : null;
                var schema = AttributeHelper.GetNamedArgumentValue(tableAttr, BLiteConventions.SchemaNamedArg);
                
                var collectionName = !string.IsNullOrEmpty(tableName) ? tableName! : entityInfo.Name;
                if (!string.IsNullOrEmpty(schema))
                {
                    collectionName = $"{schema}.{collectionName}";
                }
                entityInfo.CollectionName = collectionName;
            }
            
            // Analyze properties of the root entity
            var symbolCache = new Dictionary<string, INamedTypeSymbol>();
            AnalyzeProperties(entityType, entityInfo.Properties, symbolCache);

            // Check if entity needs reflection-based deserialization
            // Include properties with private setters, init-only setters, or DDD backing field pattern
            entityInfo.HasPrivateSetters = entityInfo.Properties.Any(p =>
                (!p.HasPublicSetter && p.HasAnySetter) || p.HasInitOnlySetter || p.HasPrivateBackingFieldAccess);
            
            // Constructor selection: priority queue (protected > public > private, 0-params > N-params)
            var hasRequiredMembers = entityInfo.Properties.Any(p => p.HasCSharpRequiredKeyword);
            var (selectedParams, ctorIsPublic) = SelectConstructor(entityType, entityInfo.Properties, hasRequiredMembers);
            entityInfo.SelectedConstructorParameters = selectedParams;
            entityInfo.SelectedConstructorIsPublic = ctorIsPublic;
            // Derive HasPrivateOrNoConstructor for code paths that still check it
            entityInfo.HasPrivateOrNoConstructor = selectedParams == null || !ctorIsPublic;
            if (selectedParams == null)
            {
                diagnostics?.Add(new BLiteDiagnostic(
                    "BLITE010",
                    $"BLite: no viable constructor found for '{entityInfo.Name}'. " +
                     "GetUninitializedObject will be used — field initializers will NOT run. " +
                     "Consider adding a public or protected constructor.",
                    isError: false));
            }

            // Analyze nested types recursively
            // We use a dictionary for nested types to ensure uniqueness by name
            var analyzedTypes = new HashSet<string>();
            AnalyzeNestedTypesRecursive(entityInfo.Properties, entityInfo.NestedTypes, semanticModel, analyzedTypes, 1, BLiteConventions.DefaultMaxNestedTypeDepth, symbolCache, diagnostics);
            
            // Determine ID property
            // entityInfo.IdProperty is computed from Properties.FirstOrDefault(p => p.IsKey)
            
            if (entityInfo.IdProperty == null)
            {
                // Fallback to convention: property named "Id"
                var idProp = entityInfo.Properties.FirstOrDefault(p => p.Name == BLiteConventions.DefaultIdPropertyName);
                if (idProp != null)
                {
                    idProp.IsKey = true;
                }
            }
            
            // Check for AutoId (int/long keys)
            if (entityInfo.IdProperty != null)
            {
                var idType = entityInfo.IdProperty.TypeName.TrimEnd('?');
                if (idType == BLiteConventions.AutoIdTypeInt || idType == BLiteConventions.AutoIdTypeInt32 || idType == BLiteConventions.AutoIdTypeLong || idType == BLiteConventions.AutoIdTypeInt64)
                {
                    entityInfo.AutoId = true;
                }
            }
            
            return entityInfo;
        }

        private static void AnalyzeProperties(INamedTypeSymbol typeSymbol, List<PropertyInfo> properties, Dictionary<string, INamedTypeSymbol>? symbolCache = null)
        {
            // Collect properties from the entire inheritance hierarchy
            var seenProperties = new HashSet<string>();
            var currentType = typeSymbol;
            
            while (currentType != null && currentType.SpecialType != SpecialType.System_Object)
            {
                var sourceProps = currentType.GetMembers()
                    .OfType<IPropertySymbol>()
                    .Where(p => p.DeclaredAccessibility == Accessibility.Public && !p.IsStatic);
                    
                foreach (var prop in sourceProps)
                {
                    // Skip if already seen (overridden property in derived class takes precedence)
                    if (!seenProperties.Add(prop.Name))
                        continue;
                        
                    if (AttributeHelper.ShouldIgnore(prop))
                        continue;
                    
                    // Auto-properties with private setters from compiled assemblies (metadata)
                    // appear as getter-only (prop.SetMethod == null) because Roslyn does not expose
                    // private accessors across assembly boundaries. However, they always have a
                    // compiler-generated backing field named "<PropertyName>k__BackingField".
                    // Detect this case so the property is included with HasAnySetter=true, but
                    // exclude get-only auto-properties whose compiler-generated backing field is
                    // readonly (those truly have no setter and cannot be assigned).
                    bool hasCompilerGeneratedBackingField = false;
                    if (prop.SetMethod == null && !SyntaxHelper.HasBackingField(prop))
                    {
                        var expectedBackingFieldName = $"<{prop.Name}{BLiteConventions.CompilerBackingFieldSuffix}";
                        var compilerGeneratedBackingField = prop.ContainingType.GetMembers()
                            .OfType<Microsoft.CodeAnalysis.IFieldSymbol>()
                            .FirstOrDefault(f => f.Name == expectedBackingFieldName);

                        hasCompilerGeneratedBackingField = compilerGeneratedBackingField != null
                            && !compilerGeneratedBackingField.IsReadOnly;

                        // Fallback for reference assemblies: private backing fields are stripped from
                        // reference-assembly metadata. If no backing field was found but the property
                        // comes from an external (compiled) assembly and is not abstract, assume it
                        // has a private setter — the most common case for auditable entity properties.
                        if (!hasCompilerGeneratedBackingField
                            && !prop.IsAbstract
                            && prop.DeclaringSyntaxReferences.IsEmpty)
                        {
                            hasCompilerGeneratedBackingField = true;
                        }
                    }

                    // Skip computed getter-only properties (no setter, no backing field)
                    bool isReadOnlyGetter = prop.SetMethod == null && !SyntaxHelper.HasBackingField(prop) && !hasCompilerGeneratedBackingField;
                    Microsoft.CodeAnalysis.IFieldSymbol? conventionalBackingField = null;
                    if (isReadOnlyGetter)
                    {
                        // DDD pattern: public IReadOnlyCollection<T> Lines => _lines.AsReadOnly()
                        // Check for a conventional private backing field named _propertyName.
                        conventionalBackingField = SyntaxHelper.FindConventionalBackingField(prop);
                        if (conventionalBackingField == null)
                            continue; // True computed property — skip as before
                        // Fall through: backing field found, include this property
                    }

                var columnAttr = AttributeHelper.GetAttribute(prop, BLiteConventions.ColumnAttribute);
                var bsonFieldName = AttributeHelper.GetAttributeStringValue(prop, BLiteConventions.BsonPropertyAttribute) ?? 
                                    AttributeHelper.GetAttributeStringValue(prop, BLiteConventions.JsonPropertyNameAttribute);

                if (bsonFieldName == null && columnAttr != null)
                {
                    bsonFieldName = columnAttr.ConstructorArguments.Length > 0 ? columnAttr.ConstructorArguments[0].Value?.ToString() : null;
                }
                
                    var propInfo = new PropertyInfo
                    {
                        Name = prop.Name,
                        TypeName = SyntaxHelper.GetTypeName(prop.Type),
                        BsonFieldName = bsonFieldName ?? prop.Name.ToLowerInvariant(),
                        ColumnTypeName = columnAttr != null ? AttributeHelper.GetNamedArgumentValue(columnAttr, BLiteConventions.TypeNameNamedArg) : null,
                        IsNullable = SyntaxHelper.IsNullableType(prop.Type),
                        IsKey = AttributeHelper.IsKey(prop),
                        IsRequired = AttributeHelper.HasAttribute(prop, BLiteConventions.RequiredAttribute),
                        HasCSharpRequiredKeyword = prop.IsRequired,
                        
                        HasPublicSetter = prop.SetMethod?.DeclaredAccessibility == Accessibility.Public,
                        HasInitOnlySetter = prop.SetMethod?.IsInitOnly == true,
                        // For compiled assemblies, private setters appear as null in prop.SetMethod.
                        // Use hasCompilerGeneratedBackingField to detect that a private setter exists.
                        HasAnySetter = prop.SetMethod != null || hasCompilerGeneratedBackingField,
                        IsReadOnlyGetter = isReadOnlyGetter,
                        HasPrivateBackingFieldAccess = conventionalBackingField != null,
                        BackingFieldName = conventionalBackingField != null
                            ? conventionalBackingField.Name
                            : (prop.SetMethod?.DeclaredAccessibility != Accessibility.Public)
                                ? $"<{prop.Name}{BLiteConventions.CompilerBackingFieldSuffix}"
                                : null,
                        // Track the declaring type so [UnsafeAccessor] setters can reference the correct type
                        // even when the property is inherited (e.g. from an auditable base class).
                        DeclaringTypeName = SyntaxHelper.GetFullName(prop.ContainingType),
                    };

                // MaxLength / MinLength
                propInfo.MaxLength = AttributeHelper.GetAttributeIntValue(prop, BLiteConventions.MaxLengthAttribute);
                propInfo.MinLength = AttributeHelper.GetAttributeIntValue(prop, BLiteConventions.MinLengthAttribute);
                
                var stringLengthAttr = AttributeHelper.GetAttribute(prop, BLiteConventions.StringLengthAttribute);
                if (stringLengthAttr != null)
                {
                    if (stringLengthAttr.ConstructorArguments.Length > 0 && stringLengthAttr.ConstructorArguments[0].Value is int max)
                        propInfo.MaxLength = max;
                    
                    var minLenStr = AttributeHelper.GetNamedArgumentValue(stringLengthAttr, BLiteConventions.MinimumLengthNamedArg);
                    if (int.TryParse(minLenStr, out var min))
                        propInfo.MinLength = min;
                }

                // Range
                var rangeAttr = AttributeHelper.GetAttribute(prop, BLiteConventions.RangeAttribute);
                if (rangeAttr != null && rangeAttr.ConstructorArguments.Length >= 2)
                {
                    if (rangeAttr.ConstructorArguments[0].Value is double dmin) propInfo.RangeMin = dmin;
                    else if (rangeAttr.ConstructorArguments[0].Value is int imin) propInfo.RangeMin = (double)imin;
                    
                    if (rangeAttr.ConstructorArguments[1].Value is double dmax) propInfo.RangeMax = dmax;
                    else if (rangeAttr.ConstructorArguments[1].Value is int imax) propInfo.RangeMax = (double)imax;
                }

                    if (SyntaxHelper.IsCollectionType(prop.Type, out var itemType))
                    {
                        propInfo.IsCollection = true;
                        propInfo.IsArray = prop.Type is IArrayTypeSymbol;
                        
                        // Determine concrete collection type name
                        propInfo.CollectionConcreteTypeName = SyntaxHelper.GetTypeName(prop.Type);
                        
                        if (itemType != null)
                        {
                            propInfo.CollectionItemType = SyntaxHelper.GetTypeName(itemType);
                            
                            // Check if collection item is nested object
                            if (SyntaxHelper.IsNestedObjectType(itemType))
                            {
                                propInfo.IsCollectionItemNested = true;
                                propInfo.NestedTypeName = itemType.Name;
                                propInfo.NestedTypeFullName = SyntaxHelper.GetFullName((INamedTypeSymbol)itemType);
                                if (symbolCache != null && !symbolCache.ContainsKey(propInfo.NestedTypeFullName))
                                    symbolCache[propInfo.NestedTypeFullName] = (INamedTypeSymbol)itemType;
                            }
                            // Check if collection item is an enum
                            else if (itemType.TypeKind == TypeKind.Enum && itemType is INamedTypeSymbol enumItemSymbol)
                            {
                                propInfo.IsCollectionItemEnum = true;
                                propInfo.CollectionItemEnumFullTypeName = SyntaxHelper.GetFullName(enumItemSymbol);
                                propInfo.CollectionItemEnumUnderlyingTypeName = enumItemSymbol.EnumUnderlyingType?.ToDisplayString() ?? "int";
                            }
                        }
                    }
                    // Check for Dictionary type (must come before enum and nested object checks)
                    else if (SyntaxHelper.IsDictionaryType(prop.Type, out var dictKeyType, out var dictValueType))
                    {
                        propInfo.IsDictionary = true;
                        propInfo.DictionaryKeyType   = dictKeyType   != null ? SyntaxHelper.GetTypeName(dictKeyType)   : "string";
                        propInfo.DictionaryValueType = dictValueType != null ? SyntaxHelper.GetTypeName(dictValueType) : "object";
                    }
                    // Check for enum type (direct property)
                    else if (SyntaxHelper.IsEnumType(prop.Type, out var enumUnderlying, out var enumFullName))
                    {
                        propInfo.IsEnum = true;
                        propInfo.EnumUnderlyingTypeName = enumUnderlying;
                        propInfo.EnumFullTypeName = enumFullName;
                    }
                    // Check for Nested Object
                    else if (SyntaxHelper.IsNestedObjectType(prop.Type))
                    {
                        propInfo.IsNestedObject = true;
                        propInfo.NestedTypeName = prop.Type.Name;
                        var nestedSymbol = (INamedTypeSymbol)prop.Type;
                        propInfo.NestedTypeFullName = SyntaxHelper.GetFullName(nestedSymbol);
                        if (symbolCache != null && !symbolCache.ContainsKey(propInfo.NestedTypeFullName))
                            symbolCache[propInfo.NestedTypeFullName] = nestedSymbol;
                    }

                    properties.Add(propInfo);
                }
                
                currentType = currentType.BaseType;
            }
        }

        private static void AnalyzeNestedTypesRecursive(
            List<PropertyInfo> properties,
            Dictionary<string, NestedTypeInfo> targetNestedTypes,
            SemanticModel semanticModel,
            HashSet<string> analyzedTypes,
            int currentDepth,
            int maxDepth,
            Dictionary<string, INamedTypeSymbol> symbolCache,
            List<BLiteDiagnostic>? diagnostics = null)
        {
             if (currentDepth > maxDepth) return;

             // Identify properties that reference nested types (either directly or via collection)
             var nestedProps = properties
                .Where(p => (p.IsNestedObject || p.IsCollectionItemNested) && !string.IsNullOrEmpty(p.NestedTypeFullName))
                .ToList();

             foreach (var prop in nestedProps)
             {
                 var fullTypeName = prop.NestedTypeFullName!;
                 var simpleName = prop.NestedTypeName!;

                 // Avoid cycles
                 if (analyzedTypes.Contains(fullTypeName)) continue;
                 
                 // If already in target list, skip
                 if (targetNestedTypes.ContainsKey(fullTypeName)) continue;

                 // Try to find the symbol
                 INamedTypeSymbol? nestedTypeSymbol = null;
                 
                 // Primary: try by full metadata name (works for non-generic types)
                 nestedTypeSymbol = semanticModel.Compilation.GetTypeByMetadataName(fullTypeName);
                 
                 // Fallback: for generic instantiations (e.g. "MyNS.Wrapper<string>"), GetTypeByMetadataName
                 // returns null because it expects backtick notation ("MyNS.Wrapper`1"). Use the symbol
                 // captured during property analysis instead.
                 if (nestedTypeSymbol == null)
                     symbolCache.TryGetValue(fullTypeName, out nestedTypeSymbol);

                 if (nestedTypeSymbol == null)
                 {
                     diagnostics?.Add(new BLiteDiagnostic(
                         "BLITE001",
                         $"BLite mapper generator: could not resolve nested type '{fullTypeName}' (referenced at depth {currentDepth}). " +
                         $"Ensure the assembly containing this type is referenced. No mapper will be generated for it.",
                         isError: true));
                     continue;
                 }

                 analyzedTypes.Add(fullTypeName);

                 var nestedInfo = new NestedTypeInfo
                 {
                     Name = simpleName,
                     Namespace = nestedTypeSymbol.ContainingNamespace.ToDisplayString(),
                     FullTypeName = fullTypeName,
                     Depth = currentDepth
                 };

                 // Analyze properties of this nested type (pass symbolCache for further generics)
                 AnalyzeProperties(nestedTypeSymbol, nestedInfo.Properties, symbolCache);
                 
                 // Constructor selection for nested type
                 var nestedHasRequired = nestedInfo.Properties.Any(p => p.HasCSharpRequiredKeyword);
                 var (nestedCtorParams, nestedCtorIsPublic) = SelectConstructor(nestedTypeSymbol, nestedInfo.Properties, nestedHasRequired);
                 nestedInfo.SelectedConstructorParameters = nestedCtorParams;
                 nestedInfo.SelectedConstructorIsPublic = nestedCtorIsPublic;
                 nestedInfo.HasPrivateOrNoConstructor = nestedCtorParams == null || !nestedCtorIsPublic;
                 nestedInfo.HasPrivateSetters = nestedInfo.Properties.Any(p => (!p.HasPublicSetter && p.HasAnySetter) || p.HasInitOnlySetter);
                 
                 targetNestedTypes[fullTypeName] = nestedInfo;

                 // Recurse
                 AnalyzeNestedTypesRecursive(nestedInfo.Properties, nestedInfo.NestedTypes, semanticModel, analyzedTypes, currentDepth + 1, maxDepth, symbolCache, diagnostics);
             }
        }

        /// <summary>
        /// Selects the best constructor for deserialization using a priority queue:
        /// protected (0-param) &gt; protected (N-param) &gt; public (0-param) &gt; public (N-param) &gt; private (0-param) &gt; private (N-param).
        /// For N-param ctors, all parameters must match an entity property by name (case-insensitive).
        /// Returns (null, false) when no viable ctor is found — caller should fall back to GetUninitializedObject.
        /// </summary>
        private static (List<ConstructorParameterInfo>? selectedParams, bool isPublic) SelectConstructor(
            INamedTypeSymbol typeSymbol,
            IReadOnlyList<PropertyInfo> properties,
            bool hasRequiredMembers)
        {
            var propsByLower = new System.Collections.Generic.Dictionary<string, PropertyInfo>();
            foreach (var p in properties)
                propsByLower[p.Name.ToLowerInvariant()] = p;

            var candidates = typeSymbol.Constructors
                .Where(c => !c.IsStatic
                    && c.DeclaredAccessibility is
                        Accessibility.Public or
                        Accessibility.Protected or
                        Accessibility.ProtectedOrInternal or
                        Accessibility.ProtectedAndInternal or
                        Accessibility.Internal or
                        Accessibility.Private)
                .OrderBy(c => VisibilityPriority(c.DeclaredAccessibility))
                .ThenBy(c => c.Parameters.Length)
                .ToList();

            foreach (var ctor in candidates)
            {
                bool isPublicCtor = ctor.DeclaredAccessibility == Accessibility.Public;

                if (ctor.Parameters.Length == 0)
                {
                    // For public parameterless ctors on types with required members, avoid emitting
                    // `new T()` at the call site (CS9035). Mark as non-public so code generator uses
                    // [UnsafeAccessor(Constructor)] (NET8+) or Activator.CreateInstance (netstandard2.1).
                    bool effectivelyPublic = isPublicCtor && !hasRequiredMembers;
                    return (new List<ConstructorParameterInfo>(), effectivelyPublic);
                }

                // N-param: try to match every parameter to a property by name (case-insensitive).
                var matched = new List<ConstructorParameterInfo>();
                bool allMatched = true;
                foreach (var param in ctor.Parameters)
                {
                    if (!propsByLower.TryGetValue(param.Name.ToLowerInvariant(), out var matchedProp))
                    {
                        allMatched = false;
                        break;
                    }
                    matched.Add(new ConstructorParameterInfo(
                        param.Name,
                        SyntaxHelper.GetTypeName(param.Type),
                        matchedProp.Name,
                        SyntaxHelper.IsNullableType(param.Type)));
                }
                if (!allMatched) continue;

                // For public N-param ctors: if any required member is NOT among the ctor params,
                // emitting `new T(p1, p2)` still triggers CS9035 for the unset required member.
                bool hasUnmatchedRequired = hasRequiredMembers && properties.Any(p =>
                    p.HasCSharpRequiredKeyword &&
                    !matched.Any(m => string.Equals(m.MatchedPropertyName, p.Name, System.StringComparison.Ordinal)));
                bool effectivelyPublicNParam = isPublicCtor && !hasUnmatchedRequired;
                return (matched, effectivelyPublicNParam);
            }

            return (null, false); // No viable constructor found
        }

        private static int VisibilityPriority(Accessibility a) => a switch
        {
            Accessibility.Protected
                or Accessibility.ProtectedOrInternal
                or Accessibility.ProtectedAndInternal => 0,
            Accessibility.Public => 1,
            Accessibility.Internal => 2,
            Accessibility.Private => 3,
            _ => 4
        };
    }
}
