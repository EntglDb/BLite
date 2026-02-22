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
        public static EntityInfo Analyze(INamedTypeSymbol entityType, SemanticModel semanticModel)
        {
            var entityInfo = new EntityInfo
            {
                Name = entityType.Name,
                Namespace = entityType.ContainingNamespace.ToDisplayString(),
                FullTypeName = SyntaxHelper.GetFullName(entityType),
                CollectionName = entityType.Name.ToLowerInvariant() + "s"
            };

            var tableAttr = AttributeHelper.GetAttribute(entityType, "Table");
            if (tableAttr != null)
            {
                var tableName = tableAttr.ConstructorArguments.Length > 0 ? tableAttr.ConstructorArguments[0].Value?.ToString() : null;
                var schema = AttributeHelper.GetNamedArgumentValue(tableAttr, "Schema");
                
                var collectionName = !string.IsNullOrEmpty(tableName) ? tableName! : entityInfo.Name;
                if (!string.IsNullOrEmpty(schema))
                {
                    collectionName = $"{schema}.{collectionName}";
                }
                entityInfo.CollectionName = collectionName;
            }
            
            // Analyze properties of the root entity
            AnalyzeProperties(entityType, entityInfo.Properties);

            // Check if entity needs reflection-based deserialization
            // Include properties with private setters or init-only setters (which can't be set outside initializers)
            entityInfo.HasPrivateSetters = entityInfo.Properties.Any(p => (!p.HasPublicSetter && p.HasAnySetter) || p.HasInitOnlySetter);
            
            // Check if entity has public parameterless constructor
            var hasPublicParameterlessConstructor = entityType.Constructors
                .Any(c => c.DeclaredAccessibility == Accessibility.Public && c.Parameters.Length == 0);
            entityInfo.HasPrivateOrNoConstructor = !hasPublicParameterlessConstructor;

            // Analyze nested types recursively
            // We use a dictionary for nested types to ensure uniqueness by name
            var analyzedTypes = new HashSet<string>();
            AnalyzeNestedTypesRecursive(entityInfo.Properties, entityInfo.NestedTypes, semanticModel, analyzedTypes, 1, 3);
            
            // Determine ID property
            // entityInfo.IdProperty is computed from Properties.FirstOrDefault(p => p.IsKey)
            
            if (entityInfo.IdProperty == null)
            {
                // Fallback to convention: property named "Id"
                var idProp = entityInfo.Properties.FirstOrDefault(p => p.Name == "Id");
                if (idProp != null)
                {
                    idProp.IsKey = true;
                }
            }
            
            // Check for AutoId (int/long keys)
            if (entityInfo.IdProperty != null)
            {
                var idType = entityInfo.IdProperty.TypeName.TrimEnd('?');
                if (idType == "int" || idType == "Int32" || idType == "long" || idType == "Int64")
                {
                    entityInfo.AutoId = true;
                }
            }
            
            return entityInfo;
        }

        private static void AnalyzeProperties(INamedTypeSymbol typeSymbol, List<PropertyInfo> properties)
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
                    
                    // Skip computed getter-only properties (no setter, no backing field)
                    bool isReadOnlyGetter = prop.SetMethod == null && !SyntaxHelper.HasBackingField(prop);
                    if (isReadOnlyGetter)
                        continue;

                var columnAttr = AttributeHelper.GetAttribute(prop, "Column");
                var bsonFieldName = AttributeHelper.GetAttributeStringValue(prop, "BsonProperty") ?? 
                                    AttributeHelper.GetAttributeStringValue(prop, "JsonPropertyName");

                if (bsonFieldName == null && columnAttr != null)
                {
                    bsonFieldName = columnAttr.ConstructorArguments.Length > 0 ? columnAttr.ConstructorArguments[0].Value?.ToString() : null;
                }
                
                    var propInfo = new PropertyInfo
                    {
                        Name = prop.Name,
                        TypeName = SyntaxHelper.GetTypeName(prop.Type),
                        BsonFieldName = bsonFieldName ?? prop.Name.ToLowerInvariant(),
                        ColumnTypeName = columnAttr != null ? AttributeHelper.GetNamedArgumentValue(columnAttr, "TypeName") : null,
                        IsNullable = SyntaxHelper.IsNullableType(prop.Type),
                        IsKey = AttributeHelper.IsKey(prop),
                        IsRequired = AttributeHelper.HasAttribute(prop, "Required"),
                        
                        HasPublicSetter = prop.SetMethod?.DeclaredAccessibility == Accessibility.Public,
                        HasInitOnlySetter = prop.SetMethod?.IsInitOnly == true,
                        HasAnySetter = prop.SetMethod != null,
                        IsReadOnlyGetter = isReadOnlyGetter,
                        BackingFieldName = (prop.SetMethod?.DeclaredAccessibility != Accessibility.Public) 
                            ? $"<{prop.Name}>k__BackingField" 
                            : null
                    };

                // MaxLength / MinLength
                propInfo.MaxLength = AttributeHelper.GetAttributeIntValue(prop, "MaxLength");
                propInfo.MinLength = AttributeHelper.GetAttributeIntValue(prop, "MinLength");
                
                var stringLengthAttr = AttributeHelper.GetAttribute(prop, "StringLength");
                if (stringLengthAttr != null)
                {
                    if (stringLengthAttr.ConstructorArguments.Length > 0 && stringLengthAttr.ConstructorArguments[0].Value is int max)
                        propInfo.MaxLength = max;
                    
                    var minLenStr = AttributeHelper.GetNamedArgumentValue(stringLengthAttr, "MinimumLength");
                    if (int.TryParse(minLenStr, out var min))
                        propInfo.MinLength = min;
                }

                // Range
                var rangeAttr = AttributeHelper.GetAttribute(prop, "Range");
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
                        propInfo.NestedTypeFullName = SyntaxHelper.GetFullName((INamedTypeSymbol)prop.Type);
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
            int maxDepth)
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
                 
                 // Try by full name
                 nestedTypeSymbol = semanticModel.Compilation.GetTypeByMetadataName(fullTypeName);
                 
                 // If not found, try to resolve via semantic model (might be in the same compilation)
                 if (nestedTypeSymbol == null)
                 {
                     // This is more complex, but usually fullTypeName from ToDisplayString() is traceable.
                     // For now, let's assume GetTypeByMetadataName works for fully qualified names.
                 }

                 if (nestedTypeSymbol == null) continue;

                 analyzedTypes.Add(fullTypeName);

                 var nestedInfo = new NestedTypeInfo
                 {
                     Name = simpleName,
                     Namespace = nestedTypeSymbol.ContainingNamespace.ToDisplayString(),
                     FullTypeName = fullTypeName,
                     Depth = currentDepth
                 };

                 // Analyze properties of this nested type
                 AnalyzeProperties(nestedTypeSymbol, nestedInfo.Properties);
                 targetNestedTypes[fullTypeName] = nestedInfo;

                 // Recurse
                 AnalyzeNestedTypesRecursive(nestedInfo.Properties, nestedInfo.NestedTypes, semanticModel, analyzedTypes, currentDepth + 1, maxDepth);
             }
        }
    }
}
