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
            
            // Analyze properties of the root entity
            AnalyzeProperties(entityType, entityInfo.Properties);

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
            var sourceProps = typeSymbol.GetMembers()
                .OfType<IPropertySymbol>()
                .Where(p => p.DeclaredAccessibility == Accessibility.Public && !p.IsStatic);
                
            foreach (var prop in sourceProps)
            {
                if (AttributeHelper.ShouldIgnore(prop))
                    continue;

                var propInfo = new PropertyInfo
                {
                    Name = prop.Name,
                    TypeName = SyntaxHelper.GetTypeName(prop.Type),
                    BsonFieldName = AttributeHelper.GetAttributeStringValue(prop, "BsonProperty") ?? 
                                    AttributeHelper.GetAttributeStringValue(prop, "JsonPropertyName") ?? 
                                    prop.Name.ToLowerInvariant(),
                    IsNullable = SyntaxHelper.IsNullableType(prop.Type),
                    IsKey = AttributeHelper.IsKey(prop),
                    
                    HasPublicSetter = prop.SetMethod?.DeclaredAccessibility == Accessibility.Public,
                    HasInitOnlySetter = prop.SetMethod?.IsInitOnly == true,
                    BackingFieldName = (prop.SetMethod?.DeclaredAccessibility != Accessibility.Public) 
                        ? $"<{prop.Name}>k__BackingField" 
                        : null
                };

                // Check for Collection
                if (SyntaxHelper.IsCollectionType(prop.Type, out var itemType))
                {
                    propInfo.IsCollection = true;
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
                    }
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
