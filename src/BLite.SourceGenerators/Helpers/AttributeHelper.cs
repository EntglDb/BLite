using System;
using System.Linq;
using Microsoft.CodeAnalysis;

namespace BLite.SourceGenerators.Helpers
{
    public static class AttributeHelper
    {
        public static bool ShouldIgnore(IPropertySymbol property)
        {
            return HasAttribute(property, "BsonIgnore") || 
                   HasAttribute(property, "JsonIgnore") ||
                   HasAttribute(property, "NotMapped");
        }

        public static bool IsKey(IPropertySymbol property)
        {
            return HasAttribute(property, "Key") || 
                   HasAttribute(property, "BsonId");
        }
        
        public static string? GetAttributeStringValue(ISymbol symbol, string attributeName)
        {
            var attr = symbol.GetAttributes().FirstOrDefault(a => 
                a.AttributeClass != null && 
                (a.AttributeClass.Name == attributeName || 
                 a.AttributeClass.Name == attributeName + "Attribute"));
            
            if (attr != null && attr.ConstructorArguments.Length > 0)
            {
                return attr.ConstructorArguments[0].Value?.ToString();
            }

            return null;
        }

        public static bool HasAttribute(ISymbol symbol, string attributeName)
        {
            return symbol.GetAttributes().Any(a => 
                a.AttributeClass != null && 
                (a.AttributeClass.Name == attributeName || 
                 a.AttributeClass.Name == attributeName + "Attribute"));
        }
    }
}
