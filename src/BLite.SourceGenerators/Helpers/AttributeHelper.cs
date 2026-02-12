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
            var attr = GetAttribute(symbol, attributeName);
            if (attr != null && attr.ConstructorArguments.Length > 0)
            {
                return attr.ConstructorArguments[0].Value?.ToString();
            }

            return null;
        }

        public static int? GetAttributeIntValue(ISymbol symbol, string attributeName)
        {
            var attr = GetAttribute(symbol, attributeName);
            if (attr != null && attr.ConstructorArguments.Length > 0)
            {
                 if (attr.ConstructorArguments[0].Value is int val) return val;
            }

            return null;
        }

        public static double? GetAttributeDoubleValue(ISymbol symbol, string attributeName)
        {
            var attr = GetAttribute(symbol, attributeName);
            if (attr != null && attr.ConstructorArguments.Length > 0)
            {
                 if (attr.ConstructorArguments[0].Value is double val) return val;
                 if (attr.ConstructorArguments[0].Value is float fval) return (double)fval;
                 if (attr.ConstructorArguments[0].Value is int ival) return (double)ival;
            }

            return null;
        }

        public static string? GetNamedArgumentValue(AttributeData attr, string name)
        {
            return attr.NamedArguments.FirstOrDefault(a => a.Key == name).Value.Value?.ToString();
        }

        public static AttributeData? GetAttribute(ISymbol symbol, string attributeName)
        {
            return symbol.GetAttributes().FirstOrDefault(a => 
                a.AttributeClass != null && 
                (a.AttributeClass.Name == attributeName || 
                 a.AttributeClass.Name == attributeName + "Attribute"));
        }

        public static bool HasAttribute(ISymbol symbol, string attributeName)
        {
            return GetAttribute(symbol, attributeName) != null;
        }
    }
}
