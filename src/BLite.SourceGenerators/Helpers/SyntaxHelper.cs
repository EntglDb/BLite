using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace BLite.SourceGenerators.Helpers
{
    public static class SyntaxHelper
    {
        public static bool InheritsFrom(INamedTypeSymbol symbol, string baseTypeName)
        {
            var current = symbol.BaseType;
            while (current != null)
            {
                if (current.Name == baseTypeName)
                    return true;
                current = current.BaseType;
            }
            return false;
        }

        public static List<InvocationExpressionSyntax> FindMethodInvocations(SyntaxNode node, string methodName)
        {
            return node.DescendantNodes()
                .OfType<InvocationExpressionSyntax>()
                .Where(invocation =>
                {
                    if (invocation.Expression is MemberAccessExpressionSyntax memberAccess)
                    {
                        return memberAccess.Name.Identifier.Text == methodName;
                    }
                    return false;
                })
                .ToList();
        }

        public static string? GetGenericTypeArgument(InvocationExpressionSyntax invocation)
        {
            if (invocation.Expression is MemberAccessExpressionSyntax memberAccess &&
                memberAccess.Name is GenericNameSyntax genericName &&
                genericName.TypeArgumentList.Arguments.Count > 0)
            {
                return genericName.TypeArgumentList.Arguments[0].ToString();
            }
            return null;
        }
        
        public static string GetFullName(INamedTypeSymbol symbol)
        {
            return symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)
                .Replace("global::", "");
        }

        public static string GetTypeName(ITypeSymbol type)
        {
            if (type is INamedTypeSymbol namedType && 
                namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T)
            {
                var underlyingType = namedType.TypeArguments[0];
                return GetTypeName(underlyingType) + "?";
            }
            
            if (type is IArrayTypeSymbol arrayType)
            {
                return GetTypeName(arrayType.ElementType) + "[]";
            }

            if (type is INamedTypeSymbol nt && nt.IsTupleType)
            {
                return type.ToDisplayString();
            }
            
            return type.Name;
        }

        public static bool IsNullableType(ITypeSymbol type)
        {
            if (type is INamedTypeSymbol namedType && 
                namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T)
            {
                return true;
            }
            return type.NullableAnnotation == NullableAnnotation.Annotated;
        }

        public static bool IsCollectionType(ITypeSymbol type, out ITypeSymbol? itemType)
        {
            itemType = null;
            
            if (type is IArrayTypeSymbol arrayType)
            {
                itemType = arrayType.ElementType;
                return true;
            }
            
            if (type is INamedTypeSymbol namedType && namedType.IsGenericType)
            {
                var typeDefName = namedType.OriginalDefinition.ToDisplayString();
                
                if (typeDefName.StartsWith("System.Collections.Generic.List<") ||
                    typeDefName.StartsWith("System.Collections.Generic.IList<") ||
                    typeDefName.StartsWith("System.Collections.Generic.ICollection<") ||
                    typeDefName.StartsWith("System.Collections.Generic.IEnumerable<"))
                {
                    itemType = namedType.TypeArguments[0];
                    return true;
                }
            }
            return false;
        }

        public static bool IsPrimitiveType(ITypeSymbol type)
        {
            if (type is INamedTypeSymbol namedType && 
                namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T)
            {
                type = namedType.TypeArguments[0];
            }
            
            if (type.SpecialType != SpecialType.None && type.SpecialType != SpecialType.System_Object)
                return true;
            
            var typeName = type.Name;
            if (typeName == "Guid" || typeName == "DateTime" || typeName == "DateTimeOffset" || 
                typeName == "TimeSpan" || typeName == "Decimal" || typeName == "ObjectId")
                return true;
                
            if (type.TypeKind == TypeKind.Enum)
                return true;

            if (type is INamedTypeSymbol nt && nt.IsTupleType)
                return true;
                
            return false;
        }

        public static bool IsNestedObjectType(ITypeSymbol type)
        {
            if (IsPrimitiveType(type)) return false;
            if (type.SpecialType == SpecialType.System_String) return false;
            if (IsCollectionType(type, out _)) return false;
            if (type.SpecialType == SpecialType.System_Object) return false;
            if (type is INamedTypeSymbol nt && nt.IsTupleType) return false;
            
            return type.TypeKind == TypeKind.Class || type.TypeKind == TypeKind.Struct;
        }
    }
}
