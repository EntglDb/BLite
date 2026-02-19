using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
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

        public static string? GetPropertyName(ExpressionSyntax? expression)
        {
            if (expression == null) return null;
            if (expression is LambdaExpressionSyntax lambda)
            {
                return GetPropertyName(lambda.Body as ExpressionSyntax);
            }
            if (expression is MemberAccessExpressionSyntax memberAccess)
            {
                return memberAccess.Name.Identifier.Text;
            }
            if (expression is PrefixUnaryExpressionSyntax prefixUnary && prefixUnary.Operand is MemberAccessExpressionSyntax prefixMember)
            {
                return prefixMember.Name.Identifier.Text;
            }
             if (expression is PostfixUnaryExpressionSyntax postfixUnary && postfixUnary.Operand is MemberAccessExpressionSyntax postfixMember)
            {
                return postfixMember.Name.Identifier.Text;
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
            
            return type.ToDisplayString();
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
            
            // Exclude string (it's IEnumerable<char> but not a collection for our purposes)
            if (type.SpecialType == SpecialType.System_String)
                return false;
            
            // Handle arrays
            if (type is IArrayTypeSymbol arrayType)
            {
                itemType = arrayType.ElementType;
                return true;
            }
            
            // Check if the type itself is IEnumerable<T>
            if (type is INamedTypeSymbol namedType && namedType.IsGenericType)
            {
                var typeDefName = namedType.OriginalDefinition.ToDisplayString();
                if (typeDefName == "System.Collections.Generic.IEnumerable<T>" && namedType.TypeArguments.Length == 1)
                {
                    itemType = namedType.TypeArguments[0];
                    return true;
                }
            }
            
            // Check if the type implements IEnumerable<T> by walking all interfaces
            var enumerableInterface = type.AllInterfaces
                .FirstOrDefault(i => i.IsGenericType && 
                                     i.OriginalDefinition.ToDisplayString() == "System.Collections.Generic.IEnumerable<T>");
            
            if (enumerableInterface != null && enumerableInterface.TypeArguments.Length == 1)
            {
                itemType = enumerableInterface.TypeArguments[0];
                return true;
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
                typeName == "TimeSpan" || typeName == "DateOnly" || typeName == "TimeOnly" || 
                typeName == "Decimal" || typeName == "ObjectId")
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

        public static bool HasBackingField(IPropertySymbol property)
        {
            // Auto-properties have compiler-generated backing fields
            // Check if there's a field with the pattern <PropertyName>k__BackingField
            return property.ContainingType.GetMembers()
                .OfType<IFieldSymbol>()
                .Any(f => f.AssociatedSymbol?.Equals(property, SymbolEqualityComparer.Default) == true);
        }
    }
}
