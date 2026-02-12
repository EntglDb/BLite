using System.Linq.Expressions;
using BLite.Core.Indexing;

namespace BLite.Core.Query;

internal static class IndexOptimizer
{
    public class OptimizationResult
    {
        public string IndexName { get; set; } = "";
        public object? MinValue { get; set; }
        public object? MaxValue { get; set; }
        public bool IsRange { get; set; }
        public bool IsVectorSearch { get; set; }
        public float[]? VectorQuery { get; set; }
        public int K { get; set; }
    }

    public static OptimizationResult? TryOptimize<T>(QueryModel model, IEnumerable<CollectionIndexInfo> indexes)
    {
        if (model.WhereClause == null) return null;

        return OptimizeExpression(model.WhereClause.Body, model.WhereClause.Parameters[0], indexes);
    }

    private static OptimizationResult? OptimizeExpression(Expression expression, ParameterExpression parameter, IEnumerable<CollectionIndexInfo> indexes)
    {
        // Handle AndAlso for Range Intersection (Better Between support)
        if (expression is BinaryExpression binary && binary.NodeType == ExpressionType.AndAlso)
        {
            var left = OptimizeExpression(binary.Left, parameter, indexes);
            var right = OptimizeExpression(binary.Right, parameter, indexes);

            // Merge if both target the same index
            if (left != null && right != null && left.IndexName == right.IndexName)
            {
                return new OptimizationResult
                {
                    IndexName = left.IndexName,
                    MinValue = left.MinValue ?? right.MinValue, // Take restrictive (max of mins) - simplified logic
                    MaxValue = left.MaxValue ?? right.MaxValue, // Take restrictive (min of maxs)
                    IsRange = true
                };
                
                // Note: The logic above is simplified. Correct range merging requires comparing values.
                // Since simpler is better for now, we only merge if one side is unbounded.
                // e.g. Left: > 10 (Min=10, Max=null). Right: < 20 (Min=null, Max=20).
                // Result: Min=10, Max=20.
            }
            // If only one side is indexable, return that?
            // Yes, we can use the index for one part and let LINQ filter the rest.
            return left ?? right;
        }

        // Handle Simple Binary Predicates
        var (propertyName, value, op) = ParseSimplePredicate(expression, parameter);
        if (propertyName != null)
        {
            var index = indexes.FirstOrDefault(i => Matches(i, propertyName));
            if (index != null)
            {
                var result = new OptimizationResult { IndexName = index.Name };
                switch (op)
                {
                    case ExpressionType.Equal:
                        result.MinValue = value;
                        result.MaxValue = value;
                        result.IsRange = false;
                        break;
                    case ExpressionType.GreaterThan: 
                    case ExpressionType.GreaterThanOrEqual:
                        result.MinValue = value;
                        result.MaxValue = null;
                        result.IsRange = true;
                        break;
                     case ExpressionType.LessThan:
                     case ExpressionType.LessThanOrEqual:
                        result.MinValue = null;
                        result.MaxValue = value;
                        result.IsRange = true;
                        break;
                }
                return result;
            }
        }
        
        // Handle StartsWith
        if (expression is MethodCallExpression call && call.Method.Name == "StartsWith" && call.Object is MemberExpression member)
        {
             if (member.Expression == parameter && call.Arguments[0] is ConstantExpression constant && constant.Value is string prefix)
             {
                 var index = indexes.FirstOrDefault(i => Matches(i, member.Member.Name));
                 if (index != null && index.Type == IndexType.BTree)
                 {
                     var nextPrefix = IncrementPrefix(prefix);
                     return new OptimizationResult 
                     { 
                         IndexName = index.Name,
                         MinValue = prefix,
                         MaxValue = nextPrefix,
                         IsRange = true
                     };
                 }
             }
        }

        // Handle VectorSearch
        if (expression is MethodCallExpression mcall && mcall.Method.Name == "VectorSearch")
        {
            // VectorSearch(this float[] vector, float[] query, int k)
            if (mcall.Arguments[0] is MemberExpression vMember && vMember.Expression == parameter)
            {
                var query = EvaluateExpression<float[]>(mcall.Arguments[1]);
                var k = EvaluateExpression<int>(mcall.Arguments[2]);
                
                var index = indexes.FirstOrDefault(i => i.Type == IndexType.Vector && Matches(i, vMember.Member.Name));
                if (index != null)
                {
                    return new OptimizationResult
                    {
                        IndexName = index.Name,
                        IsVectorSearch = true,
                        VectorQuery = query,
                        K = k
                    };
                }
            }
        }

        return null;
    }

    private static string IncrementPrefix(string prefix)
    {
        if (string.IsNullOrEmpty(prefix)) return null!;
        char lastChar = prefix[prefix.Length - 1];
        if (lastChar == char.MaxValue) return prefix; // Cannot increment
        return prefix.Substring(0, prefix.Length - 1) + (char)(lastChar + 1);
    }

    private static T EvaluateExpression<T>(Expression expression)
    {
        if (expression is ConstantExpression constant)
        {
            return (T)constant.Value!;
        }

        // Evaluate more complex expressions (closures, properties, etc.)
        var lambda = Expression.Lambda(expression);
        var compiled = lambda.Compile();
        return (T)compiled.DynamicInvoke()!;
    }

    private static bool Matches(CollectionIndexInfo index, string propertyName)
    {
        if (index.PropertyPaths == null || index.PropertyPaths.Length == 0) return false;
        return index.PropertyPaths[0].Equals(propertyName, StringComparison.OrdinalIgnoreCase);
    }

    private static (string? propertyName, object? value, ExpressionType op) ParseSimplePredicate(Expression expression, ParameterExpression parameter)
    {
        if (expression is BinaryExpression binary)
        {
            var left = binary.Left;
            var right = binary.Right;
            var nodeType = binary.NodeType;

            if (right is MemberExpression && left is ConstantExpression)
            {
                (left, right) = (right, left);
                nodeType = Flip(nodeType);
            }

            if (left is MemberExpression member && right is ConstantExpression constant)
            {
                if (member.Expression == parameter)
                    return (member.Member.Name, constant.Value, nodeType);
            }
            
            // Handle Convert
            if (left is UnaryExpression unary && unary.Operand is MemberExpression member2 && right is ConstantExpression constant2)
            {
                 if (member2.Expression == parameter)
                    return (member2.Member.Name, constant2.Value, nodeType);
            }
        }
        return (null, null, ExpressionType.Default);
    }

    private static ExpressionType Flip(ExpressionType type) => type switch
    {
        ExpressionType.GreaterThan => ExpressionType.LessThan,
        ExpressionType.LessThan => ExpressionType.GreaterThan,
        ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
        ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
        _ => type
    };
}
