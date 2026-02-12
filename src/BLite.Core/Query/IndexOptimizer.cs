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

        public bool IsSpatialSearch { get; set; }
        public (double Latitude, double Longitude) SpatialPoint { get; set; }
        public double RadiusKm { get; set; }
        public (double Latitude, double Longitude) SpatialMin { get; set; }
        public (double Latitude, double Longitude) SpatialMax { get; set; }
        public SpatialQueryType SpatialType { get; set; }
    }

    public enum SpatialQueryType { Near, Within }

    public static OptimizationResult? TryOptimize<T>(QueryModel model, IEnumerable<CollectionIndexInfo> indexes)
    {
        if (model.WhereClause == null) return null;

        return OptimizeExpression(model.WhereClause.Body, model.WhereClause.Parameters[0], indexes);
    }

    private static OptimizationResult? OptimizeExpression(Expression expression, ParameterExpression parameter, IEnumerable<CollectionIndexInfo> indexes)
    {
        // ... (Existing AndAlso logic remains the same) ...
        if (expression is BinaryExpression binary && binary.NodeType == ExpressionType.AndAlso)
        {
            var left = OptimizeExpression(binary.Left, parameter, indexes);
            var right = OptimizeExpression(binary.Right, parameter, indexes);

            if (left != null && right != null && left.IndexName == right.IndexName)
            {
                return new OptimizationResult
                {
                    IndexName = left.IndexName,
                    MinValue = left.MinValue ?? right.MinValue,
                    MaxValue = left.MaxValue ?? right.MaxValue,
                    IsRange = true
                };
            }
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

        // Handle Method Calls (VectorSearch, Near, Within)
        if (expression is MethodCallExpression mcall)
        {
            // VectorSearch(this float[] vector, float[] query, int k)
            if (mcall.Method.Name == "VectorSearch" && mcall.Arguments[0] is MemberExpression vMember && vMember.Expression == parameter)
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
            
            // Near(this (double, double) point, (double, double) center, double radiusKm)
            if (mcall.Method.Name == "Near" && mcall.Arguments[0] is MemberExpression nMember && nMember.Expression == parameter)
            {
                var center = EvaluateExpression<(double, double)>(mcall.Arguments[1]);
                var radius = EvaluateExpression<double>(mcall.Arguments[2]);

                var index = indexes.FirstOrDefault(i => i.Type == IndexType.Spatial && Matches(i, nMember.Member.Name));
                if (index != null)
                {
                    return new OptimizationResult
                    {
                        IndexName = index.Name,
                        IsSpatialSearch = true,
                        SpatialType = SpatialQueryType.Near,
                        SpatialPoint = center,
                        RadiusKm = radius
                    };
                }
            }

            // Within(this (double, double) point, (double, double) min, (double, double) max)
            if (mcall.Method.Name == "Within" && mcall.Arguments[0] is MemberExpression wMember && wMember.Expression == parameter)
            {
                var min = EvaluateExpression<(double, double)>(mcall.Arguments[1]);
                var max = EvaluateExpression<(double, double)>(mcall.Arguments[2]);

                var index = indexes.FirstOrDefault(i => i.Type == IndexType.Spatial && Matches(i, wMember.Member.Name));
                if (index != null)
                {
                    return new OptimizationResult
                    {
                        IndexName = index.Name,
                        IsSpatialSearch = true,
                        SpatialType = SpatialQueryType.Within,
                        SpatialMin = min,
                        SpatialMax = max
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
