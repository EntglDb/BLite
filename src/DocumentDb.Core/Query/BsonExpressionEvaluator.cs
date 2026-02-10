using System.Linq.Expressions;
using DocumentDb.Bson;

namespace DocumentDb.Core.Query;

internal static class BsonExpressionEvaluator
{
    public static Func<BsonSpanReader, bool>? TryCompile<T>(LambdaExpression expression)
    {
        // Simple optimization for: x => x.Prop op Constant
        if (expression.Body is BinaryExpression binary)
        {
            var left = binary.Left;
            var right = binary.Right;
            var nodeType = binary.NodeType;

            // Normalize: Ensure Property is on Left
            if (right is MemberExpression && left is ConstantExpression)
            {
                (left, right) = (right, left);
                // Flip operator
                nodeType = Flip(nodeType);
            }

            if (left is MemberExpression member && right is ConstantExpression constant)
            {
                // Check if member is property of parameter
                if (member.Expression == expression.Parameters[0])
                {
                    var propertyName = member.Member.Name;
                    var value = constant.Value;
                    
                    // Handle Id mapping?
                    // If property is "Id", Bson field is "_id"
                    if (propertyName == "Id") propertyName = "_id";

                    return CreatePredicate(propertyName, value, nodeType);
                }
            }
        }

        return null;
    }

    private static ExpressionType Flip(ExpressionType type) => type switch
    {
        ExpressionType.GreaterThan => ExpressionType.LessThan,
        ExpressionType.LessThan => ExpressionType.GreaterThan,
        ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
        ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
        _ => type
    };

    private static Func<BsonSpanReader, bool>? CreatePredicate(string propertyName, object? targetValue, ExpressionType op)
    {
        // We need to return a delegate that searches for propertyName in BsonSpanReader and compares
        
        return reader => 
        {
            try 
            {
                reader.ReadDocumentSize();
                while (reader.Remaining > 0)
                {
                    var type = reader.ReadBsonType();
                    if (type == 0) break;
                    
                    var name = reader.ReadCString();
                    
                    if (name == propertyName)
                    {
                        // Found it! Read value and compare
                        return Compare(ref reader, type, targetValue, op);
                    }
                    
                    reader.SkipValue(type);
                }
            }
            catch 
            {
                return false;
            }
            return false; // Not found
        };
    }

    private static bool Compare(ref BsonSpanReader reader, BsonType type, object? target, ExpressionType op)
    {
        // This is complex because we need to handle types.
        // For MVP, handle Int32, String, ObjectId
        
        if (type == BsonType.Int32)
        {
            var val = reader.ReadInt32();
            if (target is int targetInt)
            {
                return op switch
                {
                    ExpressionType.Equal => val == targetInt,
                    ExpressionType.NotEqual => val != targetInt,
                    ExpressionType.GreaterThan => val > targetInt,
                    ExpressionType.GreaterThanOrEqual => val >= targetInt,
                    ExpressionType.LessThan => val < targetInt,
                    ExpressionType.LessThanOrEqual => val <= targetInt,
                    _ => false
                };
            }
        }
        else if (type == BsonType.String)
        {
            var val = reader.ReadString();
            if (target is string targetStr)
            {
                var cmp = string.Compare(val, targetStr, StringComparison.Ordinal);
                return op switch
                {
                    ExpressionType.Equal => cmp == 0,
                    ExpressionType.NotEqual => cmp != 0,
                    ExpressionType.GreaterThan => cmp > 0,
                    ExpressionType.GreaterThanOrEqual => cmp >= 0,
                    ExpressionType.LessThan => cmp < 0,
                    ExpressionType.LessThanOrEqual => cmp <= 0,
                    _ => false
                };
            }
        }
        else if (type == BsonType.ObjectId && target is ObjectId targetId)
        {
            var val = reader.ReadObjectId();
             // ObjectId only supports Equal check easily unless we implement complex logic
            if (op == ExpressionType.Equal) return val.Equals(targetId);
            if (op == ExpressionType.NotEqual) return !val.Equals(targetId);
        }
        
        return false;
    }
}
