using System.Linq.Expressions;
using BLite.Bson;

namespace BLite.Core.Query;

internal static class BsonExpressionEvaluator
{
    // Types that BsonSpanReader can directly compare.
    // If the value extracted from a closure is not one of these, we skip BSON-level
    // optimization and let the in-memory LINQ filter handle correctness.
    private static readonly HashSet<Type> _knownBsonPrimitives =
    [
        typeof(int), typeof(long), typeof(double), typeof(decimal),
        typeof(bool), typeof(string), typeof(DateTime), typeof(DateTimeOffset),
        typeof(ObjectId)
    ];

    public static BsonReaderPredicate? TryCompile<T>(LambdaExpression expression)
        => TryCompileBody(expression.Body, expression.Parameters[0]);

    /// <summary>
    /// Recursively compiles an expression node into a <see cref="BsonReaderPredicate"/>.
    /// Handles:
    /// - <c>AndAlso</c>: skips sides that don't touch the parameter (e.g. null-guard closures)
    /// - <c>.Equals()</c> method calls: treated as equality
    /// - Closure captures: evaluated at plan-time via <see cref="TryEvaluate"/>
    /// </summary>
    private static BsonReaderPredicate? TryCompileBody(Expression body, ParameterExpression parameter)
    {
        // ── AndAlso ────────────────────────────────────────────────────────────
        // Pattern: (item.Id != null) && e.Id.Equals(item.Id)
        // The null-guard left side doesn't touch the lambda parameter — skip it.
        if (body is BinaryExpression andAlso && andAlso.NodeType == ExpressionType.AndAlso)
        {
            bool leftTouches  = TouchesParameter(andAlso.Left,  parameter);
            bool rightTouches = TouchesParameter(andAlso.Right, parameter);

            if (leftTouches && !rightTouches)  return TryCompileBody(andAlso.Left,  parameter);
            if (rightTouches && !leftTouches)  return TryCompileBody(andAlso.Right, parameter);

            if (leftTouches && rightTouches)
            {
                var lp = TryCompileBody(andAlso.Left,  parameter);
                var rp = TryCompileBody(andAlso.Right, parameter);
                if (lp != null && rp != null) return reader => lp(reader) && rp(reader);
                return lp ?? rp;
            }

            return null;
        }

        // ── .Equals() method call ──────────────────────────────────────────────
        // Pattern: e.Prop.Equals(closureVar)  or  e.Prop.Equals(constant)
        if (body is MethodCallExpression methodCall &&
            methodCall.Method.Name == "Equals" &&
            methodCall.Arguments.Count == 1 &&
            methodCall.Object is MemberExpression equalsOnMember &&
            equalsOnMember.Expression == parameter)
        {
            var propertyName = equalsOnMember.Member.Name.ToLowerInvariant();
            if (propertyName == "id") propertyName = "_id";

            var (ok, value) = TryEvaluate(methodCall.Arguments[0]);
            if (ok && IsKnownBsonPrimitive(value?.GetType()))
                return CreatePredicate(propertyName, value, ExpressionType.Equal);

            return null;
        }

        // ── Simple binary: e.Prop op constant (or e.Prop op closureCapture) ───
        if (body is BinaryExpression binary)
        {
            var left = binary.Left;
            var right = binary.Right;
            var nodeType = binary.NodeType;

            // Normalize: Ensure Property is on Left
            if (right is MemberExpression rMember && rMember.Expression == parameter &&
                !IsDirectParameterAccess(left, parameter))
            {
                (left, right) = (right, left);
                nodeType = Flip(nodeType);
            }

            if (left is MemberExpression member && member.Expression == parameter)
            {
                var propertyName = member.Member.Name.ToLowerInvariant();
                if (propertyName == "id") propertyName = "_id";

                // Right side: ConstantExpression or closure capture (any non-parameter expr)
                var (ok, value) = TryEvaluate(right);
                if (ok && IsKnownBsonPrimitive(value?.GetType()))
                    return CreatePredicate(propertyName, value, nodeType);
            }
        }

        return null;
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static bool IsDirectParameterAccess(Expression expr, ParameterExpression p)
        => expr is MemberExpression m && m.Expression == p;

    /// <summary>
    /// Returns <c>true</c> if any node in <paramref name="expression"/> references <paramref name="parameter"/>.
    /// </summary>
    private static bool TouchesParameter(Expression expression, ParameterExpression parameter)
    {
        var visitor = new ParameterTouchVisitor(parameter);
        visitor.Visit(expression);
        return visitor.Found;
    }

    private sealed class ParameterTouchVisitor : ExpressionVisitor
    {
        private readonly ParameterExpression _target;
        public bool Found { get; private set; }
        public ParameterTouchVisitor(ParameterExpression target) => _target = target;
        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == _target) Found = true;
            return node;
        }
    }

    /// <summary>
    /// Evaluates an expression to a concrete value at query-plan time.
    /// Handles both <see cref="ConstantExpression"/> and closure captures (e.g. <c>value(closure).field</c>).
    /// Returns <c>(false, null)</c> if evaluation fails.
    /// </summary>
    private static (bool Ok, object? Value) TryEvaluate(Expression expression)
    {
        try
        {
            if (expression is ConstantExpression constant)
                return (true, constant.Value);

            var lambda = Expression.Lambda(expression);
            return (true, lambda.Compile().DynamicInvoke());
        }
        catch
        {
            return (false, null);
        }
    }

    private static bool IsKnownBsonPrimitive(Type? type)
    {
        if (type == null) return false; // null value: skip BSON optimization, let in-memory LINQ handle it
        type = Nullable.GetUnderlyingType(type) ?? type;
        return _knownBsonPrimitives.Contains(type);
    }

    private static ExpressionType Flip(ExpressionType type) => type switch
    {
        ExpressionType.GreaterThan => ExpressionType.LessThan,
        ExpressionType.LessThan => ExpressionType.GreaterThan,
        ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
        ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
        _ => type
    };

    private static BsonReaderPredicate? CreatePredicate(string propertyName, object? targetValue, ExpressionType op)
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
                    
                    var name = reader.ReadElementHeader();
                    
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
