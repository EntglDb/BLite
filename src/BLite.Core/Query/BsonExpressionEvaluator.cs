using System.Linq.Expressions;
using System.Reflection;
using BLite.Bson;
using BLite.Core.Metadata;

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

    public static BsonReaderPredicate? TryCompile<T>(LambdaExpression expression, ValueConverterRegistry? registry = null)
        => TryCompileBody(expression.Body, expression.Parameters[0], registry);

    /// <summary>
    /// Recursively compiles an expression node into a <see cref="BsonReaderPredicate"/>.
    /// Handles:
    /// - <c>AndAlso</c>: skips sides that don't touch the parameter (e.g. null-guard closures)
    /// - <c>.Equals()</c> method calls: treated as equality
    /// - Closure captures: evaluated at plan-time via <see cref="TryEvaluate"/>
    /// </summary>
    private static BsonReaderPredicate? TryCompileBody(Expression body, ParameterExpression parameter, ValueConverterRegistry? registry = null)
    {
        // ── AndAlso ────────────────────────────────────────────────────────────
        // Pattern: (item.Id != null) && e.Id.Equals(item.Id)
        // The null-guard left side doesn't touch the lambda parameter — skip it.
        if (body is BinaryExpression andAlso && andAlso.NodeType == ExpressionType.AndAlso)
        {
            bool leftTouches  = TouchesParameter(andAlso.Left,  parameter);
            bool rightTouches = TouchesParameter(andAlso.Right, parameter);

            if (leftTouches && !rightTouches)  return TryCompileBody(andAlso.Left,  parameter, registry);
            if (rightTouches && !leftTouches)  return TryCompileBody(andAlso.Right, parameter, registry);

            if (leftTouches && rightTouches)
            {
                var lp = TryCompileBody(andAlso.Left,  parameter, registry);
                var rp = TryCompileBody(andAlso.Right, parameter, registry);
                if (lp != null && rp != null) return reader => lp(reader) && rp(reader);
                return lp ?? rp;
            }

            return null;
        }

        // ── Bare bool member: e => e.IsActive  →  IsActive == true ──────────────
        if (body is MemberExpression bareM &&
            bareM.Expression == parameter &&
            bareM.Type == typeof(bool))
        {
            var bsonName = bareM.Member.Name.ToLowerInvariant();
            if (bsonName == "id") bsonName = "_id";
            return CreatePredicate(bsonName, true, ExpressionType.Equal);
        }

        // ── Logical NOT on bool member: e => !e.IsActive  →  IsActive == false ─
        if (body is UnaryExpression { NodeType: ExpressionType.Not } notExpr &&
            notExpr.Operand is MemberExpression notM &&
            notM.Expression == parameter &&
            notM.Type == typeof(bool))
        {
            var bsonName = notM.Member.Name.ToLowerInvariant();
            if (bsonName == "id") bsonName = "_id";
            return CreatePredicate(bsonName, false, ExpressionType.Equal);
        }

        // ── .Equals() method call ──────────────────────────────────────────────
        // Pattern: e.Prop.Equals(closureVar)  or  e.Prop.Equals(constant)
        if (body is MethodCallExpression methodCall &&
            methodCall.Method.Name == "Equals" &&
            methodCall.Arguments.Count == 1 &&
            methodCall.Object is MemberExpression equalsOnMember &&
            equalsOnMember.Expression == parameter)
        {
            var fieldName   = equalsOnMember.Member.Name;
            var bsonName    = fieldName.ToLowerInvariant();
            if (bsonName == "id") bsonName = "_id";

            var (ok, value) = TryEvaluate(methodCall.Arguments[0]);
            if (ok && IsKnownBsonPrimitive(value?.GetType()))
                return CreatePredicate(bsonName, value, ExpressionType.Equal);

            // Try ValueObject → provider conversion
            if (ok && value != null &&
                registry?.TryConvert(fieldName, value, out var pv) == true &&
                IsKnownBsonPrimitive(pv?.GetType()))
                return CreatePredicate(bsonName, pv, ExpressionType.Equal);

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
                var fieldName = member.Member.Name;
                var bsonName  = fieldName.ToLowerInvariant();
                // "Id" and "_id" both refer to the BSON primary-key field.
                // The serializer writes the root-entity primary key as "_id" regardless of the
                // C# property name when the property is "Id".  Nested-entity mappers write "id".
                // Normalise both here so that predicates on x.Id always scan for "_id".
                if (bsonName == "id" || bsonName == "_id") bsonName = "_id";

                // Right side: ConstantExpression or closure capture (any non-parameter expr)
                var (ok, value) = TryEvaluate(right);
                if (ok && IsKnownBsonPrimitive(value?.GetType()))
                    return CreatePredicate(bsonName, value, nodeType);

                // Try ValueObject → provider conversion
                if (ok && value != null &&
                    registry?.TryConvert(fieldName, value, out var pv) == true &&
                    IsKnownBsonPrimitive(pv?.GetType()))
                    return CreatePredicate(bsonName, pv, nodeType);
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
    /// Fast path: walks <c>MemberExpression(ConstantExpression, ...)</c> chains using
    /// <see cref="FieldInfo"/>/<see cref="PropertyInfo"/> directly — no <c>Compile()</c>/<c>DynamicInvoke()</c>.
    /// Falls back to <c>Expression.Lambda.Compile()</c> only for genuinely complex sub-trees.
    /// Returns <c>(false, null)</c> if evaluation fails.
    /// </summary>
    private static (bool Ok, object? Value) TryEvaluate(Expression expression)
    {
        try
        {
            if (expression is ConstantExpression constant)
                return (true, constant.Value);

            if (TryWalkMemberChain(expression, out var walked))
                return (true, walked);

            // Rare fallback: arbitrary sub-expression (method calls, arithmetic, etc.)
            var lambda = Expression.Lambda(expression);
            return (true, lambda.Compile().DynamicInvoke());
        }
        catch
        {
            return (false, null);
        }
    }

    /// <summary>
    /// Walks a chain of <see cref="MemberExpression"/> nodes rooted in a <see cref="ConstantExpression"/>
    /// (i.e. a compiler-generated closure capture) and resolves the value via reflection —
    /// this is the common case for <c>x => x.Id == localVar</c> predicates.
    /// No lambda compilation or <c>DynamicInvoke</c> required.
    /// </summary>
    private static bool TryWalkMemberChain(Expression expr, out object? value)
    {
        var chain = new Stack<MemberInfo>();
        Expression? current = expr;

        while (current is MemberExpression me)
        {
            chain.Push(me.Member);
            current = me.Expression;
        }

        if (current is not ConstantExpression root)
        {
            value = null;
            return false;
        }

        object? obj = root.Value;
        foreach (var member in chain)
        {
            if (obj is null) { value = null; return true; }
            obj = member is FieldInfo fi
                ? fi.GetValue(obj)
                : ((PropertyInfo)member).GetValue(obj);
        }

        value = obj;
        return true;
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
        else if (type == BsonType.Int64)
        {
            var val = reader.ReadInt64();
            if (target is long targetLong)
            {
                return op switch
                {
                    ExpressionType.Equal => val == targetLong,
                    ExpressionType.NotEqual => val != targetLong,
                    ExpressionType.GreaterThan => val > targetLong,
                    ExpressionType.GreaterThanOrEqual => val >= targetLong,
                    ExpressionType.LessThan => val < targetLong,
                    ExpressionType.LessThanOrEqual => val <= targetLong,
                    _ => false
                };
            }
        }
        else if (type == BsonType.Double)
        {
            var val = reader.ReadDouble();
            double? targetDouble = target switch
            {
                double d => d,
                float f => f,
                decimal dec => (double)dec,
                _ => null
            };
            if (targetDouble.HasValue)
            {
                return op switch
                {
                    ExpressionType.Equal => val == targetDouble.Value,
                    ExpressionType.NotEqual => val != targetDouble.Value,
                    ExpressionType.GreaterThan => val > targetDouble.Value,
                    ExpressionType.GreaterThanOrEqual => val >= targetDouble.Value,
                    ExpressionType.LessThan => val < targetDouble.Value,
                    ExpressionType.LessThanOrEqual => val <= targetDouble.Value,
                    _ => false
                };
            }
        }
        else if (type == BsonType.Boolean)
        {
            var val = reader.ReadBoolean();
            if (target is bool targetBool)
            {
                return op switch
                {
                    ExpressionType.Equal => val == targetBool,
                    ExpressionType.NotEqual => val != targetBool,
                    _ => false
                };
            }
        }
        else if (type == BsonType.DateTime)
        {
            var val = reader.ReadDateTime();
            if (target is DateTime targetDt)
            {
                // Normalise both sides to UTC ticks for a reliable comparison.
                var valTicks = val.ToUniversalTime().Ticks;
                var targetTicks = targetDt.ToUniversalTime().Ticks;
                return op switch
                {
                    ExpressionType.Equal => valTicks == targetTicks,
                    ExpressionType.NotEqual => valTicks != targetTicks,
                    ExpressionType.GreaterThan => valTicks > targetTicks,
                    ExpressionType.GreaterThanOrEqual => valTicks >= targetTicks,
                    ExpressionType.LessThan => valTicks < targetTicks,
                    ExpressionType.LessThanOrEqual => valTicks <= targetTicks,
                    _ => false
                };
            }
            if (target is DateTimeOffset targetDto)
            {
                var valTicks = val.ToUniversalTime().Ticks;
                var targetTicks = targetDto.ToUniversalTime().Ticks;
                return op switch
                {
                    ExpressionType.Equal => valTicks == targetTicks,
                    ExpressionType.NotEqual => valTicks != targetTicks,
                    ExpressionType.GreaterThan => valTicks > targetTicks,
                    ExpressionType.GreaterThanOrEqual => valTicks >= targetTicks,
                    ExpressionType.LessThan => valTicks < targetTicks,
                    ExpressionType.LessThanOrEqual => valTicks <= targetTicks,
                    _ => false
                };
            }
        }
        else if (type == BsonType.ObjectId && target is ObjectId targetId)
        {
            var val = reader.ReadObjectId();
            if (op == ExpressionType.Equal) return val.Equals(targetId);
            if (op == ExpressionType.NotEqual) return !val.Equals(targetId);
        }

        return false;
    }
}
