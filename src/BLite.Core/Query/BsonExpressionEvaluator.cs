// NOTE: BsonExpressionEvaluator uses Expression.Lambda and Compile() as a fallback
// for complex expression evaluation. These calls require dynamic code and are
// suppressed here because this class is used internally from annotated execution paths.
#pragma warning disable IL3050
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
    private static readonly HashSet<Type> _knownBsonPrimitives = BuildKnownBsonPrimitives();

    private static HashSet<Type> BuildKnownBsonPrimitives()
    {
        var set = new HashSet<Type>
        {
            typeof(int), typeof(long), typeof(double), typeof(decimal),
            typeof(bool), typeof(string), typeof(DateTime), typeof(DateTimeOffset),
            typeof(ObjectId), typeof(TimeSpan), typeof(Guid)
        };
#if NET6_0_OR_GREATER
        set.Add(typeof(DateOnly));
        set.Add(typeof(TimeOnly));
#endif
        return set;
    }

    public static BsonReaderPredicate? TryCompile<T>(
        LambdaExpression expression,
        ValueConverterRegistry? registry = null,
        IReadOnlyDictionary<string, ushort>? keyMap = null)
        => TryCompileBody(expression.Body, expression.Parameters[0], registry, keyMap);

    /// <summary>
    /// Attempts to compile the logical negation of <paramref name="expression"/> into a BSON-level predicate.
    /// Returns <c>null</c> if the inner expression cannot be compiled.
    /// Used by <see cref="BTreeQueryable{T}.AllAsync"/> to find the first document that violates
    /// the predicate (early-exit O(1) instead of full scan).
    /// </summary>
    public static BsonReaderPredicate? TryCompileInverse<T>(
        LambdaExpression expression,
        ValueConverterRegistry? registry = null,
        IReadOnlyDictionary<string, ushort>? keyMap = null)
    {
        var inner = TryCompileBody(expression.Body, expression.Parameters[0], registry, keyMap);
        if (inner == null) return null;
        // Capture inner to avoid closure over mutable variable.
        var captured = inner;
        return reader => !captured(reader);
    }

    /// <summary>
    /// Recursively compiles an expression node into a <see cref="BsonReaderPredicate"/>.
    /// Handles:
    /// - <c>AndAlso</c> / <c>OrElse</c>: skips sides that don't touch the parameter
    /// - <c>.Equals()</c> method calls: treated as equality
    /// - Null checks: <c>x.Prop == null</c> / <c>x.Prop != null</c>
    /// - IN operator: <c>list.Contains(x.Prop)</c> / <c>Enumerable.Contains(list, x.Prop)</c>
    /// - String methods: <c>x.Prop.Contains(s)</c>, <c>StartsWith</c>, <c>EndsWith</c>
    /// - Static string helpers: <c>string.IsNullOrEmpty(x.Prop)</c>, <c>string.IsNullOrWhiteSpace(x.Prop)</c>
    /// - Enum comparisons (stored as Int32/Int64 in BSON)
    /// - Nullable member: <c>x.NullableProp.HasValue</c>, <c>x.NullableProp.Value op v</c>
    /// - CompareTo: <c>x.Prop.CompareTo(v) op 0</c>
    /// - Closure captures: evaluated at plan-time via <see cref="TryEvaluate"/>
    /// </summary>
    private static BsonReaderPredicate? TryCompileBody(Expression body, ParameterExpression parameter, ValueConverterRegistry? registry = null, IReadOnlyDictionary<string, ushort>? keyMap = null)
    {
        // ── AndAlso ────────────────────────────────────────────────────────────
        // Pattern: (item.Id != null) && e.Id.Equals(item.Id)
        // The null-guard left side doesn't touch the lambda parameter — skip it.
        if (body is BinaryExpression andAlso && andAlso.NodeType == ExpressionType.AndAlso)
        {
            bool leftTouches  = TouchesParameter(andAlso.Left,  parameter);
            bool rightTouches = TouchesParameter(andAlso.Right, parameter);

            if (leftTouches && !rightTouches)  return TryCompileBody(andAlso.Left,  parameter, registry, keyMap);
            if (rightTouches && !leftTouches)  return TryCompileBody(andAlso.Right, parameter, registry, keyMap);

            if (leftTouches && rightTouches)
            {
                var lp = TryCompileBody(andAlso.Left,  parameter, registry, keyMap);
                var rp = TryCompileBody(andAlso.Right, parameter, registry, keyMap);
                if (lp != null && rp != null) return reader => lp(reader) && rp(reader);
                return lp ?? rp;
            }

            return null;
        }

        // ── OrElse ─────────────────────────────────────────────────────────────
        // <see cref="BsonReaderPredicate"/> is declared as
        // <c>delegate bool BsonReaderPredicate(BsonSpanReader reader)</c>.
        // Because <see cref="BsonSpanReader"/> is a ref struct and the parameter
        // is <em>not</em> passed by <c>ref</c>, every invocation receives its own
        // value-copy of the reader.  This means both sides of the OR always start
        // scanning from the same position (the beginning of the document), making
        // independent compilation of lp and rp correct without any seek/reset.
        if (body is BinaryExpression orElse && orElse.NodeType == ExpressionType.OrElse)
        {
            bool leftTouches  = TouchesParameter(orElse.Left,  parameter);
            bool rightTouches = TouchesParameter(orElse.Right, parameter);

            if (leftTouches && !rightTouches)  return TryCompileBody(orElse.Left,  parameter, registry, keyMap);
            if (rightTouches && !leftTouches)  return TryCompileBody(orElse.Right, parameter, registry, keyMap);

            if (leftTouches && rightTouches)
            {
                var lp = TryCompileBody(orElse.Left,  parameter, registry, keyMap);
                var rp = TryCompileBody(orElse.Right, parameter, registry, keyMap);
                if (lp != null && rp != null) return reader => lp(reader) || rp(reader);
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
            return CreatePredicate(bsonName, true, ExpressionType.Equal, keyMap);
        }

        // ── Nullable.HasValue: e => e.NullableProp.HasValue  →  field is not null ──
        if (body is MemberExpression { Member.Name: "HasValue" } hasValueExpr &&
            hasValueExpr.Expression is MemberExpression innerHasValueMember &&
            innerHasValueMember.Expression == parameter &&
            Nullable.GetUnderlyingType(innerHasValueMember.Type) != null)
        {
            var bsonName = innerHasValueMember.Member.Name.ToLowerInvariant();
            if (bsonName == "id") bsonName = "_id";
            return CreateNullCheckPredicate(bsonName, expectNull: false, keyMap);
        }

        // ── Logical NOT ────────────────────────────────────────────────────────
        if (body is UnaryExpression { NodeType: ExpressionType.Not } notExpr)
        {
            // Fast path: !e.BoolProp → BoolProp == false
            if (notExpr.Operand is MemberExpression notM &&
                notM.Expression == parameter &&
                notM.Type == typeof(bool))
            {
                var bsonName = notM.Member.Name.ToLowerInvariant();
                if (bsonName == "id") bsonName = "_id";
                return CreatePredicate(bsonName, false, ExpressionType.Equal, keyMap);
            }
            // General: !(compilable sub-expression) — negate the inner predicate.
            var innerNot = TryCompileBody(notExpr.Operand, parameter, registry, keyMap);
            if (innerNot != null)
                return reader => !innerNot(reader);
            return null;
        }

        // ── Method calls ───────────────────────────────────────────────────────
        if (body is MethodCallExpression mc)
        {
            // .Equals() on a property: e.Prop.Equals(closureVar)
            if (mc.Method.Name == "Equals" &&
                mc.Arguments.Count == 1 &&
                mc.Object is MemberExpression equalsOnMember &&
                equalsOnMember.Expression == parameter)
            {
                var fieldName   = equalsOnMember.Member.Name;
                var bsonName    = fieldName.ToLowerInvariant();
                if (bsonName == "id") bsonName = "_id";

                var (ok, value) = TryEvaluate(mc.Arguments[0]);
                if (ok && IsKnownBsonPrimitive(value?.GetType()))
                    return CreatePredicate(bsonName, value, ExpressionType.Equal, keyMap);

                // Try ValueObject → provider conversion
                if (ok && value != null &&
                    registry?.TryConvert(fieldName, value, out var pv) == true &&
                    IsKnownBsonPrimitive(pv?.GetType()))
                    return CreatePredicate(bsonName, pv, ExpressionType.Equal, keyMap);

                return null;
            }

            // String instance methods on a property: e.Prop.Contains(s), StartsWith, EndsWith
            if (mc.Object is MemberExpression strMember &&
                strMember.Expression == parameter &&
                strMember.Type == typeof(string) &&
                mc.Arguments.Count == 1 &&
                mc.Method.Name is "Contains" or "StartsWith" or "EndsWith")
            {
                var bsonName = strMember.Member.Name.ToLowerInvariant();
                if (bsonName == "id") bsonName = "_id";

                var (ok, value) = TryEvaluate(mc.Arguments[0]);
                if (ok && value is string pattern)
                    return CreateStringMethodPredicate(bsonName, mc.Method.Name, pattern, keyMap);

                return null;
            }

            // Static string helpers: string.IsNullOrEmpty(x.Prop) / string.IsNullOrWhiteSpace(x.Prop)
            if (mc.Object == null &&
                mc.Method.DeclaringType == typeof(string) &&
                mc.Method.Name is "IsNullOrEmpty" or "IsNullOrWhiteSpace" &&
                mc.Arguments.Count == 1 &&
                mc.Arguments[0] is MemberExpression staticStrMember &&
                staticStrMember.Expression == parameter &&
                staticStrMember.Type == typeof(string))
            {
                var bsonName = staticStrMember.Member.Name.ToLowerInvariant();
                if (bsonName == "id") bsonName = "_id";
                bool checkWhiteSpace = mc.Method.Name == "IsNullOrWhiteSpace";
                return CreateIsNullOrEmptyPredicate(bsonName, checkWhiteSpace, keyMap);
            }

            // IN operator: list.Contains(x.Prop) or Enumerable.Contains(list, x.Prop)
            if (mc.Method.Name == "Contains")
            {
                // Instance method: list.Contains(x.Prop)  [also handles Convert-wrapped member]
                if (mc.Object != null &&
                    mc.Arguments.Count == 1)
                {
                    var argUnwrapped = UnwrapConvert(mc.Arguments[0]);
                    if (argUnwrapped is MemberExpression inMember &&
                        inMember.Expression == parameter)
                    {
                        var (ok, collection) = TryEvaluate(mc.Object);
                        if (ok && collection != null)
                            return TryCreateInPredicate(inMember, collection, keyMap);
                    }
                }

                // Extension method: Enumerable.Contains(list, x.Prop)  or
                // MemoryExtensions.Contains(op_Implicit(array), x.Prop)
                if (mc.Object == null &&
                    mc.Arguments.Count == 2)
                {
                    var argUnwrapped = UnwrapConvert(mc.Arguments[1]);
                    if (argUnwrapped is MemberExpression enumInMember &&
                        enumInMember.Expression == parameter)
                    {
                        var (ok, collection) = TryEvaluateCollection(mc.Arguments[0]);
                        if (ok && collection != null)
                            return TryCreateInPredicate(enumInMember, collection, keyMap);
                    }
                }
            }
        }

        // ── Simple binary: e.Prop op constant (or e.Prop op closureCapture) ───
        if (body is BinaryExpression binary)
        {
            var left = binary.Left;
            var right = binary.Right;
            var nodeType = binary.NodeType;

            // ── CompareTo(v) op 0 ────────────────────────────────────────────────
            // Pattern: x.Prop.CompareTo(value) op 0  →  equivalent to  x.Prop op value
            if (nodeType is ExpressionType.Equal or ExpressionType.NotEqual or
                ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual or
                ExpressionType.LessThan or ExpressionType.LessThanOrEqual)
            {
                var ct = TryCompileCompareTo(left, right, nodeType, parameter, registry, keyMap);
                if (ct != null) return ct;
                // Also try flipped (0 op x.Prop.CompareTo(value))
                ct = TryCompileCompareTo(right, left, Flip(nodeType), parameter, registry, keyMap);
                if (ct != null) return ct;
            }

            // Unwrap Convert nodes: e.g. enum comparisons produce
            // Equal(Convert(x.Role, Int32), Convert(3, Int32)) in expression trees.
            // Also unwrap Nullable<T>.Value accessors: x.NullableProp.Value op v
            Expression leftInner  = UnwrapNullableValue(UnwrapConvert(left));
            Expression rightInner = UnwrapNullableValue(UnwrapConvert(right));

            // Normalize: Ensure Property is on Left (also support Convert-wrapped members)
            bool rightIsParam = rightInner is MemberExpression rMbr && rMbr.Expression == parameter;
            bool leftIsParam  = leftInner  is MemberExpression lMbr && lMbr.Expression == parameter;

            if (rightIsParam && !leftIsParam)
            {
                (left, right, leftInner, rightInner) = (right, left, rightInner, leftInner);
                nodeType = Flip(nodeType);
            }

            if (leftInner is MemberExpression member && member.Expression == parameter)
            {
                var fieldName = member.Member.Name;
                var bsonName  = fieldName.ToLowerInvariant();
                // "Id" and "_id" both refer to the BSON primary-key field.
                if (bsonName == "id" || bsonName == "_id") bsonName = "_id";

                // Right side: ConstantExpression or closure capture (any non-parameter expr)
                var (ok, value) = TryEvaluate(rightInner);

                // Null check: x.Prop == null or x.Prop != null
                if (ok && value == null &&
                    nodeType is ExpressionType.Equal or ExpressionType.NotEqual)
                    return CreateNullCheckPredicate(bsonName, nodeType == ExpressionType.Equal, keyMap);

                if (ok && IsKnownBsonPrimitive(value?.GetType()))
                    return CreatePredicate(bsonName, value, nodeType, keyMap);

                // Enum comparison: enums are stored as Int32/Int64 — convert and compare.
                if (ok && value != null && value.GetType().IsEnum)
                    return CreatePredicate(bsonName, Convert.ToInt64(value), nodeType, keyMap);

                // Try ValueObject → provider conversion
                if (ok && value != null &&
                    registry?.TryConvert(fieldName, value, out var pv) == true &&
                    IsKnownBsonPrimitive(pv?.GetType()))
                    return CreatePredicate(bsonName, pv, nodeType, keyMap);
            }
        }

        return null;
    }

    /// <summary>
    /// Tries to compile a <c>x.Prop.CompareTo(value) op 0</c> pattern into a predicate.
    /// <paramref name="maybeMethodCall"/> must be the CompareTo call expression;
    /// <paramref name="maybeZero"/> must evaluate to integer zero.
    /// </summary>
    private static BsonReaderPredicate? TryCompileCompareTo(
        Expression maybeMethodCall, Expression maybeZero, ExpressionType op,
        ParameterExpression parameter, ValueConverterRegistry? registry,
        IReadOnlyDictionary<string, ushort>? keyMap)
    {
        if (maybeMethodCall is not MethodCallExpression ctMc) return null;
        if (ctMc.Method.Name != "CompareTo" || ctMc.Arguments.Count != 1) return null;

        var (zeroOk, zeroVal) = TryEvaluate(maybeZero);
        if (!zeroOk || zeroVal is not int zi || zi != 0) return null;

        // The instance on which CompareTo is called: unwrap Convert / Nullable.Value
        var instanceExpr = UnwrapNullableValue(UnwrapConvert(ctMc.Object!));
        if (instanceExpr is not MemberExpression ctMember || ctMember.Expression != parameter)
            return null;

        var fieldName = ctMember.Member.Name;
        var bsonName  = fieldName.ToLowerInvariant();
        if (bsonName == "id" || bsonName == "_id") bsonName = "_id";

        var (ok, value) = TryEvaluate(ctMc.Arguments[0]);
        if (!ok) return null;

        if (IsKnownBsonPrimitive(value?.GetType()))
            return CreatePredicate(bsonName, value, op, keyMap);

        if (value != null && value.GetType().IsEnum)
            return CreatePredicate(bsonName, Convert.ToInt64(value), op, keyMap);

        if (value != null &&
            registry?.TryConvert(fieldName, value, out var pv) == true &&
            IsKnownBsonPrimitive(pv?.GetType()))
            return CreatePredicate(bsonName, pv, op, keyMap);

        return null;
    }

    // ── Additional predicate factories ────────────────────────────────────────

    /// <summary>
    /// Creates a predicate that checks whether <paramref name="fieldName"/> is BSON Null
    /// (or absent from the document). When <paramref name="expectNull"/> is false the
    /// predicate returns true for any non-null value.
    /// </summary>
    internal static BsonReaderPredicate CreateNullCheckPredicate(string fieldName, bool expectNull, IReadOnlyDictionary<string, ushort>? keyMap = null)
    {
        ushort fieldId_ = 0;
        var hasFieldId = keyMap != null && keyMap.TryGetValue(fieldName, out fieldId_);
        var capturedId = hasFieldId ? fieldId_ : (ushort)0;
        return reader =>
        {
            try
            {
                reader.ReadDocumentSize();
                if (hasFieldId && reader.TrySeekToField(capturedId, out var seekType))
                {
                    bool isNull = seekType == BsonType.Null;
                    return expectNull == isNull;
                }
                while (reader.Remaining > 0)
                {
                    var type = reader.ReadBsonType();
                    if (type == 0) break;
                    var name = reader.ReadElementHeader();
                    if (name == fieldName)
                    {
                        bool isNull = type == BsonType.Null;
                        return expectNull == isNull;
                    }
                    reader.SkipValue(type);
                }
            }
            catch { return false; }
            // Field absent: treat as null
            return expectNull;
        };
    }

    /// <summary>
    /// Creates a predicate for <c>x.Prop.Contains(pattern)</c>, <c>StartsWith</c>, or <c>EndsWith</c>.
    /// The match is performed directly on the raw UTF-8 bytes stored in the BSON buffer —
    /// no managed string is allocated for the field value at eval-time.
    /// </summary>
    /// <remarks>
    /// The pattern is encoded to UTF-8 once at plan-time and captured in the closure.
    /// Ordinal string comparison is semantically equivalent to ordinal UTF-8 byte comparison
    /// (UTF-8 is a prefix-free, bijective encoding of Unicode code points), so this is
    /// semantically identical to <c>string.Contains(pattern, StringComparison.Ordinal)</c>.
    /// </remarks>
    internal static BsonReaderPredicate CreateStringMethodPredicate(string fieldName, string methodName, string pattern, IReadOnlyDictionary<string, ushort>? keyMap = null)
    {
        // Pre-encode the pattern to UTF-8 bytes once at plan-time.
        var patternBytes = System.Text.Encoding.UTF8.GetBytes(pattern);
        ushort fieldId_ = 0;
        var hasFieldId = keyMap != null && keyMap.TryGetValue(fieldName, out fieldId_);
        var capturedId = hasFieldId ? fieldId_ : (ushort)0;

        return reader =>
        {
            try
            {
                reader.ReadDocumentSize();
                if (hasFieldId && reader.TrySeekToField(capturedId, out var seekType))
                {
                    if (seekType != BsonType.String) return false;
                    var valueBytes = reader.ReadStringRawBytes();
                    return methodName switch
                    {
                        "Contains"   => valueBytes.IndexOf(patternBytes) >= 0,
                        "StartsWith" => valueBytes.StartsWith(patternBytes),
                        "EndsWith"   => valueBytes.EndsWith(patternBytes),
                        _            => false
                    };
                }
                while (reader.Remaining > 0)
                {
                    var type = reader.ReadBsonType();
                    if (type == 0) break;
                    var name = reader.ReadElementHeader();
                    if (name == fieldName)
                    {
                        if (type != BsonType.String) { reader.SkipValue(type); return false; }
                        var valueBytes = reader.ReadStringRawBytes();
                        return methodName switch
                        {
                            "Contains"   => valueBytes.IndexOf(patternBytes) >= 0,
                            "StartsWith" => valueBytes.StartsWith(patternBytes),
                            "EndsWith"   => valueBytes.EndsWith(patternBytes),
                            _            => false
                        };
                    }
                    reader.SkipValue(type);
                }
            }
            catch { return false; }
            return false;
        };
    }

    /// <summary>
    /// Creates a predicate for <c>string.IsNullOrEmpty(x.Prop)</c> or
    /// <c>string.IsNullOrWhiteSpace(x.Prop)</c>.
    /// </summary>
    private static BsonReaderPredicate CreateIsNullOrEmptyPredicate(string fieldName, bool checkWhiteSpace, IReadOnlyDictionary<string, ushort>? keyMap = null)
    {
        ushort fieldId_ = 0;
        var hasFieldId = keyMap != null && keyMap.TryGetValue(fieldName, out fieldId_);
        var capturedId = hasFieldId ? fieldId_ : (ushort)0;
        return reader =>
        {
            try
            {
                reader.ReadDocumentSize();
                if (hasFieldId && reader.TrySeekToField(capturedId, out var seekType))
                {
                    if (seekType == BsonType.Null) return true;
                    if (seekType != BsonType.String) return false;
                    var val = reader.ReadString();
                    return checkWhiteSpace ? string.IsNullOrWhiteSpace(val) : string.IsNullOrEmpty(val);
                }
                while (reader.Remaining > 0)
                {
                    var type = reader.ReadBsonType();
                    if (type == 0) break;
                    var name = reader.ReadElementHeader();
                    if (name == fieldName)
                    {
                        if (type == BsonType.Null) return true;
                        if (type != BsonType.String) { reader.SkipValue(type); return false; }
                        var val = reader.ReadString();
                        return checkWhiteSpace ? string.IsNullOrWhiteSpace(val) : string.IsNullOrEmpty(val);
                    }
                    reader.SkipValue(type);
                }
            }
            catch { return false; }
            // Field absent — treat as null → true
            return true;
        };
    }

    /// <summary>
    /// Tries to build an IN predicate from a collection instance obtained from a closure.
    /// Returns <c>null</c> if the collection element type is not a supported BSON primitive.
    /// </summary>
    private static BsonReaderPredicate? TryCreateInPredicate(MemberExpression memberExpr, object collection, IReadOnlyDictionary<string, ushort>? keyMap = null)
    {
        var bsonName = memberExpr.Member.Name.ToLowerInvariant();
        if (bsonName == "id") bsonName = "_id";

        // Build a HashSet<object> at plan-time to enable O(1) lookups at eval-time.
        var items = new HashSet<object?>();
        var hasLong = false;
        foreach (var item in (System.Collections.IEnumerable)collection)
        {
            if (item != null && !IsKnownBsonPrimitive(item.GetType()) && !item.GetType().IsEnum)
                return null; // unsupported element type
            if (item != null && item.GetType().IsEnum)
            {
                items.Add(Convert.ToInt64(item));
                hasLong = true;
            }
            // Guid — stored as string in BSON; normalise so Contains works at eval-time
            else if (item is Guid g)
            {
                items.Add(g.ToString());
            }
            // TimeSpan — stored as Int64 ticks
            else if (item is TimeSpan ts)
            {
                items.Add(ts.Ticks);
                hasLong = true;
            }
#if NET6_0_OR_GREATER
            // DateOnly — stored as Int32 day number
            else if (item is DateOnly d)
            {
                items.Add(d.DayNumber);
            }
            // TimeOnly — stored as Int64 ticks
            else if (item is TimeOnly to)
            {
                items.Add(to.Ticks);
                hasLong = true;
            }
#endif
            else
            {
                items.Add(item);
            }
        }

        // Capture as immutable set for the predicate closure.
        var capturedItems = items;
        var capturedHasLong = hasLong;
        ushort fieldId_ = 0;
        var hasFieldId = keyMap != null && keyMap.TryGetValue(bsonName, out fieldId_);
        var capturedId = hasFieldId ? fieldId_ : (ushort)0;

        return reader =>
        {
            try
            {
                reader.ReadDocumentSize();

                BsonType typeToRead;
                if (hasFieldId && reader.TrySeekToField(capturedId, out typeToRead))
                    goto readValue;

                while (reader.Remaining > 0)
                {
                    var scanType = reader.ReadBsonType();
                    if (scanType == 0) break;
                    var name = reader.ReadElementHeader();
                    if (name == bsonName) { typeToRead = scanType; goto readValue; }
                    reader.SkipValue(scanType);
                }
                return capturedItems.Contains(null);

                readValue:
                object? readValue = typeToRead switch
                {
                    BsonType.Int32   => capturedHasLong ? (object)Convert.ToInt64(reader.ReadInt32()) : reader.ReadInt32(),
                    BsonType.Int64   => reader.ReadInt64(),
                    BsonType.String  => reader.ReadString(),
                    BsonType.Double  => reader.ReadDouble(),
                    BsonType.Decimal128 => reader.ReadDecimal128(),
                    BsonType.Boolean => reader.ReadBoolean(),
                    BsonType.ObjectId => reader.ReadObjectId(),
                    BsonType.DateTime => reader.ReadDateTime(),
                    BsonType.Null    => null,
                    _                => null
                };
                return capturedItems.Contains(readValue);
            }
            catch { return false; }
        };
    }

    /// <summary>
    /// Creates an IN predicate from a field name and a pre-built set of normalised values.
    /// Used by <see cref="BsonPredicateBuilder.In{T}"/> to avoid LINQ expression-tree overhead.
    /// </summary>
    internal static BsonReaderPredicate CreateInPredicateDirect(
        string fieldName,
        System.Collections.Generic.HashSet<object?> items,
        bool hasLong,
        IReadOnlyDictionary<string, ushort>? keyMap = null)
    {
        ushort fieldId_ = 0;
        var hasFieldId = keyMap != null && keyMap.TryGetValue(fieldName, out fieldId_);
        var capturedId = hasFieldId ? fieldId_ : (ushort)0;
        var capturedItems = items;
        var capturedHasLong = hasLong;

        return reader =>
        {
            try
            {
                reader.ReadDocumentSize();

                BsonType typeToRead;
                if (hasFieldId && reader.TrySeekToField(capturedId, out typeToRead))
                    goto readValue;

                while (reader.Remaining > 0)
                {
                    var scanType = reader.ReadBsonType();
                    if (scanType == 0) break;
                    var name = reader.ReadElementHeader();
                    if (name == fieldName) { typeToRead = scanType; goto readValue; }
                    reader.SkipValue(scanType);
                }
                return capturedItems.Contains(null);

                readValue:
                object? readValue = typeToRead switch
                {
                    BsonType.Int32   => capturedHasLong ? (object)System.Convert.ToInt64(reader.ReadInt32()) : reader.ReadInt32(),
                    BsonType.Int64   => reader.ReadInt64(),
                    BsonType.String  => reader.ReadString(),
                    BsonType.Double  => reader.ReadDouble(),
                    BsonType.Decimal128 => reader.ReadDecimal128(),
                    BsonType.Boolean => reader.ReadBoolean(),
                    BsonType.ObjectId => reader.ReadObjectId(),
                    BsonType.DateTime => reader.ReadDateTime(),
                    BsonType.Null    => null,
                    _                => null
                };
                return capturedItems.Contains(readValue);
            }
            catch { return false; }
        };
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Like <see cref="TryEvaluate"/> but also handles the .NET 10 pattern where the C#
    /// compiler wraps an array in an implicit <c>ReadOnlySpan&lt;T&gt;</c> conversion when
    /// calling <c>MemoryExtensions.Contains</c>.  In that case the actual array lives in the
    /// single argument of the <c>op_Implicit</c> call; we evaluate that inner expression
    /// directly so that the array can be iterated as a normal <see cref="System.Collections.IEnumerable"/>.
    /// </summary>
    private static (bool Ok, object? Value) TryEvaluateCollection(Expression expression)
    {
        // MemoryExtensions.Contains wraps: op_Implicit(array) → ReadOnlySpan<T>
        // Unwrap one level of op_Implicit to recover the underlying array.
        if (expression is MethodCallExpression { Method.Name: "op_Implicit", Object: null } implicitCall
            && implicitCall.Arguments.Count == 1)
        {
            var inner = TryEvaluate(implicitCall.Arguments[0]);
            if (inner.Ok && inner.Value is System.Collections.IEnumerable)
                return inner;
        }

        return TryEvaluate(expression);
    }

    private static bool IsDirectParameterAccess(Expression expr, ParameterExpression p)
        => expr is MemberExpression m && m.Expression == p;

    /// <summary>
    /// Unwraps a single <c>Convert</c> / <c>ConvertChecked</c> node if present.
    /// Enum comparisons are compiled to <c>Equal(Convert(x.Role,Int32), Convert(3,Int32))</c>
    /// by the C# compiler; stripping the outer Convert lets us inspect the inner expression.
    /// </summary>
    private static Expression UnwrapConvert(Expression expr)
        => expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u
            ? u.Operand
            : expr;

    /// <summary>
    /// Unwraps a <c>Nullable&lt;T&gt;.Value</c> member access, so that
    /// <c>x.NullableProp.Value op v</c> is treated as direct field access <c>x.NullableProp</c>.
    /// </summary>
    private static Expression UnwrapNullableValue(Expression expr)
    {
        if (expr is MemberExpression { Member.Name: "Value" } valMember &&
            valMember.Expression != null &&
            Nullable.GetUnderlyingType(valMember.Expression.Type) != null)
            return valMember.Expression;
        return expr;
    }

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

    internal static BsonReaderPredicate? CreatePredicate(string propertyName, object? targetValue, ExpressionType op, IReadOnlyDictionary<string, ushort>? keyMap = null)
    {
        ushort fieldId_ = 0;
        var hasFieldId = keyMap != null && keyMap.TryGetValue(propertyName, out fieldId_);
        var capturedId = hasFieldId ? fieldId_ : (ushort)0;

        return reader =>
        {
            try
            {
                reader.ReadDocumentSize();

                if (hasFieldId && reader.TrySeekToField(capturedId, out var seekType))
                    return Compare(ref reader, seekType, targetValue, op);

                while (reader.Remaining > 0)
                {
                    var type = reader.ReadBsonType();
                    if (type == 0) break;

                    var name = reader.ReadElementHeader();

                    if (name == propertyName)
                        return Compare(ref reader, type, targetValue, op);

                    reader.SkipValue(type);
                }
            }
            catch
            {
                return false;
            }
            return false;
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
            // Enum stored as Int32, compared to a long (result of Convert.ToInt64(enumValue))
            if (target is long targetLongFromEnum)
            {
                var valL = (long)val;
                return op switch
                {
                    ExpressionType.Equal => valL == targetLongFromEnum,
                    ExpressionType.NotEqual => valL != targetLongFromEnum,
                    ExpressionType.GreaterThan => valL > targetLongFromEnum,
                    ExpressionType.GreaterThanOrEqual => valL >= targetLongFromEnum,
                    ExpressionType.LessThan => valL < targetLongFromEnum,
                    ExpressionType.LessThanOrEqual => valL <= targetLongFromEnum,
                    _ => false
                };
            }
            // Cross-type: int field compared against decimal or double (e.g. x.Count > 5.0m)
            if (target is decimal targetDecFromInt32)
            {
                var valD = (decimal)val;
                return op switch
                {
                    ExpressionType.Equal            => valD == targetDecFromInt32,
                    ExpressionType.NotEqual         => valD != targetDecFromInt32,
                    ExpressionType.GreaterThan      => valD >  targetDecFromInt32,
                    ExpressionType.GreaterThanOrEqual => valD >= targetDecFromInt32,
                    ExpressionType.LessThan         => valD <  targetDecFromInt32,
                    ExpressionType.LessThanOrEqual  => valD <= targetDecFromInt32,
                    _ => false
                };
            }
            if (target is double targetDblFromInt32 || target is float)
            {
                var targetDbl = target is float f32 ? (double)f32 : (double)target!;
                var valD = (double)val;
                return op switch
                {
                    ExpressionType.Equal            => valD == targetDbl,
                    ExpressionType.NotEqual         => valD != targetDbl,
                    ExpressionType.GreaterThan      => valD >  targetDbl,
                    ExpressionType.GreaterThanOrEqual => valD >= targetDbl,
                    ExpressionType.LessThan         => valD <  targetDbl,
                    ExpressionType.LessThanOrEqual  => valD <= targetDbl,
                    _ => false
                };
            }
#if NET6_0_OR_GREATER
            // DateOnly comparison — stored as Int32 day number
            if (target is DateOnly targetDo)
            {
                return op switch
                {
                    ExpressionType.Equal            => val == targetDo.DayNumber,
                    ExpressionType.NotEqual         => val != targetDo.DayNumber,
                    ExpressionType.GreaterThan      => val >  targetDo.DayNumber,
                    ExpressionType.GreaterThanOrEqual => val >= targetDo.DayNumber,
                    ExpressionType.LessThan         => val <  targetDo.DayNumber,
                    ExpressionType.LessThanOrEqual  => val <= targetDo.DayNumber,
                    _ => false
                };
            }
#endif
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
            // Guid comparison — stored as its string representation
            if (target is Guid targetGuid && Guid.TryParse(val, out var parsedGuid))
            {
                return op switch
                {
                    ExpressionType.Equal    => parsedGuid == targetGuid,
                    ExpressionType.NotEqual => parsedGuid != targetGuid,
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
            // Cross-type: long field vs int target (common when int constant is unwrapped from Convert)
            if (target is int targetIntFromInt64)
            {
                var targetL = (long)targetIntFromInt64;
                return op switch
                {
                    ExpressionType.Equal            => val == targetL,
                    ExpressionType.NotEqual         => val != targetL,
                    ExpressionType.GreaterThan      => val >  targetL,
                    ExpressionType.GreaterThanOrEqual => val >= targetL,
                    ExpressionType.LessThan         => val <  targetL,
                    ExpressionType.LessThanOrEqual  => val <= targetL,
                    _ => false
                };
            }
            if (target is decimal targetDecFromInt64)
            {
                var valD = (decimal)val;
                return op switch
                {
                    ExpressionType.Equal            => valD == targetDecFromInt64,
                    ExpressionType.NotEqual         => valD != targetDecFromInt64,
                    ExpressionType.GreaterThan      => valD >  targetDecFromInt64,
                    ExpressionType.GreaterThanOrEqual => valD >= targetDecFromInt64,
                    ExpressionType.LessThan         => valD <  targetDecFromInt64,
                    ExpressionType.LessThanOrEqual  => valD <= targetDecFromInt64,
                    _ => false
                };
            }
            if (target is double targetDblFromInt64 || target is float)
            {
                var targetDbl = target is float f64 ? (double)f64 : (double)target!;
                var valD = (double)val;
                return op switch
                {
                    ExpressionType.Equal            => valD == targetDbl,
                    ExpressionType.NotEqual         => valD != targetDbl,
                    ExpressionType.GreaterThan      => valD >  targetDbl,
                    ExpressionType.GreaterThanOrEqual => valD >= targetDbl,
                    ExpressionType.LessThan         => valD <  targetDbl,
                    ExpressionType.LessThanOrEqual  => valD <= targetDbl,
                    _ => false
                };
            }
            // TimeSpan comparison — stored as Int64 ticks
            if (target is TimeSpan targetTs)
            {
                return op switch
                {
                    ExpressionType.Equal            => val == targetTs.Ticks,
                    ExpressionType.NotEqual         => val != targetTs.Ticks,
                    ExpressionType.GreaterThan      => val >  targetTs.Ticks,
                    ExpressionType.GreaterThanOrEqual => val >= targetTs.Ticks,
                    ExpressionType.LessThan         => val <  targetTs.Ticks,
                    ExpressionType.LessThanOrEqual  => val <= targetTs.Ticks,
                    _ => false
                };
            }
#if NET6_0_OR_GREATER
            // TimeOnly comparison — stored as Int64 ticks
            if (target is TimeOnly targetTo)
            {
                return op switch
                {
                    ExpressionType.Equal            => val == targetTo.Ticks,
                    ExpressionType.NotEqual         => val != targetTo.Ticks,
                    ExpressionType.GreaterThan      => val >  targetTo.Ticks,
                    ExpressionType.GreaterThanOrEqual => val >= targetTo.Ticks,
                    ExpressionType.LessThan         => val <  targetTo.Ticks,
                    ExpressionType.LessThanOrEqual  => val <= targetTo.Ticks,
                    _ => false
                };
            }
#endif
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
        else if (type == BsonType.Decimal128)
        {
            var val = reader.ReadDecimal128();
            decimal? targetDecimal = target switch
            {
                decimal d  => d,
                int     i  => (decimal)i,
                long    l  => (decimal)l,
                double  d  => (decimal)d,
                float   f  => (decimal)f,
                _          => null
            };
            if (targetDecimal.HasValue)
            {
                return op switch
                {
                    ExpressionType.Equal            => val == targetDecimal.Value,
                    ExpressionType.NotEqual         => val != targetDecimal.Value,
                    ExpressionType.GreaterThan      => val >  targetDecimal.Value,
                    ExpressionType.GreaterThanOrEqual => val >= targetDecimal.Value,
                    ExpressionType.LessThan         => val <  targetDecimal.Value,
                    ExpressionType.LessThanOrEqual  => val <= targetDecimal.Value,
                    _ => false
                };
            }
        }

        return false;
    }

    /// <summary>
    /// Creates a <see cref="BsonReaderProjector{TResult}"/> that reads the value of
    /// <paramref name="fieldName"/> from a BSON document and returns it as <typeparamref name="TResult"/>.
    /// Returns <c>null</c> from the projector when the field is absent or the type cannot be converted.
    /// No <c>Expression.Compile()</c> is used — the projector is built from direct BSON reads.
    /// </summary>
    internal static BsonReaderProjector<TResult> CreateFieldProjector<TResult>(
        string fieldName,
        IReadOnlyDictionary<string, ushort>? keyMap = null)
    {
        ushort fieldId_ = 0;
        var hasFieldId = keyMap != null && keyMap.TryGetValue(fieldName, out fieldId_);
        var capturedId = hasFieldId ? fieldId_ : (ushort)0;

        return reader =>
        {
            try
            {
                reader.ReadDocumentSize();

                BsonType type;
                bool found;

                if (hasFieldId && reader.TrySeekToField(capturedId, out type))
                {
                    found = true;
                }
                else
                {
                    type = BsonType.Null;
                    found = false;
                    while (reader.Remaining > 0)
                    {
                        var t = reader.ReadBsonType();
                        if (t == 0) break;
                        var name = reader.ReadElementHeader();
                        if (name == fieldName) { type = t; found = true; break; }
                        reader.SkipValue(t);
                    }
                }

                if (!found) return default;
                return ReadBsonFieldAs<TResult>(ref reader, type);
            }
            catch
            {
                return default;
            }
        };
    }

    private static TResult? ReadBsonFieldAs<TResult>(ref BsonSpanReader reader, BsonType type)
    {
        if (type == BsonType.Int32)
        {
            var val = reader.ReadInt32();
            if (typeof(TResult) == typeof(int)) return (TResult)(object)val;
            if (typeof(TResult) == typeof(long)) return (TResult)(object)(long)val;
            if (typeof(TResult) == typeof(double)) return (TResult)(object)(double)val;
            if (typeof(TResult) == typeof(decimal)) return (TResult)(object)(decimal)val;
            return default;
        }
        if (type == BsonType.Int64)
        {
            var val = reader.ReadInt64();
            if (typeof(TResult) == typeof(long)) return (TResult)(object)val;
            if (typeof(TResult) == typeof(int)) return (TResult)(object)(int)val;
            if (typeof(TResult) == typeof(double)) return (TResult)(object)(double)val;
            if (typeof(TResult) == typeof(decimal)) return (TResult)(object)(decimal)val;
            return default;
        }
        if (type == BsonType.Double)
        {
            var val = reader.ReadDouble();
            if (typeof(TResult) == typeof(double)) return (TResult)(object)val;
            if (typeof(TResult) == typeof(float)) return (TResult)(object)(float)val;
            if (typeof(TResult) == typeof(decimal)) return (TResult)(object)(decimal)val;
            return default;
        }
        if (type == BsonType.Decimal128)
        {
            var val = reader.ReadDecimal128();
            if (typeof(TResult) == typeof(decimal)) return (TResult)(object)val;
            if (typeof(TResult) == typeof(double)) return (TResult)(object)(double)val;
            return default;
        }
        if (type == BsonType.String)
        {
            var val = reader.ReadString();
            if (typeof(TResult) == typeof(string)) return (TResult)(object)val;
            return default;
        }
        reader.SkipValue(type);
        return default;
    }
}
