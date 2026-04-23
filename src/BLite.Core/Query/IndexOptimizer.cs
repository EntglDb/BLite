using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using BLite.Bson;
using BLite.Core.Indexing;
using BLite.Core.Metadata;

namespace BLite.Core.Query;

internal static class IndexOptimizer
{
    public class OptimizationResult
    {
        public string IndexName { get; set; } = "";
        public object? MinValue { get; set; }
        public object? MaxValue { get; set; }
        public IReadOnlyList<object?>? InValues { get; set; }

        /// <summary>
        /// Describes how completely the index scan covers the WHERE expression.
        /// <see cref="FilterCompleteness.Exact"/> means the index result is the full WHERE result
        /// and no in-memory post-filter is needed.  Any other value means the caller must still
        /// apply the full compiled predicate after reading candidates from the index.
        /// </summary>
        public FilterCompleteness FilterCompleteness { get; set; } = FilterCompleteness.Exact;

        /// <summary>
        /// <c>true</c> when a compound <c>AND</c> predicate has at least one clause that
        /// targets a non-indexed field.  The index can be used to narrow the candidate set,
        /// but the full predicate must still be evaluated per document.  Operations that
        /// need an exact count (e.g. <c>CountAsync</c>) must fall through to a document
        /// scan instead of counting index entries directly.
        /// </summary>
        public bool HasResiduePredicate { get; set; }

        /// <summary>
        /// <c>true</c> when the lower bound is inclusive (<c>&gt;=</c>, <c>==</c>);
        /// <c>false</c> for a strict greater-than (<c>&gt;</c>).
        /// Meaningful only when <see cref="MinValue"/> is non-null.
        /// </summary>
        public bool StartInclusive { get; set; } = true;

        /// <summary>
        /// <c>true</c> when the upper bound is inclusive (<c>&lt;=</c>, <c>==</c>);
        /// <c>false</c> for a strict less-than (<c>&lt;</c>).
        /// Meaningful only when <see cref="MaxValue"/> is non-null.
        /// </summary>
        public bool EndInclusive { get; set; } = true;
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

        /// <summary>
        /// True when this result was produced by <see cref="TryOptimizeOrderBy"/> rather than
        /// <see cref="TryOptimize{T}"/>.  The caller should skip WHERE filtering and use the
        /// index scan as the ordered data source, applying Take(N) directly.
        /// </summary>
        public bool IsOrderByOptimization { get; set; }
    }

    public enum SpatialQueryType { Near, Within }

    /// <summary>
    /// Describes how completely an index scan covers a WHERE expression.
    /// </summary>
    public enum FilterCompleteness
    {
        /// <summary>
        /// The index scan fully covers the WHERE clause; no in-memory post-filter is needed.
        /// Applies to equality, inclusive-bound ranges (>= / <=), boolean members, and compound
        /// AND expressions where both branches map to the same index.
        /// </summary>
        Exact,

        /// <summary>
        /// The BTree scan uses inclusive bounds, but the original operator was strict (> or <).
        /// The boundary value(s) must be excluded by applying the compiled predicate as a post-filter.
        /// </summary>
        StrictBoundary,

        /// <summary>
        /// The index covers only one branch of a compound AND expression.
        /// The full WHERE predicate must be evaluated as a post-filter to satisfy the remaining condition.
        /// </summary>
        PartialAnd,
    }

    [RequiresDynamicCode("Index optimization may use Expression.Compile() to evaluate complex expressions.")]
    public static OptimizationResult? TryOptimize<T>(QueryModel model, IEnumerable<CollectionIndexInfo> indexes, ValueConverterRegistry? registry = null)
    {
        if (model.WhereClause == null) return null;

        return OptimizeExpression(model.WhereClause.Body, model.WhereClause.Parameters[0], indexes, registry);
    }

    /// <summary>
    /// Overload that works directly on a WHERE lambda, without a full <see cref="QueryModel"/>.
    /// Used by <c>DocumentCollection.FetchAsync</c> so the index-selection logic lives in one place.
    /// </summary>
    [RequiresDynamicCode("Index optimization may use Expression.Compile() to evaluate complex expressions.")]
    public static OptimizationResult? TryOptimize<T>(LambdaExpression whereClause, IEnumerable<CollectionIndexInfo> indexes, ValueConverterRegistry? registry = null)
        => OptimizeExpression(whereClause.Body, whereClause.Parameters[0], indexes, registry);

    /// <summary>
    /// Attempts to satisfy an <c>OrderBy[Descending](field).Skip(S).Take(N)</c> query entirely
    /// from a secondary BTree index, avoiding a full <c>FindAll()</c> + in-memory sort.
    /// <para>
    /// Prerequisites: no WHERE clause, a simple single-field OrderBy on an indexed field, and
    /// a Take(N) limit.  An optional Skip(S) is supported; the caller passes the skip and take
    /// counts directly to <c>QueryIndexAsync</c>, which skips index entries before reading
    /// documents so that only the requested window is ever deserialised.
    /// </para>
    /// </summary>
    public static OptimizationResult? TryOptimizeOrderBy(QueryModel model, IEnumerable<CollectionIndexInfo> indexes)
    {
        // Only applicable when there is no WHERE and a Take limit.
        if (model.WhereClause != null) return null;
        if (model.OrderByClause == null) return null;
        if (!model.Take.HasValue) return null;

        var body = model.OrderByClause.Body;
        var param = model.OrderByClause.Parameters[0];

        // The OrderBy key selector must be a simple property access on the entity parameter.
        string? propName = body switch
        {
            MemberExpression me when me.Expression == param => me.Member.Name,
            // Handle Convert(property) — e.g. enum fields
            UnaryExpression { NodeType: ExpressionType.Convert } ue
                when ue.Operand is MemberExpression me2 && me2.Expression == param => me2.Member.Name,
            _ => null
        };

        if (propName is null) return null;

        CollectionIndexInfo? index = null;
        foreach (var idx in indexes) { if (idx.Type == IndexType.BTree && Matches(idx, propName)) { index = idx; break; } }
        if (index is null) return null;

        return new OptimizationResult
        {
            IndexName = index.Name,
            IsOrderByOptimization = true
        };
    }

    [RequiresDynamicCode("Index optimization may use Expression.Compile() to evaluate complex expressions.")]
    private static OptimizationResult? OptimizeExpression(Expression expression, ParameterExpression parameter, IEnumerable<CollectionIndexInfo> indexes, ValueConverterRegistry? registry = null)
    {
        // ... (Existing AndAlso logic remains the same) ...
        if (expression is BinaryExpression binary && binary.NodeType == ExpressionType.AndAlso)
        {
            var left = OptimizeExpression(binary.Left, parameter, indexes, registry);
            var right = OptimizeExpression(binary.Right, parameter, indexes, registry);

            if (left != null && right != null && left.IndexName == right.IndexName)
            {
                // Both sides are covered by the same index — combine into a single range.
                // Propagate the stricter completeness: if either branch required a post-filter
                // (StrictBoundary), the merged range still needs one.
                // The inclusivity of each bound comes from whichever side owns that bound.
                var mergedCompleteness = (left.FilterCompleteness == FilterCompleteness.Exact &&
                                          right.FilterCompleteness == FilterCompleteness.Exact)
                    ? FilterCompleteness.Exact
                    : FilterCompleteness.StrictBoundary;
                bool startInclusive = left.MinValue != null ? left.StartInclusive : right.StartInclusive;
                bool endInclusive   = left.MaxValue != null ? left.EndInclusive   : right.EndInclusive;
                return new OptimizationResult
                {
                    IndexName = left.IndexName,
                    MinValue = left.MinValue ?? right.MinValue,
                    MaxValue = left.MaxValue ?? right.MaxValue,
                    IsRange = true,
                    FilterCompleteness = mergedCompleteness,
                    StartInclusive = startInclusive,
                    EndInclusive = endInclusive
                };
            }
            // Only one side of the AND is indexable — the index narrows candidates but
            // does not fully satisfy the WHERE; caller must post-filter.
            if (left != null)  { left.FilterCompleteness  = FilterCompleteness.PartialAnd; left.HasResiduePredicate  = true; return left; }
            if (right != null) { right.FilterCompleteness = FilterCompleteness.PartialAnd; right.HasResiduePredicate = true; return right; }
            return null;
        }

        // Handle OR over exact matches on the same indexed field:
        // x => x.Prop == a || x.Prop == b  → multi-point index probes.
        if (expression is BinaryExpression orBinary && orBinary.NodeType == ExpressionType.OrElse)
        {
            var left = OptimizeExpression(orBinary.Left, parameter, indexes, registry);
            var right = OptimizeExpression(orBinary.Right, parameter, indexes, registry);

            if (left != null && right != null &&
                left.IndexName == right.IndexName &&
                left.FilterCompleteness == FilterCompleteness.Exact &&
                right.FilterCompleteness == FilterCompleteness.Exact &&
                TryGetPointValues(left, out var leftValues) &&
                TryGetPointValues(right, out var rightValues))
            {
                var merged = new List<object?>(leftValues.Count + rightValues.Count);
                var seen = new HashSet<object?>(s_inValueComparer);
                foreach (var v in leftValues)
                {
                    if (seen.Add(v)) merged.Add(v);
                }
                foreach (var v in rightValues)
                {
                    if (seen.Add(v)) merged.Add(v);
                }

                return new OptimizationResult
                {
                    IndexName = left.IndexName,
                    InValues = merged,
                    IsRange = false,
                    FilterCompleteness = FilterCompleteness.Exact,
                    StartInclusive = true,
                    EndInclusive = true
                };
            }
            return null;
        }

        // Handle bare bool member: e => e.IsActive  (equivalent to e.IsActive == true)
        // Handle logical NOT over bool member: e => !e.IsActive  (equivalent to e.IsActive == false)
        if (expression is MemberExpression bareMember &&
            bareMember.Expression == parameter &&
            bareMember.Type == typeof(bool))
        {
            CollectionIndexInfo? ix = null;
            foreach (var idx in indexes) { if (Matches(idx, bareMember.Member.Name)) { ix = idx; break; } }
            if (ix != null)
                return new OptimizationResult { IndexName = ix.Name, MinValue = true, MaxValue = true, IsRange = false, FilterCompleteness = FilterCompleteness.Exact };
        }

        if (expression is UnaryExpression { NodeType: ExpressionType.Not } notExpr &&
            notExpr.Operand is MemberExpression notMember &&
            notMember.Expression == parameter &&
            notMember.Type == typeof(bool))
        {
            CollectionIndexInfo? ix = null;
            foreach (var idx in indexes) { if (Matches(idx, notMember.Member.Name)) { ix = idx; break; } }
            if (ix != null)
                return new OptimizationResult { IndexName = ix.Name, MinValue = false, MaxValue = false, IsRange = false, FilterCompleteness = FilterCompleteness.Exact };
        }

        // Handle Simple Binary Predicates
        var (propertyName, value, op) = ParseSimplePredicate(expression, parameter, registry);
        if (propertyName != null)
        {
            CollectionIndexInfo? index = null;
            foreach (var idx in indexes) { if (Matches(idx, propertyName)) { index = idx; break; } }
            if (index != null)
            {
                var result = new OptimizationResult { IndexName = index.Name };
                switch (op)
                {
                    case ExpressionType.Equal:
                        if (value == null)
                        {
                            // Null equality: use DBNull.Value as sentinel so QueryIndexAsync
                            // scans exactly the NullSentinel key (the null bucket in the B-tree).
                            result.MinValue = DBNull.Value;
                            result.MaxValue = DBNull.Value;
                        }
                        else
                        {
                            result.MinValue = value;
                            result.MaxValue = value;
                        }
                        result.IsRange = false;
                        result.FilterCompleteness = FilterCompleteness.Exact;   // equality on indexed field: scan is exact
                        result.StartInclusive = true;
                        result.EndInclusive   = true;
                        break;
                    case ExpressionType.GreaterThanOrEqual:
                        result.MinValue = value;
                        result.MaxValue = null;
                        result.IsRange = true;
                        result.FilterCompleteness = FilterCompleteness.Exact;   // BTree Range includes lower bound => exact
                        result.StartInclusive = true;
                        result.EndInclusive   = true;  // upper bound is unbounded
                        break;
                    case ExpressionType.GreaterThan:
                        result.MinValue = value;
                        result.MaxValue = null;
                        result.IsRange = true;
                        result.FilterCompleteness = FilterCompleteness.StrictBoundary;  // BTree Range includes lower bound; need post-filter to exclude it
                        result.StartInclusive = false; // strict: boundary value itself is excluded
                        result.EndInclusive   = true;  // upper bound is unbounded
                        break;
                    case ExpressionType.LessThanOrEqual:
                        result.MinValue = null;
                        result.MaxValue = value;
                        result.IsRange = true;
                        result.FilterCompleteness = FilterCompleteness.Exact;   // BTree Range includes upper bound => exact
                        result.StartInclusive = true;  // lower bound is unbounded
                        result.EndInclusive   = true;
                        break;
                    case ExpressionType.LessThan:
                        result.MinValue = null;
                        result.MaxValue = value;
                        result.IsRange = true;
                        result.FilterCompleteness = FilterCompleteness.StrictBoundary;  // BTree Range includes upper bound; need post-filter to exclude it
                        result.StartInclusive = true;  // lower bound is unbounded
                        result.EndInclusive   = false; // strict: boundary value itself is excluded
                        break;
                    default:
                        // NotEqual and any future operators cannot be satisfied by a single
                        // contiguous B-tree range scan.  Return null so the caller falls through
                        // to BsonExpressionEvaluator (Strategy 2) or full-scan (Strategy 3),
                        // both of which evaluate the predicate correctly in-memory.
                        return null;
                }
                return result;
            }
        }

        if (TryParseContainsInPredicate(expression, parameter, registry, out var inPropertyPath, out var inValues))
        {
            CollectionIndexInfo? index = null;
            foreach (var idx in indexes) { if (Matches(idx, inPropertyPath)) { index = idx; break; } }
            if (index != null)
            {
                return new OptimizationResult
                {
                    IndexName = index.Name,
                    InValues = inValues,
                    IsRange = false,
                    FilterCompleteness = FilterCompleteness.Exact,
                    StartInclusive = true,
                    EndInclusive = true
                };
            }
        }

        // Handle StartsWith
        if (expression is MethodCallExpression call && call.Method.Name == "StartsWith" && call.Object is MemberExpression member)
        {
             if (member.Expression == parameter && call.Arguments[0] is ConstantExpression constant && constant.Value is string prefix)
             {
                 CollectionIndexInfo? index = null;
                 foreach (var idx in indexes) { if (Matches(idx, member.Member.Name) && idx.Type == IndexType.BTree) { index = idx; break; } }
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
                
                CollectionIndexInfo? index = null;
                foreach (var idx in indexes) { if (idx.Type == IndexType.Vector && Matches(idx, vMember.Member.Name)) { index = idx; break; } }
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

                CollectionIndexInfo? index = null;
                foreach (var idx in indexes) { if (idx.Type == IndexType.Spatial && Matches(idx, nMember.Member.Name)) { index = idx; break; } }
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

                CollectionIndexInfo? index = null;
                foreach (var idx in indexes) { if (idx.Type == IndexType.Spatial && Matches(idx, wMember.Member.Name)) { index = idx; break; } }
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

    private static bool TryGetPointValues(OptimizationResult result, out IReadOnlyList<object?> values)
    {
        if (result.InValues != null)
        {
            values = result.InValues;
            return true;
        }

        if (!result.IsRange && Equals(result.MinValue, result.MaxValue))
        {
            values = [result.MinValue];
            return true;
        }

        values = Array.Empty<object?>();
        return false;
    }

    private static string IncrementPrefix(string prefix)
    {
        if (string.IsNullOrEmpty(prefix)) return null!;
        char lastChar = prefix[prefix.Length - 1];
        if (lastChar == char.MaxValue) return prefix; // Cannot increment
        return prefix.Substring(0, prefix.Length - 1) + (char)(lastChar + 1);
    }

    [RequiresDynamicCode("Fallback expression evaluation uses Expression.Compile() which requires dynamic code generation.")]
    private static T EvaluateExpression<T>(Expression expression)
    {
        if (expression is ConstantExpression constant)
            return (T)constant.Value!;

        // Fast path: closure capture chain — no Compile/DynamicInvoke needed.
        if (TryWalkMemberChain(expression, out var walked))
            return (T)walked!;

        // Rare fallback: arithmetic, ternary, method calls, etc.
        var lambda = Expression.Lambda(expression);
        return (T)lambda.Compile().DynamicInvoke()!;
    }

    /// <summary>
    /// Resolves a <see cref="MemberExpression"/> chain rooted in a <see cref="ConstantExpression"/>
    /// (compiler-generated closure) via <see cref="FieldInfo"/>/<see cref="PropertyInfo"/> only.
    /// </summary>
    private static bool TryWalkMemberChain(Expression expr, out object? value)
    {
        // Fast path: depth-1 closure capture (e.g. `x => x.Prop == capturedVar`).
        // The compiler generates a DisplayClass with a field; the RHS is:
        //   MemberExpression { Member = capturedVar_field, Expression = ConstantExpression(displayClass) }
        // This is by far the most common case — avoid Stack<MemberInfo> allocation entirely.
        if (expr is MemberExpression me1 && me1.Expression is ConstantExpression root1)
        {
            value = me1.Member is FieldInfo fi1
                ? fi1.GetValue(root1.Value)
                : ((PropertyInfo)me1.Member).GetValue(root1.Value);
            return true;
        }

        // General path: arbitrarily deep chain of member accesses on a closure root.
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

    private static bool Matches(CollectionIndexInfo index, string propertyName)
    {
        if (index.PropertyPaths == null || index.PropertyPaths.Length == 0) return false;
        var indexPath = index.PropertyPaths[0];
        if (indexPath.Equals(propertyName, StringComparison.OrdinalIgnoreCase)) return true;

        // Treat "_id" (BSON primary-key name) and "Id" (C# property name) as equivalent so that
        // an index created with either name can be found when the expression visitor extracts the
        // other.  The serializer writes the field as "_id" for root documents and "id" for nested
        // documents, but both refer to the same primary-key property.
        static string Normalize(string s) =>
            s.Equals("_id", StringComparison.OrdinalIgnoreCase) ? "Id" : s;

        return Normalize(indexPath).Equals(Normalize(propertyName), StringComparison.OrdinalIgnoreCase);
    }

    [RequiresDynamicCode("Index optimization may use Expression.Compile() to evaluate complex expressions.")]
    private static (string? propertyName, object? value, ExpressionType op) ParseSimplePredicate(Expression expression, ParameterExpression parameter, ValueConverterRegistry? registry = null)
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
                // Extract full property path (supports nested properties like Address.City.Name)
                var propertyPath = ExtractMemberPath(member, parameter);
                if (propertyPath != null)
                    return (propertyPath, constant.Value, nodeType);
            }
            
            // Handle Convert (e.g. enum property compared to int constant)
            if (left is UnaryExpression unary && unary.Operand is MemberExpression member2 && right is ConstantExpression constant2)
            {
                var propertyPath = ExtractMemberPath(member2, parameter);
                if (propertyPath != null)
                {
                    // If the property is an enum but the constant is the underlying numeric type,
                    // convert back to the enum so ConvertToIndexKey uses the enum branch (same as Insert).
                    var propType = unary.Operand.Type;
                    var val2 = constant2.Value;
                    if (propType.IsEnum && val2 != null && !val2.GetType().IsEnum)
                        val2 = Enum.ToObject(propType, val2);
                    return (propertyPath, val2, nodeType);
                }
            }

            // Handle closure captures on the right side (e.g. e.Prop == closureVar.Field)
            if (left is MemberExpression memberLeft)
            {
                var propertyPath = ExtractMemberPath(memberLeft, parameter);
                if (propertyPath != null)
                {
                    try
                    {
                        var val = EvaluateExpression<object>(right);
                        val = TryApplyConverter(propertyPath, val, registry);
                        if (IsIndexableValue(val))
                            return (propertyPath, val, nodeType);
                    }
                    catch { }
                }
            }

            // Handle Convert on left with closure on right
            if (left is UnaryExpression unaryLeft && unaryLeft.Operand is MemberExpression memberLeft2)
            {
                var propertyPath = ExtractMemberPath(memberLeft2, parameter);
                if (propertyPath != null)
                {
                    try
                    {
                        var val = EvaluateExpression<object>(right);
                        // If the property is an enum, convert the value back to the enum type.
                        var propType = unaryLeft.Operand.Type;
                        if (propType.IsEnum && val != null && !val.GetType().IsEnum)
                            val = Enum.ToObject(propType, val);
                        val = TryApplyConverter(propertyPath, val, registry);
                        if (IsIndexableValue(val) || (val != null && val.GetType().IsEnum))
                            return (propertyPath, val, nodeType);
                    }
                    catch { }
                }
            }
        }

        // Handle .Equals() method call: e.Id.Equals(item.Id) or e.Prop.Equals(constant)
        if (expression is MethodCallExpression equalsCall &&
            equalsCall.Method.Name == "Equals" &&
            equalsCall.Arguments.Count == 1 &&
            equalsCall.Object is MemberExpression equalsOnMember)
        {
            var propertyPath = ExtractMemberPath(equalsOnMember, parameter);
            if (propertyPath != null)
            {
                try
                {
                    var val = EvaluateExpression<object>(equalsCall.Arguments[0]);
                    val = TryApplyConverter(propertyPath, val, registry);
                    if (IsIndexableValue(val))
                        return (propertyPath, val, ExpressionType.Equal);
                }
                catch { }
            }
        }

        return (null, null, ExpressionType.Default);
    }

    [RequiresDynamicCode("Index optimization may use Expression.Compile() to evaluate complex expressions.")]
    private static bool TryParseContainsInPredicate(
        Expression expression,
        ParameterExpression parameter,
        ValueConverterRegistry? registry,
        out string propertyPath,
        out IReadOnlyList<object?> values)
    {
        propertyPath = string.Empty;
        values = Array.Empty<object?>();

        if (expression is not MethodCallExpression call || call.Method.Name != "Contains")
            return false;

        static Expression UnwrapConvert(Expression e)
        {
            while (e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                e = u.Operand;
            return e;
        }

        Expression? collectionExpr = null;
        Expression? memberExpr = null;

        if (call.Object != null && call.Arguments.Count == 1)
        {
            // Instance Contains: list.Contains(x.Prop)
            collectionExpr = call.Object;
            memberExpr = call.Arguments[0];
        }
        else if (call.Object == null && (call.Arguments.Count == 2 || call.Arguments.Count == 3))
        {
            // Enumerable.Contains(list, x.Prop) or MemoryExtensions.Contains(span, x.Prop, comparer)
            // where the comparer argument is typically null.
            if (call.Arguments.Count == 3)
            {
                object? comparer;
                try
                {
                    comparer = EvaluateExpression<object>(call.Arguments[2]);
                }
                catch
                {
                    return false;
                }

                if (comparer != null)
                    return false;
            }

            collectionExpr = call.Arguments[0];
            memberExpr = call.Arguments[1];
        }
        else
        {
            return false;
        }

        var unwrappedMember = UnwrapConvert(memberExpr);
        if (unwrappedMember is not MemberExpression member)
            return false;

        var extractedPath = ExtractMemberPath(member, parameter);
        if (extractedPath == null)
            return false;
        propertyPath = extractedPath;

        if (collectionExpr is MethodCallExpression { Method.Name: "op_Implicit", Object: null } implicitCall &&
            implicitCall.Arguments.Count == 1)
        {
            collectionExpr = implicitCall.Arguments[0];
        }

        object? enumerableObj;
        try
        {
            enumerableObj = EvaluateExpression<object>(collectionExpr);
        }
        catch
        {
            return false;
        }

        if (enumerableObj is not System.Collections.IEnumerable enumerable || enumerableObj is string)
            return false;

        var list = new List<object?>();
        var seen = new HashSet<object?>(s_inValueComparer);
        foreach (var raw in enumerable)
        {
            var converted = TryApplyConverter(propertyPath, raw, registry);
            if (!IsIndexableValue(converted))
                return false;
            var normalized = converted ?? DBNull.Value;
            if (seen.Add(normalized))
            {
                list.Add(normalized);
            }
        }

        values = list;
        return true;
    }

    private static readonly IEqualityComparer<object?> s_inValueComparer = new InValueComparer();

    private sealed class InValueComparer : IEqualityComparer<object?>
    {
        bool IEqualityComparer<object?>.Equals(object? x, object? y)
        {
            if (ReferenceEquals(x, y)) return true;
            if (x is null || y is null) return false;
            if (x is byte[] xb && y is byte[] yb) return xb.AsSpan().SequenceEqual(yb);
            return x.Equals(y);
        }

        int IEqualityComparer<object?>.GetHashCode(object? obj)
        {
            if (obj is null) return 0;
            if (obj is byte[] bytes)
            {
                var hc = new HashCode();
                foreach (var b in bytes) hc.Add(b);
                return hc.ToHashCode();
            }
            return obj.GetHashCode();
        }
    }

    private static object? TryApplyConverter(string propertyPath, object? value, ValueConverterRegistry? registry)
    {
        if (registry == null || value == null) return value;
        // Use top-level property name (first segment) for registry lookup
        var topProp = propertyPath.Contains('.') ? propertyPath[..propertyPath.IndexOf('.')] : propertyPath;
        return registry.TryConvert(topProp, value, out var pv) ? pv : value;
    }

    private static readonly HashSet<Type> _knownBsonPrimitives =
    [
        typeof(int), typeof(long), typeof(double), typeof(decimal),
        typeof(bool), typeof(string), typeof(DateTime), typeof(DateTimeOffset),
        typeof(ObjectId), typeof(Guid), typeof(byte[])
    ];

    /// <summary>
    /// Returns <c>true</c> when <paramref name="value"/> can be used directly as an index
    /// key (i.e. it is a BSON-native type). Non-primitive values (e.g. ValueObjects) must be
    /// converted via <see cref="TryApplyConverter"/> before being accepted as index keys.
    /// </summary>
    private static bool IsIndexableValue(object? value)
    {
        if (value == null) return true; // null is valid for range bounds
        var type = Nullable.GetUnderlyingType(value.GetType()) ?? value.GetType();
        return _knownBsonPrimitives.Contains(type);
    }

    /// <summary>
    /// Extracts full dot-notation path from a MemberExpression chain.
    /// Returns null if the expression doesn't start from the expected parameter.
    /// Example: p.Address.City.Name -> "Address.City.Name"
    /// </summary>
    private static string? ExtractMemberPath(MemberExpression memberExpr, ParameterExpression expectedParameter)
    {
        var parts = new List<string>();
        Expression? current = memberExpr;

        while (current is MemberExpression member)
        {
            parts.Insert(0, member.Member.Name);
            current = member.Expression;
        }

        // Should terminate at the parameter (e.g., 'p' in p => p.Name)
        if (current != expectedParameter)
            return null;

        return string.Join(".", parts);
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
