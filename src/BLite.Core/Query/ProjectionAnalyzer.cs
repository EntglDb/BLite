using System.Linq.Expressions;

namespace BLite.Core.Query;

/// <summary>Describes a single property of <typeparamref name="T"/> accessed by a SELECT lambda.</summary>
internal sealed class FieldAccess
{
    /// <summary>C# property name as declared on the entity (PascalCase).</summary>
    public string PropertyName { get; init; } = default!;

    /// <summary>
    /// BSON field name as stored on disk.
    /// Follows the convention: <c>PropName.ToLowerInvariant()</c>, except <c>Id</c> → <c>_id</c>.
    /// </summary>
    public string BsonName { get; init; } = default!;

    /// <summary>CLR type of the property (e.g. <c>typeof(string)</c>, <c>typeof(int)</c>).</summary>
    public Type ClrType { get; init; } = default!;

    /// <summary>Zero-based slot index in the <c>object?[]</c> values array passed to the compiled selector.</summary>
    public int Index { get; init; }
}

/// <summary>Result of analyzing a SELECT lambda expression.</summary>
internal sealed class ProjectionAnalysis
{
    /// <summary>
    /// <c>true</c> when every member access is a direct flat property/field of the lambda parameter
    /// (no nested paths, no method calls on T members).
    /// Only when <c>IsSimple</c> is <c>true</c> will <see cref="BsonProjectionCompiler"/> attempt
    /// to compile a push-down reader.
    /// </summary>
    public bool IsSimple { get; init; }

    /// <summary>
    /// Ordered list of T properties accessed by the lambda.
    /// Empty when <see cref="IsSimple"/> is <c>false</c>.
    /// </summary>
    public FieldAccess[] Fields { get; init; } = [];
}

/// <summary>
/// Statically analyses a LINQ SELECT lambda expression to determine which properties of the
/// source entity <c>T</c> it accesses, and whether a BSON-level push-down projection is feasible.
/// </summary>
internal static class ProjectionAnalyzer
{
    /// <summary>
    /// Analyses <paramref name="lambda"/> and returns the accessed fields and a simplicity flag.
    /// Guarantees that duplicate property names are deduplicated (each property appears once).
    /// </summary>
    public static ProjectionAnalysis Analyze(LambdaExpression lambda)
    {
        if (lambda.Parameters.Count != 1)
            return new ProjectionAnalysis { IsSimple = false, Fields = [] };

        var visitor = new FieldAccessVisitor(lambda.Parameters[0]);
        visitor.Visit(lambda.Body);

        if (!visitor.IsSimple)
            return new ProjectionAnalysis { IsSimple = false, Fields = [] };

        var fields = visitor.Accessed
            .Select((kv, i) => new FieldAccess
            {
                PropertyName = kv.Key,
                BsonName = ToBsonName(kv.Key),
                ClrType = kv.Value,
                Index = i
            })
            .ToArray();

        return new ProjectionAnalysis { IsSimple = true, Fields = fields };
    }

    internal static string ToBsonName(string propertyName)
        => string.Equals(propertyName, "Id", StringComparison.Ordinal) ? "_id"
           : propertyName.ToLowerInvariant();

    // ─── Scalar type guard ────────────────────────────────────────────────────

    /// <summary>
    /// Returns <c>true</c> when <paramref name="type"/> is a CLR type that
    /// <see cref="BsonProjectionCompiler"/> can read as a boxed scalar from raw BSON bytes.
    /// Complex types (nested documents, collections, arbitrary classes/structs) are excluded
    /// so that the push-down never silently returns <c>null</c> in place of a real sub-document.
    /// </summary>
    internal static bool IsSupportedScalarType(Type type)
    {
        // Unwrap Nullable<T>
        var underlying = Nullable.GetUnderlyingType(type) ?? type;

        return underlying.IsPrimitive           // bool, char, byte, sbyte, short, ushort,
                                                // int, uint, long, ulong, float, double
            || underlying == typeof(decimal)
            || underlying == typeof(string)
            || underlying == typeof(DateTime)
            || underlying == typeof(DateTimeOffset)
            || underlying == typeof(DateOnly)
            || underlying == typeof(TimeOnly)
            || underlying == typeof(TimeSpan)
            || underlying == typeof(Guid)
            || underlying.FullName == "BLite.Bson.ObjectId"  // no hard dep on BLite.Bson here
            || underlying.IsEnum;               // enums stored as Int32/Int64 in BSON
    }

    // ─── Visitor ─────────────────────────────────────────────────────────────

    private sealed class FieldAccessVisitor : ExpressionVisitor
    {
        private readonly ParameterExpression _param;

        public bool IsSimple { get; private set; } = true;

        /// <summary>Ordered unique property names → CLR type (insertion order preserved).</summary>
        public OrderedDictionary<string, Type> Accessed { get; } = new();

        public FieldAccessVisitor(ParameterExpression param) => _param = param;

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression == _param)
            {
                // Direct flat property/field on the lambda parameter.
                // Only accept types that BsonProjectionCompiler can read as scalar box values.
                if (!IsSupportedScalarType(node.Type))
                {
                    IsSimple = false;
                    return base.VisitMember(node);
                }
                Accessed.TryAdd(node.Member.Name, node.Type);
            }
            else if (node.Expression is not null and not ConstantExpression)
            {
                // Nested path (x.Address.City) or unrecognised source → not simple.
                IsSimple = false;
            }
            return base.VisitMember(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            // Method call directly on a T property, e.g. x.Name.ToUpper() — not simple.
            if (node.Object is MemberExpression { Expression: ParameterExpression p } && p == _param)
                IsSimple = false;

            return base.VisitMethodCall(node);
        }
    }
}
