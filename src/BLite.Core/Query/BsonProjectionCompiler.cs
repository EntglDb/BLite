using System.Linq.Expressions;
using BLite.Bson;

namespace BLite.Core.Query;

/// <summary>
/// Compiles a SELECT (and optional WHERE) lambda expression into a single-pass
/// <c>Func&lt;BsonSpanReader, TResult?&gt;</c> that reads only the fields required
/// by the projection from raw BSON bytes, without ever instantiating <typeparamref name="T"/>.
/// </summary>
/// <remarks>
/// <para>Returns <c>null</c> whenever compilation is not safe or possible — this is always
/// a soft failure: the caller should fall through to the standard materialise-then-project path.</para>
/// <para>Supported projection shapes (IsSimple = true):</para>
/// <list type="bullet">
///   <item><c>x =&gt; new Dto(x.F1, x.F2)</c> — constructor projection</item>
///   <item><c>x =&gt; new { x.F1, x.F2 }</c> — anonymous-type projection</item>
///   <item><c>x =&gt; x.F1</c> — single-field projection</item>
/// </list>
/// </remarks>
internal static class BsonProjectionCompiler
{
    /// <summary>
    /// Tries to compile a push-down projector for the given SELECT (and optional WHERE) lambda.
    /// </summary>
    /// <typeparam name="T">Source entity type.</typeparam>
    /// <typeparam name="TResult">Projection result type.</typeparam>
    /// <param name="selectLambda">SELECT expression, e.g. <c>x =&gt; new Dto(x.Status, x.Total)</c>.</param>
    /// <param name="whereLambda">
    ///   Optional WHERE predicate.  When supplied the projector returns <c>null</c> (= skipped) for
    ///   documents that fail the predicate, so the caller's scan loop naturally omits them.
    /// </param>
    public static Func<BsonSpanReader, TResult?>? TryCompile<T, TResult>(
        LambdaExpression selectLambda,
        LambdaExpression? whereLambda = null)
    {
        // ── 1. Analyse access patterns ────────────────────────────────────────
        var selAnalysis = ProjectionAnalyzer.Analyze(selectLambda);
        if (!selAnalysis.IsSimple) return null;

        ProjectionAnalysis? whereAnalysis = null;
        if (whereLambda != null)
        {
            whereAnalysis = ProjectionAnalyzer.Analyze(whereLambda);
            if (!whereAnalysis.IsSimple) return null;
        }

        // ── 2. Build the merged field list ────────────────────────────────────
        //    SELECT fields are assigned indices 0..n; additional WHERE-only fields
        //    are appended so that all fields are collected in one BSON pass.
        var fields = MergeFields(selAnalysis.Fields, whereAnalysis?.Fields);
        if (fields.Length == 0) return null; // nothing to project

        // ── 3. Compile  Func<object?[], TResult>  from SELECT ─────────────────
        var arrayParam = Expression.Parameter(typeof(object?[]), "_v");

        Func<object?[], TResult> selectorFromArray;
        try
        {
            var selRewriter = new MemberToArrayRewriter(selectLambda.Parameters[0], fields, arrayParam);
            var selBody = EnsureType(selRewriter.Visit(selectLambda.Body), typeof(TResult));
            selectorFromArray = Expression.Lambda<Func<object?[], TResult>>(selBody, arrayParam).Compile();
        }
        catch
        {
            return null; // projection shape is too complex for expression-tree rewriting
        }

        // ── 4. Optionally compile  Func<object?[], bool>  from WHERE ──────────
        Func<object?[], bool>? predicateFromArray = null;
        if (whereLambda != null)
        {
            try
            {
                var whereRewriter = new MemberToArrayRewriter(whereLambda.Parameters[0], fields, arrayParam);
                var whereBody = EnsureType(whereRewriter.Visit(whereLambda.Body), typeof(bool));
                predicateFromArray = Expression.Lambda<Func<object?[], bool>>(whereBody, arrayParam).Compile();
            }
            catch
            {
                return null;
            }
        }

        // ── 5. Build the BSON-level closure ───────────────────────────────────
        // Captured by the closure: field names array, element count, compiled delegates.
        // reader (BsonSpanReader, ref struct) is only a parameter — never captured.
        var fieldNames = fields.Select(f => f.BsonName).ToArray();
        var n = fields.Length;
        var sel = selectorFromArray;
        var pred = predicateFromArray;

        return reader =>
        {
            var values = new object?[n];
            try
            {
                reader.ReadDocumentSize();
                while (reader.Remaining > 1)
                {
                    var bsonType = reader.ReadBsonType();
                    if (bsonType == BsonType.EndOfDocument) break;
                    var name = reader.ReadElementHeader();
                    var idx = Array.IndexOf(fieldNames, name);
                    if (idx >= 0)
                    {
                        // Read and box the value; advance the reader position.
                        switch (bsonType)
                        {
                            case BsonType.Double:    values[idx] = reader.ReadDouble();    break;
                            case BsonType.String:    values[idx] = reader.ReadString();    break;
                            case BsonType.ObjectId:  values[idx] = reader.ReadObjectId();  break;
                            case BsonType.Boolean:   values[idx] = reader.ReadBoolean();   break;
                            case BsonType.DateTime:  values[idx] = reader.ReadDateTime();  break;
                            case BsonType.Int32:     values[idx] = reader.ReadInt32();     break;
                            case BsonType.Int64:     values[idx] = reader.ReadInt64();     break;
                            case BsonType.Decimal128: values[idx] = reader.ReadDecimal128(); break;
                            case BsonType.Null:      values[idx] = null;                   break;
                            default:
                                // Complex / unsupported type: skip so the reader does not
                                // get out of sync; leave the slot null.
                                reader.SkipValue(bsonType);
                                break;
                        }
                    }
                    else
                    {
                        reader.SkipValue(bsonType);
                    }
                }
            }
            catch
            {
                return default;
            }

            // Apply WHERE predicate on the collected values
            if (pred != null && !pred(values))
                return default;

            // Construct TResult
            try { return sel(values); }
            catch { return default; }
        };
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Merges SELECT fields and optional WHERE-only fields, deduplicating by property name.
    /// SELECT fields keep their original indices; WHERE-only fields are appended.
    /// </summary>
    private static FieldAccess[] MergeFields(FieldAccess[] primary, FieldAccess[]? secondary)
    {
        if (secondary is null || secondary.Length == 0) return primary;

        var result = new List<FieldAccess>(primary);
        var seen = new HashSet<string>(primary.Select(f => f.PropertyName));

        foreach (var f in secondary)
        {
            if (seen.Add(f.PropertyName))
            {
                result.Add(new FieldAccess
                {
                    PropertyName = f.PropertyName,
                    BsonName = f.BsonName,
                    ClrType = f.ClrType,
                    Index = result.Count
                });
            }
        }

        // Re-normalise indices (primary fields already have correct indices; new ones are correct too)
        return [.. result];
    }

    private static Expression EnsureType(Expression expr, Type targetType)
        => expr.Type == targetType ? expr : Expression.Convert(expr, targetType);

    // ─── MemberToArrayRewriter ────────────────────────────────────────────────

    /// <summary>
    /// Rewrites a lambda body replacing every direct member access on the source parameter
    /// (<c>x.Status</c>) with a properly-typed unbox from an <c>object?[]</c> slot.
    /// </summary>
    private sealed class MemberToArrayRewriter(
        ParameterExpression sourceParam,
        FieldAccess[] fields,
        ParameterExpression arrayParam) : ExpressionVisitor
    {
        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression != sourceParam)
                return base.VisitMember(node);

            var field = Array.Find(fields, f => f.PropertyName == node.Member.Name);
            if (field is null)
            {
                // Field not in our set (shouldn't happen post-analysis, but be safe)
                return base.VisitMember(node);
            }

            Expression slotExpr = Expression.ArrayIndex(arrayParam, Expression.Constant(field.Index));
            var targetType = node.Type;

            if (targetType.IsValueType && Nullable.GetUnderlyingType(targetType) is null)
            {
                // Non-nullable value type (int, double, bool, …): coalesce null to default
                // to avoid unbox-of-null InvalidCastException at runtime.
                var defaultObj = Expression.Constant(
                    Activator.CreateInstance(targetType), typeof(object));
                slotExpr = Expression.Coalesce(slotExpr, defaultObj);
                return Expression.Convert(slotExpr, targetType);
            }

            // Reference types (string, object) and nullable value types (int?, double?):
            // a direct Convert handles both null-as-null and value unboxing.
            return Expression.Convert(slotExpr, targetType);
        }
    }
}
