using System.Linq.Expressions;
using BLite.Bson;

namespace BLite.Core.Query;

/// <summary>
/// Public AOT-safe query utilities for source-generated interceptors.
/// Wraps <see cref="BsonExpressionEvaluator"/> internals behind a public surface
/// so that generated interceptor code in consumer projects can call it.
/// </summary>
public static class BLiteAotHelper
{
    /// <summary>
    /// Attempts to compile a LINQ WHERE lambda into a BSON-level predicate without using
    /// <c>Expression.Compile()</c>. Returns <c>null</c> if the expression cannot be
    /// translated (caller should fall back to the standard query path).
    /// </summary>
    /// <typeparam name="T">The entity type being queried.</typeparam>
    /// <param name="lambda">The WHERE lambda expression to compile.</param>
    public static BsonReaderPredicate? TryCompileWherePredicate<T>(LambdaExpression lambda)
        => BsonExpressionEvaluator.TryCompile<T>(lambda);
}
