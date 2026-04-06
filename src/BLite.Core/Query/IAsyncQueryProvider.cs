using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;

namespace BLite.Core.Query;

/// <summary>
/// Extends <see cref="IQueryProvider"/> with a true-async execution path.
/// <see cref="BTreeQueryable{T}"/> uses this interface to call <see cref="ExecuteAsync{TResult}"/>
/// directly, avoiding the double-<c>Task.Run</c> overhead of the synchronous
/// <see cref="IQueryProvider.Execute{TResult}"/> path.
/// </summary>
internal interface IAsyncQueryProvider : IQueryProvider
{
    [RequiresDynamicCode("BLite LINQ queries use Expression.Compile() and MakeGenericMethod which require dynamic code generation.")]
    [RequiresUnreferencedCode("BLite LINQ queries use reflection to resolve methods and types at runtime. Ensure all entity types and their members are preserved.")]
    Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken ct);
}
