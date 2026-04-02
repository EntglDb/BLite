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
    Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken ct);
}
