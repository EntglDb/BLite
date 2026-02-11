using System.Collections;
using System.Linq;
using System.Linq.Expressions;

namespace BLite.Core.Query;

internal class BTreeQueryable<T> : IOrderedQueryable<T>
{
    public BTreeQueryable(IQueryProvider provider, Expression expression)
    {
        Provider = provider;
        Expression = expression;
    }

    public BTreeQueryable(IQueryProvider provider)
    {
        Provider = provider;
        Expression = Expression.Constant(this);
    }

    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider { get; }

    public IEnumerator<T> GetEnumerator()
    {
        return Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
