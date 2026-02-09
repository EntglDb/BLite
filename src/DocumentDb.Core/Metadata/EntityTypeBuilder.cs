using System.Linq.Expressions;

namespace DocumentDb.Core.Metadata;

public class EntityTypeBuilder<T> where T : class
{
    public string? CollectionName { get; private set; }
    public List<IndexBuilder<T>> Indexes { get; } = new();

    public EntityTypeBuilder<T> ToCollection(string name)
    {
        CollectionName = name;
        return this;
    }

    public EntityTypeBuilder<T> HasIndex<TKey>(Expression<Func<T, TKey>> keySelector, string? name = null, bool unique = false)
    {
        Indexes.Add(new IndexBuilder<T>(keySelector, name, unique));
        return this;
    }
}

public class IndexBuilder<T>
{
    public LambdaExpression KeySelector { get; }
    public string? Name { get; }
    public bool IsUnique { get; }

    public IndexBuilder(LambdaExpression keySelector, string? name, bool unique)
    {
        KeySelector = keySelector;
        Name = name;
        IsUnique = unique;
    }
}
