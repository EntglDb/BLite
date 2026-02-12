using BLite.Core.Indexing;
using System.Linq.Expressions;

namespace BLite.Core.Metadata;

public class EntityTypeBuilder<T> where T : class
{
    public string? CollectionName { get; private set; }
    public List<IndexBuilder<T>> Indexes { get; } = new();
    public LambdaExpression? PrimaryKeySelector { get; private set; }
    public bool ValueGeneratedOnAdd { get; private set; }
    public string? PrimaryKeyName { get; private set; }

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

    public EntityTypeBuilder<T> HasVectorIndex<TKey>(Expression<Func<T, TKey>> keySelector, int dimensions, VectorMetric metric = VectorMetric.Cosine, string? name = null)
    {
        Indexes.Add(new IndexBuilder<T>(keySelector, name, false, IndexType.Vector, dimensions, metric));
        return this;
    }

    public EntityTypeBuilder<T> HasKey<TKey>(Expression<Func<T, TKey>> keySelector)
    {
        PrimaryKeySelector = keySelector;
        PrimaryKeyName = ExpressionAnalyzer.ExtractPropertyPaths(keySelector).FirstOrDefault() ?? "_id";
        return this;
    }

    public PropertyBuilder Property<TProperty>(Expression<Func<T, TProperty>> propertyExpression)
    {
        var propertyName = ExpressionAnalyzer.ExtractPropertyPaths(propertyExpression).FirstOrDefault();
        return new PropertyBuilder(this, propertyName);
    }

    public class PropertyBuilder
    {
        private readonly EntityTypeBuilder<T> _parent;
        private readonly string? _propertyName;

        public PropertyBuilder(EntityTypeBuilder<T> parent, string? propertyName)
        {
            _parent = parent;
            _propertyName = propertyName;
        }

        public PropertyBuilder ValueGeneratedOnAdd()
        {
            if (_propertyName == _parent.PrimaryKeyName)
            {
                _parent.ValueGeneratedOnAdd = true;
            }
            return this;
        }
    }
}

public class IndexBuilder<T>
{
    public LambdaExpression KeySelector { get; }
    public string? Name { get; }
    public bool IsUnique { get; }
    public IndexType Type { get; }
    public int Dimensions { get; }
    public VectorMetric Metric { get; }

    public IndexBuilder(LambdaExpression keySelector, string? name, bool unique, IndexType type = IndexType.BTree, int dimensions = 0, VectorMetric metric = VectorMetric.Cosine)
    {
        KeySelector = keySelector;
        Name = name;
        IsUnique = unique;
        Type = type;
        Dimensions = dimensions;
        Metric = metric;
    }
}
