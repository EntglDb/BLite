using BLite.Core.GDPR;
using BLite.Core.Indexing;
using System.Linq.Expressions;
using BLite.Core.Retention;

namespace BLite.Core.Metadata;

/// <summary>
/// Non-generic accessor that allows callers holding an untyped reference to an
/// <see cref="EntityTypeBuilder{T}"/> (e.g. <c>DocumentDbContext._model</c>) to read
/// the resolved <see cref="GdprMode"/> without reflection.
/// </summary>
internal interface IGdprModeAccessor
{
    /// <summary>The GDPR enforcement mode configured for this entity type.</summary>
    GdprMode GdprMode { get; }
}

/// <summary>
/// Fluent API for configuring entity mappings and indexes.
/// 
/// Configuration Priority (highest to lowest):
/// 1. ModelBuilder (OnModelCreating) - analyzed by Source Generator at compile-time
/// 2. Attributes on entity classes ([Table], [Key], [BsonConverter], etc.)
/// 3. Conventions (property named "Id", class name + "s" for collection, etc.)
/// 
/// The Source Generator analyzes OnModelCreating and embeds all configurations into the generated mapper.
/// At runtime, only indexes are applied dynamically from ModelBuilder.
/// </summary>
public class EntityTypeBuilder<T> : IGdprModeAccessor where T : class
{
    public string? CollectionName { get; private set; }
    public List<IndexBuilder<T>> Indexes { get; } = new();
    public LambdaExpression? PrimaryKeySelector { get; private set; }
    public bool ValueGeneratedOnAdd { get; private set; }
    public string? PrimaryKeyName { get; private set; }
    public Dictionary<string, Type> PropertyConverters { get; } = new();
    public string? TimeSeriesTtlField { get; private set; }
    public TimeSpan? TimeSeriesRetention { get; private set; }
    public RetentionPolicy? RetentionPolicy { get; private set; }

    /// <summary>
    /// Personal-data annotations set via the fluent
    /// <see cref="PropertyBuilderGdprExtensions.HasPersonalData{T}"/> extension.
    /// Key = property name, Value = (sensitivity, isTimestamp).
    /// </summary>
    public Dictionary<string, (DataSensitivity Sensitivity, bool IsTimestamp)> PersonalDataProperties { get; }
        = new(StringComparer.Ordinal);

    /// <summary>
    /// GDPR enforcement profile for this collection.
    /// Set via <see cref="EntityTypeBuilderGdprExtensions.HasGdprMode{T}"/> (fluent)
    /// or by placing <see cref="GDPR.GdprModeAttribute"/> on the entity class — the
    /// attribute is read by <see cref="ModelBuilder.Entity{T}"/> at model-building time,
    /// and the fluent call takes precedence when called afterwards.
    /// Defaults to <see cref="GDPR.GdprMode.None"/> (no enforcement).
    /// </summary>
    public GDPR.GdprMode GdprMode { get; internal set; } = GDPR.GdprMode.None;

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

    public EntityTypeBuilder<T> HasSpatialIndex<TKey>(Expression<Func<T, TKey>> keySelector, string? name = null)
    {
        Indexes.Add(new IndexBuilder<T>(keySelector, name, false, IndexType.Spatial));
        return this;
    }

    public EntityTypeBuilder<T> HasKey<TKey>(Expression<Func<T, TKey>> keySelector)
    {
        PrimaryKeySelector = keySelector;
        PrimaryKeyName = ExpressionAnalyzer.ExtractPropertyPaths(keySelector).FirstOrDefault() ?? "_id";
        return this;
    }

    /// <summary>
    /// Configures the collection as a TimeSeries with automatic TTL-based pruning.
    /// Documents older than <paramref name="retention"/> are automatically removed.
    /// </summary>
    /// <param name="timestampSelector">Expression pointing to the DateTime property to use as the timestamp.</param>
    /// <param name="retention">How long to retain documents.</param>
    public EntityTypeBuilder<T> HasTimeSeries(Expression<Func<T, DateTime>> timestampSelector, TimeSpan retention)
    {
        TimeSeriesTtlField = ExpressionAnalyzer.ExtractPropertyPaths(timestampSelector)
            .FirstOrDefault()?.ToLowerInvariant();
        TimeSeriesRetention = retention;
        return this;
    }

    /// <summary>
    /// Configures a generalized retention policy for this collection.
    /// The policy is persisted in collection metadata and evaluated on the specified triggers.
    /// </summary>
    /// <param name="configure">Action that configures the retention policy via the fluent builder.</param>
    public EntityTypeBuilder<T> HasRetentionPolicy(Action<RetentionPolicyBuilder<T>> configure)
    {
        if (configure == null) throw new ArgumentNullException(nameof(configure));
        var builder = new RetentionPolicyBuilder<T>();
        configure(builder);
        RetentionPolicy = builder.Build();
        return this;
    }

    /// <summary>
    /// Configures a property for custom mapping behavior.
    /// Use .HasConversion() to specify value converters for complex types (including Id properties).
    /// Example: modelBuilder.Entity&lt;Order&gt;().Property(x => x.Id).HasConversion&lt;OrderIdConverter&gt;();
    /// </summary>
    public PropertyBuilder Property<TProperty>(Expression<Func<T, TProperty>> propertyExpression)
    {
        var propertyName = ExpressionAnalyzer.ExtractPropertyPaths(propertyExpression).FirstOrDefault();
        return new PropertyBuilder(this, propertyName);
    }

    /// <summary>
    /// Fluent builder for configuring individual property mappings.
    /// Always explicitly tied to a specific property via Property(x => x.PropertyName).
    /// </summary>
    public class PropertyBuilder
    {
        private readonly EntityTypeBuilder<T> _parent;
        private readonly string? _propertyName;

        public PropertyBuilder(EntityTypeBuilder<T> parent, string? propertyName)
        {
            _parent = parent;
            _propertyName = propertyName;
        }

        /// <summary>
        /// Marks the property for automatic value generation on insert (applies to primary keys).
        /// </summary>
        public PropertyBuilder ValueGeneratedOnAdd()
        {
            if (_propertyName == _parent.PrimaryKeyName)
            {
                _parent.ValueGeneratedOnAdd = true;
            }
            return this;
        }

        /// <summary>
        /// Specifies a value converter for this property to convert between model and storage types.
        /// The converter must inherit from ValueConverter&lt;TModel, TProvider&gt;.
        /// Example: Property(x => x.Id).HasConversion&lt;OrderIdConverter&gt;()
        /// This works for any property, including primary keys.
        /// </summary>
        public PropertyBuilder HasConversion<TConverter>()
        {
            if (!string.IsNullOrEmpty(_propertyName))
            {
                _parent.PropertyConverters[_propertyName] = typeof(TConverter);
            }
            return this;
        }

        /// <summary>
        /// Records a personal-data annotation for this property on the parent builder.
        /// Called by <see cref="PropertyBuilderGdprExtensions.HasPersonalData{T}"/>; not intended
        /// for direct use.
        /// </summary>
        internal void SetPersonalData(DataSensitivity sensitivity, bool isTimestamp)
        {
            if (!string.IsNullOrEmpty(_propertyName))
            {
                _parent.PersonalDataProperties[_propertyName] = (sensitivity, isTimestamp);
            }
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
