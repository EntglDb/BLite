namespace BLite.Core.Indexing;

/// <summary>
/// Types of indices supported
/// </summary>
public enum IndexType : byte
{
    /// <summary>B+Tree index for range queries and ordering</summary>
    BTree = 1,
    
    /// <summary>Hash index for exact match lookups</summary>
    Hash = 2,
    
    /// <summary>Unique index constraint</summary>
    Unique = 3,

    /// <summary>Vector index (HNSW) for similarity search</summary>
    Vector = 4,

    /// <summary>Geospatial index (R-Tree) for spatial queries</summary>
    Spatial = 5
}

/// <summary>
/// Distance metrics for vector search
/// </summary>
public enum VectorMetric : byte
{
    /// <summary>Cosine Similarity (Standard for embeddings)</summary>
    Cosine = 1,

    /// <summary>Euclidean Distance (L2)</summary>
    L2 = 2,

    /// <summary>Dot Product</summary>
    DotProduct = 3
}

/// <summary>
/// Index options and configuration.
/// Implemented as readonly struct for efficiency.
/// </summary>
public readonly struct IndexOptions
{
    public IndexType Type { get; init; }
    public bool Unique { get; init; }
    public string[] Fields { get; init; }

    // Vector search options
    public int Dimensions { get; init; }
    public VectorMetric Metric { get; init; }
    public int M { get; init; } // Min number of connections per node
    public int EfConstruction { get; init; } // Size of dynamic candidate list for construction

    public static IndexOptions CreateBTree(params string[] fields) => new()
    {
        Type = IndexType.BTree,
        Unique = false,
        Fields = fields
    };

    public static IndexOptions CreateUnique(params string[] fields) => new()
    {
        Type = IndexType.BTree,
        Unique = true,
        Fields = fields
    };

    public static IndexOptions CreateHash(params string[] fields) => new()
    {
        Type = IndexType.Hash,
        Unique = false,
        Fields = fields
    };

    public static IndexOptions CreateVector(int dimensions, VectorMetric metric = VectorMetric.Cosine, int m = 16, int ef = 200, params string[] fields) => new()
    {
        Type = IndexType.Vector,
        Unique = false,
        Fields = fields,
        Dimensions = dimensions,
        Metric = metric,
        M = m,
        EfConstruction = ef
    };

    public static IndexOptions CreateSpatial(params string[] fields) => new()
    {
        Type = IndexType.Spatial,
        Unique = false,
        Fields = fields
    };
}
