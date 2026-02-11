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
    Unique = 3
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
}
