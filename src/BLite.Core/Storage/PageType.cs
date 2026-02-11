namespace BLite.Core;

/// <summary>
/// Page types in the database file
/// </summary>
public enum PageType : byte
{
    /// <summary>Empty/free page</summary>
    Empty = 0,
    
    /// <summary>File header page (page 0)</summary>
    Header = 1,
    
    /// <summary>Collection metadata page</summary>
    Collection = 2,
    
    /// <summary>Data page containing documents</summary>
    Data = 3,
    
    /// <summary>Index B+Tree node page</summary>
    Index = 4,
    
    /// <summary>Free page list</summary>
    FreeList = 5,
    
    /// <summary>Overflow page for large documents</summary>
    Overflow = 6,

    /// <summary>Page marked as free/reusable</summary>
    Free = 10,

    /// <summary>Dictionary page for string interning</summary>
    Dictionary = 7,

    /// <summary>Schema versioning page</summary>
    Schema = 8
}
