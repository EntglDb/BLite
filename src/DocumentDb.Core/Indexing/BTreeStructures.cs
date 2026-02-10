using DocumentDb.Bson;
using DocumentDb.Core.Storage;

namespace DocumentDb.Core.Indexing;

/// <summary>
/// Represents an entry in an index mapping a key to a document location.
/// Implemented as struct for memory efficiency.
/// </summary>
public struct IndexEntry : IComparable<IndexEntry>, IComparable
{
    public IndexKey Key { get; set; }
    public DocumentLocation Location { get; set; }

    public IndexEntry(IndexKey key, DocumentLocation location)
    {
        Key = key;
        Location = location;
    }
    
    // Backward compatibility: constructor that takes ObjectId (for migration)
    // Will be removed once all code is migrated
    [Obsolete("Use constructor with DocumentLocation instead")]
    public IndexEntry(IndexKey key, ObjectId documentId)
    {
        Key = key;
        // Create a temporary location (will be replaced by proper implementation)
        Location = new DocumentLocation(0, 0);
    }

    public int CompareTo(IndexEntry other)
    {
        return Key.CompareTo(other.Key);
    }

    public int CompareTo(object? obj)
    {
        if (obj is IndexEntry other) return CompareTo(other);
        throw new ArgumentException("Object is not an IndexEntry");
    }
}

/// <summary>
/// B+Tree node for index storage.
/// Uses struct for node metadata to minimize allocations.
/// </summary>
public struct BTreeNodeHeader
{
    public uint PageId { get; set; }
    public bool IsLeaf { get; set; }
    public ushort EntryCount { get; set; }
    public uint ParentPageId { get; set; }
    public uint NextLeafPageId { get; set; }  // For leaf nodes only
    public uint PrevLeafPageId { get; set; }  // For leaf nodes only (added for reverse scan)

    public void WriteTo(Span<byte> destination)
    {
        if (destination.Length < 20)
            throw new ArgumentException("Destination must be at least 20 bytes");

        BitConverter.TryWriteBytes(destination[0..4], PageId);
        destination[4] = (byte)(IsLeaf ? 1 : 0);
        BitConverter.TryWriteBytes(destination[5..7], EntryCount);
        BitConverter.TryWriteBytes(destination[7..11], ParentPageId);
        BitConverter.TryWriteBytes(destination[11..15], NextLeafPageId);
        BitConverter.TryWriteBytes(destination[15..19], PrevLeafPageId);
    }

    public static BTreeNodeHeader ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.Length < 20)
            throw new ArgumentException("Source must be at least 16 bytes");

        var header = new BTreeNodeHeader
        {
            PageId = BitConverter.ToUInt32(source[0..4]),
            IsLeaf = source[4] != 0,
            EntryCount = BitConverter.ToUInt16(source[5..7]),
            ParentPageId = BitConverter.ToUInt32(source[7..11]),
            NextLeafPageId = BitConverter.ToUInt32(source[11..15])
        };

        if (source.Length >= 20)
        {
            header.PrevLeafPageId = BitConverter.ToUInt32(source[15..19]);
        }

        return header;
    }
}
