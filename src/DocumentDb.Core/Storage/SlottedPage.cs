using System.Runtime.InteropServices;

namespace DocumentDb.Core.Storage;

/// <summary>
/// Header for slotted pages supporting multiple variable-size documents per page.
/// Fixed 24-byte structure at start of each data page.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 24)]
public struct SlottedPageHeader
{
    /// <summary>Page ID</summary>
    [FieldOffset(0)]
    public uint PageId;
    
    /// <summary>Type of page (Data, Overflow, Index, Metadata)</summary>
    [FieldOffset(4)]
    public PageType PageType;
    
    /// <summary>Number of slot entries in this page</summary>
    [FieldOffset(8)]
    public ushort SlotCount;
    
    /// <summary>Offset where free space starts (grows down with slots)</summary>
    [FieldOffset(10)]
    public ushort FreeSpaceStart;
    
    /// <summary>Offset where free space ends (grows up with data)</summary>
    [FieldOffset(12)]
    public ushort FreeSpaceEnd;
    
    /// <summary>Next overflow page ID (0 if none)</summary>
    [FieldOffset(14)]
    public uint NextOverflowPage;
    
    /// <summary>Transaction ID that last modified this page</summary>
    [FieldOffset(18)]
    public uint TransactionId;
    
    /// <summary>Reserved for future use</summary>
    [FieldOffset(22)]
    public ushort Reserved;

    public const int Size = 24;
    
    /// <summary>
    /// Gets available free space in bytes
    /// </summary>
    public readonly int AvailableFreeSpace => FreeSpaceEnd - FreeSpaceStart;
    
    /// <summary>
    /// Writes header to span
    /// </summary>
    public readonly void WriteTo(Span<byte> destination)
    {
        if (destination.Length < Size)
            throw new ArgumentException($"Destination must be at least {Size} bytes");
        
        MemoryMarshal.Write(destination, in this);
    }
    
    /// <summary>
    /// Reads header from span
    /// </summary>
    public static SlottedPageHeader ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.Length < Size)
            throw new ArgumentException($"Source must be at least {Size} bytes");
        
        return MemoryMarshal.Read<SlottedPageHeader>(source);
    }
}

/// <summary>
/// Slot entry pointing to a document within a page.
/// Fixed 8-byte structure in slot array.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 8)]
public struct SlotEntry
{
    /// <summary>Offset to document data within page</summary>
    [FieldOffset(0)]
    public ushort Offset;
    
    /// <summary>Length of document data in bytes</summary>
    [FieldOffset(2)]
    public ushort Length;
    
    /// <summary>Slot flags (deleted, overflow, etc.)</summary>
    [FieldOffset(4)]
    public SlotFlags Flags;

    public const int Size = 8;
    
    /// <summary>
    /// Writes slot entry to span
    /// </summary>
    public readonly void WriteTo(Span<byte> destination)
    {
        if (destination.Length < Size)
            throw new ArgumentException($"Destination must be at least {Size} bytes");
        
        MemoryMarshal.Write(destination, in this);
    }
    
    /// <summary>
    /// Reads slot entry from span
    /// </summary>
    public static SlotEntry ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.Length < Size)
            throw new ArgumentException($"Source must be at least {Size} bytes");
        
        return MemoryMarshal.Read<SlotEntry>(source);
    }
}

/// <summary>
/// Flags for slot entries
/// </summary>
[Flags]
public enum SlotFlags : uint
{
    /// <summary>Slot is active and contains data</summary>
    None = 0,
    
    /// <summary>Slot is marked as deleted (can be reused)</summary>
    Deleted = 1 << 0,
    
    /// <summary>Document continues in overflow pages</summary>
    HasOverflow = 1 << 1,
    
    /// <summary>Document data is compressed</summary>
    Compressed = 1 << 2,
}

/// <summary>
/// Location of a document within the database.
/// Maps ObjectId to specific page and slot.
/// </summary>
public readonly struct DocumentLocation
{
    public uint PageId { get; init; }
    public ushort SlotIndex { get; init; }
    
    public DocumentLocation(uint pageId, ushort slotIndex)
    {
        PageId = pageId;
        SlotIndex = slotIndex;
    }
}
