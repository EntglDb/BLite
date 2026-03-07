using System.Runtime.InteropServices;

namespace BLite.Core.Storage;

/// <summary>
/// Represents a page header in the database file.
/// Fixed 32-byte structure at the start of each page.
/// Implemented as struct for efficient memory layout.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 32)]
public struct PageHeader
{
    /// <summary>Page ID (offset in pages from start of file)</summary>
    [FieldOffset(0)]
    public uint PageId;
    
    /// <summary>Type of this page</summary>
    [FieldOffset(4)]
    public PageType PageType;
    
    /// <summary>Number of free bytes in this page</summary>
    [FieldOffset(5)]
    public ushort FreeBytes;
    
    /// <summary>ID of next page in linked list (0 if none). For Page 0 (Header), this points to the First Free Page.</summary>
    [FieldOffset(7)]
    public uint NextPageId;
    
    /// <summary>Transaction ID that last modified this page</summary>
    [FieldOffset(11)]
    public ulong TransactionId;
    
    /// <summary>Checksum for data integrity (CRC32)</summary>
    [FieldOffset(19)]
    public uint Checksum;
    
    /// <summary>Dictionary Root Page ID (Only used in Page 0 / File Header)</summary>
    [FieldOffset(23)]
    public uint DictionaryRootPageId;

    /// <summary>Key-Value store root page ID (Only used in Page 0 / File Header). 0 = not initialised.</summary>
    [FieldOffset(27)]
    public uint KvRootPageId;

    [FieldOffset(31)]
    private byte _reserved9;

    /// <summary>
    /// Writes the header to a span
    /// </summary>
    public readonly void WriteTo(Span<byte> destination)
    {
        if (destination.Length < 32)
            throw new ArgumentException("Destination must be at least 32 bytes");

#if NET5_0_OR_GREATER
        MemoryMarshal.Write(destination, in this);
#else
        var copy = this;
        MemoryMarshal.Write(destination, ref copy);
#endif
    }

    /// <summary>
    /// Reads a header from a span
    /// </summary>
    public static PageHeader ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.Length < 32)
            throw new ArgumentException("Source must be at least 32 bytes");

        return MemoryMarshal.Read<PageHeader>(source);
    }
}
