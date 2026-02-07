using System.Runtime.InteropServices;

namespace DocumentDb.Core.Storage;

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
    
    /// <summary>ID of next page in linked list (0 if none)</summary>
    [FieldOffset(7)]
    public uint NextPageId;
    
    /// <summary>Transaction ID that last modified this page</summary>
    [FieldOffset(11)]
    public ulong TransactionId;
    
    /// <summary>Checksum for data integrity (CRC32)</summary>
    [FieldOffset(19)]
    public uint Checksum;
    
    /// <summary>Reserved for future use - padding to 32 bytes</summary>
    [FieldOffset(23)]
    private byte _reserved1;
    [FieldOffset(24)]
    private byte _reserved2;
    [FieldOffset(25)]
    private byte _reserved3;
    [FieldOffset(26)]
    private byte _reserved4;
    [FieldOffset(27)]
    private byte _reserved5;
    [FieldOffset(28)]
    private byte _reserved6;
    [FieldOffset(29)]
    private byte _reserved7;
    [FieldOffset(30)]
    private byte _reserved8;
    [FieldOffset(31)]
    private byte _reserved9;

    /// <summary>
    /// Writes the header to a span
    /// </summary>
    public readonly void WriteTo(Span<byte> destination)
    {
        if (destination.Length < 32)
            throw new ArgumentException("Destination must be at least 32 bytes");

        MemoryMarshal.Write(destination, in this);
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
