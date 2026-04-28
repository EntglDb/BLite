using System.Runtime.InteropServices;

namespace BLite.Core.Storage;

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

#if NET5_0_OR_GREATER
        MemoryMarshal.Write(destination, in this);
#else
        var copy = this;
        MemoryMarshal.Write(destination, ref copy);
#endif
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

#if NET5_0_OR_GREATER
        MemoryMarshal.Write(destination, in this);
#else
        var copy = this;
        MemoryMarshal.Write(destination, ref copy);
#endif
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
/// Shared utility methods for slotted-page maintenance.
/// </summary>
internal static class SlottedPageUtils
{
    /// <summary>
    /// Compacts a slotted data page by packing live documents to the top of the data area,
    /// then zero-fills the resulting free space so that deleted document bytes are
    /// unrecoverable (secure erase / GDPR Art. 17 support).
    /// <para>
    /// Deleted slot entries are retained in the slot array so that existing
    /// <see cref="DocumentLocation"/> values (stored in the B-Tree primary index) remain valid.
    /// Only the document data area is compacted; <c>SlotCount</c> and <c>FreeSpaceStart</c>
    /// are unchanged.
    /// </para>
    /// </summary>
    /// <param name="buffer">
    ///   Full page buffer (exactly <c>pageSize</c> bytes). Modified in-place.
    /// </param>
    /// <returns>
    ///   <c>true</c> if the page contained deleted slots and was compacted;
    ///   <c>false</c> if there were no deleted slots and the page was unchanged.
    /// </returns>
    internal static bool CompactAndErase(Span<byte> buffer)
    {
        var header = SlottedPageHeader.ReadFrom(buffer);
        if (header.SlotCount == 0) return false;

        // Gather live (non-deleted) slots, preserving their original indices.
        int deletedCount = 0;
        for (ushort i = 0; i < header.SlotCount; i++)
        {
            var slotOffset = SlottedPageHeader.Size + (i * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(buffer.Slice(slotOffset, SlotEntry.Size));
            if ((slot.Flags & SlotFlags.Deleted) != 0)
                deletedCount++;
        }

        if (deletedCount == 0)
        {
            // No deleted slots: zero-fill existing free space for completeness.
            buffer.Slice(header.FreeSpaceStart,
                header.FreeSpaceEnd - header.FreeSpaceStart).Clear();
            return false;
        }

        // Pack live document data from top of page downward using a temp copy
        // to avoid read-after-write corruption.  Slot indices are preserved so
        // primary-index DocumentLocation values remain valid after compaction.
        var temp = System.Buffers.ArrayPool<byte>.Shared.Rent(buffer.Length);
        try
        {
            buffer.CopyTo(temp);
            ushort newEnd = (ushort)buffer.Length;

            for (ushort i = 0; i < header.SlotCount; i++)
            {
                var entryOffset = SlottedPageHeader.Size + (i * SlotEntry.Size);
                var slot = SlotEntry.ReadFrom(buffer.Slice(entryOffset, SlotEntry.Size));
                if ((slot.Flags & SlotFlags.Deleted) != 0)
                    continue;

                newEnd -= slot.Length;
                temp.AsSpan(slot.Offset, slot.Length).CopyTo(buffer.Slice(newEnd, slot.Length));

                var updatedSlot = slot;
                updatedSlot.Offset = newEnd;
                updatedSlot.WriteTo(buffer.Slice(entryOffset, SlotEntry.Size));
            }

            header.FreeSpaceEnd = newEnd;
            header.WriteTo(buffer);

            // Zero-fill the entire free space area (between slot directory and data area).
            buffer.Slice(header.FreeSpaceStart, header.FreeSpaceEnd - header.FreeSpaceStart).Clear();
            return true;
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(temp);
        }
    }
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
    
    /// <summary>
    /// Serializes DocumentLocation to a byte span (6 bytes: 4 for PageId + 2 for SlotIndex)
    /// </summary>
    public void WriteTo(Span<byte> destination)
    {
        if (destination.Length < 6)
            throw new ArgumentException("Destination must be at least 6 bytes", nameof(destination));
            
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(destination, PageId);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(4), SlotIndex);
    }
    
    /// <summary>
    /// Deserializes DocumentLocation from a byte span (6 bytes)
    /// </summary>
    public static DocumentLocation ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.Length < 6)
            throw new ArgumentException("Source must be at least 6 bytes", nameof(source));
            
        var pageId = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(source);
        var slotIndex = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(4));
        
        return new DocumentLocation(pageId, slotIndex);
    }
    
    /// <summary>
    /// Size in bytes when serialized
    /// </summary>
    public const int SerializedSize = 6;
}
