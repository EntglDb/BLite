using System.Buffers.Binary;
using System.Runtime.InteropServices;
using BLite.Core.Indexing;
using BLite.Core.Indexing.Internal;

namespace BLite.Core.Storage;

/// <summary>
/// Page for storing R-Tree nodes for Geospatial Indexing.
/// </summary>
internal struct SpatialPage
{
    // Layout:
    // [PageHeader (32)]
    // [IsLeaf (1)]
    // [Level (1)]
    // [EntryCount (2)]
    // [ParentPageId (4)]
    // [Padding (8)]
    // [Entries (Contiguous)...]
    // 
    // Each Entry: [MBR (4 * 8 = 32)] [Pointer (6)] = 38 bytes

    private const int IsLeafOffset = 32;
    private const int LevelOffset = 33;
    private const int EntryCountOffset = 34;
    private const int ParentPageIdOffset = 36;
    private const int DataOffset = 48;

    public const int EntrySize = 38; // 32 (GeoBox) + 6 (Pointer)

    public static void Initialize(Span<byte> page, uint pageId, bool isLeaf, byte level)
    {
        var header = new PageHeader
        {
            PageId = pageId,
            PageType = PageType.Spatial,
            FreeBytes = (ushort)(page.Length - DataOffset),
            NextPageId = 0,
            TransactionId = 0
        };
        header.WriteTo(page);

        page[IsLeafOffset] = (byte)(isLeaf ? 1 : 0);
        page[LevelOffset] = level;
        BinaryPrimitives.WriteUInt16LittleEndian(page.Slice(EntryCountOffset), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(page.Slice(ParentPageIdOffset), 0);
    }

    public static bool GetIsLeaf(ReadOnlySpan<byte> page) => page[IsLeafOffset] == 1;
    public static byte GetLevel(ReadOnlySpan<byte> page) => page[LevelOffset];
    public static ushort GetEntryCount(ReadOnlySpan<byte> page) => BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(EntryCountOffset));
    public static void SetEntryCount(Span<byte> page, ushort count) => BinaryPrimitives.WriteUInt16LittleEndian(page.Slice(EntryCountOffset), count);

    public static uint GetParentPageId(ReadOnlySpan<byte> page) => BinaryPrimitives.ReadUInt32LittleEndian(page.Slice(ParentPageIdOffset));
    public static void SetParentPageId(Span<byte> page, uint parentId) => BinaryPrimitives.WriteUInt32LittleEndian(page.Slice(ParentPageIdOffset), parentId);

    public static int GetMaxEntries(int pageSize) => (pageSize - DataOffset) / EntrySize;

    public static void WriteEntry(Span<byte> page, int index, GeoBox mbr, DocumentLocation pointer)
    {
        int offset = DataOffset + (index * EntrySize);
        var entrySpan = page.Slice(offset, EntrySize);

        // Write MBR (4 doubles)
        var doubles = MemoryMarshal.Cast<byte, double>(entrySpan.Slice(0, 32));
        doubles[0] = mbr.MinLat;
        doubles[1] = mbr.MinLon;
        doubles[2] = mbr.MaxLat;
        doubles[3] = mbr.MaxLon;

        // Write Pointer (6 bytes)
        pointer.WriteTo(entrySpan.Slice(32, 6));
    }

    public static void ReadEntry(ReadOnlySpan<byte> page, int index, out GeoBox mbr, out DocumentLocation pointer)
    {
        int offset = DataOffset + (index * EntrySize);
        var entrySpan = page.Slice(offset, EntrySize);

        var doubles = MemoryMarshal.Cast<byte, double>(entrySpan.Slice(0, 32));
        mbr = new GeoBox(doubles[0], doubles[1], doubles[2], doubles[3]);
        pointer = DocumentLocation.ReadFrom(entrySpan.Slice(32, 6));
    }

    public static GeoBox CalculateMBR(ReadOnlySpan<byte> page)
    {
        ushort count = GetEntryCount(page);
        if (count == 0) return GeoBox.Empty;

        GeoBox result = GeoBox.Empty;
        for (int i = 0; i < count; i++)
        {
            ReadEntry(page, i, out var mbr, out _);
            if (i == 0) result = mbr;
            else result = result.ExpandTo(mbr);
        }
        return result;
    }
}
