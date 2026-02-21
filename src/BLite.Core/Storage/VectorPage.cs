using System.Runtime.InteropServices;
using BLite.Core.Indexing;

namespace BLite.Core.Storage;

/// <summary>
/// Page for storing HNSW Vector Index nodes.
/// Each page stores a fixed number of nodes based on vector dimensions and M.
/// </summary>
public struct VectorPage
{
    // Layout:
    // [PageHeader (32)]
    // [Dimensions (4)]
    // [MaxM (4)]
    // [NodeSize (4)]
    // [NodeCount (4)]
    // [Nodes Data (Contiguous)...]

    private const int DimensionsOffset = 32;
    private const int MaxMOffset = 36;
    private const int NodeSizeOffset = 40;
    private const int NodeCountOffset = 44;
    private const int DataOffset = 48;

    public static void IncrementNodeCount(Span<byte> page)
    {
        int count = GetNodeCount(page);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(page.Slice(NodeCountOffset), count + 1);
    }

    public static void Initialize(Span<byte> page, uint pageId, int dimensions, int maxM)
    {
        var header = new PageHeader
        {
            PageId = pageId,
            PageType = PageType.Vector,
            FreeBytes = (ushort)(page.Length - DataOffset),
            NextPageId = 0,
            TransactionId = 0
        };
        header.WriteTo(page);

        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(page.Slice(DimensionsOffset), dimensions);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(page.Slice(MaxMOffset), maxM);
        
        // Node Size Calculation:
        // Location (6) + MaxLevel (1) + Vector (dim * 4) + Links (maxM * 10 * 6) -- estimating 10 levels for simplicity
        // Better: Node size is variable? No, let's keep it fixed per index configuration to avoid fragmentation.
        // HNSW standard: level 0 has 2*M links, levels > 0 have M links.
        // Max level is typically < 16. Let's reserve space for 16 levels.
        int nodeSize = 6 + 1 + (dimensions * 4) + (maxM * (2 + 15) * 6); 
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(page.Slice(NodeSizeOffset), nodeSize);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(page.Slice(NodeCountOffset), 0);
    }

    public static int GetNodeCount(ReadOnlySpan<byte> page) =>
        System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(page.Slice(NodeCountOffset));

    public static int GetNodeSize(ReadOnlySpan<byte> page) =>
        System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(page.Slice(NodeSizeOffset));

    public static int GetMaxNodes(ReadOnlySpan<byte> page) =>
        (page.Length - DataOffset) / GetNodeSize(page);

    /// <summary>
    /// Writes a node to the page at the specified index.
    /// </summary>
    public static void WriteNode(Span<byte> page, int nodeIndex, DocumentLocation loc, int maxLevel, ReadOnlySpan<float> vector, int dimensions)
    {
        int nodeSize = GetNodeSize(page);
        int offset = DataOffset + (nodeIndex * nodeSize);
        var nodeSpan = page.Slice(offset, nodeSize);

        // 1. Document Location
        loc.WriteTo(nodeSpan.Slice(0, 6));

        // 2. Max Level
        nodeSpan[6] = (byte)maxLevel;

        // 3. Vector
        var vectorSpan = MemoryMarshal.Cast<byte, float>(nodeSpan.Slice(7, dimensions * 4));
        vector.CopyTo(vectorSpan);

        // 4. Links: zero all link slots so GetNeighbors stops at the first empty (PageId == 0) entry.
        //    ArrayPool buffers are not zeroed, so this is mandatory.
        nodeSpan.Slice(7 + dimensions * 4).Clear();
    }

    public static void ReadNodeData(ReadOnlySpan<byte> page, int nodeIndex, out DocumentLocation loc, out int maxLevel, Span<float> vector)
    {
        int nodeSize = GetNodeSize(page);
        int offset = DataOffset + (nodeIndex * nodeSize);
        var nodeSpan = page.Slice(offset, nodeSize);

        loc = DocumentLocation.ReadFrom(nodeSpan.Slice(0, 6));
        maxLevel = nodeSpan[6];
        
        var vectorSource = MemoryMarshal.Cast<byte, float>(nodeSpan.Slice(7, vector.Length * 4));
        vectorSource.CopyTo(vector);
    }

    public static Span<byte> GetLinksSpan(Span<byte> page, int nodeIndex, int level, int dimensions, int maxM)
    {
        int nodeSize = GetNodeSize(page);
        int nodeOffset = DataOffset + (nodeIndex * nodeSize);
        
        // Link offset: Location(6) + MaxLevel(1) + Vector(dim*4)
        int linkBaseOffset = nodeOffset + 7 + (dimensions * 4);
        
        int levelOffset;
        if (level == 0)
        {
            levelOffset = 0;
        }
        else
        {
            // Level 0 has 2*M links
            levelOffset = (2 * maxM * 6) + ((level - 1) * maxM * 6);
        }

        int count = (level == 0) ? (2 * maxM) : maxM;
        return page.Slice(linkBaseOffset + levelOffset, count * 6);
    }
}
