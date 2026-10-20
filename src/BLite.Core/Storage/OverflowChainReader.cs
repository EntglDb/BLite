using System.Buffers;
using System.Buffers.Binary;

namespace BLite.Core.Storage;

/// <summary>
/// Reassembles a document that a typed <c>DocumentCollection</c> stored across overflow pages,
/// so that the schema-less read paths (<c>DynamicCollection</c>, and through it BLQL, Studio and
/// <c>BLiteMigration</c>) can read every document the typed API wrote (#301).
/// <para>
/// A slot flagged <see cref="SlotFlags.HasOverflow"/> holds
/// <c>[TotalLength (4)] [FirstOverflowPage (4)] [first chunk of the document]</c>; every overflow
/// page holds <c>PageSize - SlottedPageHeader.Size</c> bytes of payload after its header and
/// names the next page in <see cref="SlottedPageHeader.NextOverflowPage"/> (0 ends the chain).
/// This is the layout <c>DocumentCollection.InsertWithOverflow</c> writes and
/// <c>DocumentCollection.ReadRawBytesAt</c> reads.
/// </para>
/// </summary>
internal static class OverflowChainReader
{
    /// <summary>Bytes of slot payload that precede the first chunk: total length and first overflow page id.</summary>
    public const int SlotMetadataSize = 8;

    /// <summary>
    /// Reads the whole document whose first chunk is in <paramref name="slot"/> of
    /// <paramref name="pageBuffer"/>. Returns <c>null</c> when the slot does not carry an
    /// overflow document or the chain is inconsistent (truncated, or shorter than its declared
    /// length), never a partial document.
    /// </summary>
    public static byte[]? Read(StorageEngine storage, ReadOnlySpan<byte> pageBuffer, in SlotEntry slot, ulong txnId)
    {
        if (!TryReadHead(pageBuffer, slot, out var totalLength, out var nextPageId, out var firstChunk))
            return null;

        var document = new byte[totalLength];
        firstChunk.CopyTo(document);
        int offset = firstChunk.Length;

        var overflowBuffer = ArrayPool<byte>.Shared.Rent(storage.PageSize);
        try
        {
            while (nextPageId != 0 && offset < totalLength)
            {
                storage.ReadPage(nextPageId, txnId, overflowBuffer);
                nextPageId = AppendChunk(storage.PageSize, overflowBuffer, document, ref offset);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(overflowBuffer);
        }

        return offset == totalLength ? document : null;
    }

    /// <summary>Async counterpart of <see cref="Read"/>, reading the chain with <see cref="StorageEngine.ReadPageAsync"/>.</summary>
    public static async ValueTask<byte[]?> ReadAsync(StorageEngine storage, byte[] pageBuffer, SlotEntry slot, ulong txnId, CancellationToken ct)
    {
        // Everything taken from the page buffer is copied before the first await: the caller's
        // buffer may be pooled, and a span cannot live across an await anyway.
        if (!TryReadHead(pageBuffer, slot, out var totalLength, out var nextPageId, out var firstChunk))
            return null;

        var document = new byte[totalLength];
        firstChunk.CopyTo(document);
        int offset = firstChunk.Length;

        var overflowBuffer = ArrayPool<byte>.Shared.Rent(storage.PageSize);
        try
        {
            while (nextPageId != 0 && offset < totalLength)
            {
                ct.ThrowIfCancellationRequested();
                await storage.ReadPageAsync(nextPageId, txnId, overflowBuffer.AsMemory(0, storage.PageSize), ct).ConfigureAwait(false);
                nextPageId = AppendChunk(storage.PageSize, overflowBuffer, document, ref offset);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(overflowBuffer);
        }

        return offset == totalLength ? document : null;
    }

    /// <summary>
    /// The page ids of the overflow chain that starts in <paramref name="slot"/>, in chain
    /// order, so a caller that deletes the document can free them. Empty when the slot carries
    /// no overflow document. Stops at a page that does not look like an overflow page, so a
    /// chain corrupted by an earlier failure cannot free a page another document uses.
    /// </summary>
    public static List<uint> ChainPages(StorageEngine storage, ReadOnlySpan<byte> pageBuffer, in SlotEntry slot, ulong txnId)
    {
        var pages = new List<uint>();
        if (!TryReadHead(pageBuffer, slot, out var totalLength, out var nextPageId, out var firstChunk))
            return pages;

        int offset = firstChunk.Length;
        var overflowBuffer = ArrayPool<byte>.Shared.Rent(storage.PageSize);
        try
        {
            while (nextPageId != 0 && offset < totalLength)
            {
                storage.ReadPage(nextPageId, txnId, overflowBuffer);
                var header = SlottedPageHeader.ReadFrom(overflowBuffer);
                if (header.PageType != PageType.Overflow || pages.Contains(nextPageId))
                    break;
                pages.Add(nextPageId);
                offset += Math.Min(storage.PageSize - SlottedPageHeader.Size, totalLength - offset);
                nextPageId = header.NextOverflowPage;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(overflowBuffer);
        }
        return pages;
    }

    private static bool TryReadHead(ReadOnlySpan<byte> pageBuffer, in SlotEntry slot, out int totalLength, out uint nextPageId, out ReadOnlySpan<byte> firstChunk)
    {
        totalLength = 0;
        nextPageId = 0;
        firstChunk = default;

        if ((slot.Flags & SlotFlags.HasOverflow) == 0) return false;
        if (slot.Length < SlotMetadataSize || slot.Offset + slot.Length > pageBuffer.Length) return false;

        var payload = pageBuffer.Slice(slot.Offset, slot.Length);
        totalLength = BinaryPrimitives.ReadInt32LittleEndian(payload);
        nextPageId = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(4));

        int firstChunkSize = slot.Length - SlotMetadataSize;
        if (totalLength <= 0 || firstChunkSize > totalLength) return false;

        firstChunk = payload.Slice(SlotMetadataSize, firstChunkSize);
        return true;
    }

    /// <summary>Copies one overflow page's payload into <paramref name="document"/> and returns the next page id.</summary>
    private static uint AppendChunk(int pageSize, byte[] overflowBuffer, byte[] document, ref int offset)
    {
        var header = SlottedPageHeader.ReadFrom(overflowBuffer);
        int chunkSize = Math.Min(pageSize - SlottedPageHeader.Size, document.Length - offset);
        overflowBuffer.AsSpan(SlottedPageHeader.Size, chunkSize).CopyTo(document.AsSpan(offset));
        offset += chunkSize;
        return header.NextOverflowPage;
    }
}
