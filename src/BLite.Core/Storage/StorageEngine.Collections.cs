using System;
using System.Collections.Generic;
using System.IO;
using BLite.Bson;
using BLite.Core.Indexing;
using BLite.Core.Collections;

namespace BLite.Core.Storage;

public class CollectionMetadata
{
    public string Name { get; set; } = string.Empty;
    public uint PrimaryRootPageId { get; set; }
    public uint SchemaRootPageId { get; set; }
    public List<IndexMetadata> Indexes { get; } = new();
}

public class IndexMetadata
{
    public string Name { get; set; } = string.Empty;
    public bool IsUnique { get; set; }
    public IndexType Type { get; set; }
    public string[] PropertyPaths { get; set; } = Array.Empty<string>();
    public int Dimensions { get; set; }
    public VectorMetric Metric { get; set; }
    public uint RootPageId { get; set; }
}

public sealed partial class StorageEngine
{
    public CollectionMetadata? GetCollectionMetadata(string name)
    {
        var buffer = new byte[PageSize];
        uint pageId = 1;

        while (pageId != 0)
        {
            ReadPage(pageId, null, buffer);
            var header = SlottedPageHeader.ReadFrom(buffer);
            if (header.PageType != PageType.Collection)
                break;

            for (ushort i = 0; i < header.SlotCount; i++)
            {
                var slotOffset = SlottedPageHeader.Size + (i * SlotEntry.Size);
                var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));

                if ((slot.Flags & SlotFlags.Deleted) != 0) continue;

                try
                {
                    using var ms = new MemoryStream(buffer, slot.Offset, slot.Length, false);
                    using var reader = new BinaryReader(ms);

                    var collName = reader.ReadString();
                    if (!string.Equals(collName, name, StringComparison.OrdinalIgnoreCase))
                        continue;

                    var metadata = new CollectionMetadata { Name = collName };
                    metadata.PrimaryRootPageId = reader.ReadUInt32();
                    metadata.SchemaRootPageId  = reader.ReadUInt32();

                    var indexCount = reader.ReadInt32();
                    for (int j = 0; j < indexCount; j++)
                    {
                        var idx = new IndexMetadata
                        {
                            Name       = reader.ReadString(),
                            IsUnique   = reader.ReadBoolean(),
                            Type       = (IndexType)reader.ReadByte(),
                            RootPageId = reader.ReadUInt32()
                        };

                        var pathCount = reader.ReadInt32();
                        idx.PropertyPaths = new string[pathCount];
                        for (int k = 0; k < pathCount; k++)
                            idx.PropertyPaths[k] = reader.ReadString();

                        if (idx.Type == IndexType.Vector)
                        {
                            idx.Dimensions = reader.ReadInt32();
                            idx.Metric     = (VectorMetric)reader.ReadByte();
                        }

                        metadata.Indexes.Add(idx);
                    }
                    return metadata;
                }
                catch
                {
                    // Ignore malformed slots
                }
            }

            pageId = header.NextOverflowPage;
        }

        return null;
    }

    public void SaveCollectionMetadata(CollectionMetadata metadata)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(metadata.Name);
        writer.Write(metadata.PrimaryRootPageId);
        writer.Write(metadata.SchemaRootPageId);
        writer.Write(metadata.Indexes.Count);
        foreach (var idx in metadata.Indexes)
        {
            writer.Write(idx.Name);
            writer.Write(idx.IsUnique);
            writer.Write((byte)idx.Type);
            writer.Write(idx.RootPageId);
            writer.Write(idx.PropertyPaths.Length);
            foreach (var path in idx.PropertyPaths)
                writer.Write(path);

            if (idx.Type == IndexType.Vector)
            {
                writer.Write(idx.Dimensions);
                writer.Write((byte)idx.Metric);
            }
        }

        var newData = stream.ToArray();

        // ── Pass 1: walk the chain to find the existing entry and the first page
        //           with enough free space to store the new serialised record. ──

        var loadedPages    = new List<(uint PageId, byte[] Buffer)>();
        uint chainPageId   = 1;
        int foundOnPageIdx = -1; // index into loadedPages
        int foundSlotIndex = -1;
        int bestWriteIdx   = -1; // first page with enough room

        while (chainPageId != 0)
        {
            var buf    = new byte[PageSize];
            ReadPage(chainPageId, null, buf);
            int thisIdx = loadedPages.Count;
            loadedPages.Add((chainPageId, buf));

            var hdr = SlottedPageHeader.ReadFrom(buf);
            if (hdr.PageType != PageType.Collection)
                break;

            // Locate existing slot for this collection name
            if (foundSlotIndex == -1)
            {
                for (ushort i = 0; i < hdr.SlotCount; i++)
                {
                    var so   = SlottedPageHeader.Size + (i * SlotEntry.Size);
                    var slot = SlotEntry.ReadFrom(buf.AsSpan(so));
                    if ((slot.Flags & SlotFlags.Deleted) != 0) continue;

                    try
                    {
                        using var ms     = new MemoryStream(buf, slot.Offset, slot.Length, false);
                        using var rdr    = new BinaryReader(ms);
                        var existingName = rdr.ReadString();
                        if (string.Equals(existingName, metadata.Name, StringComparison.OrdinalIgnoreCase))
                        {
                            foundOnPageIdx = thisIdx;
                            foundSlotIndex = i;
                            break;
                        }
                    }
                    catch { }
                }
            }

            // First page with enough room for the new entry
            if (bestWriteIdx == -1 && hdr.AvailableFreeSpace >= newData.Length + SlotEntry.Size)
                bestWriteIdx = thisIdx;

            chainPageId = hdr.NextOverflowPage;
        }

        // ── Pass 2: mark old entry as Deleted (in its in-memory buffer) ──
        if (foundSlotIndex >= 0)
        {
            var (_, fbuf) = loadedPages[foundOnPageIdx];
            var so   = SlottedPageHeader.Size + (foundSlotIndex * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(fbuf.AsSpan(so));
            slot.Flags |= SlotFlags.Deleted;
            slot.WriteTo(fbuf.AsSpan(so));
        }

        // ── Pass 3: resolve write-target page ──
        byte[] targetBuf;
        uint   targetPageId;

        if (bestWriteIdx >= 0)
        {
            (targetPageId, targetBuf) = loadedPages[bestWriteIdx];
        }
        else
        {
            // No existing page has room → allocate a new Collection overflow page
            targetPageId = AllocatePage();
            targetBuf = new byte[PageSize];
            var newHdr = new SlottedPageHeader
            {
                PageId           = targetPageId,
                PageType         = PageType.Collection,
                SlotCount        = 0,
                FreeSpaceStart   = SlottedPageHeader.Size,
                FreeSpaceEnd     = (ushort)PageSize,
                NextOverflowPage = 0,
                TransactionId    = 0
            };
            newHdr.WriteTo(targetBuf);

            // Append new page to the end of the chain
            var (lastPid, lastBuf) = loadedPages[loadedPages.Count - 1];
            var lastHdr = SlottedPageHeader.ReadFrom(lastBuf);
            lastHdr.NextOverflowPage = targetPageId;
            lastHdr.WriteTo(lastBuf);
            WritePageImmediate(lastPid, lastBuf);

            // Flush the deletion page if it differs from lastPage
            if (foundSlotIndex >= 0 && foundOnPageIdx != loadedPages.Count - 1)
            {
                var (fpid, fbuf) = loadedPages[foundOnPageIdx];
                WritePageImmediate(fpid, fbuf);
            }

            loadedPages.Add((targetPageId, targetBuf));
        }

        // If deletion page != target page, flush it now
        if (foundSlotIndex >= 0)
        {
            var (fpid, fbuf) = loadedPages[foundOnPageIdx];
            if (fpid != targetPageId)
                WritePageImmediate(fpid, fbuf);
        }

        // ── Pass 4: insert new slot into targetBuf ──
        var th = SlottedPageHeader.ReadFrom(targetBuf);

        // Defensive check (should not trigger given the logic above)
        if (th.AvailableFreeSpace < newData.Length + SlotEntry.Size)
            throw new InvalidOperationException(
                $"Not enough space in Collection page {targetPageId} to save collection metadata.");

        int docOff = th.FreeSpaceEnd - newData.Length;
        newData.CopyTo(targetBuf.AsSpan(docOff));

        int  newSlotOff = SlottedPageHeader.Size + (th.SlotCount * SlotEntry.Size);
        var  newSlot    = new SlotEntry
        {
            Offset = (ushort)docOff,
            Length = (ushort)newData.Length,
            Flags  = SlotFlags.None
        };
        newSlot.WriteTo(targetBuf.AsSpan(newSlotOff));

        th.SlotCount++;
        th.FreeSpaceEnd   = (ushort)docOff;
        th.FreeSpaceStart = (ushort)(SlottedPageHeader.Size + (th.SlotCount * SlotEntry.Size));
        th.WriteTo(targetBuf);

        WritePageImmediate(targetPageId, targetBuf);
    }

    public IReadOnlyList<CollectionMetadata> GetAllCollectionsMetadata()
    {
        var result = new List<CollectionMetadata>();
        var buffer = new byte[PageSize];
        uint pageId = 1;

        while (pageId != 0)
        {
            ReadPage(pageId, null, buffer);
            var header = SlottedPageHeader.ReadFrom(buffer);
            if (header.PageType != PageType.Collection)
                break;

            for (ushort i = 0; i < header.SlotCount; i++)
            {
                var slotOffset = SlottedPageHeader.Size + (i * SlotEntry.Size);
                var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));

                if ((slot.Flags & SlotFlags.Deleted) != 0) continue;

                try
                {
                    using var ms = new MemoryStream(buffer, slot.Offset, slot.Length, false);
                    using var reader = new BinaryReader(ms);

                    var collName = reader.ReadString();
                    var metadata = new CollectionMetadata { Name = collName };
                    metadata.PrimaryRootPageId = reader.ReadUInt32();
                    metadata.SchemaRootPageId  = reader.ReadUInt32();

                    var indexCount = reader.ReadInt32();
                    for (int j = 0; j < indexCount; j++)
                    {
                        var idx = new IndexMetadata
                        {
                            Name       = reader.ReadString(),
                            IsUnique   = reader.ReadBoolean(),
                            Type       = (IndexType)reader.ReadByte(),
                            RootPageId = reader.ReadUInt32()
                        };

                        var pathCount = reader.ReadInt32();
                        idx.PropertyPaths = new string[pathCount];
                        for (int k = 0; k < pathCount; k++)
                            idx.PropertyPaths[k] = reader.ReadString();

                        if (idx.Type == IndexType.Vector)
                        {
                            idx.Dimensions = reader.ReadInt32();
                            idx.Metric     = (VectorMetric)reader.ReadByte();
                        }

                        metadata.Indexes.Add(idx);
                    }

                    result.Add(metadata);
                }
                catch
                {
                    // Ignora slot malformati
                }
            }

            pageId = header.NextOverflowPage;
        }

        return result;
    }

    /// <summary>
    /// Registers all BSON keys used by a set of mappers into the global dictionary.
    /// </summary>
    public void RegisterMappers(IEnumerable<IDocumentMapper> mappers)
    {
        var allKeys = mappers.SelectMany(m => m.UsedKeys).Distinct();
        RegisterKeys(allKeys);
    }
}