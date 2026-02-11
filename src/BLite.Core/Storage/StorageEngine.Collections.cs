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
}

public sealed partial class StorageEngine
{
    public CollectionMetadata? GetCollectionMetadata(string name)
    {
        var buffer = new byte[PageSize];
        ReadPage(1, null, buffer);
        
        var header = SlottedPageHeader.ReadFrom(buffer);
        if (header.PageType != PageType.Collection || header.SlotCount == 0) 
            return null;

        for (ushort i = 0; i < header.SlotCount; i++)
        {
            var slotOffset = SlottedPageHeader.Size + (i * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));
            
            if ((slot.Flags & SlotFlags.Deleted) != 0) continue;

            var dataSpan = buffer.AsSpan(slot.Offset, slot.Length);
            
            try
            {
                using var ms = new MemoryStream(dataSpan.ToArray());
                using var reader = new BinaryReader(ms);

                var collName = reader.ReadString();
                if (!string.Equals(collName, name, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var metadata = new CollectionMetadata { Name = collName };
                metadata.PrimaryRootPageId = reader.ReadUInt32();
                metadata.SchemaRootPageId = reader.ReadUInt32();
                
                var indexCount = reader.ReadInt32();
                for (int j = 0; j < indexCount; j++)
                {
                    var idx = new IndexMetadata
                    {
                        Name = reader.ReadString(),
                        IsUnique = reader.ReadBoolean(),
                        Type = (IndexType)reader.ReadByte()
                    };
                    
                    var pathCount = reader.ReadInt32();
                    idx.PropertyPaths = new string[pathCount];
                    for (int k = 0; k < pathCount; k++)
                        idx.PropertyPaths[k] = reader.ReadString();
                        
                    metadata.Indexes.Add(idx);
                }
                return metadata;
            }
            catch
            {
                // Ignore malformed slots
            }
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
            writer.Write(idx.PropertyPaths.Length);
            foreach (var path in idx.PropertyPaths)
            {
                writer.Write(path);
            }
        }
        
        var newData = stream.ToArray();
        
        var buffer = new byte[PageSize];
        ReadPage(1, null, buffer);
        
        var header = SlottedPageHeader.ReadFrom(buffer);
        int existingSlotIndex = -1;
        
        for (ushort i = 0; i < header.SlotCount; i++)
        {
            var slotOffset = SlottedPageHeader.Size + (i * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));
            if ((slot.Flags & SlotFlags.Deleted) != 0) continue;
            
            try
            {
                using var ms = new MemoryStream(buffer, slot.Offset, slot.Length, false);
                using var reader = new BinaryReader(ms);
                var name = reader.ReadString();
                
                if (string.Equals(name, metadata.Name, StringComparison.OrdinalIgnoreCase))
                {
                    existingSlotIndex = i;
                    break;
                }
            } 
            catch { }
        }
        
        if (existingSlotIndex >= 0)
        {
             var slotOffset = SlottedPageHeader.Size + (existingSlotIndex * SlotEntry.Size);
             var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));
             slot.Flags |= SlotFlags.Deleted;
             slot.WriteTo(buffer.AsSpan(slotOffset));
        }
        
        if (header.AvailableFreeSpace < newData.Length + SlotEntry.Size)
        {
            // Compact logic omitted as per current architecture
            throw new InvalidOperationException("Not enough space in Metadata Page (Page 1) to save collection metadata.");
        }
        
        int docOffset = header.FreeSpaceEnd - newData.Length;
        newData.CopyTo(buffer.AsSpan(docOffset));
        
        ushort slotIndex;
        if (existingSlotIndex >= 0)
        {
            slotIndex = (ushort)existingSlotIndex;
        }
        else
        {
             slotIndex = header.SlotCount;
             header.SlotCount++;
        }
        
        var newSlotEntryOffset = SlottedPageHeader.Size + (slotIndex * SlotEntry.Size);
        var newSlot = new SlotEntry
        {
            Offset = (ushort)docOffset,
            Length = (ushort)newData.Length,
            Flags = SlotFlags.None
        };
        newSlot.WriteTo(buffer.AsSpan(newSlotEntryOffset));
        
        header.FreeSpaceEnd = (ushort)docOffset;
        if (existingSlotIndex == -1)
        {
             header.FreeSpaceStart = (ushort)(SlottedPageHeader.Size + (header.SlotCount * SlotEntry.Size));
        }
        
        header.WriteTo(buffer);
        WritePageImmediate(1, buffer);
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
