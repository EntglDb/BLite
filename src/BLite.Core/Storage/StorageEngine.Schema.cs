using System;
using System.Collections.Generic;
using System.IO;
using BLite.Bson;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    public List<BsonSchema> GetSchemas(uint rootPageId)
    {
        var schemas = new List<BsonSchema>();
        if (rootPageId == 0) return schemas;

        // Collect all raw bytes from the schema page chain into one contiguous buffer.
        // This allows a single schema document to span multiple pages transparently.
        var allBytes = CollectSchemaPageBytes(rootPageId);
        if (allBytes.Length == 0) return schemas;

        var reader = new BsonSpanReader(allBytes, GetKeyReverseMap());
        while (reader.Remaining >= 4)
        {
            var docSize = reader.PeekInt32();
            if (docSize <= 0 || docSize > reader.Remaining) break;

            var schema = BsonSchema.FromBson(ref reader);
            schemas.Add(schema);
        }

        return schemas;
    }

    /// <summary>
    /// Appends a new schema version. Supports schemas larger than a single page by
    /// streaming the serialized bytes across a chain of schema pages.
    /// Returns the root page ID (which may be newly allocated when rootPageId is 0).
    /// </summary>
    public uint AppendSchema(uint rootPageId, BsonSchema schema)
    {
        // Serialize schema to an exactly-sized buffer.
        // CalculateSize() avoids a fixed PageSize limit and prevents buffer overflow.
        var schemaBytes = SerializeSchema(schema);
        int schemaSize = schemaBytes.Length;
        int pagePayload = PageSize - 32; // usable bytes per page (after the 32-byte header)

        var buffer = new byte[PageSize];
        int schemaOffset = 0;
        uint currentPageId;
        int currentUsed; // bytes already occupied in the current page body

        if (rootPageId == 0)
        {
            // No schema storage yet — create the first schema page.
            currentPageId = AllocatePage();
            rootPageId = currentPageId;
            InitializeSchemaPage(buffer, currentPageId);
            currentUsed = 0;
        }
        else
        {
            // Navigate to the last page in the existing chain.
            currentPageId = rootPageId;
            while (true)
            {
                ReadPage(currentPageId, null, buffer);
                var h = PageHeader.ReadFrom(buffer);
                if (h.NextPageId == 0) break;
                currentPageId = h.NextPageId;
            }
            var lastHeader = PageHeader.ReadFrom(buffer);
            currentUsed = pagePayload - lastHeader.FreeBytes;
        }

        // Write schema bytes in chunks, allocating new pages as needed.
        while (schemaOffset < schemaSize)
        {
            var pageHeader = PageHeader.ReadFrom(buffer);
            int available = pageHeader.FreeBytes;

            if (available == 0)
            {
                // Current page is full — link a new page and continue there.
                var newPageId = AllocatePage();
                pageHeader.NextPageId = newPageId;
                pageHeader.WriteTo(buffer);
                WritePageImmediate(currentPageId, buffer);

                currentPageId = newPageId;
                InitializeSchemaPage(buffer, currentPageId);
                currentUsed = 0;
                available = pagePayload;
                pageHeader = PageHeader.ReadFrom(buffer);
            }

            int chunk = Math.Min(schemaSize - schemaOffset, available);
            schemaBytes.AsSpan(schemaOffset, chunk).CopyTo(buffer.AsSpan(32 + currentUsed));

            pageHeader.FreeBytes -= (ushort)chunk;
            pageHeader.WriteTo(buffer);
            WritePageImmediate(currentPageId, buffer);

            schemaOffset += chunk;
            currentUsed += chunk;
        }

        return rootPageId;
    }

    /// <summary>
    /// Serializes <paramref name="schema"/> to a byte array sized exactly by
    /// <see cref="BsonSchema.CalculateSize"/>, avoiding any fixed-size buffer limit.
    /// </summary>
    private byte[] SerializeSchema(BsonSchema schema)
    {
        int requiredSize = schema.CalculateSize();
        var buffer = new byte[requiredSize];
        var writer = new BsonSpanWriter(buffer, GetFrozenKeyMap());
        schema.ToBson(ref writer);
        return buffer;
    }

    /// <summary>
    /// Accumulates all schema page bodies into a single flat byte array so that
    /// schema documents spanning multiple pages can be read atomically.
    /// </summary>
    private byte[] CollectSchemaPageBytes(uint rootPageId)
    {
        var buffer = new byte[PageSize];
        using var stream = new MemoryStream();

        var pageId = rootPageId;
        while (pageId != 0)
        {
            ReadPage(pageId, null, buffer);
            var header = PageHeader.ReadFrom(buffer);
            if (header.PageType != PageType.Schema) break;

            int used = PageSize - 32 - header.FreeBytes;
            if (used > 0)
                stream.Write(buffer, 32, used);

            pageId = header.NextPageId;
        }

        return stream.ToArray();
    }

    private void InitializeSchemaPage(Span<byte> page, uint pageId)
    {
        var header = new PageHeader
        {
            PageId = pageId,
            PageType = PageType.Schema,
            FreeBytes = (ushort)(page.Length - 32),
            NextPageId = 0,
            TransactionId = 0,
            Checksum = 0
        };
        page.Clear();
        header.WriteTo(page);
    }

    private void AppendToSchemaPage(Span<byte> page, ref BsonSpanReader reader)
    {
        // reader contains the BSON doc
        var doc = reader.RemainingBytes();
        doc.CopyTo(page.Slice(32));
    }
}
