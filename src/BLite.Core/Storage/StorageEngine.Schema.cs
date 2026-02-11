using System;
using System.Collections.Generic;
using BLite.Bson;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    public List<BsonSchema> GetSchemas(uint rootPageId)
    {
        var schemas = new List<BsonSchema>();
        if (rootPageId == 0) return schemas;

        var pageId = rootPageId;
        var buffer = new byte[PageSize];

        while (pageId != 0)
        {
            ReadPage(pageId, null, buffer);
            var header = PageHeader.ReadFrom(buffer);
            
            if (header.PageType != PageType.Schema) break;

            int used = PageSize - 32 - header.FreeBytes;
            if (used > 0)
            {
                var reader = new BsonSpanReader(buffer.AsSpan(32, used));
                while (reader.Remaining >= 4)
                {
                    var docSize = reader.PeekInt32();
                    if (docSize <= 0 || docSize > reader.Remaining) break;

                    var schema = BsonSchema.FromBson(ref reader);
                    schemas.Add(schema);
                }
            }

            pageId = header.NextPageId;
        }

        return schemas;
    }

    /// <summary>
    /// Appends a new schema version. Returns the root page ID (which might be new if it was 0 initially).
    /// </summary>
    public uint AppendSchema(uint rootPageId, BsonSchema schema)
    {
        var buffer = new byte[PageSize];
        
        // Serialize schema to temporary buffer to calculate size
        var tempBuffer = new byte[PageSize];
        var tempWriter = new BsonSpanWriter(tempBuffer);
        schema.ToBson(ref tempWriter);
        var schemaSize = tempWriter.Position;

        if (rootPageId == 0)
        {
            rootPageId = AllocatePage();
            InitializeSchemaPage(buffer, rootPageId);
            tempBuffer.AsSpan(0, schemaSize).CopyTo(buffer.AsSpan(32));
            
            var header = PageHeader.ReadFrom(buffer);
            header.FreeBytes = (ushort)(PageSize - 32 - schemaSize);
            header.WriteTo(buffer);

            WritePageImmediate(rootPageId, buffer);
            return rootPageId;
        }

        // Find last page in chain
        uint currentPageId = rootPageId;
        uint lastPageId = rootPageId;
        while (currentPageId != 0)
        {
            ReadPage(currentPageId, null, buffer);
            var header = PageHeader.ReadFrom(buffer);
            lastPageId = currentPageId;
            if (header.NextPageId == 0) break;
            currentPageId = header.NextPageId;
        }

        // Buffer now contains the last page
        var lastHeader = PageHeader.ReadFrom(buffer);
        int currentUsed = PageSize - 32 - lastHeader.FreeBytes;
        int lastOffset = 32 + currentUsed;
        
        if (lastHeader.FreeBytes >= schemaSize)
        {
            // Fits in current page
            tempBuffer.AsSpan(0, schemaSize).CopyTo(buffer.AsSpan(lastOffset));
            
            lastHeader.FreeBytes -= (ushort)schemaSize;
            lastHeader.WriteTo(buffer);

            WritePageImmediate(lastPageId, buffer);
        }
        else
        {
            // Allocate new page
            var newPageId = AllocatePage();
            lastHeader.NextPageId = newPageId;
            lastHeader.WriteTo(buffer);
            WritePageImmediate(lastPageId, buffer);

            InitializeSchemaPage(buffer, newPageId);
            tempBuffer.AsSpan(0, schemaSize).CopyTo(buffer.AsSpan(32));
            
            var newHeader = PageHeader.ReadFrom(buffer);
            newHeader.FreeBytes = (ushort)(PageSize - 32 - schemaSize);
            newHeader.WriteTo(buffer);

            WritePageImmediate(newPageId, buffer);
        }

        return rootPageId;
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
