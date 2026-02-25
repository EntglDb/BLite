using System;
using BLite.Bson;
using BLite.Core.Transactions;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    private const int MaxInsertedBeforePrune = 1000;
    private static readonly TimeSpan MinPruneInterval = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Specialized insertion for TimeSeries-designated collections.
    /// Manages TimeSeriesPages and triggers pruning logic if metadata rules are met.
    /// </summary>
    public DocumentLocation InsertTimeSeries(string collectionName, BsonDocument document, ITransaction transaction)
    {
        var meta = GetCollectionMetadata(collectionName);
        if (meta == null || !meta.IsTimeSeries)
            throw new InvalidOperationException($"Collection {collectionName} is not a TimeSeries collection.");

        // 1. Get timestamp from document
        long timestamp = 0;
        if (meta.TtlFieldName != null && document.TryGetValue(meta.TtlFieldName, out var val))
        {
            if (val.Type == BsonType.DateTime)
                timestamp = val.AsDateTime.Ticks;
            else if (val.Type == BsonType.Int64)
                timestamp = val.AsInt64;
        }
        
        if (timestamp == 0) timestamp = DateTime.UtcNow.Ticks;

        // 2. Prepare BSON data
        var bsonData = document.RawData;

        // 3. Find/Allocate TimeSeries Page
        uint pageId = meta.TimeSeriesHeadPageId;
        byte[] buffer = new byte[PageSize];
        
        if (pageId == 0)
        {
            pageId = AllocatePage();
            TimeSeriesPage.Initialize(buffer, pageId);
            meta.TimeSeriesHeadPageId = pageId;
            // Write it immediately to set the type
            WritePageImmediate(pageId, buffer);
        }

        // 4. Try Insert into current page (LIFO for TS usually means find the last page)
        uint lastPageId = pageId;
        ushort slotIndex = 0;
        
        while (true)
        {
            ReadPage(lastPageId, transaction.TransactionId, buffer);
            var header = PageHeader.ReadFrom(buffer);
            int currentWritePos = PageSize - header.FreeBytes;
            
            if (TimeSeriesPage.TryInsert(buffer, bsonData, timestamp))
            {
                WritePageImmediate(lastPageId, buffer);
                slotIndex = (ushort)currentWritePos;
                break;
            }
            
            if (header.NextPageId == 0)
            {
                // Allocate new page
                uint newPageId = AllocatePage();
                header.NextPageId = newPageId;
                header.WriteTo(buffer);
                WritePageImmediate(lastPageId, buffer);
                
                TimeSeriesPage.Initialize(buffer, newPageId);
                TimeSeriesPage.TryInsert(buffer, bsonData, timestamp);
                WritePageImmediate(newPageId, buffer);
                lastPageId = newPageId;
                slotIndex = (ushort)TimeSeriesPage.DataOffset;
                break;
            }
            lastPageId = header.NextPageId;
        }

        // 5. Update Metadata Counters
        meta.InsertedSinceLastPruning++;
        bool shouldPrune = meta.InsertedSinceLastPruning >= MaxInsertedBeforePrune ||
                          (DateTime.UtcNow.Ticks - meta.LastPruningTimestamp) >= MinPruneInterval.Ticks;

        if (shouldPrune && meta.RetentionPolicyMs > 0)
        {
            PruneTimeSeriesInternal(meta, transaction);
            meta.InsertedSinceLastPruning = 0;
            meta.LastPruningTimestamp = DateTime.UtcNow.Ticks;
        }

        SaveCollectionMetadata(meta);

        return new DocumentLocation(lastPageId, slotIndex);
    }

    private void PruneTimeSeriesInternal(CollectionMetadata meta, ITransaction transaction)
        => PruneTimeSeries(meta, transaction);

    /// <summary>
    /// Walks the TimeSeries page chain and frees all pages whose LastTimestamp is older than
    /// the collection's RetentionPolicyMs.  Called internally on insert threshold and publicly
    /// via <c>DynamicCollection.ForcePrune()</c> for testing.
    /// NOTE (v1): does not clean up stale BTree primary-index entries for removed pages.
    /// </summary>
    public void PruneTimeSeries(CollectionMetadata meta, ITransaction transaction)
    {
        long cutoffTicks = DateTime.UtcNow.Ticks - (meta.RetentionPolicyMs * TimeSpan.TicksPerMillisecond);
        uint currentPageId = meta.TimeSeriesHeadPageId;
        uint previousPageId = 0;
        byte[] buffer = new byte[PageSize];

        while (currentPageId != 0)
        {
            ReadPage(currentPageId, transaction.TransactionId, buffer);
            var header = PageHeader.ReadFrom(buffer);
            long lastTs = TimeSeriesPage.GetLastTimestamp(buffer);

            // If the ENTIRE page is older than cutoff, remove it from the chain.
            if (lastTs < cutoffTicks)
            {
                uint nextId = header.NextPageId;
                FreePage(currentPageId);
                
                if (previousPageId == 0)
                    meta.TimeSeriesHeadPageId = nextId;
                else
                {
                    byte[] prevBuffer = new byte[PageSize];
                    ReadPage(previousPageId, transaction.TransactionId, prevBuffer);
                    var prevHeader = PageHeader.ReadFrom(prevBuffer);
                    prevHeader.NextPageId = nextId;
                    prevHeader.WriteTo(prevBuffer);
                    WritePageImmediate(previousPageId, prevBuffer);
                }
                currentPageId = nextId;
            }
            else
            {
                // Pages are generally time-ordered.
                break;
            }
        }
    }
}
