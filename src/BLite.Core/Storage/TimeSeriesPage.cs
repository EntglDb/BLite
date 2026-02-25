using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using BLite.Bson;

namespace BLite.Core.Storage;

/// <summary>
/// Specialized page for TimeSeries data.
/// Optimized for append-only, time-ordered entries with automatic pruning support.
/// Layout:
/// [0-31]   Standard PageHeader
/// [32-39]  LastTimestamp (long ticks)
/// [40-43]  EntryCount (int)
/// [44-47]  FirstEntryOffset (int) - usually 48
/// [48...]  Data entries (Packed BSON documents)
/// </summary>
public sealed class TimeSeriesPage
{
    public const int DataOffset = 48;
    
    public static long GetLastTimestamp(ReadOnlySpan<byte> page) => MemoryMarshal.Read<long>(page.Slice(32, 8));
    public static void SetLastTimestamp(Span<byte> page, long timestamp) => MemoryMarshal.Write(page.Slice(32, 8), in timestamp);
    
    public static int GetEntryCount(ReadOnlySpan<byte> page) => MemoryMarshal.Read<int>(page.Slice(40, 4));
    public static void SetEntryCount(Span<byte> page, int count) => MemoryMarshal.Write(page.Slice(40, 4), in count);

    public static void Initialize(Span<byte> page, uint pageId)
    {
        page.Clear();
        var header = new PageHeader
        {
            PageId = pageId,
            PageType = PageType.TimeSeries,
            FreeBytes = (ushort)(page.Length - DataOffset),
            NextPageId = 0
        };
        header.WriteTo(page);
        SetEntryCount(page, 0);
        SetLastTimestamp(page, 0);
    }

    public static bool TryInsert(Span<byte> page, ReadOnlySpan<byte> bsonData, long timestamp)
    {
        var header = PageHeader.ReadFrom(page);
        int entrySize = bsonData.Length;

        if (header.FreeBytes < entrySize)
            return false;

        int count = GetEntryCount(page);
        int currentWritePos = page.Length - header.FreeBytes;
        
        bsonData.CopyTo(page.Slice(currentWritePos));
        
        SetEntryCount(page, count + 1);
        SetLastTimestamp(page, timestamp);
        
        header.FreeBytes -= (ushort)entrySize;
        header.WriteTo(page);
        
        return true;
    }

    /// <summary>
    /// Reads all entries from a page buffer into a list.
    /// Cannot use IEnumerable/yield because ReadOnlySpan is a ref struct and cannot be
    /// captured by an iterator state machine.
    /// </summary>
    public static List<BsonDocument> ReadEntries(byte[] page)
    {
        var results = new List<BsonDocument>();
        int count = GetEntryCount(page);
        int offset = DataOffset;
        
        for (int i = 0; i < count; i++)
        {
            if (offset + 4 > page.Length) break;
            int size = BitConverter.ToInt32(page, offset);
            if (size <= 0 || offset + size > page.Length) break;
            
            var copy = new byte[size];
            Array.Copy(page, offset, copy, 0, size);
            results.Add(new BsonDocument(copy));
            offset += size;
        }
        
        return results;
    }
}
