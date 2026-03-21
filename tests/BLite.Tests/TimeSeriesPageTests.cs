using System;
using BLite.Core;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Unit tests for <see cref="TimeSeriesPage"/>.
/// Validates Initialize, TryInsert, and ReadEntries against the raw page buffer.
/// </summary>
public class TimeSeriesPageTests
{
    // Minimal valid BSON document: {length=5, no elements, null-terminator}
    // BSON spec: first 4 bytes = total doc length (LE), last byte = 0x00
    private static readonly byte[] MinimalBson = { 5, 0, 0, 0, 0 };

    private const int PageSize = 512;

    // ─── DataOffset constant ─────────────────────────────────────────────────

    [Fact]
    public void DataOffset_Is48()
    {
        Assert.Equal(48, TimeSeriesPage.DataOffset);
    }

    // ─── Initialize ──────────────────────────────────────────────────────────

    [Fact]
    public void Initialize_SetsZeroTimestampAndZeroEntryCount()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        Assert.Equal(0L, TimeSeriesPage.GetLastTimestamp(page));
        Assert.Equal(0, TimeSeriesPage.GetEntryCount(page));
    }

    [Fact]
    public void Initialize_SetsFreeBytes_EqualToPageSizeMinusDataOffset()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 2);

        var header = PageHeader.ReadFrom(page);
        Assert.Equal((ushort)(PageSize - TimeSeriesPage.DataOffset), header.FreeBytes);
    }

    [Fact]
    public void Initialize_SetsPageType_ToTimeSeries()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 3);

        var header = PageHeader.ReadFrom(page);
        Assert.Equal(PageType.TimeSeries, header.PageType);
    }

    [Fact]
    public void Initialize_SetsNextPageId_ToZero()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 4);

        var header = PageHeader.ReadFrom(page);
        Assert.Equal(0u, header.NextPageId);
    }

    // ─── TryInsert ───────────────────────────────────────────────────────────

    [Fact]
    public void TryInsert_FreshPage_ReturnsTrue()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        bool result = TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 100L);

        Assert.True(result);
    }

    [Fact]
    public void TryInsert_IncrementsEntryCount()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 100L);

        Assert.Equal(1, TimeSeriesPage.GetEntryCount(page));
    }

    [Fact]
    public void TryInsert_UpdatesLastTimestamp()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        const long ts = 1_700_000_000_000L;
        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: ts);

        Assert.Equal(ts, TimeSeriesPage.GetLastTimestamp(page));
    }

    [Fact]
    public void TryInsert_ReducesFreeBytes_ByEntrySize()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        var headerBefore = PageHeader.ReadFrom(page);
        ushort freeBefore = headerBefore.FreeBytes;

        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 100L);

        var headerAfter = PageHeader.ReadFrom(page);
        Assert.Equal(freeBefore - MinimalBson.Length, headerAfter.FreeBytes);
    }

    [Fact]
    public void TryInsert_MultipleEntries_EntryCountMatchesInsertedCount()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 1L);
        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 2L);
        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 3L);

        Assert.Equal(3, TimeSeriesPage.GetEntryCount(page));
    }

    [Fact]
    public void TryInsert_MultipleEntries_LastTimestampIsLatest()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 10L);
        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 20L);
        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 30L);

        Assert.Equal(30L, TimeSeriesPage.GetLastTimestamp(page));
    }

    [Fact]
    public void TryInsert_WhenPageFull_ReturnsFalse()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        // Fill until full (512-48=464 free, each entry=5 → max 92 inserts, 4 bytes remaining)
        int inserted = 0;
        while (TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: inserted))
            inserted++;

        // Next insert must fail
        bool result = TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 9999L);

        Assert.False(result);
    }

    [Fact]
    public void TryInsert_EntryCountUnchanged_AfterFullPageFail()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        while (TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 1L)) { }
        int countBeforeFailedInsert = TimeSeriesPage.GetEntryCount(page);

        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 9999L);

        Assert.Equal(countBeforeFailedInsert, TimeSeriesPage.GetEntryCount(page));
    }

    // ─── ReadEntries ─────────────────────────────────────────────────────────

    [Fact]
    public void ReadEntries_EmptyPage_ReturnsEmptyList()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        var entries = TimeSeriesPage.ReadEntries(page);

        Assert.Empty(entries);
    }

    [Fact]
    public void ReadEntries_AfterOneInsert_ReturnsOneDocument()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 100L);

        var entries = TimeSeriesPage.ReadEntries(page);

        Assert.Single(entries);
    }

    [Fact]
    public void ReadEntries_AfterMultipleInserts_ReturnsAllDocuments()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 1L);
        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 2L);
        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 3L);

        var entries = TimeSeriesPage.ReadEntries(page);

        Assert.Equal(3, entries.Count);
    }

    [Fact]
    public void ReadEntries_CountMatchesGetEntryCount()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 1L);
        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 2L);

        var entries = TimeSeriesPage.ReadEntries(page);
        int storedCount = TimeSeriesPage.GetEntryCount(page);

        Assert.Equal(storedCount, entries.Count);
    }

    [Fact]
    public void ReadEntries_EachDocument_HasExpectedSize()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        TimeSeriesPage.TryInsert(page, MinimalBson, timestamp: 100L);

        var entries = TimeSeriesPage.ReadEntries(page);

        Assert.Equal(MinimalBson.Length, entries[0].Size);
    }
}
