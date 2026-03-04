using BLite.Core.Storage;

namespace BLite.NetStandard21.Tests;

public class TimeSeriesPageNetStandardTests
{
    private const int PageSize = 512;

    [Fact]
    public void TimeSeriesPage_Initialize_SetsCorrectDefaults()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        Assert.Equal(0L, TimeSeriesPage.GetLastTimestamp(page));
        Assert.Equal(0, TimeSeriesPage.GetEntryCount(page));
    }

    [Fact]
    public void TimeSeriesPage_SetAndGet_Timestamp()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 1);

        const long expected = 1_700_000_000_000L;
        TimeSeriesPage.SetLastTimestamp(page, expected);

        Assert.Equal(expected, TimeSeriesPage.GetLastTimestamp(page));
    }

    [Fact]
    public void TimeSeriesPage_SetAndGet_EntryCount()
    {
        var page = new byte[PageSize];
        TimeSeriesPage.Initialize(page, pageId: 2);

        const int expected = 42;
        TimeSeriesPage.SetEntryCount(page, expected);

        Assert.Equal(expected, TimeSeriesPage.GetEntryCount(page));
    }
}
