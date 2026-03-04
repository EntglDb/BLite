using BLite.Core;
using BLite.Core.Storage;

namespace BLite.NetStandard21.Tests;

public class PageHeaderNetStandardTests
{
    [Fact]
    public void PageHeader_WriteTo_ReadFrom_RoundTrip()
    {
        var original = new PageHeader
        {
            PageId = 42,
            PageType = PageType.Data,
            FreeBytes = 1234,
            NextPageId = 7,
            TransactionId = 9999,
            Checksum = 0xDEADBEEF,
            DictionaryRootPageId = 3
        };

        var buffer = new byte[64];
        original.WriteTo(buffer);

        var restored = PageHeader.ReadFrom(buffer);

        Assert.Equal(original.PageId, restored.PageId);
        Assert.Equal(original.PageType, restored.PageType);
        Assert.Equal(original.FreeBytes, restored.FreeBytes);
        Assert.Equal(original.NextPageId, restored.NextPageId);
        Assert.Equal(original.TransactionId, restored.TransactionId);
        Assert.Equal(original.Checksum, restored.Checksum);
        Assert.Equal(original.DictionaryRootPageId, restored.DictionaryRootPageId);
    }

    [Fact]
    public void SlottedPageHeader_WriteTo_ReadFrom_RoundTrip()
    {
        var original = new SlottedPageHeader
        {
            PageId = 5,
            PageType = PageType.Collection,
            SlotCount = 10,
            FreeSpaceStart = 100,
            FreeSpaceEnd = 4096,
            NextOverflowPage = 0,
            TransactionId = 12345
        };

        var buffer = new byte[64];
        original.WriteTo(buffer);

        var restored = SlottedPageHeader.ReadFrom(buffer);

        Assert.Equal(original.PageId, restored.PageId);
        Assert.Equal(original.PageType, restored.PageType);
        Assert.Equal(original.SlotCount, restored.SlotCount);
        Assert.Equal(original.FreeSpaceStart, restored.FreeSpaceStart);
        Assert.Equal(original.FreeSpaceEnd, restored.FreeSpaceEnd);
        Assert.Equal(original.NextOverflowPage, restored.NextOverflowPage);
        Assert.Equal(original.TransactionId, restored.TransactionId);
    }
}
