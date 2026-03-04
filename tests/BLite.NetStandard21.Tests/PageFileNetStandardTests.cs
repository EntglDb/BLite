using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BLite.Core.Storage;

namespace BLite.NetStandard21.Tests;

public sealed class PageFileNetStandardTests : IDisposable
{
    private readonly string _dbPath;
    private readonly PageFileConfig _config;

    public PageFileNetStandardTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_ns21_pf_{Guid.NewGuid():N}.db");
        _config = PageFileConfig.Small;
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
    }

    private PageFile CreateAndOpen()
    {
        var pf = new PageFile(_dbPath, _config);
        pf.Open();
        return pf;
    }

    [Fact]
    public async Task ReadPageAsync_SingleRead_ReturnsCorrectData()
    {
        using var pf = CreateAndOpen();

        var pageId = pf.AllocatePage();
        var writeData = new byte[_config.PageSize];
        writeData[0] = 0xAA;
        writeData[_config.PageSize - 1] = 0xBB;
        pf.WritePage(pageId, writeData);

        var readBuffer = new byte[_config.PageSize];
        await pf.ReadPageAsync(pageId, readBuffer, CancellationToken.None);

        Assert.Equal(0xAA, readBuffer[0]);
        Assert.Equal(0xBB, readBuffer[_config.PageSize - 1]);
    }

    [Fact]
    public async Task ReadPageAsync_ConcurrentReads_DoNotDeadlock()
    {
        using var pf = CreateAndOpen();

        var pageId = pf.AllocatePage();
        var writeData = new byte[_config.PageSize];
        writeData[0] = 0x42;
        pf.WritePage(pageId, writeData);

        const int readers = 20;
        var tasks = new Task[readers];
        for (int i = 0; i < readers; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                var buf = new byte[_config.PageSize];
                await pf.ReadPageAsync(pageId, buf, CancellationToken.None);
                Assert.Equal(0x42, buf[0]);
            });
        }

        await Task.WhenAll(tasks);
    }

    [Fact]
    public async Task ReadPageAsync_WithCancellation_ThrowsOperationCanceledException()
    {
        using var pf = CreateAndOpen();
        var pageId = pf.AllocatePage();
        pf.WritePage(pageId, new byte[_config.PageSize]);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var buffer = new Memory<byte>(new byte[_config.PageSize]);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => pf.ReadPageAsync(pageId, buffer, cts.Token).AsTask());
    }
}
