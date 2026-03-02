using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BLite.Core.Storage;

namespace BLite.NetStandard21.Tests;

public sealed class StorageEngineNetStandardTests : IDisposable
{
    private readonly string _dbPath;

    public StorageEngineNetStandardTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_ns21_se_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var walPath = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(walPath)) File.Delete(walPath);
    }

    private StorageEngine CreateEngine()
        => new StorageEngine(_dbPath, PageFileConfig.Small);

    [Fact]
    public async Task ReadPageAsync_FromWalCache_ReturnsCorrectly_OnNetStandard()
    {
        using var engine = CreateEngine();
        var txn = engine.BeginTransaction();

        var pageId = engine.AllocatePage();
        var pageData = new byte[engine.PageSize];
        pageData[0] = 0xCA;
        pageData[1] = 0xFE;
        engine.WritePage(pageId, txn.TransactionId, pageData);

        var destination = new Memory<byte>(new byte[engine.PageSize]);
        // The ValueTask from WAL-cache path (netstandard2.1: returns `default`)
        var vt = engine.ReadPageAsync(pageId, txn.TransactionId, destination, CancellationToken.None);
        await vt;

        Assert.Equal(0xCA, destination.Span[0]);
        Assert.Equal(0xFE, destination.Span[1]);

        engine.RollbackTransaction(txn);
    }

    [Fact]
    public async Task ReadPageAsync_FromWalIndex_ReturnsCorrectly_OnNetStandard()
    {
        using var engine = CreateEngine();
        var txn = engine.BeginTransaction();

        var pageId = engine.AllocatePage();
        var pageData = new byte[engine.PageSize];
        pageData[0] = 0xBE;
        pageData[1] = 0xEF;
        engine.WritePage(pageId, txn.TransactionId, pageData);

        // Commit moves WAL cache → WAL index
        engine.CommitTransaction(txn);

        var destination = new Memory<byte>(new byte[engine.PageSize]);
        // The ValueTask from WAL-index path (netstandard2.1: returns `default`)
        var vt = engine.ReadPageAsync(pageId, null, destination, CancellationToken.None);
        await vt;

        Assert.Equal(0xBE, destination.Span[0]);
        Assert.Equal(0xEF, destination.Span[1]);
    }
}
