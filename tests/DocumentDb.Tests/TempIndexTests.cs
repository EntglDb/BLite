using DocumentDb.Bson;
using DocumentDb.Core;
using DocumentDb.Core.Indexing;
using DocumentDb.Core.Storage;
using Xunit;

namespace DocumentDb.Tests;

public class TempIndexTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly PageFile _pageFile;

    public TempIndexTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"test_temp_index_{Guid.NewGuid()}.db");
        _pageFile = new PageFile(_testDbPath, PageFileConfig.Default);
        _pageFile.Open();
    }

    public void Dispose()
    {
        _pageFile.Dispose();
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);
    }

    [Fact]
    public void BTreeIndex_SingleInsert_ShouldWork()
    {
        var options = IndexOptions.CreateBTree("testField");
        var btree = new BTreeIndex(_pageFile, options);
        var key = new IndexKey(100);
        var docId = ObjectId.NewObjectId();
        btree.Insert(key, docId);
        Assert.True(btree.TryFind(key, out var foundId));
        Assert.Equal(docId, foundId);
    }
}
