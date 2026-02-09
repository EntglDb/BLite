using DocumentDb.Core.Storage;
using DocumentDb.Core.Indexing;
using DocumentDb.Bson;
using Xunit;
using DocumentDb.Core.Transactions;

namespace DocumentDb.Tests;

public class BTreeDeleteTests : IDisposable
{
    private readonly string _tempFile;
    private readonly PageFile _pageFile;
    private readonly WriteAheadLog _wal;
    private readonly BTreeIndex _index;
    private StorageEngine _storageEngine;

    public BTreeDeleteTests()
    {
        _tempFile = Path.GetTempFileName();
        _pageFile = new PageFile(_tempFile, PageFileConfig.Default);
        _pageFile.Open();
        _wal = new WriteAheadLog(Path.ChangeExtension(_tempFile, ".wal"));
        _storageEngine = new StorageEngine(_pageFile, _wal);
        _index = new BTreeIndex(_storageEngine, new IndexOptions());
    }

    public void Dispose()
    {
        _pageFile.Dispose();
        if (File.Exists(_tempFile))
            File.Delete(_tempFile);
    }

    [Fact]
    public void Delete_SingleItem_RemovesItem()
    {
        // Arrange
        var key = new IndexKey(BitConverter.GetBytes(100));
        var id = ObjectId.NewObjectId();

        _index.Insert(key, id);
        
        // Verify Insert
        Assert.True(_index.TryFind(key, out var foundId));
        Assert.Equal(id, foundId);

        // Act
        var deleted = _index.Delete(key, id);

        // Assert
        Assert.True(deleted, "Delete returned false");
        Assert.False(_index.TryFind(key, out _), "Item still found after delete");
    }

    [Fact]
    public void Delete_NonExistentItem_ReturnsFalse()
    {
        // Arrange
        var key = new IndexKey(BitConverter.GetBytes(100));
        var id = ObjectId.NewObjectId();

        // Act
        var deleted = _index.Delete(key, id);

        // Assert
        Assert.False(deleted);
    }

    [Fact]
    public void Delete_MultipleItems_RemovesCorrectItem()
    {
        // Arrange
        var key1 = new IndexKey(BitConverter.GetBytes(1));
        var id1 = ObjectId.NewObjectId();
        
        var key2 = new IndexKey(BitConverter.GetBytes(2));
        var id2 = ObjectId.NewObjectId();
        
        var key3 = new IndexKey(BitConverter.GetBytes(3));
        var id3 = ObjectId.NewObjectId();

        _index.Insert(key2, id2);
        _index.Insert(key1, id1);
        _index.Insert(key3, id3);

        // Act
        var deleted = _index.Delete(key2, id2);

        // Assert
        Assert.True(deleted);
        Assert.False(_index.TryFind(key2, out _));
        
        Assert.True(_index.TryFind(key1, out var found1));
        Assert.Equal(id1, found1);
        
        Assert.True(_index.TryFind(key3, out var found3));
        Assert.Equal(id3, found3);
    }
}
