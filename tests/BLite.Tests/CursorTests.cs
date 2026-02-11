using BLite.Core.Storage;
using BLite.Core.Indexing;
using BLite.Bson;
using Xunit;

namespace BLite.Tests;

public class CursorTests : IDisposable
{
    private readonly string _testFile;
    private readonly StorageEngine _storage;
    private readonly BTreeIndex _index;

    public CursorTests()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"docdb_cursor_test_{Guid.NewGuid()}.db");
        _storage = new StorageEngine(_testFile, PageFileConfig.Default);
        
        var options = IndexOptions.CreateBTree("test");
        _index = new BTreeIndex(_storage, options); 
        
        SeedData();
    }

    private void SeedData()
    {
        var txnId = _storage.BeginTransaction().TransactionId;
        
        // Insert 10, 20, 30
        _index.Insert(IndexKey.Create(10), new DocumentLocation(1, 0), txnId);
        _index.Insert(IndexKey.Create(20), new DocumentLocation(2, 0), txnId);
        _index.Insert(IndexKey.Create(30), new DocumentLocation(3, 0), txnId);
        
        _storage.CommitTransaction(txnId);
    }

    [Fact]
    public void MoveToFirst_ShouldPositionAtFirst()
    {
        using var cursor = _index.CreateCursor(0);
        Assert.True(cursor.MoveToFirst());
        Assert.Equal(IndexKey.Create(10), cursor.Current.Key);
    }

    [Fact]
    public void MoveToLast_ShouldPositionAtLast()
    {
        using var cursor = _index.CreateCursor(0);
        Assert.True(cursor.MoveToLast());
        Assert.Equal(IndexKey.Create(30), cursor.Current.Key);
    }

    [Fact]
    public void MoveNext_ShouldTraverseForward()
    {
        using var cursor = _index.CreateCursor(0);
        cursor.MoveToFirst();
        
        Assert.True(cursor.MoveNext());
        Assert.Equal(IndexKey.Create(20), cursor.Current.Key);
        
        Assert.True(cursor.MoveNext());
        Assert.Equal(IndexKey.Create(30), cursor.Current.Key);
        
        Assert.False(cursor.MoveNext()); // End
    }

    [Fact]
    public void MovePrev_ShouldTraverseBackward()
    {
        using var cursor = _index.CreateCursor(0);
        cursor.MoveToLast();
        
        Assert.True(cursor.MovePrev());
        Assert.Equal(IndexKey.Create(20), cursor.Current.Key);
        
        Assert.True(cursor.MovePrev());
        Assert.Equal(IndexKey.Create(10), cursor.Current.Key);
        
        Assert.False(cursor.MovePrev()); // Start
    }
    
    [Fact]
    public void Seek_ShouldPositionExact_OrNext()
    {
        using var cursor = _index.CreateCursor(0);
        
        // Exact
        Assert.True(cursor.Seek(IndexKey.Create(20)));
        Assert.Equal(IndexKey.Create(20), cursor.Current.Key);
        
        // Non-exact (15 -> should land on 20)
        Assert.False(cursor.Seek(IndexKey.Create(15)));
        Assert.Equal(IndexKey.Create(20), cursor.Current.Key);
        
        // Non-exact (35 -> should be invalid/end)
        Assert.False(cursor.Seek(IndexKey.Create(35)));
        // Current should throw invalid
        Assert.Throws<InvalidOperationException>(() => cursor.Current);
    }

    public void Dispose()
    {
        _storage.Dispose();
        if (File.Exists(_testFile)) File.Delete(_testFile);
    }
}
