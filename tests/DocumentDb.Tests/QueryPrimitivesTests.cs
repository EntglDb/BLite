using DocumentDb.Core.Storage;
using DocumentDb.Core.Indexing;
using DocumentDb.Bson;
using Xunit;

namespace DocumentDb.Tests;

public class QueryPrimitivesTests : IDisposable
{
    private readonly string _testFile;
    private readonly StorageEngine _storage;
    private readonly BTreeIndex _index;

    public QueryPrimitivesTests()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"docdb_test_{Guid.NewGuid()}.db");
        _storage = new StorageEngine(_testFile, PageFileConfig.Default);
        
        // Initialize simple index
        var options = IndexOptions.CreateBTree("test");
        _index = new BTreeIndex(_storage, options); 
        
        SeedData();
    }

    private void SeedData()
    {
        // Insert keys: 10, 20, 30, 40, 50
        // And strings: "A", "AB", "ABC", "B", "C"
        
        var txnId = _storage.BeginTransaction().TransactionId;
        
        Insert(10, txnId);
        Insert(20, txnId);
        Insert(30, txnId);
        Insert(40, txnId);
        Insert(50, txnId);
        
        Insert("A", txnId);
        Insert("AB", txnId);
        Insert("ABC", txnId);
        Insert("B", txnId);
        Insert("C", txnId);
        
        _storage.CommitTransaction(txnId);
    }

    private void Insert(dynamic value, ulong txnId)
    {
        IndexKey key;
        if (value is int i) key = IndexKey.Create(i);
        else if (value is string s) key = IndexKey.Create(s);
        else throw new ArgumentException();

        _index.Insert(key, new DocumentLocation(1, 1), txnId);
    }

    [Fact]
    public void Equal_ShouldFindExactMatch()
    {
        var key = IndexKey.Create(30);
        var result = _index.Equal(key, 0).ToList();
        
        Assert.Single(result);
        Assert.Equal(key, result[0].Key);
    }
    
    [Fact]
    public void Equal_ShouldReturnEmpty_WhenNotFound()
    {
        var key = IndexKey.Create(25);
        var result = _index.Equal(key, 0).ToList();
        
        Assert.Empty(result);
    }

    [Fact]
    public void GreaterThan_ShouldReturnMatches()
    {
        var key = IndexKey.Create(30);
        var result = _index.GreaterThan(key, orEqual: false, 0).ToList();
        
        Assert.True(result.Count >= 2); 
        Assert.Equal(IndexKey.Create(40), result[0].Key);
        Assert.Equal(IndexKey.Create(50), result[1].Key);
    }

    [Fact]
    public void GreaterThanOrEqual_ShouldReturnMatches()
    {
        var key = IndexKey.Create(30);
        var result = _index.GreaterThan(key, orEqual: true, 0).ToList();
        
        Assert.True(result.Count >= 3); 
        Assert.Equal(IndexKey.Create(30), result[0].Key);
        Assert.Equal(IndexKey.Create(40), result[1].Key);
        Assert.Equal(IndexKey.Create(50), result[2].Key);
    }

    [Fact]
    public void LessThan_ShouldReturnMatches()
    {
        var key = IndexKey.Create(30);
        var result = _index.LessThan(key, orEqual: false, 0).ToList();
        
        Assert.Equal(2, result.Count); // 20, 10 (Order is backward?)
        // LessThan yields backward?
        // Implementation: MovePrev(). So yes, 20 then 10.
        Assert.Equal(IndexKey.Create(20), result[0].Key);
        Assert.Equal(IndexKey.Create(10), result[1].Key);
    }

    [Fact]
    public void Between_ShouldReturnRange()
    {
        var start = IndexKey.Create(20);
        var end = IndexKey.Create(40);
        var result = _index.Between(start, end, startInclusive: true, endInclusive: true, 0).ToList();
        
        Assert.Equal(3, result.Count); // 20, 30, 40
        Assert.Equal(IndexKey.Create(20), result[0].Key);
        Assert.Equal(IndexKey.Create(30), result[1].Key);
        Assert.Equal(IndexKey.Create(40), result[2].Key);
    }

    [Fact]
    public void StartsWith_ShouldReturnPrefixMatches()
    {
        var result = _index.StartsWith("AB", 0).ToList();
        
        Assert.Equal(2, result.Count); // AB, ABC
        Assert.Equal(IndexKey.Create("AB"), result[0].Key);
        Assert.Equal(IndexKey.Create("ABC"), result[1].Key);
    }

    [Fact]
    public void Like_ShouldSupportWildcards()
    {
        // "A%" -> A, AB, ABC
        var result = _index.Like("A%", 0).ToList();
        Assert.Equal(3, result.Count);

        // "%B%" -> AB, ABC, B
        var result2 = _index.Like("%B%", 0).ToList();
        // A (no), AB (yes), ABC (yes), B (yes), C (no)
        Assert.Equal(3, result2.Count); // AB, ABC, B. Wait, order?
        // Index order: A, AB, ABC, B, C.
        // AB ok. ABC ok. B ok.
    }
    
    [Fact]
    public void Like_Underscore_ShouldMatchSingleChar()
    {
        // "_B" -> AB (yes), B (no: len 1), ABC (no)
        var result = _index.Like("_B", 0).ToList();
        Assert.Single(result);
        Assert.Equal(IndexKey.Create("AB"), result[0].Key);
    }

    [Fact]
    public void In_ShouldReturnSpecificKeys()
    {
        var keys = new[] { IndexKey.Create(10), IndexKey.Create(30), IndexKey.Create(50), IndexKey.Create(99) };
        var result = _index.In(keys, 0).ToList();
        
        Assert.Equal(3, result.Count); // 10, 30, 50. (99 missing)
        Assert.Equal(IndexKey.Create(10), result[0].Key);
        Assert.Equal(IndexKey.Create(30), result[1].Key);
        Assert.Equal(IndexKey.Create(50), result[2].Key);
    }

    public void Dispose()
    {
        _storage.Dispose();
        File.Delete(_testFile);
    }
}
