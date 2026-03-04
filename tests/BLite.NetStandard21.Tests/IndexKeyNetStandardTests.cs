using BLite.Core.Indexing;

namespace BLite.NetStandard21.Tests;

public class IndexKeyNetStandardTests
{
    [Fact]
    public void IndexKey_HashCode_IsDeterministic()
    {
        var key = new IndexKey(new byte[] { 1, 2, 3, 4, 5 });
        var hash1 = key.GetHashCode();
        var hash2 = key.GetHashCode();

        Assert.Equal(hash1, hash2);
    }

    [Fact]
    public void IndexKey_HashCode_DiffersForDifferentKeys()
    {
        var key1 = new IndexKey(new byte[] { 1, 2, 3 });
        var key2 = new IndexKey(new byte[] { 4, 5, 6 });

        // Hash codes should differ for different data (with high probability)
        Assert.NotEqual(key1.GetHashCode(), key2.GetHashCode());
    }

    [Fact]
    public void IndexKey_Equality_WorksCorrectly()
    {
        var data = new byte[] { 10, 20, 30 };
        var key1 = new IndexKey(data);
        var key2 = new IndexKey(data);
        var key3 = new IndexKey(new byte[] { 10, 20, 31 });

        Assert.True(key1 == key2);
        Assert.False(key1 != key2);
        Assert.True(key1.Equals(key2));

        Assert.False(key1 == key3);
        Assert.True(key1 != key3);
        Assert.False(key1.Equals(key3));
    }
}
