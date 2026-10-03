using BLite.Bson;
using BLite.Core.Indexing;

namespace BLite.Tests;

/// <summary>
/// Decimal index keys must be exact and order-preserving (issue #150): the old encoding went through
/// <c>(double)value</c>, so decimals sharing a double collapsed onto one key.
/// </summary>
public class IndexKeyDecimalTests
{
    private static readonly decimal[] Sorted =
    [
        decimal.MinValue,
        -1234567890123456.80m,
        -1234567890123456.79m,
        -1234567890123456.78m,
        -1000m,
        -1.5m,
        -1.45m,
        -0.0000000000000000000000000001m,
        0m,
        0.0000000000000000000000000001m,
        0.5m,
        1m,
        1.45m,
        1.5m,
        9m,
        10m,
        1234567890123456.77m,
        1234567890123456.78m,
        1234567890123456.79m,
        1234567890123456.80m,
        decimal.MaxValue,
    ];

    [Fact]
    public void Decimal_KeyOrder_MatchesNumericOrder()
    {
        for (int i = 0; i < Sorted.Length; i++)
        for (int j = 0; j < Sorted.Length; j++)
        {
            var expected = Math.Sign(Sorted[i].CompareTo(Sorted[j]));
            var actual = Math.Sign(new IndexKey(Sorted[i]).CompareTo(new IndexKey(Sorted[j])));
            Assert.True(expected == actual, $"{Sorted[i]} vs {Sorted[j]}: expected {expected}, got {actual}");
        }
    }

    [Fact]
    public void Decimal_RoundTrips()
    {
        foreach (var v in Sorted)
            Assert.Equal(v, new IndexKey(v).As<decimal>());
    }

    [Fact]
    public void Decimal_NeighboursBeyondDoublePrecision_GetDistinctKeys()
    {
        var a = new IndexKey(1234567890123456.78m);
        var b = new IndexKey(1234567890123456.79m);
        Assert.NotEqual(a, b);
        Assert.True(a < b);
    }

    [Fact]
    public void Decimal_TrailingZeros_AreNormalized()
    {
        Assert.Equal(new IndexKey(5m), new IndexKey(5.00m));
        Assert.Equal(new IndexKey(0m), new IndexKey(0.000m));
        Assert.Equal(new IndexKey(-12.5m), new IndexKey(-12.500m));
    }

    [Fact]
    public void Decimal_Create_UsesDecimalEncoding()
    {
        Assert.Equal(new IndexKey(3.25m), IndexKey.Create(3.25m));
    }

    [Fact]
    public void Decimal_Key_SortsAfterGenericKeys_AndBelowMaxKey()
    {
        Assert.True(new IndexKey(decimal.MaxValue) > IndexKey.NullSentinelNext);
        Assert.True(new IndexKey(decimal.MaxValue) < IndexKey.MaxKey);
    }
}
