using BLite.Bson;
using BLite.Core;
using BLite.Core.Query.Blql;

namespace BLite.Tests;

/// <summary>
/// BLQL compares two integers exactly (#268). Int32 and Int64 pairs went through <c>double</c>,
/// which cannot tell apart integers above 2^53 that differ by less than the double spacing:
/// <c>{t: {$gt: long.MaxValue - 1}}</c> missed <c>long.MaxValue</c> and <c>{t: 2^53}</c> matched
/// <c>2^53 + 1</c>.
/// </summary>
public sealed class BlqlInt64CompareTests : IDisposable
{
    private const long TwoPow53 = 1L << 53;

    private readonly string _dir;

    public BlqlInt64CompareTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"blite_blql_int64_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    [Fact]
    public void Comparer_Orders_Large_Integers_Exactly()
    {
        Assert.True(BsonValueComparer.Compare(BsonValue.FromInt64(long.MaxValue), BsonValue.FromInt64(long.MaxValue - 1)) > 0);
        Assert.True(BsonValueComparer.Compare(BsonValue.FromInt64(TwoPow53 + 1), BsonValue.FromInt64(TwoPow53)) > 0);
        Assert.Equal(0, BsonValueComparer.Compare(BsonValue.FromInt64(TwoPow53 + 1), BsonValue.FromInt64(TwoPow53 + 1)));
        Assert.True(BsonValueComparer.Compare(BsonValue.FromInt64(long.MinValue), BsonValue.FromInt64(long.MinValue + 1)) < 0);
        // Mixed Int32/Int64 compare as long.
        Assert.Equal(0, BsonValueComparer.Compare(BsonValue.FromInt32(42), BsonValue.FromInt64(42)));
        Assert.True(BsonValueComparer.Compare(BsonValue.FromInt32(int.MaxValue), BsonValue.FromInt64(int.MaxValue + 1L)) < 0);
        // A Double side still compares through double: 2^53 + 1 rounds to 2^53.
        Assert.Equal(0, BsonValueComparer.Compare(BsonValue.FromInt64(TwoPow53 + 1), BsonValue.FromDouble(TwoPow53)));
        // Integer against Decimal128 stays exact.
        Assert.True(BsonValueComparer.Compare(BsonValue.FromInt64(long.MaxValue), BsonValue.FromDecimal(long.MaxValue - 1m)) > 0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Filters_On_Large_Int64_Values_Match_Exactly(bool indexed)
    {
        using var engine = new BLiteEngine(Path.Combine(_dir, indexed ? "indexed.db" : "plain.db"));
        var col = engine.GetOrCreateCollection("items");
        if (indexed) await col.CreateIndexAsync("t");
        foreach (var (n, t) in new[] { ("max", long.MaxValue), ("max-1", long.MaxValue - 1), ("2^53+1", TwoPow53 + 1) })
            await col.InsertAsync(col.CreateDocument(["n", "t"], b => b.AddString("n", n).AddInt64("t", t)));

        List<string> Names(BlqlFilter f) => col.Query(f).ToList()
            .Select(d => d.TryGetString("n", out var n) ? n : "?").OrderBy(n => n, StringComparer.Ordinal).ToList();

        Assert.Equal(new[] { "max" }, Names(BlqlFilter.Gt("t", long.MaxValue - 1)));
        Assert.Equal(new[] { "max-1" }, Names(BlqlFilter.Eq("t", long.MaxValue - 1)));
        Assert.Empty(Names(BlqlFilter.Eq("t", TwoPow53)));
        Assert.Equal(new[] { "2^53+1" }, Names(BlqlFilter.Eq("t", TwoPow53 + 1)));
        Assert.Equal(new[] { "2^53+1", "max-1" }, Names(BlqlFilter.Lt("t", long.MaxValue)));
    }
}
