using BLite.Core.Indexing;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Every NaN gets one index key, below -Infinity (#257). The sort-preserving double encoding keyed a
/// NaN by its bits, so a NaN with the sign bit clear (what Arm64 computes for 0.0 / 0.0) sorted above
/// +Infinity and one with the sign bit set (<see cref="double.NaN"/>, what x64 computes) below
/// -Infinity: the order of an index-served query depended on the CPU that wrote the value.
/// </summary>
public sealed class IndexKeyNaNTests : IDisposable
{
    private static readonly double PositiveNaN = BitConverter.Int64BitsToDouble(0x7FF8_0000_0000_0000L);
    private static readonly double PayloadNaN = BitConverter.Int64BitsToDouble(0x7FF0_0000_0000_0001L);
    private static readonly double NegativePayloadNaN = BitConverter.Int64BitsToDouble(unchecked((long)0xFFF0_0000_0000_0001UL));

    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"blite_nan_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var p in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
            try { if (File.Exists(p)) File.Delete(p); } catch { }
    }

    public static IEnumerable<object[]> NaNs() => new[]
    {
        new object[] { PositiveNaN },
        new object[] { PayloadNaN },
        new object[] { NegativePayloadNaN },
        new object[] { double.NaN },
    };

    [Theory]
    [MemberData(nameof(NaNs))]
    public void Every_NaN_Has_The_Key_Of_Double_NaN_Below_Negative_Infinity(double nan)
    {
        Assert.True(double.IsNaN(nan));
        var key = IndexKey.Create(nan);

        Assert.Equal(IndexKey.Create(double.NaN), key);
        Assert.True(key < IndexKey.Create(double.NegativeInfinity));
        Assert.True(key < IndexKey.Create(double.MinValue));
        Assert.True(double.IsNaN(key.As<double>()));
    }

    [Fact]
    public void Index_Orders_NaNs_First_And_Excludes_Them_From_A_Lower_Bounded_Range()
    {
        using var storage = new StorageEngine(_dbPath, PageFileConfig.Default);
        var index = new BTreeIndex(storage, IndexOptions.CreateBTree("ratio"));
        var txn = storage.BeginTransaction().TransactionId;

        index.Insert(IndexKey.Create(PositiveNaN), new DocumentLocation(1, 0), txn);       // d1
        index.Insert(IndexKey.Create(1.0), new DocumentLocation(2, 0), txn);               // d2
        index.Insert(IndexKey.Create(double.NaN), new DocumentLocation(3, 0), txn);        // d3
        index.Insert(IndexKey.Create(double.PositiveInfinity), new DocumentLocation(4, 0), txn);

        var ascending = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txn)
            .Select(e => e.Location.PageId).ToList();
        // The two NaNs share a key and keep their insertion order between themselves.
        Assert.Equal(new uint[] { 1, 3 }, ascending.Take(2).OrderBy(p => p));
        Assert.Equal(new uint[] { 2, 4 }, ascending.Skip(2));

        var aboveHalf = index.Range(IndexKey.Create(0.5), IndexKey.MaxKey, IndexDirection.Forward, txn)
            .Select(e => e.Location.PageId).ToList();
        Assert.Equal(new uint[] { 2, 4 }, aboveHalf);

        var atLeastMax = index.Range(IndexKey.Create(double.MaxValue), IndexKey.MaxKey, IndexDirection.Forward, txn)
            .Select(e => e.Location.PageId).ToList();
        Assert.Equal(new uint[] { 4 }, atLeastMax);
    }
}
