using BLite.Bson;
using BLite.Core.Indexing;
using BLite.Core.Query;
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

    /// <summary>
    /// An index written before the fix keeps a NaN with the sign bit clear under its former key,
    /// above +Infinity, which an update or delete of the document no longer removes. Opening the
    /// collection rebuilds such an index; one holding +Infinity but no former NaN key is left alone.
    /// </summary>
    [Fact]
    public async Task Index_Holding_The_Former_Positive_NaN_Key_Is_Rebuilt_On_Open()
    {
        var seed = new[]
        {
            new SzDoc { Name = "nan", Ratio = PositiveNaN },
            new SzDoc { Name = "one", Ratio = 1.0 },
            new SzDoc { Name = "inf", Ratio = double.PositiveInfinity },
        };
        // What an earlier release wrote for PositiveNaN: its bits with the sign bit set.
        var legacyUserKey = new byte[] { 0xFF, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

        using (var db = new SzIndexedContext(_dbPath))
        {
            await db.Docs.InsertBulkAsync(seed);
            var index = RatioIndex(db);
            var nan = (await db.Docs.FindAllAsync().ToListAsync()).Single(x => x.Name == "nan");

            var current = index.BTreeIndex!.Range(IndexKey.NullSentinelNext, IndexKey.MaxKey).ToList()
                .Single(e => e.Key.Data.StartsWith(new IndexKey(PositiveNaN).Data)
                             && e.Key.Data.EndsWith(new IndexKey(nan.Id).Data));
            var payload = legacyUserKey.Concat(new IndexKey(nan.Id).Data.ToArray()).ToArray();
            using (var txn = db.BeginTransaction())
            {
                Assert.True(index.BTreeIndex.Delete(current.Key, current.Location, txn.TransactionId));
                index.BTreeIndex.Insert(new IndexKey(payload), current.Location, txn.TransactionId);
                await txn.CommitAsync();
            }
            Assert.Contains(AllKeys(index), k => k.Data.StartsWith(new IndexKey(legacyUserKey).Data));
        }

        uint ratioRoot;
        using (var db = new SzIndexedContext(_dbPath))
        {
            var index = RatioIndex(db);
            Assert.DoesNotContain(AllKeys(index), k => k.Data.StartsWith(new IndexKey(legacyUserKey).Data));
            Assert.Single(AllKeys(index), k => k.Data.StartsWith(new IndexKey(double.NaN).Data));
            Assert.Equal(3, AllKeys(index).Count);

            // The entry now goes away with its document.
            var nan = (await db.Docs.FindAllAsync().ToListAsync()).Single(x => x.Name == "nan");
            Assert.True(await db.Docs.DeleteAsync(nan.Id));
            Assert.Equal(2, AllKeys(index).Count);
            ratioRoot = index.RootPageId;
        }

        using (var db = new SzIndexedContext(_dbPath))
        {
            // The +Infinity key left is not a former NaN key: nothing left to rebuild.
            Assert.Equal(ratioRoot, RatioIndex(db).RootPageId);
            Assert.Equal(["inf", "one"], (await db.Docs.AsQueryable().Where(x => x.Ratio >= 1.0).ToListAsync())
                .Select(x => x.Name).Order(StringComparer.Ordinal));
        }
    }

    private static CollectionSecondaryIndex<ObjectId, SzDoc> RatioIndex(SzIndexedContext db)
        => db.Docs.IndexManager.GetAllIndexes().Single(i => i.PropertyPaths.Single() == nameof(SzDoc.Ratio));

    private static List<IndexKey> AllKeys(CollectionSecondaryIndex<ObjectId, SzDoc> index)
        => index.BTreeIndex!.Range(IndexKey.NullSentinelNext, IndexKey.MaxKey).Select(e => e.Key).ToList();
}
