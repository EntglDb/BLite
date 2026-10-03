using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;
using BLite.Core.Query;
using BLite.Core.Query.Blql;
using BLite.Core.Storage;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// End-to-end tests for secondary indexes on <c>decimal</c> properties (issue #150): the key used
/// to be <c>(double)value</c>, so decimals sharing a double collapsed onto one key and index-backed
/// queries returned the wrong documents. <c>Product.Price</c> has an index in <see cref="TestDbContext"/>.
/// </summary>
public class DecimalIndexTests : IDisposable
{
    private readonly string _dbPath;

    // All four values are 1234567890123456.8 as double.
    private static readonly decimal[] Values =
        [1234567890123456.77m, 1234567890123456.78m, 1234567890123456.79m, 1234567890123456.80m];
    private const decimal Probe = 1234567890123456.78m;

    public DecimalIndexTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"decimal_index_{Guid.NewGuid()}.db");
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(Path.ChangeExtension(_dbPath, ".wal"));
    }

    private static void TryDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); } catch { }
    }

    private static async Task SeedAsync(TestDbContext db)
    {
        for (int i = 0; i < Values.Length; i++)
            await db.Products.InsertAsync(new Product { Id = i + 1, Title = $"P{i + 1}", Price = Values[i] });
    }

    private static CollectionSecondaryIndex<int, Product> PriceIndex(TestDbContext db)
        => db.Products.IndexManager.GetAllIndexes().Single(i => i.PropertyPaths.Single() == nameof(Product.Price));

    // ── Typed collection, index-backed LINQ ──────────────────────────────────

    [Fact]
    public async Task Equality_WithIndex_ReturnsOnlyTheExactValue()
    {
        using var db = new TestDbContext(_dbPath);
        await SeedAsync(db);

        var hits = await db.Products.AsQueryable().Where(p => p.Price == Probe).ToListAsync();

        Assert.Single(hits);
        Assert.Equal(Probe, hits[0].Price);
    }

    [Fact]
    public async Task Range_WithIndex_RespectsDecimalOrder()
    {
        using var db = new TestDbContext(_dbPath);
        await SeedAsync(db);

        var gte = await db.Products.AsQueryable().Where(p => p.Price >= Probe).Select(p => p.Price).ToListAsync();
        var lt  = await db.Products.AsQueryable().Where(p => p.Price < Probe).Select(p => p.Price).ToListAsync();

        Assert.Equal(new[] { 1234567890123456.78m, 1234567890123456.79m, 1234567890123456.80m }, gte.OrderBy(v => v).ToList());
        Assert.Equal(new[] { 1234567890123456.77m }, lt);
    }

    [Fact]
    public async Task Count_WithIndex_IsExact()
    {
        using var db = new TestDbContext(_dbPath);
        await SeedAsync(db);

        Assert.Equal(1, await db.Products.AsQueryable().CountAsync(p => p.Price == Probe));
        Assert.Equal(3, await db.Products.AsQueryable().CountAsync(p => p.Price >= Probe));
        Assert.Equal(2, await db.Products.AsQueryable().CountAsync(p => p.Price > Probe));
    }

    [Fact]
    public async Task OrderBy_WithIndex_SortsNumerically()
    {
        using var db = new TestDbContext(_dbPath);
        foreach (var (price, i) in new decimal[] { 10m, -1.5m, 0m, 9.99m, -1.45m, 1234567890123456.78m, 1234567890123456.77m }.Select((p, i) => (p, i)))
            await db.Products.InsertAsync(new Product { Id = i + 1, Title = $"P{i}", Price = price });

        var asc  = await db.Products.AsQueryable().OrderBy(p => p.Price).Select(p => p.Price).ToListAsync();
        var desc = await db.Products.AsQueryable().OrderByDescending(p => p.Price).Select(p => p.Price).ToListAsync();

        Assert.Equal(new[] { -1.5m, -1.45m, 0m, 9.99m, 10m, 1234567890123456.77m, 1234567890123456.78m }, asc);
        Assert.Equal(asc.AsEnumerable().Reverse().ToList(), desc);
    }

    [Fact]
    public async Task TrailingZeros_DoNotSplitTheKey()
    {
        using var db = new TestDbContext(_dbPath);
        await db.Products.InsertAsync(new Product { Id = 1, Title = "a", Price = 5m });
        await db.Products.InsertAsync(new Product { Id = 2, Title = "b", Price = 5.00m });
        await db.Products.InsertAsync(new Product { Id = 3, Title = "c", Price = 5.01m });

        var hits = await db.Products.AsQueryable().Where(p => p.Price == 5.000m).ToListAsync();

        Assert.Equal(2, hits.Count);
    }

    [Fact]
    public async Task DoubleBound_AgainstDecimalIndex_StillWorks()
    {
        // A bound of another numeric type is converted to decimal before being encoded, so an
        // index keyed with the decimal encoding can still serve it (it used to hold double keys).
        using var db = new TestDbContext(_dbPath);
        await db.Products.InsertAsync(new Product { Id = 1, Title = "a", Price = 1.5m });
        await db.Products.InsertAsync(new Product { Id = 2, Title = "b", Price = 10.25m });
        await db.Products.InsertAsync(new Product { Id = 3, Title = "c", Price = 100m });

        var index = PriceIndex(db);

        Assert.Equal(2, index.Range(5.0, null).Count());
        Assert.Single(index.Range(null, 5.0));
        Assert.Single(index.Range(10, 11));
        Assert.Equal(2, index.CountRange(1.5f, 10.25, startInclusive: true, endInclusive: true));
        Assert.NotNull(index.Seek(100.0));
    }

    // ── Legacy index migration ───────────────────────────────────────────────

    [Fact]
    public async Task LegacyDoubleKeyedIndex_IsRebuiltOnOpen()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            await SeedAsync(db);
            var index = PriceIndex(db);

            // Recreate the on-disk state an older release leaves behind: composite keys made of the
            // double encoding of the price (generic 0x02 prefix) followed by the document id key.
            using (var txn = db.BeginTransaction())
            {
                foreach (var product in await db.Products.AsQueryable().ToListAsync())
                {
                    var dbl = new IndexKey((double)product.Price).Data;
                    var id  = new IndexKey(product.Id).Data;
                    var payload = new byte[dbl.Length - 1 + id.Length];
                    dbl.Slice(1).CopyTo(payload);
                    id.CopyTo(payload.AsSpan(dbl.Length - 1));
                    index.BTreeIndex!.Insert(new IndexKey(payload), new DocumentLocation(2, (ushort)product.Id), txn.TransactionId);
                }
                await txn.CommitAsync();
            }
            db.Products.IndexManager.SetKeyFormat(index.Name, IndexMetadata.LegacyKeyFormat);

            Assert.Contains(AllKeys(index), k => k.Data[0] == 0x02);
        }

        using (var db = new TestDbContext(_dbPath))
        {
            var index = PriceIndex(db);

            Assert.Equal(IndexMetadata.CurrentKeyFormat, index.KeyFormat);
            Assert.DoesNotContain(AllKeys(index), k => k.Data[0] == 0x02);
            Assert.Equal(Values.Length, AllKeys(index).Count);

            var hits = await db.Products.AsQueryable().Where(p => p.Price == Probe).ToListAsync();
            Assert.Single(hits);
            Assert.Equal(3, await db.Products.AsQueryable().CountAsync(p => p.Price >= Probe));
        }

        using (var db = new TestDbContext(_dbPath))
        {
            // Second open: nothing left to migrate, the rebuilt tree is kept.
            var index = PriceIndex(db);
            Assert.Equal(IndexMetadata.CurrentKeyFormat, index.KeyFormat);
            Assert.Equal(Values.Length, AllKeys(index).Count);
        }
    }

    [Fact]
    public async Task KeyFormat_RoundTripsThroughMetadata()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            await SeedAsync(db);
            Assert.Equal(IndexMetadata.CurrentKeyFormat, PriceIndex(db).KeyFormat);
        }

        using (var db = new TestDbContext(_dbPath))
        {
            Assert.Equal(IndexMetadata.CurrentKeyFormat, PriceIndex(db).KeyFormat);
            Assert.Equal(Values.Length, AllKeys(PriceIndex(db)).Count);
        }
    }

    private static List<IndexKey> AllKeys(CollectionSecondaryIndex<int, Product> index)
        => index.BTreeIndex!.Range(IndexKey.NullSentinelNext, IndexKey.MaxKey).Select(e => e.Key).ToList();

    // ── BLQL (no index: BsonValueComparer) ───────────────────────────────────

    [Fact]
    public async Task Blql_Decimal128_ComparesExactly()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("figures");
        foreach (var v in Values)
            await col.InsertAsync(col.CreateDocument(["amount"], b => b.AddDecimal("amount", v)));

        var probe = BsonValue.FromDecimal(Probe);
        Assert.Single(col.Query().Filter(BlqlFilter.Eq("amount", probe)).ToList());
        Assert.Equal(3, col.Query().Filter(BlqlFilter.Gte("amount", probe)).ToList().Count);
        Assert.Single(col.Query().Filter(BlqlFilter.Lt("amount", probe)).ToList());
    }
}
