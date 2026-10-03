using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Query;
using BLite.Core.Transactions;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Regression tests for the primary-index root split performed inside a transaction
/// (root split happens on the 65th key, see <c>BTreeIndex.MaxEntriesPerNode</c>).
/// The new root page used to be published in memory and in the collection metadata before
/// the transaction committed, so rolling back — or reading from another transaction — left
/// the collection pointing at a page that was never written, and every scan spun forever.
/// </summary>
public class TransactionRootSplitTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"root_split_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var f in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
            if (File.Exists(f)) File.Delete(f);
    }

    private static async Task<List<User>> ReadAllAsync(TestDbContext db)
        => await Task.Run(async () => await db.Users.AsQueryable().ToListAsync()).WaitAsync(Timeout);

    private static Task InsertManyAsync(TestDbContext db, ITransaction txn, int count)
        => Task.Run(async () =>
        {
            for (int i = 0; i < count; i++)
                await db.Users.InsertAsync(new User { Id = ObjectId.NewObjectId(), Name = $"U{i}", Age = i }, txn);
        });

    [Theory]
    [InlineData(64)]
    [InlineData(65)]
    [InlineData(500)]
    public async Task Rollback_OfInsertsIntoEmptyCollection_LeavesCollectionReadable(int count)
    {
        using (var db = new TestDbContext(_dbPath))
        {
            using (var txn = db.BeginTransaction())
            {
                await InsertManyAsync(db, txn, count);
                await txn.RollbackAsync();
            }

            Assert.Empty(await ReadAllAsync(db));

            // The collection must still be usable afterwards.
            await db.Users.InsertAsync(new User { Id = ObjectId.NewObjectId(), Name = "after", Age = 1 });
            Assert.Single(await ReadAllAsync(db));
        }

        // The rollback must not leave anything behind in the file either.
        using var db2 = new TestDbContext(_dbPath);
        var all = await ReadAllAsync(db2);
        Assert.Single(all);
        Assert.Equal("after", all[0].Name);
    }

    [Theory]
    [InlineData(10)]
    [InlineData(64)]
    [InlineData(65)]
    [InlineData(500)]
    public async Task ReadOutsideTransaction_WhileLargeTransactionIsOpen_SeesOnlyCommittedData(int uncommitted)
    {
        using var db = new TestDbContext(_dbPath);
        await db.Users.InsertAsync(new User { Id = ObjectId.NewObjectId(), Name = "committed", Age = 0 });

        using var txn = db.BeginTransaction();
        await InsertManyAsync(db, txn, uncommitted);

        var visible = await ReadAllAsync(db);
        Assert.Single(visible);
        Assert.Equal("committed", visible[0].Name);

        await txn.CommitAsync();

        Assert.Equal(uncommitted + 1, (await ReadAllAsync(db)).Count);
    }

    [Fact]
    public async Task Commit_OfRootSplit_PersistsNewRoot_AcrossReopen()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            using var txn = db.BeginTransaction();
            await InsertManyAsync(db, txn, 300);
            await txn.CommitAsync();
        }

        using var db2 = new TestDbContext(_dbPath);
        Assert.Equal(300, (await ReadAllAsync(db2)).Count);
    }

    [Fact]
    public async Task Session_CommitOfRootSplit_PersistsNewRoot_AcrossReopen()
    {
        using (var engine = new BLite.Core.BLiteEngine(_dbPath))
        {
            using var session = engine.OpenSession();
            session.BeginTransaction();
            for (int i = 0; i < 200; i++)
                await session.InsertAsync("items", engine.CreateDocument(["x"], b => b.AddInt32("x", i)));
            await session.CommitAsync();
        }

        using var engine2 = new BLite.Core.BLiteEngine(_dbPath);
        using var session2 = engine2.OpenSession();
        int count = 0;
        await foreach (var _ in session2.FindAllAsync("items"))
            count++;
        Assert.Equal(200, count);
    }

    // ── Spatial (R-Tree) and vector (HNSW) indexes: same publish-before-commit problem ──

    private static async Task<List<GeoEntity>> NearAllAsync(TestDbContext db)
        => await Task.Run(async () =>
        {
            var found = new List<GeoEntity>();
            await foreach (var g in db.GeoItems.NearAsync("idx_spatial", (40.0, -74.0), radiusKm: 5000))
                found.Add(g);
            return found;
        }).WaitAsync(Timeout);

    [Fact]
    public async Task Spatial_RootSplit_RolledBack_LeavesIndexUsable()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            using (var txn = db.BeginTransaction())
            {
                for (int i = 0; i < 1000; i++) // more than one R-Tree leaf holds (429 at 16 KB pages), so the root splits
                    await db.GeoItems.InsertAsync(new GeoEntity { Id = ObjectId.NewObjectId(), Name = $"G{i}", Location = (40.0 + i * 0.001, -74.0) }, txn);
                await txn.RollbackAsync();
            }

            Assert.Empty(await NearAllAsync(db));

            await db.GeoItems.InsertAsync(new GeoEntity { Id = ObjectId.NewObjectId(), Name = "after", Location = (40.0, -74.0) });
            Assert.Single(await NearAllAsync(db));
        }

        using var db2 = new TestDbContext(_dbPath);
        Assert.Single(await NearAllAsync(db2));
    }

    [Fact]
    public async Task Spatial_ReadOutsideTransaction_WhileRootSplitIsUncommitted_SeesOnlyCommittedData()
    {
        using var db = new TestDbContext(_dbPath);
        await db.GeoItems.InsertAsync(new GeoEntity { Id = ObjectId.NewObjectId(), Name = "committed", Location = (40.0, -74.0) });

        using var txn = db.BeginTransaction();
        for (int i = 0; i < 1000; i++)
            await db.GeoItems.InsertAsync(new GeoEntity { Id = ObjectId.NewObjectId(), Name = $"G{i}", Location = (40.0 + i * 0.001, -74.0) }, txn);

        Assert.Single(await NearAllAsync(db));

        await txn.CommitAsync();
        Assert.Equal(1001, (await NearAllAsync(db)).Count);
    }

    [Fact]
    public async Task Vector_FirstInsert_RolledBack_LeavesIndexUsable()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            using (var txn = db.BeginTransaction())
            {
                for (int i = 0; i < 10; i++)
                    await db.VectorItems.InsertAsync(new VectorEntity { Title = $"V{i}", Embedding = [i, 1.0f, 0.0f] }, txn);
                await txn.RollbackAsync();
            }

            var none = await Task.Run(async () => await db.VectorItems.VectorSearchAsync("idx_vector", [1.0f, 1.0f, 1.0f], 5).ToListAsync()).WaitAsync(Timeout);
            Assert.Empty(none);

            await db.VectorItems.InsertAsync(new VectorEntity { Title = "after", Embedding = [1.0f, 0.0f, 0.0f] });
            var found = await Task.Run(async () => await db.VectorItems.VectorSearchAsync("idx_vector", [1.0f, 1.0f, 1.0f], 5).ToListAsync()).WaitAsync(Timeout);
            Assert.Single(found);
        }

        using var db2 = new TestDbContext(_dbPath);
        var reopened = await Task.Run(async () => await db2.VectorItems.VectorSearchAsync("idx_vector", [1.0f, 1.0f, 1.0f], 5).ToListAsync()).WaitAsync(Timeout);
        Assert.Single(reopened);
    }

    [Fact]
    public async Task Session_NewSecondaryIndex_RootSplitInTransaction_PersistsAcrossReopen()
    {
        using (var engine = new BLite.Core.BLiteEngine(_dbPath))
        {
            var col = engine.GetOrCreateCollection("indexed");
            await col.CreateIndexAsync("x");

            using var session = engine.OpenSession();
            session.BeginTransaction();
            for (int i = 0; i < 200; i++)
                await session.InsertAsync("indexed", engine.CreateDocument(["x"], b => b.AddInt32("x", i)));
            await session.CommitAsync();
        }

        using var engine2 = new BLite.Core.BLiteEngine(_dbPath);
        var col2 = engine2.GetOrCreateCollection("indexed");
        var results = col2.Query().Filter(BLite.Core.Query.Blql.BlqlFilter.Gte("x", 0)).ToList();
        Assert.Equal(200, results.Count);
    }
}
