using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;
using BLite.Core.Query;
using BLite.Core.Storage;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Regression tests for #233: a delete that merged two B-Tree nodes freed the emptied page at
/// once, outside the transaction. Rolling the transaction back left the committed tree pointing
/// at a free page, which the next allocation handed out again (and which read back as garbage
/// after a reopen). A page a transaction stops using must be freed only when it commits.
/// </summary>
public class BTreeRollbackFreeTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"btree_rbfree_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var f in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
            if (File.Exists(f)) File.Delete(f);
    }

    private static PageFileConfig ConfigFor(int pageSize) => PageFileConfig.Small with { PageSize = pageSize };

    private static string Key(int i) => $"k{i:D6}";

    private static DocumentLocation Loc(int i) => new((uint)(1 + i / 60000), (ushort)(i % 60000));

    // ── BTreeIndex driven directly ───────────────────────────────────────────

    [Theory]
    [InlineData(4096, false)]
    [InlineData(4096, true)]
    [InlineData(8192, false)]
    [InlineData(8192, true)]
    [InlineData(16384, false)]
    [InlineData(16384, true)]
    public void BTree_RolledBackMergingDeletes_LeaveTheTreeIntact_BeforeAndAfterReopen(int pageSize, bool deleteAll)
    {
        const int count = 3000;
        var config = ConfigFor(pageSize);
        var model = new List<(string Key, DocumentLocation Loc)>();
        uint rootPageId;

        using (var storage = new StorageEngine(_dbPath, config))
        {
            var index = new BTreeIndex(storage, IndexOptions.CreateBTree("field"));
            rootPageId = index.RootPageId;

            var txn = storage.BeginTransaction().TransactionId;
            for (int i = 0; i < count; i++)
            {
                index.Insert(IndexKey.Create(Key(i)), Loc(i), txn);
                model.Add((Key(i), Loc(i)));
            }
            storage.CommitTransactionAsync(txn).GetAwaiter().GetResult();

            // Delete all (root collapse) or all but every 50th entry (merges at every level),
            // then roll back.
            var del = storage.BeginTransaction().TransactionId;
            for (int i = 0; i < count; i++)
                if (deleteAll || i % 50 != 0)
                    Assert.True(index.Delete(IndexKey.Create(Key(i)), Loc(i), del));
            storage.RollbackTransactionAsync(del).GetAwaiter().GetResult();

            Verify(index, model, 0);

            // New pages must not be any of the committed tree's pages.
            var ins = storage.BeginTransaction().TransactionId;
            for (int i = count; i < 2 * count; i++)
            {
                index.Insert(IndexKey.Create(Key(i)), Loc(i), ins);
                model.Add((Key(i), Loc(i)));
            }
            storage.CommitTransactionAsync(ins).GetAwaiter().GetResult();

            Verify(index, model, 0);
        }

        using (var storage = new StorageEngine(_dbPath, config))
        {
            var index = new BTreeIndex(storage, IndexOptions.CreateBTree("field"), rootPageId);
            Verify(index, model, 0);
        }
    }

    [Theory]
    [InlineData(4096, 1)]
    [InlineData(4096, 2)]
    [InlineData(8192, 3)]
    [InlineData(8192, 4)]
    [InlineData(16384, 5)]
    [InlineData(16384, 6)]
    public void BTree_RandomCommittedAndRolledBackBatches_MatchModel_BeforeAndAfterReopen(int pageSize, int seed)
    {
        var rng = new Random(seed);
        var config = ConfigFor(pageSize);
        var model = new List<(string Key, DocumentLocation Loc)>();
        uint rootPageId;
        int next = 0;

        using (var storage = new StorageEngine(_dbPath, config))
        {
            var index = new BTreeIndex(storage, IndexOptions.CreateBTree("field"));
            rootPageId = index.RootPageId;
            int maxKeySize = index.MaxKeySize;

            for (int round = 0; round < 24; round++)
            {
                bool rollback = rng.Next(2) == 0;
                var txn = storage.BeginTransaction().TransactionId;
                var working = new List<(string Key, DocumentLocation Loc)>(model);

                // Mostly deletes, so nodes merge, with some inserts so nodes also split.
                int deletes = rng.Next(working.Count / 2, working.Count + 1);
                foreach (var victim in working.OrderBy(_ => rng.Next()).Take(deletes).ToList())
                {
                    Assert.True(index.Delete(IndexKey.Create(victim.Key), victim.Loc, txn));
                    working.Remove(victim);
                }
                int inserts = rng.Next(200, 1500);
                for (int i = 0; i < inserts; i++)
                {
                    var key = RandomKey(rng, maxKeySize);
                    var loc = Loc(next++);
                    index.Insert(IndexKey.Create(key), loc, txn);
                    working.Add((key, loc));
                }

                if (rollback)
                {
                    storage.RollbackTransactionAsync(txn).GetAwaiter().GetResult();
                }
                else
                {
                    // Alternate between the group commit and the direct commit path.
                    if (round % 2 == 0)
                        storage.CommitTransactionAsync(txn, CancellationToken.None).GetAwaiter().GetResult();
                    else
                        storage.CommitTransactionAsync(txn).GetAwaiter().GetResult();
                    model = working;
                }

                Verify(index, model, 0);
                if (round % 6 == 5)
                    storage.CheckpointAsync().GetAwaiter().GetResult();
            }
        }

        using (var storage = new StorageEngine(_dbPath, config))
        {
            var index = new BTreeIndex(storage, IndexOptions.CreateBTree("field"), rootPageId);
            Verify(index, model, 0);
        }
    }

    [Fact]
    public void BTree_CommittedMergingDeletes_FreeTheirPages_ForReuse()
    {
        using var storage = new StorageEngine(_dbPath, ConfigFor(4096));
        var index = new BTreeIndex(storage, IndexOptions.CreateBTree("field"));

        uint pagesAfterFirstCycle = 0;
        for (int cycle = 0; cycle < 8; cycle++)
        {
            var ins = storage.BeginTransaction().TransactionId;
            for (int i = 0; i < 3000; i++)
                index.Insert(IndexKey.Create(Key(i)), Loc(i), ins);
            storage.CommitTransactionAsync(ins, CancellationToken.None).GetAwaiter().GetResult();

            var del = storage.BeginTransaction().TransactionId;
            for (int i = 0; i < 3000; i++)
                Assert.True(index.Delete(IndexKey.Create(Key(i)), Loc(i), del));
            storage.CommitTransactionAsync(del, CancellationToken.None).GetAwaiter().GetResult();

            // Freed pages with a WAL image return to the free list at the next truncation.
            storage.CheckpointAsync().GetAwaiter().GetResult();

            Assert.Empty(index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, 0));
            if (cycle == 0) pagesAfterFirstCycle = storage.PageCount;
        }

        // Each cycle reuses the pages the previous one freed, instead of growing the file.
        Assert.True(storage.PageCount <= pagesAfterFirstCycle + 4,
            $"file grew from {pagesAfterFirstCycle} to {storage.PageCount} pages");
    }

    // ── DynamicCollection index ──────────────────────────────────────────────

    [Theory]
    [InlineData(4096)]
    [InlineData(8192)]
    [InlineData(16384)]
    public async Task Dynamic_RolledBackDeletes_KeepEveryIndexEntry_BeforeAndAfterReopen(int pageSize)
    {
        const int count = 2000;
        var config = ConfigFor(pageSize);
        var ids = new List<BsonId>();

        using (var engine = new BLiteEngine(_dbPath, config))
        {
            var col = engine.GetOrCreateCollection("people");
            await col.CreateIndexAsync("name", "idx", unique: false);
            for (int i = 0; i < count; i++)
            {
                var name = Key(i);
                ids.Add(await col.InsertAsync(col.CreateDocument(["name"], b => b.AddString("name", name))));
            }

            using (var txn = engine.BeginTransaction())
            {
                foreach (var id in ids)
                    Assert.True(await col.DeleteAsync(id, txn));
                await txn.RollbackAsync();
            }

            await VerifyDynamicAsync(col, count);

            // Pages allocated now must not be pages the index still uses.
            for (int i = count; i < 2 * count; i++)
            {
                var name = Key(i);
                await col.InsertAsync(col.CreateDocument(["name"], b => b.AddString("name", name)));
            }
            await VerifyDynamicAsync(col, 2 * count);
        }

        using (var engine = new BLiteEngine(_dbPath, config))
        {
            var col = engine.GetOrCreateCollection("people");
            await VerifyDynamicAsync(col, 2 * count);
        }
    }

    private static async Task VerifyDynamicAsync(DynamicCollection col, int count)
    {
        Assert.Equal(count, await col.CountAsync());
        var forward = await col.QueryIndexAsync("idx", null, null).ToListAsync();
        var backward = await col.QueryIndexAsync("idx", null, null, false).ToListAsync();
        Assert.Equal(count, forward.Count);
        Assert.Equal(count, backward.Count);
        for (int i = 0; i < count; i += 97)
        {
            var name = Key(i);
            Assert.Single(await col.QueryIndexAsync("idx", name, name).ToListAsync());
            Assert.Single(await col.QueryIndexAsync("idx", name, name, false).ToListAsync());
        }
    }

    // ── Typed collection secondary index ────────────────────────────────────

    [Fact]
    public async Task Typed_RolledBackDeletes_KeepEveryIndexEntry_BeforeAndAfterReopen()
    {
        const int count = 1500;
        var ids = new List<ObjectId>();

        using (var db = new TestDbContext(_dbPath))
        {
            await db.PeopleV2.CreateIndexAsync(p => p.Name, "idx_name", unique: false);
            for (int i = 0; i < count; i++)
                ids.Add(await db.PeopleV2.InsertAsync(new PersonV2 { Name = Key(i), Age = i }));

            using (var txn = await db.BeginTransactionAsync())
            {
                foreach (var id in ids)
                    Assert.True(await db.PeopleV2.DeleteAsync(id, txn));
                await txn.RollbackAsync();
            }

            await VerifyTypedAsync(db, count);

            for (int i = count; i < 2 * count; i++)
                await db.PeopleV2.InsertAsync(new PersonV2 { Name = Key(i), Age = i });
            await VerifyTypedAsync(db, 2 * count);
        }

        using (var reopened = new TestDbContext(_dbPath))
            await VerifyTypedAsync(reopened, 2 * count);
    }

    private static async Task VerifyTypedAsync(TestDbContext db, int count)
    {
        Assert.Equal(count, await db.PeopleV2.CountAsync());
        for (int i = 0; i < count; i += 89)
        {
            var name = Key(i);
            var hits = await db.PeopleV2.AsQueryable().Where(p => p.Name == name).ToListAsync();
            Assert.Single(hits);
            Assert.Equal(i, hits[0].Age);
        }
    }

    // ── Overflow pages of a deleted document ────────────────────────────────

    [Fact]
    public async Task Typed_RolledBackDeleteOfLargeDocument_KeepsItsOverflowPages()
    {
        // Deleting a document that spans overflow pages freed them at once, like the merge.
        string Big(char c) => new string(c, 60_000);
        ObjectId id;

        using (var db = new TestDbContext(_dbPath))
        {
            id = await db.PeopleV2.InsertAsync(new PersonV2 { Name = Big('a'), Age = 1 });

            using (var txn = await db.BeginTransactionAsync())
            {
                Assert.True(await db.PeopleV2.DeleteAsync(id, txn));
                await txn.RollbackAsync();
            }

            // These would take the overflow pages if the rollback had left them free.
            for (int i = 0; i < 4; i++)
                await db.PeopleV2.InsertAsync(new PersonV2 { Name = Big((char)('b' + i)), Age = 2 + i });

            var doc = await db.PeopleV2.FindByIdAsync(id);
            Assert.NotNull(doc);
            Assert.Equal(Big('a'), doc!.Name);
        }

        using (var reopened = new TestDbContext(_dbPath))
        {
            var doc = await reopened.PeopleV2.FindByIdAsync(id);
            Assert.NotNull(doc);
            Assert.Equal(Big('a'), doc!.Name);
            Assert.Equal(5, await reopened.PeopleV2.CountAsync());
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static string RandomKey(Random rng, int maxKeySize)
    {
        int roll = rng.Next(100);
        int length = roll < 80 ? rng.Next(1, 30)
                   : roll < 97 ? rng.Next(60, 400)
                   : rng.Next(maxKeySize / 2, maxKeySize - 20);
        var chars = new char[length];
        for (int i = 0; i < length; i++) chars[i] = (char)('a' + rng.Next(6));
        var s = new string(chars);
        while (IndexKey.Create(s).Data.Length > maxKeySize) s = s[..^1];
        return s;
    }

    private static void Verify(BTreeIndex index, List<(string Key, DocumentLocation Loc)> model, ulong txn)
    {
        static string L(DocumentLocation l) => $"{l.PageId}:{l.SlotIndex}";

        var expected = model.Select(m => (Key: IndexKey.Create(m.Key), m.Loc)).OrderBy(m => m.Key).ToList();

        var forward = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txn).ToList();
        Assert.Equal(expected.Count, forward.Count);
        Assert.Equal(expected.Select(e => e.Key).ToList(), forward.Select(e => e.Key).ToList());
        Assert.Equal(expected.Select(e => L(e.Loc)).OrderBy(s => s), forward.Select(e => L(e.Location)).OrderBy(s => s));

        var backward = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Backward, txn).ToList();
        Assert.Equal(forward.Select(e => e.Key).Reverse().ToList(), backward.Select(e => e.Key).ToList());

        Assert.Equal(expected.Count, index.CountRange(IndexKey.MinKey, IndexKey.MaxKey, txn));

        var failures = new List<string>();
        foreach (var group in model.GroupBy(m => m.Key).Where((_, i) => i % 13 == 0))
        {
            var key = IndexKey.Create(group.Key);
            if (index.CountRange(key, key, txn) != group.Count()) failures.Add($"CountRange '{group.Key}'");
            if (!index.TryFind(key, out _, txn)) failures.Add($"TryFind '{group.Key}'");
        }
        Assert.Empty(failures);
    }
}
