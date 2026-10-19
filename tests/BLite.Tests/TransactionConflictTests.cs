using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Two transactions that are open at the same time and write the same page must never both
/// commit with one of them silently lost (#304). The first committer wins; the second commit
/// either succeeds with both writes intact or throws <see cref="BLiteTransactionConflictException"/>
/// and is rolled back.
/// </summary>
public class TransactionConflictTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"blite_conflict_{Guid.NewGuid():N}");

    public TransactionConflictTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    private string DbPath(string name) => Path.Combine(_dir, name + ".db");

    /// <summary>
    /// Commits <paramref name="second"/> and reports whether it was rejected as a conflict.
    /// A rejected transaction must be rolled back.
    /// </summary>
    private static async Task<bool> CommitSecondAsync(ITransaction second)
    {
        try
        {
            await second.CommitAsync();
            return false;
        }
        catch (BLiteTransactionConflictException)
        {
            Assert.Equal(TransactionState.Aborted, second.State);
            return true;
        }
    }

    [Fact]
    public async Task Typed_two_open_transactions_on_one_collection_never_lose_a_commit()
    {
        var path = DbPath("typed");
        ObjectId a, b;
        bool conflict;
        using (var db = new TestDbContext(path))
        {
            await db.Users.InsertAsync(new User { Name = "seed" });
            await db.SaveChangesAsync();

            var t1 = db.BeginTransaction();
            var t2 = db.BeginTransaction();
            var first = new User { Name = "from-t1" };
            var second = new User { Name = "from-t2" };
            await db.Users.InsertAsync(first, t1);
            await db.Users.InsertAsync(second, t2);
            a = first.Id; b = second.Id;

            await t1.CommitAsync();
            conflict = await CommitSecondAsync(t2);

            Assert.NotNull(await db.Users.FindByIdAsync(a));
            Assert.Equal(!conflict, await db.Users.FindByIdAsync(b) is not null);
            Assert.Equal(conflict ? 2 : 3, await db.Users.CountAsync());
            t1.Dispose(); t2.Dispose();
        }

        using (var db = new TestDbContext(path))
        {
            Assert.NotNull(await db.Users.FindByIdAsync(a));
            Assert.Equal(conflict ? 2 : 3, await db.Users.CountAsync());
        }
    }

    [Fact]
    public async Task Dynamic_two_open_transactions_on_one_collection_never_lose_a_commit()
    {
        var path = DbPath("dynamic");
        BsonId a, b;
        bool conflict;
        using (var engine = new BLiteEngine(path))
        {
            var col = engine.GetOrCreateCollection("people");
            await col.InsertAsync(col.CreateDocument(["name"], x => x.AddString("name", "seed")));

            var t1 = engine.BeginTransaction();
            var t2 = engine.BeginTransaction();
            a = await col.InsertAsync(col.CreateDocument(["name"], x => x.AddString("name", "from-t1")), t1);
            b = await col.InsertAsync(col.CreateDocument(["name"], x => x.AddString("name", "from-t2")), t2);

            await t1.CommitAsync();
            conflict = await CommitSecondAsync(t2);

            Assert.NotNull(await col.FindByIdAsync(a));
            Assert.Equal(!conflict, await col.FindByIdAsync(b) is not null);
            Assert.Equal(conflict ? 2 : 3, await col.CountAsync());
            t1.Dispose(); t2.Dispose();
        }

        using (var engine = new BLiteEngine(path))
        {
            var col = engine.GetOrCreateCollection("people");
            Assert.NotNull(await col.FindByIdAsync(a));
            Assert.Equal(conflict ? 2 : 3, await col.CountAsync());
        }
    }

    [Fact]
    public async Task Two_sessions_with_open_transactions_never_lose_a_commit()
    {
        var path = DbPath("sessions");
        BsonId a, b;
        bool conflict;
        using (var engine = new BLiteEngine(path))
        {
            var col = engine.GetOrCreateCollection("orders");
            await col.InsertAsync(col.CreateDocument(["n"], x => x.AddInt32("n", 0)));

            using var s1 = engine.OpenSession();
            using var s2 = engine.OpenSession();
            var tx1 = s1.BeginTransaction();
            var tx2 = s2.BeginTransaction();
            a = await s1.InsertAsync("orders", col.CreateDocument(["n"], x => x.AddInt32("n", 1)));
            b = await s2.InsertAsync("orders", col.CreateDocument(["n"], x => x.AddInt32("n", 2)));

            await s1.CommitAsync();
            try
            {
                await s2.CommitAsync();
                conflict = false;
            }
            catch (BLiteTransactionConflictException)
            {
                conflict = true;
                Assert.Equal(TransactionState.Aborted, tx2.State);
            }

            Assert.NotNull(await col.FindByIdAsync(a));
            Assert.Equal(!conflict, await col.FindByIdAsync(b) is not null);
            Assert.Equal(conflict ? 2 : 3, await col.CountAsync());
        }

        using (var engine = new BLiteEngine(path))
            Assert.Equal(conflict ? 2 : 3, await engine.GetOrCreateCollection("orders").CountAsync());
    }

    [Fact]
    public async Task An_open_transaction_never_overwrites_an_auto_commit_write_to_the_same_page()
    {
        var path = DbPath("autocommit");
        using var db = new TestDbContext(path);
        await db.Users.InsertAsync(new User { Name = "seed" });
        await db.SaveChangesAsync();

        using var t1 = db.BeginTransaction();
        var inTxn = new User { Name = "in-transaction" };
        await db.Users.InsertAsync(inTxn, t1);

        var auto = new User { Name = "auto-commit" };
        await db.Users.InsertAsync(auto);        // commits on its own, same data page and leaf

        bool conflict = await CommitSecondAsync(t1);

        Assert.NotNull(await db.Users.FindByIdAsync(auto.Id));
        Assert.Equal(!conflict, await db.Users.FindByIdAsync(inTxn.Id) is not null);
        Assert.Equal(conflict ? 2 : 3, await db.Users.CountAsync());
    }

    [Fact]
    public async Task Transactions_on_different_collections_both_commit()
    {
        var path = DbPath("disjoint");
        using var db = new TestDbContext(path);
        await db.Users.InsertAsync(new User { Name = "seed" });
        await db.TestDocuments.InsertAsync(new TestDocument { Category = "seed" });
        await db.SaveChangesAsync();

        using var t1 = db.BeginTransaction();
        using var t2 = db.BeginTransaction();
        var user = new User { Name = "u" };
        var doc = new TestDocument { Category = "d" };
        await db.Users.InsertAsync(user, t1);
        await db.TestDocuments.InsertAsync(doc, t2);

        await t1.CommitAsync();
        await t2.CommitAsync();

        Assert.NotNull(await db.Users.FindByIdAsync(user.Id));
        Assert.NotNull(await db.TestDocuments.FindByIdAsync(doc.Id));
    }

    [Fact]
    public async Task A_rejected_transaction_leaves_no_trace_and_can_be_retried()
    {
        var path = DbPath("retry");
        using (var db = new TestDbContext(path))
        {
            await db.Users.InsertAsync(new User { Name = "seed" });
            await db.SaveChangesAsync();

            var t1 = db.BeginTransaction();
            var t2 = db.BeginTransaction();
            await db.Users.InsertAsync(new User { Name = "from-t1" }, t1);
            var loser = new User { Name = "from-t2" };
            await db.Users.InsertAsync(loser, t2);
            await t1.CommitAsync();

            // On this shape (same data page, same primary leaf) the second commit must be rejected.
            await Assert.ThrowsAsync<BLiteTransactionConflictException>(async () => await t2.CommitAsync());
            Assert.Equal(TransactionState.Aborted, t2.State);
            Assert.Null(await db.Users.FindByIdAsync(loser.Id));
            t1.Dispose(); t2.Dispose();

            // The free-space index and caches are back in step: a retry and a plain insert work.
            using (var retry = db.BeginTransaction())
            {
                await db.Users.InsertAsync(new User { Name = "from-t2-retry" }, retry);
                await retry.CommitAsync();
            }
            await db.Users.InsertAsync(new User { Name = "after" });
            await db.SaveChangesAsync();
            Assert.Equal(4, await db.Users.CountAsync());
        }

        using (var db = new TestDbContext(path))
            Assert.Equal(4, await db.Users.CountAsync());
    }

    [Fact]
    public async Task Sequential_transactions_on_one_collection_never_conflict()
    {
        var path = DbPath("sequential");
        using var db = new TestDbContext(path);
        for (int i = 0; i < 50; i++)
        {
            using var t = db.BeginTransaction();
            await db.Users.InsertAsync(new User { Name = $"u{i}" }, t);
            await t.CommitAsync();
        }
        Assert.Equal(50, await db.Users.CountAsync());
    }

    [Fact]
    public async Task Concurrent_sessions_inserting_without_a_transaction_lose_nothing()
    {
        // Each session builds its own DynamicCollection for "concurrent". With a lock per instance
        // their auto-commit inserts ran concurrently on the same pages and most were lost, while
        // every InsertAsync returned an id.
        var path = DbPath("sessions-auto");
        using var engine = new BLiteEngine(path);
        engine.RegisterKeys(["n"]);
        for (int run = 0; run < 5; run++)
        {
            var ids = await Task.WhenAll(Enumerable.Range(0, 10).Select(async i =>
            {
                using var session = engine.OpenSession();
                return await session.InsertAsync("concurrent", engine.CreateDocument(["n"], b => b.AddInt32("n", i)));
            }));
            Assert.Equal(10, ids.Distinct().Count());
        }
        Assert.Equal(50, await engine.GetOrCreateCollection("concurrent").CountAsync());
    }

    [Fact]
    public async Task Concurrent_auto_commit_writers_never_conflict()
    {
        var path = DbPath("concurrent-auto");
        using var db = new TestDbContext(path);
        var writers = Enumerable.Range(0, 4).Select(w => Task.Run(async () =>
        {
            for (int i = 0; i < 25; i++)
                await db.Users.InsertAsync(new User { Name = $"w{w}-{i}" });
        })).ToArray();
        await Task.WhenAll(writers);
        Assert.Equal(100, await db.Users.CountAsync());
    }

    // ── Review of #310 ─────────────────────────────────────────────────────────────────────

    /// <summary>A long lock timeout: these tests race writers on purpose and must not fail on #303.</summary>
    private static readonly PageFileConfig LoadTolerant = PageFileConfig.Default with { LockTimeout = LockTimeout.From(30_000, 30_000) };

    [Fact]
    public async Task Typed_auto_commit_inserts_racing_explicit_transactions_retry_instead_of_throwing()
    {
        // An explicit commit does not take the collection lock, so it can land between an
        // auto-commit insert's reads and its commit. That insert is rejected and must run again,
        // not throw BLiteTransactionConflictException to a caller who never opened a transaction.
        using var db = new TestDbContext(DbPath("typed-auto-vs-explicit"), LoadTolerant);
        await db.Users.InsertAsync(new User { Name = "seed" });

        var autoIds = new System.Collections.Concurrent.ConcurrentBag<ObjectId>();
        var explicitIds = new System.Collections.Concurrent.ConcurrentBag<ObjectId>();
        var autoWriters = Enumerable.Range(0, 3).Select(w => Task.Run(async () =>
        {
            for (int i = 0; i < 100; i++)
            {
                var user = new User { Name = $"auto-{w}-{i}" };
                await db.Users.InsertAsync(user);
                autoIds.Add(user.Id);
            }
        }));
        var explicitWriters = Enumerable.Range(0, 3).Select(w => Task.Run(async () =>
        {
            for (int i = 0; i < 100; i++)
            {
                using var t = db.BeginTransaction();
                var user = new User { Name = $"explicit-{w}-{i}" };
                await db.Users.InsertAsync(user, t);
                if (!await CommitSecondAsync(t))
                    explicitIds.Add(user.Id);
            }
        }));
        await Task.WhenAll(autoWriters.Concat(explicitWriters));

        Assert.Equal(300, autoIds.Count);
        foreach (var id in autoIds.Concat(explicitIds))
            Assert.NotNull(await db.Users.FindByIdAsync(id));
        Assert.Equal(1 + 300 + explicitIds.Count, await db.Users.CountAsync());
    }

    [Fact]
    public async Task Dynamic_auto_commit_inserts_racing_explicit_transactions_retry_instead_of_throwing()
    {
        using var engine = new BLiteEngine(DbPath("dynamic-auto-vs-explicit"), LoadTolerant);
        engine.RegisterKeys(["n"]);
        var col = engine.GetOrCreateCollection("items");
        await col.InsertAsync(engine.CreateDocument(["n"], b => b.AddInt32("n", -1)));

        var autoIds = new System.Collections.Concurrent.ConcurrentBag<BsonId>();
        var explicitIds = new System.Collections.Concurrent.ConcurrentBag<BsonId>();
        var autoWriters = Enumerable.Range(0, 3).Select(w => Task.Run(async () =>
        {
            for (int i = 0; i < 100; i++)
                autoIds.Add(await col.InsertAsync(engine.CreateDocument(["n"], b => b.AddInt32("n", i))));
        }));
        var explicitWriters = Enumerable.Range(0, 3).Select(w => Task.Run(async () =>
        {
            for (int i = 0; i < 100; i++)
            {
                using var t = engine.BeginTransaction();
                var id = await col.InsertAsync(engine.CreateDocument(["n"], b => b.AddInt32("n", i)), t);
                if (!await CommitSecondAsync(t))
                    explicitIds.Add(id);
            }
        }));
        await Task.WhenAll(autoWriters.Concat(explicitWriters));

        Assert.Equal(300, autoIds.Count);
        foreach (var id in autoIds.Concat(explicitIds))
            Assert.NotNull(await col.FindByIdAsync(id));
        Assert.Equal(1 + 300 + explicitIds.Count, await col.CountAsync());
    }

    [Fact]
    public async Task Page_versions_are_pruned_once_no_open_transaction_needs_them()
    {
        // Before, the prune ran while the committing transactions were still registered, so it
        // never found the map unused: after enough commits it only grew.
        using var storage = new StorageEngine(DbPath("prune"), PageFileConfig.Small);
        var pages = Enumerable.Range(0, StorageEngine.PageVersionPruneThreshold + 1000).Select(_ => storage.AllocatePage()).ToArray();
        var data = new byte[storage.PageSize];
        foreach (var chunk in pages.Chunk(500))
        {
            using var t = storage.BeginTransaction();
            foreach (var page in chunk)
                storage.WritePage(page, t.TransactionId, data);
            await t.CommitAsync();
        }

        Assert.InRange(storage.TrackedPageVersionCount, 1, StorageEngine.PageVersionPruneThreshold);
        Assert.Equal(0, storage.TrackedTransactionCount);

        // Conflicts are still detected on the pages whose versions were forgotten.
        var buffer = new byte[storage.PageSize];
        using var t1 = storage.BeginTransaction();
        using var t2 = storage.BeginTransaction();
        storage.ReadPage(pages[0], t1.TransactionId, buffer);
        storage.ReadPage(pages[0], t2.TransactionId, buffer);
        storage.WritePage(pages[0], t1.TransactionId, data);
        storage.WritePage(pages[0], t2.TransactionId, data);
        await t1.CommitAsync();
        await Assert.ThrowsAsync<BLiteTransactionConflictException>(async () => await t2.CommitAsync());
    }

    [Fact]
    public async Task Multi_process_an_open_transaction_conflicts_with_a_commit_of_the_other_process()
    {
        // Two engines on the same files stand in for two processes.
        var path = DbPath("multi-process");
        var config = PageFileConfig.Default with { AllowMultiProcessAccess = true };
        using var a = new StorageEngine(path, config);
        using var b = new StorageEngine(path, config);
        var page = a.AllocatePage();
        await CommitPageAsync(a, page, 0x01);

        using var ta = a.BeginTransaction();
        Assert.Equal(0x01, ReadPage(a, page, ta.TransactionId));
        await CommitPageAsync(b, page, 0x02);           // B commits the page A's transaction read
        a.WritePage(page, ta.TransactionId, Filled(a.PageSize, 0x03));

        await Assert.ThrowsAsync<BLiteTransactionConflictException>(async () => await ta.CommitAsync());
        Assert.Equal(TransactionState.Aborted, ta.State);
        using (var check = b.BeginTransaction())
            Assert.Equal(0x02, ReadPage(b, page, check.TransactionId));

        // Run again: it reads B's commit first and goes through.
        using (var retry = a.BeginTransaction())
        {
            Assert.Equal(0x02, ReadPage(a, page, retry.TransactionId));
            a.WritePage(page, retry.TransactionId, Filled(a.PageSize, 0x03));
            await retry.CommitAsync();
        }
        using (var check = b.BeginTransaction())
            Assert.Equal(0x03, ReadPage(b, page, check.TransactionId));
    }

    [Fact]
    public async Task Collection_per_file_mode_shares_one_write_lock_per_collection()
    {
        var path = DbPath("per-file");
        using var engine = new BLiteEngine(path, PageFileConfig.Server(path) with { LockTimeout = LockTimeout.From(30_000, 30_000) });
        Assert.Same(engine.Storage.GetCollectionWriteLock("orders"), engine.Storage.GetCollectionWriteLock("ORDERS"));
        Assert.NotSame(engine.Storage.GetCollectionWriteLock("orders"), engine.Storage.GetCollectionWriteLock("events"));

        // Every session builds its own DynamicCollection: with a lock per instance their
        // auto-commit inserts ran concurrently on the same pages and were lost.
        engine.RegisterKeys(["n"]);
        for (int run = 0; run < 5; run++)
        {
            await Task.WhenAll(Enumerable.Range(0, 10).Select(async i =>
            {
                using var session = engine.OpenSession();
                await session.InsertAsync("orders", engine.CreateDocument(["n"], x => x.AddInt32("n", i)));
            }));
        }
        Assert.Equal(50, await engine.GetOrCreateCollection("orders").CountAsync());

        // Open transactions on one collection still never both commit over each other.
        var col = engine.GetOrCreateCollection("orders");
        var t1 = engine.BeginTransaction();
        var t2 = engine.BeginTransaction();
        var a = await col.InsertAsync(engine.CreateDocument(["n"], x => x.AddInt32("n", 100)), t1);
        var b = await col.InsertAsync(engine.CreateDocument(["n"], x => x.AddInt32("n", 200)), t2);
        await t1.CommitAsync();
        bool conflict = await CommitSecondAsync(t2);
        Assert.NotNull(await col.FindByIdAsync(a));
        Assert.Equal(!conflict, await col.FindByIdAsync(b) is not null);
        Assert.Equal(conflict ? 51 : 52, await col.CountAsync());
        t1.Dispose(); t2.Dispose();
    }

    [Fact]
    public async Task Prepare_votes_no_for_a_transaction_whose_commit_would_be_rejected()
    {
        using var db = new TestDbContext(DbPath("prepare"));
        await db.Users.InsertAsync(new User { Name = "seed" });

        var t1 = db.BeginTransaction();
        var t2 = db.BeginTransaction();
        await db.Users.InsertAsync(new User { Name = "from-t1" }, t1);
        await db.Users.InsertAsync(new User { Name = "from-t2" }, t2);
        await t1.CommitAsync();

        // Same data page and primary leaf as in A_rejected_transaction_leaves_no_trace_and_can_be_retried.
        Assert.False(await t2.PrepareAsync());
        await t2.RollbackAsync();
        Assert.Equal(2, await db.Users.CountAsync());
        t1.Dispose(); t2.Dispose();
    }

    [Fact]
    public async Task A_conflict_in_the_engine_commit_overload_leaves_nothing_behind()
    {
        using var db = new TestDbContext(DbPath("engine-commit"));
        await db.Users.InsertAsync(new User { Name = "seed" });
        var storage = db.Storage;

        // Committed through StorageEngine.CommitTransactionAsync(Transaction), not Transaction.CommitAsync.
        var t1 = (Transaction)db.BeginTransaction();
        var t2 = (Transaction)db.BeginTransaction();
        await db.Users.InsertAsync(new User { Name = "from-t1" }, t1);
        var loser = new User { Name = "from-t2" };
        await db.Users.InsertAsync(loser, t2);
        await storage.CommitTransactionAsync(t1);

        await Assert.ThrowsAsync<BLiteTransactionConflictException>(() => storage.CommitTransactionAsync(t2));
        Assert.Equal(TransactionState.Aborted, t2.State);
        Assert.Equal(0, storage.TrackedTransactionCount);   // no base versions left
        Assert.Equal(0, storage.ActiveTransactionCount);    // no uncommitted pages left
        Assert.Null(await db.Users.FindByIdAsync(loser.Id));

        await db.Users.InsertAsync(new User { Name = "after" });
        Assert.Equal(3, await db.Users.CountAsync());
    }

    private static async Task CommitPageAsync(StorageEngine engine, uint pageId, byte value)
    {
        using var tx = engine.BeginTransaction();
        engine.WritePage(pageId, tx.TransactionId, Filled(engine.PageSize, value));
        await tx.CommitAsync();
    }

    private static byte ReadPage(StorageEngine engine, uint pageId, ulong transactionId)
    {
        var buffer = new byte[engine.PageSize];
        engine.ReadPage(pageId, transactionId, buffer);
        return buffer[0];
    }

    private static byte[] Filled(int size, byte value)
    {
        var data = new byte[size];
        Array.Fill(data, value);
        return data;
    }
}
