using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Tests for <see cref="BLiteSession"/> — the per-client session abstraction that
/// enables concurrent, independent transaction contexts on a shared <see cref="BLiteEngine"/>.
/// </summary>
public class BLiteSessionTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;

    public BLiteSessionTests()
    {
        _dbPath = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"blite_session_{Guid.NewGuid()}.db");
        _engine = new BLiteEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        TryDelete(_dbPath);
        TryDelete(System.IO.Path.ChangeExtension(_dbPath, ".wal"));
    }

    private static void TryDelete(string path)
    {
        try { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); } catch { }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 1. Basic session lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void OpenSession_ReturnsNewSession()
    {
        using var session = _engine.OpenSession();
        Assert.NotNull(session);
    }

    [Fact]
    public void OpenSession_ReturnsIndependentInstances()
    {
        using var s1 = _engine.OpenSession();
        using var s2 = _engine.OpenSession();
        Assert.NotSame(s1, s2);
    }

    [Fact]
    public void Dispose_RollsBackActiveTransaction()
    {
        var session = _engine.OpenSession();
        var txn = session.BeginTransaction();
        Assert.NotNull(txn);

        // Dispose without committing → should roll back
        session.Dispose();

        // Subsequent call should throw ObjectDisposedException
        Assert.Throws<ObjectDisposedException>(() => session.BeginTransaction());
    }

    [Fact]
    public void Dispose_CalledTwice_DoesNotThrow()
    {
        var session = _engine.OpenSession();
        session.Dispose();
        session.Dispose(); // second Dispose should be a no-op
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 2. Per-session transaction isolation
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task BeginTransaction_TwoSessions_HaveIndependentTransactionIds()
    {
        using var s1 = _engine.OpenSession();
        using var s2 = _engine.OpenSession();

        var t1 = await s1.BeginTransactionAsync();
        var t2 = await s2.BeginTransactionAsync();

        Assert.NotEqual(t1.TransactionId, t2.TransactionId);
    }

    [Fact]
    public async Task CommittingOneSession_DoesNotAffectOtherSessionsTransaction()
    {
        using var s1 = _engine.OpenSession();
        using var s2 = _engine.OpenSession();

        await s1.BeginTransactionAsync();
        await s2.BeginTransactionAsync();

        // Commit session 1 — session 2's transaction must remain active
        await s1.CommitAsync();
        Assert.NotNull(s2.CurrentTransaction);
    }

    [Fact]
    public async Task RollingBackOneSession_DoesNotAffectOtherSessions()
    {
        using var s1 = _engine.OpenSession();
        using var s2 = _engine.OpenSession();

        await s1.BeginTransactionAsync();
        var t2 = await s2.BeginTransactionAsync();

        s1.Rollback();

        Assert.Equal(t2.TransactionId, s2.CurrentTransaction!.TransactionId);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. CRUD via sessions — Insert / FindById / UpdateAsync / Delete
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Insert_AndFindById_RoundTrip()
    {
        using var session = _engine.OpenSession();

        var doc = _engine.CreateDocument(["name", "value"], b => b
            .AddString("name", "Alice")
            .AddInt32("value", 42));

        var id = await session.InsertAsync("users", doc);
        var found = await session.FindByIdAsync("users", id);

        Assert.NotNull(found);
    }

    [Fact]
    public async Task Update_PersistsChange()
    {
        using var session = _engine.OpenSession();

        var doc = _engine.CreateDocument(["name"], b => b.AddString("name", "Bob"));
        var id = await session.InsertAsync("persons", doc);

        var updated = _engine.CreateDocument(["name"], b => b.AddString("name", "Robert"));
        var result = await session.UpdateAsync("persons", id, updated);

        Assert.True(result);
    }

    [Fact]
    public async Task Delete_RemovesDocument()
    {
        using var session = _engine.OpenSession();

        var doc = _engine.CreateDocument(["tag"], b => b.AddString("tag", "temp"));
        var id = await session.InsertAsync("tags", doc);

        var deleted = await session.DeleteAsync("tags", id);
        Assert.True(deleted);

        var found = await session.FindByIdAsync("tags", id);
        Assert.Null(found);
    }

    [Fact]
    public async Task FindAll_ReturnsInsertedDocuments()
    {
        using var session = _engine.OpenSession();

        await session.InsertAsync("items", _engine.CreateDocument(["x"], b => b.AddInt32("x", 1)));
        await session.InsertAsync("items", _engine.CreateDocument(["x"], b => b.AddInt32("x", 2)));
        await session.InsertAsync("items", _engine.CreateDocument(["x"], b => b.AddInt32("x", 3)));

        var all = (await session.FindAllAsync("items").ToListAsync());
        Assert.Equal(3, all.Count);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. Multiple concurrent sessions on the same collection
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TwoSessions_WriteToSameCollection_BothCommit()
    {
        using var s1 = _engine.OpenSession();
        using var s2 = _engine.OpenSession();

        var id1 = await s1.InsertAsync("messages", _engine.CreateDocument(["author"], b => b.AddString("author", "Alice")));
        var id2 = await s2.InsertAsync("messages", _engine.CreateDocument(["author"], b => b.AddString("author", "Bob")));

        // Both sessions should have committed successfully
        Assert.False(id1.IsEmpty);
        Assert.False(id2.IsEmpty);
        Assert.NotEqual(id1, id2);
    }

    [Fact]
    public async Task WrittenBySessionA_VisibleToSessionBAfterCommit()
    {
        using var s1 = _engine.OpenSession();

        var id = await s1.InsertAsync("notes", _engine.CreateDocument(["msg"], b => b.AddString("msg", "hello")));
        // Open s2 after s1 has already committed
        using var s2 = _engine.OpenSession();
        var found = await s2.FindByIdAsync("notes", id);

        Assert.NotNull(found);
    }

    [Fact]
    public async Task ConcurrentAsyncSessions_AllInsertSuccessfully()
    {
        const int sessionCount = 10;
        const string coll = "concurrent";

        // Pre-register the key before spawning concurrent tasks to avoid any
        // dictionary-growth races in the global C-BSON key map.
        _engine.RegisterKeys(["n"]);

        var tasks = Enumerable.Range(0, sessionCount).Select(async i =>
        {
            using var session = _engine.OpenSession();
            var doc = _engine.CreateDocument(["n"], b => b.AddInt32("n", i));
            return await session.InsertAsync(coll, doc);
        });

        var ids = await Task.WhenAll(tasks);

        // All IDs must be distinct
        Assert.Equal(sessionCount, ids.Distinct().Count());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 5. Manual transaction scoping across operations
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ManualTransaction_RollbackPreventsVisibility()
    {
        using var session = _engine.OpenSession();
        using var reader = _engine.OpenSession();

        var col = session.GetOrCreateCollection("kv");
        await session.BeginTransactionAsync();
        var doc = _engine.CreateDocument(["k"], b => b.AddInt32("k", 99));
        var id = await col.InsertAsync(doc);
        session.Rollback();

        // After rollback the document must not be visible from another session
        var found = await reader.FindByIdAsync("kv", id);
        Assert.Null(found);
    }

    [Fact]
    public async Task ManualTransaction_CommitMakesChangesVisible()
    {
        using var writer = _engine.OpenSession();
        using var reader = _engine.OpenSession();

        var col = writer.GetOrCreateCollection("shared");
        await writer.BeginTransactionAsync();
        var doc = _engine.CreateDocument(["v"], b => b.AddInt32("v", 7));
        var id = await col.InsertAsync(doc);
        await writer.CommitAsync();

        var found = await reader.FindByIdAsync("shared", id);
        Assert.NotNull(found);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 6. Bulk operations
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task InsertBulk_AndFindAll_ReturnCorrectCount()
    {
        using var session = _engine.OpenSession();

        var docs = Enumerable.Range(0, 20)
            .Select(i => _engine.CreateDocument(["i"], b => b.AddInt32("i", i)))
            .ToList();

        var ids = await session.InsertBulkAsync("bulk_coll", docs);

        Assert.Equal(20, ids.Count);
        Assert.Equal(20, (await session.FindAllAsync("bulk_coll").ToListAsync()).Count);
    }

    [Fact]
    public async Task InsertBulkAsync_InsertsAllDocuments()
    {
        using var session = _engine.OpenSession();

        var docs = Enumerable.Range(0, 5)
            .Select(i => _engine.CreateDocument(["z"], b => b.AddInt32("z", i)))
            .ToList();

        var ids = await session.InsertBulkAsync("async_bulk", docs);

        Assert.Equal(5, ids.Count);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 7. Session with Server-mode (PageFileConfig.Server) engine
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ServerModeEngine_SessionInsertAndRead_Works()
    {
        var dir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"blite_srv_{Guid.NewGuid()}");
        System.IO.Directory.CreateDirectory(dir);
        var dbPath = System.IO.Path.Combine(dir, "srv.db");

        try
        {
            var config = PageFileConfig.Server(dbPath);
            using var engine = new BLiteEngine(dbPath, config);

            using var s1 = engine.OpenSession();
            using var s2 = engine.OpenSession();

            var id1 = await s1.InsertAsync("events",
                engine.CreateDocument(["payload"], b => b.AddString("payload", "event-1")));
            var id2 = await s2.InsertAsync("events",
                engine.CreateDocument(["payload"], b => b.AddString("payload", "event-2")));

            var found1 = await s2.FindByIdAsync("events", id1);
            var found2 = await s1.FindByIdAsync("events", id2);

            Assert.NotNull(found1);
            Assert.NotNull(found2);
        }
        finally
        {
            try { System.IO.Directory.Delete(dir, recursive: true); } catch { }
        }
    }
}
