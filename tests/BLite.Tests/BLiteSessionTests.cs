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
    public void BeginTransaction_TwoSessions_HaveIndependentTransactionIds()
    {
        using var s1 = _engine.OpenSession();
        using var s2 = _engine.OpenSession();

        var t1 = s1.BeginTransaction();
        var t2 = s2.BeginTransaction();

        Assert.NotEqual(t1.TransactionId, t2.TransactionId);
    }

    [Fact]
    public void CommittingOneSession_DoesNotAffectOtherSessionsTransaction()
    {
        using var s1 = _engine.OpenSession();
        using var s2 = _engine.OpenSession();

        s1.BeginTransaction();
        s2.BeginTransaction();

        // Commit session 1 — session 2's transaction must remain active
        s1.Commit();

        Assert.NotNull(s2.CurrentTransaction);
    }

    [Fact]
    public void RollingBackOneSession_DoesNotAffectOtherSessions()
    {
        using var s1 = _engine.OpenSession();
        using var s2 = _engine.OpenSession();

        s1.BeginTransaction();
        var t2 = s2.BeginTransaction();

        s1.Rollback();

        Assert.Equal(t2.TransactionId, s2.CurrentTransaction!.TransactionId);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. CRUD via sessions — Insert / FindById / Update / Delete
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void Insert_AndFindById_RoundTrip()
    {
        using var session = _engine.OpenSession();

        var doc = _engine.CreateDocument(["name", "value"], b => b
            .AddString("name", "Alice")
            .AddInt32("value", 42));

        var id = session.Insert("users", doc);
        var found = session.FindById("users", id);

        Assert.NotNull(found);
    }

    [Fact]
    public void Update_PersistsChange()
    {
        using var session = _engine.OpenSession();

        var doc = _engine.CreateDocument(["name"], b => b.AddString("name", "Bob"));
        var id = session.Insert("persons", doc);

        var updated = _engine.CreateDocument(["name"], b => b.AddString("name", "Robert"));
        var result = session.Update("persons", id, updated);

        Assert.True(result);
    }

    [Fact]
    public void Delete_RemovesDocument()
    {
        using var session = _engine.OpenSession();

        var doc = _engine.CreateDocument(["tag"], b => b.AddString("tag", "temp"));
        var id = session.Insert("tags", doc);

        var deleted = session.Delete("tags", id);
        Assert.True(deleted);

        var found = session.FindById("tags", id);
        Assert.Null(found);
    }

    [Fact]
    public void FindAll_ReturnsInsertedDocuments()
    {
        using var session = _engine.OpenSession();

        session.Insert("items", _engine.CreateDocument(["x"], b => b.AddInt32("x", 1)));
        session.Insert("items", _engine.CreateDocument(["x"], b => b.AddInt32("x", 2)));
        session.Insert("items", _engine.CreateDocument(["x"], b => b.AddInt32("x", 3)));

        var all = session.FindAll("items").ToList();
        Assert.Equal(3, all.Count);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. Multiple concurrent sessions on the same collection
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void TwoSessions_WriteToSameCollection_BothCommit()
    {
        using var s1 = _engine.OpenSession();
        using var s2 = _engine.OpenSession();

        var id1 = s1.Insert("messages", _engine.CreateDocument(["author"], b => b.AddString("author", "Alice")));
        var id2 = s2.Insert("messages", _engine.CreateDocument(["author"], b => b.AddString("author", "Bob")));

        // Both sessions should have committed successfully
        Assert.False(id1.IsEmpty);
        Assert.False(id2.IsEmpty);
        Assert.NotEqual(id1, id2);
    }

    [Fact]
    public void WrittenBySessionA_VisibleToSessionBAfterCommit()
    {
        using var s1 = _engine.OpenSession();

        var id = s1.Insert("notes", _engine.CreateDocument(["msg"], b => b.AddString("msg", "hello")));

        // Open s2 after s1 has already committed
        using var s2 = _engine.OpenSession();
        var found = s2.FindById("notes", id);

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
    public void ManualTransaction_RollbackPreventsVisibility()
    {
        using var session = _engine.OpenSession();
        using var reader = _engine.OpenSession();

        var col = session.GetOrCreateCollection("kv");
        session.BeginTransaction();
        var doc = _engine.CreateDocument(["k"], b => b.AddInt32("k", 99));
        var id = col.Insert(doc);
        session.Rollback();

        // After rollback the document must not be visible from another session
        var found = reader.FindById("kv", id);
        Assert.Null(found);
    }

    [Fact]
    public void ManualTransaction_CommitMakesChangesVisible()
    {
        using var writer = _engine.OpenSession();
        using var reader = _engine.OpenSession();

        var col = writer.GetOrCreateCollection("shared");
        writer.BeginTransaction();
        var doc = _engine.CreateDocument(["v"], b => b.AddInt32("v", 7));
        var id = col.Insert(doc);
        writer.Commit();

        var found = reader.FindById("shared", id);
        Assert.NotNull(found);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 6. Bulk operations
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void InsertBulk_AndFindAll_ReturnCorrectCount()
    {
        using var session = _engine.OpenSession();

        var docs = Enumerable.Range(0, 20)
            .Select(i => _engine.CreateDocument(["i"], b => b.AddInt32("i", i)))
            .ToList();

        var ids = session.InsertBulk("bulk_coll", docs);

        Assert.Equal(20, ids.Count);
        Assert.Equal(20, session.FindAll("bulk_coll").Count());
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
    public void ServerModeEngine_SessionInsertAndRead_Works()
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

            var id1 = s1.Insert("events",
                engine.CreateDocument(["payload"], b => b.AddString("payload", "event-1")));
            var id2 = s2.Insert("events",
                engine.CreateDocument(["payload"], b => b.AddString("payload", "event-2")));

            var found1 = s2.FindById("events", id1);
            var found2 = s1.FindById("events", id2);

            Assert.NotNull(found1);
            Assert.NotNull(found2);
        }
        finally
        {
            try { System.IO.Directory.Delete(dir, recursive: true); } catch { }
        }
    }
}
