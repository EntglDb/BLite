using BLite.Bson;
using BLite.Core;
using BLite.Core.Transactions;

namespace BLite.Tests;

/// <summary>
/// Additional tests for <see cref="BLiteEngine"/> targeting mutation survivors not yet
/// covered by the existing BLiteEngineTests: disposal guards, metadata accessors,
/// key-dictionary methods, transaction edge cases, and Checkpoint.
/// </summary>
public class BLiteEngineAdditionalTests : IDisposable
{
    private readonly string _dbPath;
    private BLiteEngine _engine;

    public BLiteEngineAdditionalTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"eng_add_{Guid.NewGuid()}.db");
        _engine = new BLiteEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();  // idempotent
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ─── Constructor validation ───────────────────────────────────────────────

    [Fact]
    public void Constructor_EmptyPath_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new BLiteEngine(""));
    }

    [Fact]
    public void Constructor_WhitespacePath_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new BLiteEngine("   "));
    }

    // ─── ThrowIfDisposed guards ───────────────────────────────────────────────

    [Fact]
    public void AfterDispose_GetOrCreateCollection_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.GetOrCreateCollection("x"));
    }

    [Fact]
    public void AfterDispose_GetCollection_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.GetCollection("x"));
    }

    [Fact]
    public void AfterDispose_ListCollections_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.ListCollections());
    }

    [Fact]
    public void AfterDispose_DropCollection_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.DropCollection("x"));
    }

    [Fact]
    public void AfterDispose_Insert_Throws()
    {
        var doc = _engine.CreateDocument(["name"], b => b.AddString("name", "x"));
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.Insert("col", doc));
    }

    [Fact]
    public void AfterDispose_Commit_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.Commit());
    }

    [Fact]
    public void AfterDispose_RegisterKeys_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.RegisterKeys(["x"]));
    }

    [Fact]
    public void AfterDispose_GetKeyMap_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.GetKeyMap());
    }

    [Fact]
    public void AfterDispose_GetKeyReverseMap_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.GetKeyReverseMap());
    }

    [Fact]
    public void AfterDispose_Checkpoint_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.Checkpoint());
    }

    [Fact]
    public void AfterDispose_BeginTransaction_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.BeginTransaction());
    }

    [Fact]
    public void AfterDispose_GetCollectionMetadata_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.GetCollectionMetadata("x"));
    }

    [Fact]
    public void AfterDispose_GetAllCollectionsMetadata_Throws()
    {
        _engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _engine.GetAllCollectionsMetadata());
    }

    [Fact]
    public void Dispose_CalledTwice_DoesNotThrow()
    {
        _engine.Dispose();
        _engine.Dispose(); // idempotent — must not throw
    }

    // ─── Key-dictionary management ────────────────────────────────────────────

    [Fact]
    public void RegisterKeys_AddsKeys_GetKeyMap_ContainsThem()
    {
        _engine.RegisterKeys(["alpha", "beta", "gamma"]);
        var map = _engine.GetKeyMap();
        Assert.True(map.ContainsKey("alpha"));
        Assert.True(map.ContainsKey("beta"));
        Assert.True(map.ContainsKey("gamma"));
    }

    [Fact]
    public void RegisterKeys_SameKeyTwice_NoError()
    {
        _engine.RegisterKeys(["dup"]);
        _engine.RegisterKeys(["dup"]); // second registration must be idempotent
        var map = _engine.GetKeyMap();
        Assert.True(map.ContainsKey("dup"));
    }

    [Fact]
    public void GetKeyReverseMap_ContainsRegisteredKeys()
    {
        _engine.RegisterKeys(["rev1", "rev2"]);
        var map = _engine.GetKeyMap();
        var rev = _engine.GetKeyReverseMap();

        // For every forward entry, the reverse map must have the corresponding name
        foreach (var kv in map)
        {
            Assert.True(rev.ContainsKey(kv.Value));
            Assert.Equal(kv.Key, rev[kv.Value]);
        }
    }

    [Fact]
    public void GetKeyMap_ReturnsNonNullDictionary()
    {
        var map = _engine.GetKeyMap();
        Assert.NotNull(map);
    }

    // ─── CollectionMetadata ───────────────────────────────────────────────────

    [Fact]
    public void GetCollectionMetadata_NonExistent_ReturnsNull()
    {
        Assert.Null(_engine.GetCollectionMetadata("ghost"));
    }

    [Fact]
    public void GetCollectionMetadata_AfterCreateAndCommit_ReturnsMetadata()
    {
        _engine.GetOrCreateCollection("meta_col");
        var doc = _engine.CreateDocument(["name"], b => b.AddString("name", "x"));
        _engine.Insert("meta_col", doc); // also commits

        var meta = _engine.GetCollectionMetadata("meta_col");
        Assert.NotNull(meta);
        Assert.Equal("meta_col", meta!.Name, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void GetAllCollectionsMetadata_ReturnsEntryForEachCommittedCollection()
    {
        _engine.GetOrCreateCollection("a_col");
        _engine.GetOrCreateCollection("b_col");
        var doc = _engine.CreateDocument(["v"], b => b.AddInt32("v", 1));
        _engine.Insert("a_col", doc);
        var doc2 = _engine.CreateDocument(["v"], b => b.AddInt32("v", 2));
        _engine.Insert("b_col", doc2);

        var all = _engine.GetAllCollectionsMetadata();
        var names = all.Select(m => m.Name).ToList();
        Assert.Contains("a_col", names, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("b_col", names, StringComparer.OrdinalIgnoreCase);
    }

    // ─── Transaction edge cases ───────────────────────────────────────────────

    [Fact]
    public void CurrentTransaction_InitiallyNull()
    {
        Assert.Null(_engine.CurrentTransaction);
    }

    [Fact]
    public void BeginTransaction_ReturnsActiveTransaction()
    {
        var txn = _engine.BeginTransaction();
        Assert.NotNull(txn);
        Assert.Equal(TransactionState.Active, txn.State);
        _engine.Rollback();
    }

    [Fact]
    public void BeginTransaction_WhenAlreadyActive_ReturnsSameTransaction()
    {
        var txn1 = _engine.BeginTransaction();
        var txn2 = _engine.BeginTransaction();
        Assert.Same(txn1, txn2);
        _engine.Rollback();
    }

    [Fact]
    public void CurrentTransaction_AfterCommit_IsNull()
    {
        _engine.BeginTransaction();
        _engine.Commit();
        Assert.Null(_engine.CurrentTransaction);
    }

    [Fact]
    public void CurrentTransaction_AfterRollback_IsNull()
    {
        _engine.BeginTransaction();
        _engine.Rollback();
        Assert.Null(_engine.CurrentTransaction);
    }

    [Fact]
    public void Commit_WithNoActiveTransaction_DoesNotThrow()
    {
        // No transaction started — Commit should be a silent no-op
        _engine.Commit(); // must not throw
    }

    [Fact]
    public void Rollback_WithNoActiveTransaction_DoesNotThrow()
    {
        // No transaction started — Rollback should be a silent no-op
        _engine.Rollback(); // must not throw
    }

    [Fact]
    public async Task CommitAsync_WithNoActiveTransaction_DoesNotThrow()
    {
        await _engine.CommitAsync(); // must not throw
    }

    [Fact]
    public async Task BeginTransactionAsync_ReturnsActiveTransaction()
    {
        var txn = await _engine.BeginTransactionAsync();
        Assert.NotNull(txn);
        Assert.Equal(TransactionState.Active, txn.State);
        _engine.Rollback();
    }

    [Fact]
    public async Task BeginTransactionAsync_WhenAlreadyActive_ReturnsSameTransaction()
    {
        var txn1 = await _engine.BeginTransactionAsync();
        var txn2 = await _engine.BeginTransactionAsync();
        Assert.Same(txn1, txn2);
        _engine.Rollback();
    }

    // ─── Checkpoint ───────────────────────────────────────────────────────────

    [Fact]
    public void Checkpoint_DoesNotThrow_AndDataRemainsIntact()
    {
        var col = _engine.GetOrCreateCollection("chk_col");
        var doc = col.CreateDocument(["_id", "name"], b =>
            b.AddId((BsonId)1).AddString("name", "BeforeCheckpoint"));
        var id = col.Insert(doc);
        _engine.Commit();

        _engine.Checkpoint(); // must not throw

        var found = col.FindById(id);
        Assert.NotNull(found);
        Assert.True(found!.TryGetString("name", out var name));
        Assert.Equal("BeforeCheckpoint", name);
    }

    // ─── CreateDocument convenience ───────────────────────────────────────────

    [Fact]
    public void CreateDocument_RegistersKeysAndBuildsDocument()
    {
        var doc = _engine.CreateDocument(
            ["fieldA", "fieldB"],
            b => b.AddString("fieldA", "hello").AddInt32("fieldB", 42));

        Assert.True(doc.TryGetString("fielda", out var a));
        Assert.Equal("hello", a);
        Assert.True(doc.TryGetInt32("fieldb", out var b));
        Assert.Equal(42, b);

        // Keys must now be in the map
        var map = _engine.GetKeyMap();
        Assert.True(map.ContainsKey("fielda") || map.ContainsKey("fieldA")); // case-insensitive storage
    }
}
