using BLite.Bson;
using BLite.Core;
using BLite.Core.Metrics;

namespace BLite.Tests;

/// <summary>
/// Integration tests for the BLite metrics subsystem.
/// </summary>
public class MetricsTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;

    public MetricsTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_metrics_{Guid.NewGuid():N}.db");
        _engine = new BLiteEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ── Opt-in behaviour ─────────────────────────────────────────────────────

    [Fact]
    public void GetMetrics_BeforeEnable_ReturnsNull()
    {
        // Metrics subsystem is opt-in; no snapshot before EnableMetrics().
        Assert.Null(_engine.GetMetrics());
    }

    [Fact]
    public void EnableMetrics_Idempotent()
    {
        _engine.EnableMetrics();
        _engine.EnableMetrics(); // must not throw
        Assert.NotNull(_engine.GetMetrics());
    }

    // ── Transaction counters ─────────────────────────────────────────────────

    [Fact]
    public async Task Metrics_TransactionCommit_IsRecorded()
    {
        _engine.EnableMetrics();

        var col = _engine.GetOrCreateCollection("items");
        var doc = col.CreateDocument(["name"], b => b.AddString("name", "alpha"));
        await _engine.InsertAsync("items", doc);

        // Give the background aggregator time to drain the channel.
        await Task.Delay(100);

        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.True(snap!.TransactionCommitsTotal >= 1,
            $"Expected at least 1 commit, got {snap.TransactionCommitsTotal}");
        Assert.True(snap.TransactionBeginsTotal >= 1,
            $"Expected at least 1 begin, got {snap.TransactionBeginsTotal}");
    }

    [Fact]
    public async Task Metrics_TransactionRollback_IsRecorded()
    {
        _engine.EnableMetrics();

        var col = _engine.GetOrCreateCollection("items");
        var doc = col.CreateDocument(["name"], b => b.AddString("name", "beta"));

        using (var txn = await _engine.BeginTransactionAsync())
        {
            await col.InsertAsync(doc);
            // Rollback without committing.
            await txn.RollbackAsync();
        }

        await Task.Delay(100);

        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.True(snap!.TransactionRollbacksTotal >= 1,
            $"Expected at least 1 rollback, got {snap.TransactionRollbacksTotal}");
    }

    [Fact]
    public async Task Metrics_AvgCommitLatency_IsNonNegative()
    {
        _engine.EnableMetrics();

        var col = _engine.GetOrCreateCollection("items");
        var doc = col.CreateDocument(["name"], b => b.AddString("name", "gamma"));
        await _engine.InsertAsync("items", doc);

        await Task.Delay(100);

        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.True(snap!.AvgCommitLatencyUs >= 0,
            $"Avg commit latency must be non-negative, got {snap.AvgCommitLatencyUs}");
    }

    // ── Collection-level counters ────────────────────────────────────────────

    [Fact]
    public async Task Metrics_CollectionInsert_IsRecorded()
    {
        _engine.EnableMetrics();

        var col = _engine.GetOrCreateCollection("users");
        var doc = col.CreateDocument(["name"], b => b.AddString("name", "Alice"));
        await _engine.InsertAsync("users", doc);

        await Task.Delay(100);

        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.True(snap!.InsertsTotal >= 1, $"Expected inserts >= 1, got {snap.InsertsTotal}");

        Assert.True(snap.Collections.TryGetValue("users", out var colSnap),
            "Expected per-collection stats for 'users'");
        Assert.True(colSnap.InsertCount >= 1,
            $"Expected at least 1 insert for 'users', got {colSnap.InsertCount}");
    }

    [Fact]
    public async Task Metrics_CollectionUpdate_IsRecorded()
    {
        _engine.EnableMetrics();

        var col = _engine.GetOrCreateCollection("users");
        var doc = col.CreateDocument(["name"], b => b.AddString("name", "Bob"));
        var id = await _engine.InsertAsync("users", doc);

        var updated = col.CreateDocument(["name"], b => b.AddString("name", "Bob Updated"));
        await _engine.UpdateAsync("users", id, updated);

        await Task.Delay(100);

        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.True(snap!.UpdatesTotal >= 1, $"Expected updates >= 1, got {snap.UpdatesTotal}");

        Assert.True(snap.Collections.TryGetValue("users", out var colSnap));
        Assert.True(colSnap.UpdateCount >= 1);
    }

    [Fact]
    public async Task Metrics_CollectionDelete_IsRecorded()
    {
        _engine.EnableMetrics();

        var col = _engine.GetOrCreateCollection("users");
        var doc = col.CreateDocument(["name"], b => b.AddString("name", "Charlie"));
        var id = await _engine.InsertAsync("users", doc);

        await _engine.DeleteAsync("users", id);

        await Task.Delay(100);

        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.True(snap!.DeletesTotal >= 1, $"Expected deletes >= 1, got {snap.DeletesTotal}");

        Assert.True(snap.Collections.TryGetValue("users", out var colSnap));
        Assert.True(colSnap.DeleteCount >= 1);
    }

    // ── GroupCommit batch counter ────────────────────────────────────────────

    [Fact]
    public async Task Metrics_GroupCommitBatch_IsRecorded()
    {
        _engine.EnableMetrics();

        var col = _engine.GetOrCreateCollection("batch");
        for (int i = 0; i < 5; i++)
        {
            var doc = col.CreateDocument(["val"], b => b.AddInt32("val", i));
            await _engine.InsertAsync("batch", doc);
        }

        await Task.Delay(100);

        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.True(snap!.GroupCommitBatchesTotal >= 1,
            $"Expected at least 1 group-commit batch, got {snap.GroupCommitBatchesTotal}");
    }

    // ── WatchMetrics observable ───────────────────────────────────────────────

    [Fact]
    public async Task WatchMetrics_EmitsSnapshots()
    {
        _engine.EnableMetrics();

        var snapshots = new List<MetricsSnapshot>();
        using var sub = _engine.WatchMetrics(TimeSpan.FromMilliseconds(50))
            .Subscribe(s => snapshots.Add(s));

        await Task.Delay(250);

        Assert.True(snapshots.Count >= 2,
            $"Expected at least 2 snapshots in 250 ms with 50 ms interval, got {snapshots.Count}");
        Assert.All(snapshots, s => Assert.NotNull(s));
    }

    [Fact]
    public async Task WatchMetrics_EnablesMetricsAutomatically()
    {
        // WatchMetrics enables the subsystem even if EnableMetrics was never called.
        Assert.Null(_engine.GetMetrics());

        using var sub = _engine.WatchMetrics(TimeSpan.FromMilliseconds(50))
            .Subscribe(_ => { });

        await Task.Delay(100);

        Assert.NotNull(_engine.GetMetrics());
    }

    // ── SnapshotTimestamp ────────────────────────────────────────────────────

    [Fact]
    public void Snapshot_HasRecentTimestamp()
    {
        _engine.EnableMetrics();
        var before = DateTimeOffset.UtcNow.AddSeconds(-1);
        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.True(snap!.SnapshotTimestamp >= before,
            $"Snapshot timestamp {snap.SnapshotTimestamp} is earlier than {before}");
    }

    // ── Multiple collections ──────────────────────────────────────────────────

    [Fact]
    public async Task Metrics_PerCollection_TracksSeparately()
    {
        _engine.EnableMetrics();

        var colA = _engine.GetOrCreateCollection("colA");
        var colB = _engine.GetOrCreateCollection("colB");

        for (int i = 0; i < 3; i++)
        {
            var dA = colA.CreateDocument(["v"], b => b.AddInt32("v", i));
            await _engine.InsertAsync("colA", dA);
        }
        for (int i = 0; i < 7; i++)
        {
            var dB = colB.CreateDocument(["v"], b => b.AddInt32("v", i));
            await _engine.InsertAsync("colB", dB);
        }

        await Task.Delay(100);

        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.True(snap!.Collections.TryGetValue("colA", out var snapA));
        Assert.True(snap.Collections.TryGetValue("colB", out var snapB));
        Assert.True(snapA.InsertCount >= 3, $"colA inserts: {snapA.InsertCount}");
        Assert.True(snapB.InsertCount >= 7, $"colB inserts: {snapB.InsertCount}");
    }
}

/// <summary>
/// Integration tests for metrics through <see cref="DocumentDbContext"/>.
/// </summary>
public class MetricsViaDocumentDbContextTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public MetricsViaDocumentDbContextTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_metrics_ctx_{Guid.NewGuid():N}.db");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    [Fact]
    public void DbContext_GetMetrics_BeforeEnable_ReturnsNull()
    {
        Assert.Null(_db.GetMetrics());
    }

    [Fact]
    public void DbContext_EnableMetrics_Idempotent()
    {
        _db.EnableMetrics();
        _db.EnableMetrics(); // must not throw
        Assert.NotNull(_db.GetMetrics());
    }

    [Fact]
    public async Task DbContext_Metrics_RecordsTransactionCommit()
    {
        _db.EnableMetrics();

        var person = new Person { Id = 1, Name = "Metric User", Age = 30 };
        await _db.People.InsertAsync(person);
        await _db.SaveChangesAsync();

        await Task.Delay(100);

        var snap = _db.GetMetrics();
        Assert.NotNull(snap);
        Assert.True(snap!.TransactionCommitsTotal >= 1,
            $"Expected at least 1 commit, got {snap.TransactionCommitsTotal}");
    }

    [Fact]
    public async Task DbContext_WatchMetrics_EmitsSnapshots()
    {
        var snapshots = new List<MetricsSnapshot>();
        using var sub = _db.WatchMetrics(TimeSpan.FromMilliseconds(50))
            .Subscribe(s => snapshots.Add(s));

        await Task.Delay(250);

        Assert.True(snapshots.Count >= 2,
            $"Expected at least 2 snapshots, got {snapshots.Count}");
    }

    [Fact]
    public async Task DbContext_WatchMetrics_EnablesMetricsAutomatically()
    {
        Assert.Null(_db.GetMetrics());

        using var sub = _db.WatchMetrics(TimeSpan.FromMilliseconds(50))
            .Subscribe(_ => { });

        await Task.Delay(100);

        Assert.NotNull(_db.GetMetrics());
    }
}
