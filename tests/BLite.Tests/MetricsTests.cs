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
        var before = DateTimeOffset.UtcNow.AddSeconds(-5);
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

/// <summary>
/// Integration tests for the new security/audit metrics counters added to
/// <see cref="MetricsSnapshot"/>: VacuumLastRunAt, VacuumBytesFreed,
/// BackupLastSuccessAt, BackupLastDurationMs, SecurityFailedQueriesTotal,
/// and AuditEventsTotal.
/// </summary>
public class SecurityMetricsTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;

    public SecurityMetricsTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_secmetrics_{Guid.NewGuid():N}.db");
        _engine = new BLiteEngine(_dbPath);
        _engine.EnableMetrics();
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ── VacuumLastRunAt / VacuumBytesFreed ───────────────────────────────────

    [Fact]
    public void VacuumLastRunAt_NullBeforeAnyVacuum()
    {
        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.Null(snap!.VacuumLastRunAt);
        Assert.Equal(0L, snap.VacuumBytesFreed);
    }

    [Fact]
    public async Task VacuumLastRunAt_UpdatedAfterVacuum()
    {
        var before = DateTimeOffset.UtcNow.AddSeconds(-1);

        var col = _engine.GetOrCreateCollection("vac");
        var doc = col.CreateDocument(["v"], b => b.AddInt32("v", 1));
        await _engine.InsertAsync("vac", doc);

        await _engine.VacuumAsync(new VacuumOptions { TruncateFile = false });

        // Allow background channel to flush.
        await Task.Delay(200);

        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.NotNull(snap!.VacuumLastRunAt);
        Assert.True(snap.VacuumLastRunAt >= before,
            $"VacuumLastRunAt {snap.VacuumLastRunAt} expected >= {before}");
    }

    [Fact]
    public async Task VacuumBytesFreed_NonNegativeAfterVacuum()
    {
        var col = _engine.GetOrCreateCollection("vac2");
        // Insert and then delete to create some freed space.
        for (int i = 0; i < 3; i++)
        {
            var doc = col.CreateDocument(["v"], b => b.AddInt32("v", i));
            var id = await _engine.InsertAsync("vac2", doc);
            await _engine.DeleteAsync("vac2", id);
        }

        await _engine.VacuumAsync(new VacuumOptions { TruncateFile = false });
        await Task.Delay(200);

        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.True(snap!.VacuumBytesFreed >= 0,
            $"VacuumBytesFreed must be >= 0, got {snap.VacuumBytesFreed}");
    }

    [Fact]
    public async Task AuditEventsTotal_ContainsVacuumEntry_AfterVacuum()
    {
        var col = _engine.GetOrCreateCollection("vac3");
        var doc = col.CreateDocument(["v"], b => b.AddInt32("v", 1));
        await _engine.InsertAsync("vac3", doc);

        await _engine.VacuumAsync(new VacuumOptions { TruncateFile = false });
        await Task.Delay(200);

        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.True(snap!.AuditEventsTotal.TryGetValue("vacuum", out var vacCount),
            "AuditEventsTotal should contain key 'vacuum' after a VacuumAsync call.");
        Assert.True(vacCount >= 1, $"Expected vacuum audit count >= 1, got {vacCount}");
    }

    // ── BackupLastSuccessAt / BackupLastDurationMs ────────────────────────────

    [Fact]
    public void BackupLastSuccessAt_NullBeforeAnyBackup()
    {
        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.Null(snap!.BackupLastSuccessAt);
        Assert.Equal(0L, snap.BackupLastDurationMs);
    }

    [Fact]
    public async Task BackupLastSuccessAt_UpdatedAfterBackup()
    {
        var before = DateTimeOffset.UtcNow.AddSeconds(-1);
        var destPath = Path.Combine(Path.GetTempPath(), $"blite_secmetrics_backup_{Guid.NewGuid():N}.db");
        try
        {
            await _engine.BackupAsync(destPath);
            await Task.Delay(200);

            var snap = _engine.GetMetrics();
            Assert.NotNull(snap);
            Assert.NotNull(snap!.BackupLastSuccessAt);
            Assert.True(snap.BackupLastSuccessAt >= before,
                $"BackupLastSuccessAt {snap.BackupLastSuccessAt} expected >= {before}");
            Assert.True(snap.BackupLastDurationMs >= 0,
                $"BackupLastDurationMs must be >= 0, got {snap.BackupLastDurationMs}");
        }
        finally
        {
            var dir = Path.GetDirectoryName(destPath) ?? Path.GetTempPath();
            var stem = Path.GetFileNameWithoutExtension(destPath);
            foreach (var f in Directory.GetFiles(dir, $"{stem}*"))
                try { File.Delete(f); } catch { }
        }
    }

    [Fact]
    public async Task AuditEventsTotal_ContainsBackupEntry_AfterBackup()
    {
        var destPath = Path.Combine(Path.GetTempPath(), $"blite_secmetrics_backup2_{Guid.NewGuid():N}.db");
        try
        {
            await _engine.BackupAsync(destPath);
            await Task.Delay(200);

            var snap = _engine.GetMetrics();
            Assert.NotNull(snap);
            Assert.True(snap!.AuditEventsTotal.TryGetValue("backup.completed", out var cnt),
                "AuditEventsTotal should contain key 'backup.completed' after BackupAsync.");
            Assert.True(cnt >= 1, $"Expected backup.completed audit count >= 1, got {cnt}");
        }
        finally
        {
            var dir = Path.GetDirectoryName(destPath) ?? Path.GetTempPath();
            var stem = Path.GetFileNameWithoutExtension(destPath);
            foreach (var f in Directory.GetFiles(dir, $"{stem}*"))
                try { File.Delete(f); } catch { }
        }
    }

    // ── SecurityFailedQueriesTotal ────────────────────────────────────────────

    [Fact]
    public void SecurityFailedQueriesTotal_ZeroInitially()
    {
        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.Equal(0L, snap!.SecurityFailedQueriesTotal);
    }

    [Fact]
    public async Task SecurityFailedQueriesTotal_IncrementedOnBadBlqlFilter()
    {
        var col = _engine.GetOrCreateCollection("blql_sec");

        // An unknown operator like $where should throw FormatException from the parser.
        Assert.Throws<FormatException>(() =>
            col.Query("{ \"name\": { \"$where\": \"js code\" } }").ToList());

        // Allow background channel to flush.
        await Task.Delay(200);

        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.True(snap!.SecurityFailedQueriesTotal >= 1,
            $"Expected SecurityFailedQueriesTotal >= 1, got {snap.SecurityFailedQueriesTotal}");

        Assert.True(snap.AuditEventsTotal.TryGetValue("security.failed_query", out var auditCnt),
            "AuditEventsTotal should contain key 'security.failed_query'.");
        Assert.True(auditCnt >= 1, $"Expected security.failed_query audit count >= 1, got {auditCnt}");
    }

    [Fact]
    public async Task SecurityFailedQueriesTotal_NotIncrementedForValidQuery()
    {
        var col = _engine.GetOrCreateCollection("blql_ok");
        var doc = col.CreateDocument(["n"], b => b.AddString("n", "test"));
        await _engine.InsertAsync("blql_ok", doc);

        // A valid BLQL query should not increment the failed counter.
        col.Query("{ \"n\": \"test\" }").ToList();

        await Task.Delay(200);

        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.Equal(0L, snap!.SecurityFailedQueriesTotal);
    }

    [Fact]
    public async Task SecurityFailedQueriesTotal_IncrementedOnMalformedJson()
    {
        var col = _engine.GetOrCreateCollection("blql_json");

        // Malformed JSON throws JsonReaderException (a JsonException subclass) — must also be counted.
        Assert.ThrowsAny<System.Text.Json.JsonException>(() =>
            col.Query("{ not valid json !! }").ToList());

        await Task.Delay(200);

        var snap = _engine.GetMetrics();
        Assert.NotNull(snap);
        Assert.True(snap!.SecurityFailedQueriesTotal >= 1,
            $"Expected SecurityFailedQueriesTotal >= 1 for malformed JSON, got {snap.SecurityFailedQueriesTotal}");
    }

    // ── EnableDiagnosticSource option ─────────────────────────────────────────

    [Fact]
    public void EnableMetrics_WithDiagnosticSource_DoesNotThrow()
    {
        var path2 = Path.Combine(Path.GetTempPath(), $"blite_diag_{Guid.NewGuid():N}.db");
        try
        {
            using (var engine2 = new BLiteEngine(path2))
            {
                // Should not throw on any supported target framework.
                engine2.EnableMetrics(new BLite.Core.Metrics.MetricsOptions { EnableDiagnosticSource = true });
                Assert.NotNull(engine2.GetMetrics());
            } // engine2 disposed here, before file deletion
        }
        finally
        {
            if (File.Exists(path2)) File.Delete(path2);
            var w = Path.ChangeExtension(path2, ".wal");
            if (File.Exists(w)) File.Delete(w);
        }
    }
}
