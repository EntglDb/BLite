using BLite.Bson;
using BLite.Core;
using BLite.Core.Audit;

namespace BLite.Tests;

/// <summary>
/// Integration tests for the BLite audit trail subsystem (Phase 1 + Phase 2).
/// </summary>
public class AuditSinkTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;

    public AuditSinkTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_audit_{Guid.NewGuid():N}.db");
        _engine = new BLiteEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        CleanupDb(_dbPath);
    }

    private static void CleanupDb(string path)
    {
        if (File.Exists(path)) File.Delete(path);
        var wal = Path.ChangeExtension(path, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private sealed class RecordingSink : IBLiteAuditSink
    {
        public List<InsertAuditEvent> Inserts   { get; } = new();
        public List<QueryAuditEvent>  Queries   { get; } = new();
        public List<CommitAuditEvent> Commits   { get; } = new();
        public List<SlowOperationEvent> SlowOps { get; } = new();

        public void OnInsert(InsertAuditEvent e)          => Inserts.Add(e);
        public void OnQuery(QueryAuditEvent e)            => Queries.Add(e);
        public void OnCommit(CommitAuditEvent e)          => Commits.Add(e);
        public void OnSlowOperation(SlowOperationEvent e) => SlowOps.Add(e);
    }

    // ── Zero-overhead when not configured ───────────────────────────────────

    [Fact]
    public async Task AuditMetrics_Null_WhenNotConfigured()
    {
        Assert.Null(_engine.AuditMetrics);

        var col = _engine.GetOrCreateCollection("items");
        var doc = col.CreateDocument(["n"], b => b.AddString("n", "x"));
        await _engine.InsertAsync("items", doc);

        Assert.Null(_engine.AuditMetrics);
    }

    // ── ConfigureAudit validation ─────────────────────────────────────────────

    [Fact]
    public void ConfigureAudit_NullOptions_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => _engine.ConfigureAudit(null!));
    }

    [Fact]
    public void ConfigureAudit_WithSinkOnly_DoesNotEnableMetrics()
    {
        _engine.ConfigureAudit(new BLiteAuditOptions { Sink = new RecordingSink() });
        Assert.Null(_engine.AuditMetrics);
    }

    [Fact]
    public void ConfigureAudit_EnableMetrics_CreatesMetricsInstance()
    {
        _engine.ConfigureAudit(new BLiteAuditOptions { EnableMetrics = true });
        Assert.NotNull(_engine.AuditMetrics);
    }

    // ── IBLiteAuditSink.OnInsert ──────────────────────────────────────────────

    [Fact]
    public async Task Insert_ShouldInvokeAuditSink()
    {
        var sink = new RecordingSink();
        _engine.ConfigureAudit(new BLiteAuditOptions { Sink = sink });

        var col = _engine.GetOrCreateCollection("products");
        var doc = col.CreateDocument(["name"], b => b.AddString("name", "Widget"));
        await _engine.InsertAsync("products", doc);

        Assert.Single(sink.Inserts);
        Assert.Equal("products", sink.Inserts[0].CollectionName);
        Assert.True(sink.Inserts[0].Elapsed >= TimeSpan.Zero);
        Assert.True(sink.Inserts[0].DocumentSizeBytes > 0);
    }

    [Fact]
    public async Task Insert_ShouldNotInvokeAuditSink_WhenNotConfigured()
    {
        var sink = new RecordingSink();
        // Deliberately NOT calling ConfigureAudit.

        var col = _engine.GetOrCreateCollection("products");
        var doc = col.CreateDocument(["name"], b => b.AddString("name", "Widget"));
        await _engine.InsertAsync("products", doc);

        Assert.Empty(sink.Inserts);
    }

    [Fact]
    public async Task Insert_UserId_NullWhenAmbientNotSet()
    {
        var sink = new RecordingSink();
        _engine.ConfigureAudit(new BLiteAuditOptions { Sink = sink });

        AmbientAuditContext.CurrentUserId = null;

        var col = _engine.GetOrCreateCollection("items");
        var doc = col.CreateDocument(["v"], b => b.AddInt32("v", 1));
        await _engine.InsertAsync("items", doc);

        Assert.Single(sink.Inserts);
        Assert.Null(sink.Inserts[0].UserId);
    }

    [Fact]
    public async Task Insert_UserId_PropagatedFromAmbientContext()
    {
        var sink = new RecordingSink();
        _engine.ConfigureAudit(new BLiteAuditOptions { Sink = sink });

        AmbientAuditContext.CurrentUserId = "user-42";
        try
        {
            var col = _engine.GetOrCreateCollection("items");
            var doc = col.CreateDocument(["v"], b => b.AddInt32("v", 1));
            await _engine.InsertAsync("items", doc);
        }
        finally
        {
            AmbientAuditContext.CurrentUserId = null;
        }

        Assert.Single(sink.Inserts);
        Assert.Equal("user-42", sink.Inserts[0].UserId);
    }

    // ── IBLiteAuditSink.OnCommit ──────────────────────────────────────────────

    [Fact]
    public async Task Commit_ShouldInvokeAuditSink()
    {
        var sink = new RecordingSink();
        _engine.ConfigureAudit(new BLiteAuditOptions { Sink = sink });

        var col = _engine.GetOrCreateCollection("orders");
        var doc = col.CreateDocument(["price"], b => b.AddDouble("price", 9.99));
        await _engine.InsertAsync("orders", doc);

        Assert.NotEmpty(sink.Commits);
        Assert.True(sink.Commits.All(e => e.Elapsed >= TimeSpan.Zero));
    }

    // ── IBLiteAuditSink.OnQuery via DocumentDbContext (LINQ path) ─────────────

    [Fact]
    public async Task Query_ShouldInvokeAuditSink_ViaLinq()
    {
        var path = Path.Combine(Path.GetTempPath(), $"blite_qaudit_{Guid.NewGuid():N}.db");
        using var ctx = new MinimalDbContext(path);
        try
        {
            var sink = new RecordingSink();
            ctx.ConfigureAudit(new BLiteAuditOptions { Sink = sink });

            await ctx.Users.InsertAsync(new BLite.Shared.User { Name = "Alice", Age = 30 });

            // Trigger a LINQ query — goes through BTreeQueryProvider.ExecuteAsync.
            var results = ctx.Users.AsQueryable().Where(u => u.Age > 20).ToList();
            _ = results;

            Assert.NotEmpty(sink.Queries);
            Assert.True(sink.Queries[0].CollectionName.Length > 0);
            Assert.True(sink.Queries[0].Elapsed >= TimeSpan.Zero);
        }
        finally
        {
            ctx.Dispose();
            CleanupDb(path);
        }
    }

    // ── BLiteMetrics counters ─────────────────────────────────────────────────

    [Fact]
    public async Task Metrics_TotalInserts_IncrementedAfterInsert()
    {
        _engine.ConfigureAudit(new BLiteAuditOptions { EnableMetrics = true });
        var metrics = _engine.AuditMetrics!;

        var col = _engine.GetOrCreateCollection("counters");
        var doc = col.CreateDocument(["x"], b => b.AddInt32("x", 1));
        await _engine.InsertAsync("counters", doc);

        Assert.Equal(1, metrics.TotalInserts);
    }

    [Fact]
    public async Task Metrics_TotalCommits_IncrementedAfterCommit()
    {
        _engine.ConfigureAudit(new BLiteAuditOptions { EnableMetrics = true });
        var metrics = _engine.AuditMetrics!;

        var col = _engine.GetOrCreateCollection("events");
        var doc = col.CreateDocument(["t"], b => b.AddString("t", "click"));
        await _engine.InsertAsync("events", doc);

        Assert.True(metrics.TotalCommits >= 1, $"Expected at least 1 commit, got {metrics.TotalCommits}");
    }

    [Fact]
    public async Task Metrics_TotalQueries_IncrementedAfterLinqQuery()
    {
        var path = Path.Combine(Path.GetTempPath(), $"blite_qmetric_{Guid.NewGuid():N}.db");
        using var ctx = new MinimalDbContext(path);
        try
        {
            ctx.ConfigureAudit(new BLiteAuditOptions { EnableMetrics = true });
            var metrics = ctx.AuditMetrics!;

            await ctx.Users.InsertAsync(new BLite.Shared.User { Name = "Bob", Age = 25 });

            _ = ctx.Users.AsQueryable().ToList();

            Assert.True(metrics.TotalQueries >= 1, $"Expected at least 1 query, got {metrics.TotalQueries}");
        }
        finally
        {
            ctx.Dispose();
            CleanupDb(path);
        }
    }

    [Fact]
    public void Metrics_Reset_ClearsAllCounters()
    {
        _engine.ConfigureAudit(new BLiteAuditOptions { EnableMetrics = true });
        var metrics = _engine.AuditMetrics!;

        metrics.Reset();

        Assert.Equal(0, metrics.TotalInserts);
        Assert.Equal(0, metrics.TotalCommits);
        Assert.Equal(0, metrics.TotalQueries);
    }

    // ── Slow operation detection ──────────────────────────────────────────────

    [Fact]
    public async Task SlowInsert_ShouldEmitSlowOperationEvent_WhenThresholdZero()
    {
        var sink = new RecordingSink();
        _engine.ConfigureAudit(new BLiteAuditOptions
        {
            Sink               = sink,
            // Threshold = Zero so every insert triggers a slow event.
            SlowOperationThreshold = TimeSpan.Zero
        });

        var col = _engine.GetOrCreateCollection("slow");
        var doc = col.CreateDocument(["x"], b => b.AddInt32("x", 42));
        await _engine.InsertAsync("slow", doc);

        Assert.NotEmpty(sink.SlowOps);
        Assert.Contains(sink.SlowOps, e => e.OperationType == SlowOperationType.Insert);
    }

    [Fact]
    public async Task SlowQuery_ShouldEmitSlowOperationEvent_WhenThresholdZero()
    {
        var path = Path.Combine(Path.GetTempPath(), $"blite_slowq_{Guid.NewGuid():N}.db");
        using var ctx = new MinimalDbContext(path);
        try
        {
            var sink = new RecordingSink();
            ctx.ConfigureAudit(new BLiteAuditOptions
            {
                Sink                   = sink,
                SlowOperationThreshold = TimeSpan.Zero
            });

            await ctx.Users.InsertAsync(new BLite.Shared.User { Name = "Eve", Age = 40 });

            _ = ctx.Users.AsQueryable().ToList();

            Assert.Contains(sink.SlowOps, e => e.OperationType == SlowOperationType.Query);
        }
        finally
        {
            ctx.Dispose();
            CleanupDb(path);
        }
    }

    [Fact]
    public async Task SlowCommit_ShouldEmitSlowOperationEvent_WhenThresholdZero()
    {
        var sink = new RecordingSink();
        _engine.ConfigureAudit(new BLiteAuditOptions
        {
            Sink                   = sink,
            SlowOperationThreshold = TimeSpan.Zero
        });

        var col = _engine.GetOrCreateCollection("commits");
        var doc = col.CreateDocument(["z"], b => b.AddInt32("z", 0));
        await _engine.InsertAsync("commits", doc);

        Assert.Contains(sink.SlowOps, e => e.OperationType == SlowOperationType.Commit);
    }

    [Fact]
    public async Task SlowOp_NotEmitted_WhenThresholdNotExceeded()
    {
        var sink = new RecordingSink();
        _engine.ConfigureAudit(new BLiteAuditOptions
        {
            Sink                   = sink,
            // Very high threshold — should never be exceeded in a unit test.
            SlowOperationThreshold = TimeSpan.FromHours(1)
        });

        var col = _engine.GetOrCreateCollection("fast");
        var doc = col.CreateDocument(["a"], b => b.AddInt32("a", 1));
        await _engine.InsertAsync("fast", doc);

        Assert.Empty(sink.SlowOps);
    }

    // ── IAuditContextProvider (custom provider) ───────────────────────────────

    [Fact]
    public async Task CustomContextProvider_OverridesAmbientContext()
    {
        var sink = new RecordingSink();
        _engine.ConfigureAudit(new BLiteAuditOptions
        {
            Sink            = sink,
            ContextProvider = new FixedUserProvider("fixed-user")
        });

        var col = _engine.GetOrCreateCollection("ctx");
        var doc = col.CreateDocument(["d"], b => b.AddInt32("d", 1));
        await _engine.InsertAsync("ctx", doc);

        Assert.Single(sink.Inserts);
        Assert.Equal("fixed-user", sink.Inserts[0].UserId);
    }

    // ── AmbientAuditContext ───────────────────────────────────────────────────

    [Fact]
    public void AmbientAuditContext_DefaultIsNull()
    {
        AmbientAuditContext.CurrentUserId = null;
        Assert.Null(AmbientAuditContext.Instance.GetCurrentUserId());
    }

    [Fact]
    public void AmbientAuditContext_SetAndGet()
    {
        AmbientAuditContext.CurrentUserId = "test-user";
        try
        {
            Assert.Equal("test-user", AmbientAuditContext.Instance.GetCurrentUserId());
        }
        finally
        {
            AmbientAuditContext.CurrentUserId = null;
        }
    }

    // ── Phase 2: ActivitySource ───────────────────────────────────────────────

#if NET5_0_OR_GREATER
    [Fact]
    public async Task ActivitySource_EmitsCommitActivity_WhenListenerRegistered()
    {
        var activities = new System.Collections.Generic.List<System.Diagnostics.Activity>();

        using var listener = new System.Diagnostics.ActivityListener
        {
            ShouldListenTo  = source => source.Name == BLiteDiagnostics.ActivitySourceName,
            Sample          = (ref System.Diagnostics.ActivityCreationOptions<System.Diagnostics.ActivityContext> _)
                                => System.Diagnostics.ActivitySamplingResult.AllData,
            ActivityStopped = a => activities.Add(a)
        };
        System.Diagnostics.ActivitySource.AddActivityListener(listener);

        var path = Path.Combine(Path.GetTempPath(), $"blite_act_{Guid.NewGuid():N}.db");
        using var engine = new BLiteEngine(path);
        try
        {
            engine.ConfigureAudit(new BLiteAuditOptions { EnableDiagnosticSource = true });

            var col = engine.GetOrCreateCollection("act");
            var doc = col.CreateDocument(["v"], b => b.AddInt32("v", 1));
            await engine.InsertAsync("act", doc);

            await Task.Delay(100);

            Assert.Contains(activities,
                a => a.OperationName == BLiteDiagnostics.CommitActivityName);
        }
        finally
        {
            engine.Dispose();
            CleanupDb(path);
        }
    }

    [Fact]
    public async Task ActivitySource_EmitsQueryActivity_WhenListenerRegistered()
    {
        var activities = new System.Collections.Generic.List<System.Diagnostics.Activity>();

        using var listener = new System.Diagnostics.ActivityListener
        {
            ShouldListenTo  = source => source.Name == BLiteDiagnostics.ActivitySourceName,
            Sample          = (ref System.Diagnostics.ActivityCreationOptions<System.Diagnostics.ActivityContext> _)
                                => System.Diagnostics.ActivitySamplingResult.AllData,
            ActivityStopped = a => activities.Add(a)
        };
        System.Diagnostics.ActivitySource.AddActivityListener(listener);

        var path = Path.Combine(Path.GetTempPath(), $"blite_qact_{Guid.NewGuid():N}.db");
        using var ctx = new MinimalDbContext(path);
        try
        {
            ctx.ConfigureAudit(new BLiteAuditOptions { EnableDiagnosticSource = true });

            await ctx.Users.InsertAsync(new BLite.Shared.User { Name = "Carol", Age = 35 });

            _ = ctx.Users.AsQueryable().ToList();

            Assert.Contains(activities,
                a => a.OperationName == BLiteDiagnostics.QueryActivityName);
        }
        finally
        {
            ctx.Dispose();
            CleanupDb(path);
        }
    }
#endif

    // ── Helpers ───────────────────────────────────────────────────────────────

    private sealed class FixedUserProvider : IAuditContextProvider
    {
        private readonly string _userId;
        public FixedUserProvider(string userId) => _userId = userId;
        public string? GetCurrentUserId() => _userId;
    }
}
