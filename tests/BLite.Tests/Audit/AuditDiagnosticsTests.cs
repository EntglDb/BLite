using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BLite.Bson;
using BLite.Core.Audit;
using BLite.Shared;
using Xunit;

namespace BLite.Tests.Audit;

/// <summary>
/// Phase 2 audit coverage: verifies the OpenTelemetry-style ActivitySource emits
/// activities with the expected tags, that <see cref="SlowOperationEvent"/> fires
/// only when an operation exceeds the configured threshold, and that
/// <see cref="QueryStrategy"/> + <see cref="QueryAuditEvent.IndexName"/> are
/// correctly propagated end-to-end through the query pipeline.
/// </summary>
public class AuditDiagnosticsTests : IDisposable
{
    private readonly string _dbPath;

    public AuditDiagnosticsTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_diag_{Guid.NewGuid()}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    [Fact]
    public async Task ActivitySource_Emits_Commit_Activity_With_OTel_Tags()
    {
        var activities = new ConcurrentBag<Activity>();
        using var listener = RegisterListener(activities);

        var options = new BLiteAuditOptions { EnableDiagnosticSource = true };
        using (var db = new MinimalDbContext(_dbPath, options))
        {
            await db.Users.InsertAsync(new User { Name = "Activity-Alice", Age = 30 });
        }

        var commit = activities.FirstOrDefault(a => a.OperationName == BLiteDiagnostics.Activity.Commit);
        Assert.NotNull(commit);
        AssertTag(commit!, BLiteDiagnostics.Tags.DbSystem, BLiteDiagnostics.Tags.DbSystemValue);
        AssertTag(commit, BLiteDiagnostics.Tags.DbOperation, "commit");
        // Numeric tags land in TagObjects, not Tags — use GetTagItem to inspect either.
        Assert.NotNull(commit.GetTagItem(BLiteDiagnostics.Tags.TransactionId));
        Assert.NotNull(commit.GetTagItem(BLiteDiagnostics.Tags.PagesWritten));
        Assert.NotNull(commit.GetTagItem(BLiteDiagnostics.Tags.WalSizeBytes));
    }

    [Fact]
    public async Task ActivitySource_Emits_Query_Activity_With_Strategy_And_Index_Tags()
    {
        var activities = new ConcurrentBag<Activity>();
        using var listener = RegisterListener(activities);

        var options = new BLiteAuditOptions { EnableDiagnosticSource = true };
        using var db = new TestDbContext(_dbPath, options);

        // People.Age is indexed (modelBuilder.Entity<Person>().HasIndex(p => p.Age)).
        await db.People.InsertAsync(new Person { Id = 1, Name = "A", Age = 20 });
        await db.People.InsertAsync(new Person { Id = 2, Name = "B", Age = 30 });

        var hits = db.People.AsQueryable().Where(p => p.Age == 30).ToList();
        Assert.Single(hits);

        var query = activities.LastOrDefault(a => a.OperationName == BLiteDiagnostics.Activity.Query);
        Assert.NotNull(query);
        AssertTag(query!, BLiteDiagnostics.Tags.DbSystem, BLiteDiagnostics.Tags.DbSystemValue);
        AssertTag(query, BLiteDiagnostics.Tags.DbCollectionName, "people_collection");
        AssertTag(query, BLiteDiagnostics.Tags.DbOperation, "query");
        AssertTag(query, BLiteDiagnostics.Tags.QueryStrategy, QueryStrategy.IndexScan.ToString());

        var indexTag = query.Tags.FirstOrDefault(t => t.Key == BLiteDiagnostics.Tags.QueryIndexName).Value as string;
        Assert.False(string.IsNullOrEmpty(indexTag));
    }

    [Fact]
    public async Task QueryAuditEvent_Has_IndexScan_Strategy_And_IndexName_For_Indexed_Predicate()
    {
        var sink = new CapturingSink();
        var options = new BLiteAuditOptions { Sink = sink, EnableMetrics = true };

        using var db = new TestDbContext(_dbPath, options);
        await db.People.InsertAsync(new Person { Id = 1, Name = "A", Age = 20 });
        await db.People.InsertAsync(new Person { Id = 2, Name = "B", Age = 40 });
        sink.Queries.Clear();

        var older = db.People.AsQueryable().Where(p => p.Age == 40).ToList();
        Assert.Single(older);

        var evt = Assert.Single(sink.Queries);
        Assert.Equal(QueryStrategy.IndexScan, evt.Strategy);
        Assert.False(string.IsNullOrEmpty(evt.IndexName));
    }

    [Fact]
    public async Task SlowOperationEvent_Fires_When_Elapsed_Exceeds_Threshold()
    {
        var sink = new CapturingSink();
        // Use a near-zero threshold so every commit + insert + query trips the slow path.
        var options = new BLiteAuditOptions
        {
            Sink = sink,
            SlowQueryThreshold = TimeSpan.FromTicks(1),
        };

        using var db = new MinimalDbContext(_dbPath, options);
        await db.Users.InsertAsync(new User { Name = "Slow", Age = 99 });
        _ = db.Users.AsQueryable().ToList();

        Assert.Contains(sink.Slows, e => e.OperationType == SlowOperationType.Insert);
        Assert.Contains(sink.Slows, e => e.OperationType == SlowOperationType.Commit);
        Assert.Contains(sink.Slows, e => e.OperationType == SlowOperationType.Query);
        Assert.All(sink.Slows, e => Assert.True(e.Elapsed >= TimeSpan.FromTicks(1)));
    }

    [Fact]
    public async Task SlowOperationEvent_Does_Not_Fire_Below_Threshold()
    {
        var sink = new CapturingSink();
        // Threshold far above what a single in-memory op should take.
        var options = new BLiteAuditOptions
        {
            Sink = sink,
            SlowQueryThreshold = TimeSpan.FromMinutes(5),
        };

        using var db = new MinimalDbContext(_dbPath, options);
        await db.Users.InsertAsync(new User { Name = "Fast", Age = 1 });
        _ = db.Users.AsQueryable().ToList();

        Assert.Empty(sink.Slows);
    }

    [Fact]
    public async Task EnableDiagnosticSource_False_Does_Not_Emit_Activities()
    {
        var activities = new ConcurrentBag<Activity>();
        using var listener = RegisterListener(activities);

        // Sink/metrics on, but ActivitySource emission explicitly off.
        var sink = new CapturingSink();
        var options = new BLiteAuditOptions { Sink = sink, EnableDiagnosticSource = false };

        using var db = new MinimalDbContext(_dbPath, options);
        await db.Users.InsertAsync(new User { Name = "Silent", Age = 7 });

        Assert.Empty(activities);
        // Sink still fires (orthogonal to ActivitySource).
        Assert.NotEmpty(sink.Inserts);
    }

    private static ActivityListener RegisterListener(ConcurrentBag<Activity> sink)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = src => src.Name == "BLite.Core",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => sink.Add(activity),
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private static void AssertTag(Activity activity, string key, string expectedValue)
    {
        var tag = activity.Tags.FirstOrDefault(t => t.Key == key);
        Assert.Equal(expectedValue, tag.Value);
    }

    /// <summary>
    /// Sink that captures every event into ordinary <see cref="List{T}"/>s. Tests run
    /// single-threaded against this sink, so the lack of internal locking is intentional.
    /// </summary>
    private sealed class CapturingSink : IBLiteAuditSink
    {
        public List<InsertAuditEvent> Inserts { get; } = new();
        public List<QueryAuditEvent>  Queries { get; } = new();
        public List<CommitAuditEvent> Commits { get; } = new();
        public List<SlowOperationEvent> Slows { get; } = new();

        public void OnInsert(in InsertAuditEvent evt)         => Inserts.Add(evt);
        public void OnQuery(in QueryAuditEvent evt)           => Queries.Add(evt);
        public void OnCommit(in CommitAuditEvent evt)         => Commits.Add(evt);
        public void OnSlowOperation(in SlowOperationEvent evt) => Slows.Add(evt);
    }
}
