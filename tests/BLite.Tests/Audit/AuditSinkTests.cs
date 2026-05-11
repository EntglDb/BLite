using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BLite.Bson;
using BLite.Core.Audit;
using BLite.Shared;
using Xunit;

namespace BLite.Tests.Audit;

/// <summary>
/// Phase 1 coverage for the audit pipeline: verifies the sink receives an event
/// per insert / query / commit, that <see cref="BLiteMetrics"/> counters track
/// sequential and concurrent workloads, and that omitting audit configuration
/// leaves runtime behaviour byte-for-byte identical.
/// </summary>
public class AuditSinkTests : IDisposable
{
    private readonly string _dbPath;

    public AuditSinkTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_audit_{Guid.NewGuid()}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    [Fact]
    public async Task Insert_Fires_OnInsert_And_OnCommit_Once_Per_Operation()
    {
        var sink = new RecordingSink();
        var options = new BLiteAuditOptions { Sink = sink, EnableMetrics = true };

        using (var db = new MinimalDbContext(_dbPath, options))
        {
            await db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
        }

        Assert.Single(sink.Inserts);
        Assert.Equal("users",sink.Inserts[0].CollectionName);
        Assert.True(sink.Inserts[0].DocumentSizeBytes > 0);
        Assert.True(sink.Inserts[0].Elapsed >= TimeSpan.Zero);

        // Auto-commit triggers exactly one commit event for the single insert.
        Assert.Single(sink.Commits);
        Assert.True(sink.Commits[0].Elapsed >= TimeSpan.Zero);
    }

    [Fact]
    public async Task Query_Fires_OnQuery_With_Strategy_And_CollectionName()
    {
        var sink = new RecordingSink();
        var options = new BLiteAuditOptions { Sink = sink, EnableMetrics = true };

        using var db = new MinimalDbContext(_dbPath, options);
        await db.Users.InsertAsync(new User { Name = "Bob", Age = 41 });
        await db.Users.InsertAsync(new User { Name = "Carol", Age = 55 });

        sink.Clear();

        // Full-scan query: no WHERE that maps to an index, just enumerate everything.
        var list = db.Users.AsQueryable().ToList();
        Assert.Equal(2, list.Count);

        Assert.Single(sink.Queries);
        var evt = sink.Queries[0];
        Assert.Equal("users",evt.CollectionName);
        Assert.True(evt.Strategy is QueryStrategy.FullScan or QueryStrategy.BsonScan or QueryStrategy.IndexScan);
        Assert.True(evt.Elapsed >= TimeSpan.Zero);
    }

    [Fact]
    public async Task Metrics_Match_Sequential_Workload()
    {
        var options = new BLiteAuditOptions { EnableMetrics = true };

        using var db = new MinimalDbContext(_dbPath, options);
        Assert.NotNull(db.Metrics);

        for (int i = 0; i < 10; i++)
            await db.Users.InsertAsync(new User { Name = $"U{i}", Age = i });

        // One query (no WHERE, all results).
        var all = db.Users.AsQueryable().ToList();
        Assert.Equal(10, all.Count);

        Assert.Equal(10, db.Metrics!.InsertCount);
        Assert.Equal(10, db.Metrics.CommitCount); // auto-commit per insert
        Assert.Equal(1, db.Metrics.QueryCount);
        Assert.True(db.Metrics.AvgInsertMs >= 0);
        Assert.True(db.Metrics.AvgCommitMs >= 0);
    }

    [Fact]
    public async Task Metrics_Are_Consistent_Under_Concurrent_Inserts()
    {
        var options = new BLiteAuditOptions { EnableMetrics = true };

        using var db = new MinimalDbContext(_dbPath, options);
        Assert.NotNull(db.Metrics);

        const int total = 64;
        // Parallel inserts go through SemaphoreSlim-protected storage; here we just
        // assert the counters are atomic — every successful insert/commit increments
        // exactly once.
        await Parallel.ForEachAsync(
            Enumerable.Range(0, total),
            new ParallelOptions { MaxDegreeOfParallelism = 8 },
            async (i, _) =>
            {
                await db.Users.InsertAsync(new User { Name = $"P{i}", Age = i });
            });

        Assert.Equal(total, db.Metrics!.InsertCount);
        Assert.Equal(total, db.Metrics.CommitCount);
    }

    [Fact]
    public async Task Null_AuditOptions_Has_No_Observable_Side_Effects()
    {
        // No audit configuration at all — the engine must behave identically to a
        // pre-audit build: no Metrics object, no sink invocations possible.
        using var db = new MinimalDbContext(_dbPath);
        Assert.Null(db.Metrics);

        var id = await db.Users.InsertAsync(new User { Name = "Dana", Age = 22 });
        var fetched = await db.Users.FindByIdAsync(id);
        Assert.NotNull(fetched);
        Assert.Equal("Dana", fetched!.Name);
    }

    [Fact]
    public async Task EnableMetrics_False_Does_Not_Allocate_Metrics_But_Sink_Still_Fires()
    {
        var sink = new RecordingSink();
        var options = new BLiteAuditOptions { Sink = sink, EnableMetrics = false };

        using var db = new MinimalDbContext(_dbPath, options);
        Assert.Null(db.Metrics);

        await db.Users.InsertAsync(new User { Name = "Eve", Age = 27 });

        Assert.Single(sink.Inserts);
        Assert.Single(sink.Commits);
    }

    /// <summary>
    /// Thread-safe recording sink that captures every emitted event for inspection.
    /// Uses <see cref="ConcurrentBag{T}"/> so concurrent producers don't need explicit locking.
    /// </summary>
    private sealed class RecordingSink : IBLiteAuditSink
    {
        private readonly ConcurrentBag<InsertAuditEvent> _inserts = new();
        private readonly ConcurrentBag<QueryAuditEvent> _queries = new();
        private readonly ConcurrentBag<CommitAuditEvent> _commits = new();

        public System.Collections.Generic.IReadOnlyList<InsertAuditEvent> Inserts => _inserts.ToArray();
        public System.Collections.Generic.IReadOnlyList<QueryAuditEvent> Queries => _queries.ToArray();
        public System.Collections.Generic.IReadOnlyList<CommitAuditEvent> Commits => _commits.ToArray();

        public void OnInsert(in InsertAuditEvent evt) => _inserts.Add(evt);
        public void OnQuery(in QueryAuditEvent evt) => _queries.Add(evt);
        public void OnCommit(in CommitAuditEvent evt) => _commits.Add(evt);

        public void Clear()
        {
            _inserts.Clear();
            _queries.Clear();
            _commits.Clear();
        }
    }
}
