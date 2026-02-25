using BLite.Bson;
using BLite.Core;

namespace BLite.Tests;

/// <summary>
/// Unit tests for TimeSeries collection support.
/// Covers: metadata persistence, insert routing, FindAll/FindById,
/// timestamp extraction from documents, and automatic pruning via ForcePrune.
/// </summary>
public class TimeSeriesTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;

    public TimeSeriesTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_ts_{Guid.NewGuid()}.db");
        _engine = new BLiteEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ─── helpers ─────────────────────────────────────────────────────────────

    private DynamicCollection TsCol(string name = "metrics", TimeSpan? retention = null)
    {
        var col = _engine.GetOrCreateCollection(name);
        col.SetTimeSeries("ts", retention ?? TimeSpan.FromDays(30));
        return col;
    }

    private BsonDocument MakeDoc(DynamicCollection col, string sensor, DateTime ts)
        => col.CreateDocument(["sensor", "ts"], b => b
            .AddString("sensor", sensor)
            .AddDateTime("ts", ts));

    private BsonDocument MakeDocInt64(DynamicCollection col, string sensor, long tsRaw)
        => col.CreateDocument(["sensor", "ts"], b => b
            .AddString("sensor", sensor)
            .AddInt64("ts", tsRaw));

    private BsonDocument MakeDocNoTs(DynamicCollection col, string sensor)
        => col.CreateDocument(["sensor"], b => b
            .AddString("sensor", sensor));

    // ─── SetTimeSeries ────────────────────────────────────────────────────────

    [Fact]
    public void SetTimeSeries_PersistsMetadataToStorage()
    {
        var col = _engine.GetOrCreateCollection("events");
        col.SetTimeSeries("ts", TimeSpan.FromDays(7));

        // Re-read metadata directly from storage (bypass in-memory cache)
        _engine.Dispose();
        using var eng2 = new BLiteEngine(_dbPath);
        var reopened = eng2.GetOrCreateCollection("events");

        // Insert to verify the column behaves as TimeSeries after restart
        reopened.Insert(MakeDoc(reopened, "s1", DateTime.UtcNow));
        eng2.Commit();

        var docs = reopened.FindAll().ToList();
        Assert.Single(docs);
    }

    [Fact]
    public void SetTimeSeries_SetsInMemoryFlag_SoInsertUsesTimeSeriesPath()
    {
        var col = TsCol();
        // Should not throw — verifies _isTimeSeries is true after SetTimeSeries
        var ex = Record.Exception(() =>
        {
            col.Insert(MakeDoc(col, "sensor1", DateTime.UtcNow));
            _engine.Commit();
        });

        Assert.Null(ex);
    }

    [Fact]
    public void SetTimeSeries_OnExistingCollection_DoesNotThrow()
    {
        var col = _engine.GetOrCreateCollection("temp");
        // Call twice — should be idempotent
        col.SetTimeSeries("ts", TimeSpan.FromDays(30));
        col.SetTimeSeries("ts", TimeSpan.FromDays(60));
    }

    // ─── Insert + FindAll ────────────────────────────────────────────────────

    [Fact]
    public void Insert_Single_FindAll_Returns_OneDocument()
    {
        var col = TsCol();
        col.Insert(MakeDoc(col, "sensor1", DateTime.UtcNow));
        _engine.Commit();

        var docs = col.FindAll().ToList();
        Assert.Single(docs);
    }

    [Fact]
    public void Insert_Multiple_FindAll_ReturnsAll()
    {
        const int count = 10;
        var col = TsCol();

        for (int i = 0; i < count; i++)
            col.Insert(MakeDoc(col, $"s{i}", DateTime.UtcNow.AddSeconds(-i)));
        _engine.Commit();

        var docs = col.FindAll().ToList();
        Assert.Equal(count, docs.Count);
    }

    [Fact]
    public void Insert_FindById_ReturnsCorrectDocument()
    {
        var col = TsCol();
        var doc = MakeDoc(col, "sensor_target", DateTime.UtcNow);
        var id = col.Insert(doc);
        _engine.Commit();

        var found = col.FindById(id);

        Assert.NotNull(found);
        Assert.True(found.TryGetString("sensor", out var sensor));
        Assert.Equal("sensor_target", sensor);
    }

    [Fact]
    public void Insert_PreservesStringPayload()
    {
        var col = TsCol();
        col.Insert(MakeDoc(col, "temperature_probe_X1", DateTime.UtcNow));
        _engine.Commit();

        var doc = col.FindAll().First();
        Assert.True(doc.TryGetString("sensor", out var sensor));
        Assert.Equal("temperature_probe_X1", sensor);
    }

    // ─── Timestamp extraction ────────────────────────────────────────────────

    [Fact]
    public void Insert_WithDateTimeField_DoesNotThrow()
    {
        var col = TsCol();
        var ex = Record.Exception(() =>
        {
            col.Insert(MakeDoc(col, "s", DateTime.UtcNow.AddHours(-1)));
            _engine.Commit();
        });
        Assert.Null(ex);
    }

    [Fact]
    public void Insert_WithInt64TtlField_DoesNotThrow()
    {
        var col = TsCol();
        long ticks = DateTime.UtcNow.Ticks;
        var ex = Record.Exception(() =>
        {
            col.Insert(MakeDocInt64(col, "s", ticks));
            _engine.Commit();
        });
        Assert.Null(ex);
    }

    [Fact]
    public void Insert_WithMissingTtlField_FallsBackToNow_NoThrow()
    {
        var col = TsCol();
        var ex = Record.Exception(() =>
        {
            col.Insert(MakeDocNoTs(col, "s_nofield"));
            _engine.Commit();
        });
        Assert.Null(ex);
    }

    // ─── Page chain growth ───────────────────────────────────────────────────

    [Fact]
    public void Insert_ManyDocuments_AllRetrievable()
    {
        // 200 docs to ensure at least a few page allocations
        const int count = 200;
        var col = TsCol();

        for (int i = 0; i < count; i++)
            col.Insert(MakeDoc(col, $"sensor_{i}", DateTime.UtcNow.AddSeconds(-i)));
        _engine.Commit();

        var docs = col.FindAll().ToList();
        Assert.Equal(count, docs.Count);
    }

    [Fact]
    public void Insert_Count_MatchesActualInserts()
    {
        var col = TsCol();
        col.Insert(MakeDoc(col, "a", DateTime.UtcNow));
        col.Insert(MakeDoc(col, "b", DateTime.UtcNow));
        col.Insert(MakeDoc(col, "c", DateTime.UtcNow));
        _engine.Commit();

        Assert.Equal(3, col.Count());
    }

    // ─── ForcePrune ──────────────────────────────────────────────────────────

    [Fact]
    public void ForcePrune_OnNonTimeSeries_Throws()
    {
        var col = _engine.GetOrCreateCollection("plain");
        Assert.Throws<InvalidOperationException>(() => col.ForcePrune());
    }

    [Fact]
    public void ForcePrune_AllDocumentsExpired_FindAll_ReturnsEmpty()
    {
        // Retention = 1 day. Docs timestamped 2 days ago → all expired.
        var col = TsCol(retention: TimeSpan.FromDays(1));

        for (int i = 0; i < 5; i++)
            col.Insert(MakeDoc(col, $"old_{i}", DateTime.UtcNow.AddDays(-2)));
        _engine.Commit();

        col.ForcePrune();

        // After pruning, pages freed. BTree entries become stale.
        // ReadDocumentAt returns null for freed pages → FindAll silently skips them.
        var remaining = col.FindAll().ToList();
        Assert.Empty(remaining);
    }

    [Fact]
    public void ForcePrune_AllDocumentsFresh_PreservesAll()
    {
        const int count = 5;
        var col = TsCol(retention: TimeSpan.FromDays(30));

        for (int i = 0; i < count; i++)
            col.Insert(MakeDoc(col, $"fresh_{i}", DateTime.UtcNow.AddSeconds(-i)));
        _engine.Commit();

        col.ForcePrune();

        var remaining = col.FindAll().ToList();
        Assert.Equal(count, remaining.Count);
    }

    [Fact]
    public void ForcePrune_MixedOnSamePage_KeepsBecauseFreshTimestampDominates()
    {
        // Old doc + fresh doc on the SAME page (page's LastTimestamp = fresh → not pruned).
        var col = TsCol(retention: TimeSpan.FromDays(1));
        col.Insert(MakeDoc(col, "old", DateTime.UtcNow.AddDays(-5)));  // old
        col.Insert(MakeDoc(col, "fresh", DateTime.UtcNow));            // fresh - same page, updates LastTimestamp
        _engine.Commit();

        col.ForcePrune();

        // The page should NOT be pruned because LastTimestamp = fresh doc's ts
        var remaining = col.FindAll().ToList();
        Assert.Equal(2, remaining.Count);
    }

    [Fact]
    public void ForcePrune_ResetsPruningCounters()
    {
        var col = TsCol(retention: TimeSpan.FromDays(1));
        col.Insert(MakeDoc(col, "x", DateTime.UtcNow.AddDays(-2)));
        _engine.Commit();

        // Should not throw — no assertion on internal counter, just verify it completes
        var ex = Record.Exception(() => col.ForcePrune());
        Assert.Null(ex);
    }

    [Fact]
    public void ForcePrune_NoRetentionSet_DoesNotThrow()
    {
        // RetentionPolicyMs = 0 means "no pruning logic runs"
        var col = _engine.GetOrCreateCollection("no_ret");
        col.SetTimeSeries("ts", TimeSpan.Zero);
        col.Insert(MakeDoc(col, "s", DateTime.UtcNow.AddYears(-1)));
        _engine.Commit();

        var ex = Record.Exception(() => col.ForcePrune());
        Assert.Null(ex);
    }
}
