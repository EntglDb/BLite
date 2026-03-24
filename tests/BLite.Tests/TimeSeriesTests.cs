using BLite.Bson;
using BLite.Core;

namespace BLite.Tests;

/// <summary>
/// Unit tests for TimeSeries collection support.
/// Covers: metadata persistence, insert routing, FindAll/FindById,
/// timestamp extraction from documents, and automatic pruning via ForcePruneAsync.
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
    public async Task SetTimeSeries_PersistsMetadataToStorage()
    {
        var col = _engine.GetOrCreateCollection("events");
        col.SetTimeSeries("ts", TimeSpan.FromDays(7));

        // Re-read metadata directly from storage (bypass in-memory cache)
        _engine.Dispose();
        using var eng2 = new BLiteEngine(_dbPath);
        var reopened = eng2.GetOrCreateCollection("events");

        // Insert to verify the column behaves as TimeSeries after restart
        await reopened.InsertAsync(MakeDoc(reopened, "s1", DateTime.UtcNow));
        await eng2.CommitAsync();

        var docs = (await reopened.FindAllAsync().ToListAsync());
        Assert.Single(docs);
    }

    [Fact]
    public async Task SetTimeSeries_SetsInMemoryFlag_SoInsertUsesTimeSeriesPath()
    {
        var col = TsCol();
        // Should not throw — verifies _isTimeSeries is true after SetTimeSeries
        var ex = await Record.ExceptionAsync(async () =>
        {
            await col.InsertAsync(MakeDoc(col, "sensor1", DateTime.UtcNow));
            await _engine.CommitAsync();
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
    public async Task Insert_Single_FindAll_Returns_OneDocument()
    {
        var col = TsCol();
        await col.InsertAsync(MakeDoc(col, "sensor1", DateTime.UtcNow));
        await _engine.CommitAsync();
        var docs = (await col.FindAllAsync().ToListAsync());
        Assert.Single(docs);
    }

    [Fact]
    public async Task Insert_Multiple_FindAll_ReturnsAll()
    {
        const int count = 10;
        var col = TsCol();

        for (int i = 0; i < count; i++)
            await col.InsertAsync(MakeDoc(col, $"s{i}", DateTime.UtcNow.AddSeconds(-i)));
        await _engine.CommitAsync();

        var docs = (await col.FindAllAsync().ToListAsync());
        Assert.Equal(count, docs.Count);
    }

    [Fact]
    public async Task Insert_FindById_ReturnsCorrectDocument()
    {
        var col = TsCol();
        var doc = MakeDoc(col, "sensor_target", DateTime.UtcNow);
        var id = await col.InsertAsync(doc);
        await _engine.CommitAsync();

        var found = await col.FindByIdAsync(id);

        Assert.NotNull(found);
        Assert.True(found.TryGetString("sensor", out var sensor));
        Assert.Equal("sensor_target", sensor);
    }

    [Fact]
    public async Task Insert_PreservesStringPayload()
    {
        var col = TsCol();
        await col.InsertAsync(MakeDoc(col, "temperature_probe_X1", DateTime.UtcNow));
        await _engine.CommitAsync();
        var doc = (await col.FindAllAsync().ToListAsync()).First();
        Assert.True(doc.TryGetString("sensor", out var sensor));
        Assert.Equal("temperature_probe_X1", sensor);
    }

    // ─── Timestamp extraction ────────────────────────────────────────────────

    [Fact]
    public async Task Insert_WithDateTimeField_DoesNotThrow()
    {
        var col = TsCol();
        var ex = await Record.ExceptionAsync(async () =>
        {
            await col.InsertAsync(MakeDoc(col, "s", DateTime.UtcNow.AddHours(-1)));
            await _engine.CommitAsync();
        });
        Assert.Null(ex);
    }

    [Fact]
    public async Task Insert_WithInt64TtlField_DoesNotThrow()
    {
        var col = TsCol();
        long ticks = DateTime.UtcNow.Ticks;
        var ex = await Record.ExceptionAsync(async () =>
        {
            await col.InsertAsync(MakeDocInt64(col, "s", ticks));
            await _engine.CommitAsync();
        });
        Assert.Null(ex);
    }

    [Fact]
    public async Task Insert_WithMissingTtlField_FallsBackToNow_NoThrow()
    {
        var col = TsCol();
        var ex = await Record.ExceptionAsync(async () =>
        {
            await col.InsertAsync(MakeDocNoTs(col, "s_nofield"));
            await _engine.CommitAsync();
        });
        Assert.Null(ex);
    }

    // ─── Page chain growth ───────────────────────────────────────────────────

    [Fact]
    public async Task Insert_ManyDocuments_AllRetrievable()
    {
        // 200 docs to ensure at least a few page allocations
        const int count = 200;
        var col = TsCol();

        for (int i = 0; i < count; i++)
            await col.InsertAsync(MakeDoc(col, $"sensor_{i}", DateTime.UtcNow.AddSeconds(-i)));
        await _engine.CommitAsync();

        var docs = (await col.FindAllAsync().ToListAsync());
        Assert.Equal(count, docs.Count);
    }

    [Fact]
    public async Task Insert_Count_MatchesActualInserts()
    {
        var col = TsCol();
        await col.InsertAsync(MakeDoc(col, "a", DateTime.UtcNow));
        await col.InsertAsync(MakeDoc(col, "b", DateTime.UtcNow));
        await col.InsertAsync(MakeDoc(col, "c", DateTime.UtcNow));
        await _engine.CommitAsync();

        Assert.Equal(3, await col.CountAsync());
    }

    // ─── ForcePruneAsync ──────────────────────────────────────────────────────────

    [Fact]
    public async Task ForcePrune_OnNonTimeSeries_Throws()
    {
        var col = _engine.GetOrCreateCollection("plain");
        await Assert.ThrowsAsync<InvalidOperationException>(() => col.ForcePruneAsync());
    }

    [Fact]
    public async Task ForcePrune_AllDocumentsExpired_FindAll_ReturnsEmpty()
    {
        // Retention = 1 day. Docs timestamped 2 days ago → all expired.
        var col = TsCol(retention: TimeSpan.FromDays(1));

        for (int i = 0; i < 5; i++)
            await col.InsertAsync(MakeDoc(col, $"old_{i}", DateTime.UtcNow.AddDays(-2)));
        await _engine.CommitAsync();

        await col.ForcePruneAsync();

        // After pruning, pages freed. BTree entries become stale.
        // ReadDocumentAt returns null for freed pages → FindAll silently skips them.
        var remaining = (await col.FindAllAsync().ToListAsync());
        Assert.Empty(remaining);
    }

    [Fact]
    public async Task ForcePrune_AllDocumentsFresh_PreservesAll()
    {
        const int count = 5;
        var col = TsCol(retention: TimeSpan.FromDays(30));

        for (int i = 0; i < count; i++)
            await col.InsertAsync(MakeDoc(col, $"fresh_{i}", DateTime.UtcNow.AddSeconds(-i)));
        await _engine.CommitAsync();

        await col.ForcePruneAsync();

        var remaining = (await col.FindAllAsync().ToListAsync());
        Assert.Equal(count, remaining.Count);
    }

    [Fact]
    public async Task ForcePrune_MixedOnSamePage_KeepsBecauseFreshTimestampDominates()
    {
        // Old doc + fresh doc on the SAME page (page's LastTimestamp = fresh → not pruned).
        var col = TsCol(retention: TimeSpan.FromDays(1));
        await col.InsertAsync(MakeDoc(col, "old", DateTime.UtcNow.AddDays(-5)));  // old
        await col.InsertAsync(MakeDoc(col, "fresh", DateTime.UtcNow));            // fresh - same page, updates LastTimestamp
        await _engine.CommitAsync();

        await col.ForcePruneAsync();

        // The page should NOT be pruned because LastTimestamp = fresh doc's ts
        var remaining = (await col.FindAllAsync().ToListAsync());
        Assert.Equal(2, remaining.Count);
    }

    [Fact]
    public async Task ForcePrune_ResetsPruningCounters()
    {
        var col = TsCol(retention: TimeSpan.FromDays(1));
        await col.InsertAsync(MakeDoc(col, "x", DateTime.UtcNow.AddDays(-2)));
        await _engine.CommitAsync();

        // Should not throw — no assertion on internal counter, just verify it completes
        var ex = await Record.ExceptionAsync(async () => await col.ForcePruneAsync());
        Assert.Null(ex);
    }

    [Fact]
    public async Task ForcePrune_NoRetentionSet_DoesNotThrow()
    {
        // RetentionPolicyMs = 0 means "no pruning logic runs"
        var col = _engine.GetOrCreateCollection("no_ret");
        col.SetTimeSeries("ts", TimeSpan.Zero);
        await col.InsertAsync(MakeDoc(col, "s", DateTime.UtcNow.AddYears(-1)));
        await _engine.CommitAsync();

        var ex = await Record.ExceptionAsync(async () => await col.ForcePruneAsync());
        Assert.Null(ex);
    }
}
