using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Tests for TimeSeries support via the typed DocumentDbContext fluent API.
/// Covers: HasTimeSeries configuration, metadata persistence, insert routing,
/// FindAll/FindById deserialization, retention pruning, and restart persistence.
/// </summary>
public class TypedTimeSeriesTests : IDisposable
{
    private readonly string _dbPath;
    private TestDbContext _db;

    public TypedTimeSeriesTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"typed_ts_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db?.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ─── Configuration ────────────────────────────────────────────────────────

    [Fact]
    public async Task HasTimeSeries_PersistsIsTimeSeriesFlag_ToStorage()
    {
        // Metadata is written by DocumentDbContext during CreateCollection.
        // Verify it survives a full restart.
        _db.Dispose();
        using var reopened = new TestDbContext(_dbPath);

        // If metadata wasn't persisted the collection would throw on insert
        var ex = await Record.ExceptionAsync(async () =>
        {
            await reopened.SensorReadings.InsertAsync(new SensorReading
            {
                SensorId = "s1",
                Value = 1.0,
                Timestamp = DateTime.UtcNow
            });
            await reopened.SaveChangesAsync();
        });

        Assert.Null(ex);
    }

    // ─── Insert + FindAll ────────────────────────────────────────────────────

    [Fact]
    public async Task Insert_Single_FindAll_ReturnsOneDocument()
    {
        await _db.SensorReadings.InsertAsync(new SensorReading { SensorId = "s1", Value = 42.5, Timestamp = DateTime.UtcNow });
        await _db.SaveChangesAsync();

        var results = (await _db.SensorReadings.FindAllAsync().ToListAsync());

        Assert.Single(results);
        Assert.Equal("s1", results[0].SensorId);
        Assert.Equal(42.5, results[0].Value, precision: 6);
    }

    [Fact]
    public async Task Insert_Multiple_FindAll_ReturnsAll()
    {
        const int count = 10;
        for (int i = 0; i < count; i++)
            await _db.SensorReadings.InsertAsync(new SensorReading { SensorId = $"s{i}", Value = i, Timestamp = DateTime.UtcNow.AddSeconds(-i) });
        await _db.SaveChangesAsync();

        var results = (await _db.SensorReadings.FindAllAsync().ToListAsync());
        Assert.Equal(count, results.Count);
    }

    [Fact]
    public async Task Insert_FindById_ReturnsCorrectDocument()
    {
        var reading = new SensorReading { SensorId = "probe-X1", Value = 98.6, Timestamp = DateTime.UtcNow };
        var id = await _db.SensorReadings.InsertAsync(reading);
        await _db.SaveChangesAsync();
        var found = await _db.SensorReadings.FindByIdAsync(id);

        Assert.NotNull(found);
        Assert.Equal("probe-X1", found!.SensorId);
        Assert.Equal(98.6, found.Value, precision: 6);
    }

    [Fact]
    public async Task Insert_PreservesTimestampField()
    {
        var ts = new DateTime(2025, 6, 15, 12, 0, 0, DateTimeKind.Utc);
        await _db.SensorReadings.InsertAsync(new SensorReading { SensorId = "clock", Value = 0, Timestamp = ts });
        await _db.SaveChangesAsync();
        var found = (await _db.SensorReadings.FindAllAsync().ToListAsync()).Single();

        Assert.Equal(ts, found.Timestamp);
    }

    // ─── Pruning ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task ForcePrune_AllExpired_FindAll_ReturnsEmpty()
    {
        var retention = TimeSpan.FromDays(7);
        var expiredTs = DateTime.UtcNow.Subtract(retention).AddDays(-1); // 1 day beyond retention

        for (int i = 0; i < 5; i++)
            await _db.SensorReadings.InsertAsync(new SensorReading { SensorId = $"old-{i}", Value = i, Timestamp = expiredTs });
        await _db.SaveChangesAsync();

        Assert.Equal(5, await _db.SensorReadings.CountAsync());

        await _db.SensorReadings.ForcePruneAsync();

        // Stale index entries pointing to freed pages are silently skipped (v1 known behaviour).
        Assert.Empty(await _db.SensorReadings.FindAllAsync().ToListAsync());
    }

    [Fact]
    public async Task ForcePrune_RecentDocuments_NotRemoved()
    {
        await _db.SensorReadings.InsertAsync(new SensorReading { SensorId = "recent", Value = 1, Timestamp = DateTime.UtcNow });
        await _db.SaveChangesAsync();

        await _db.SensorReadings.ForcePruneAsync();

        // The page holds a recent document, so it must not be freed.
        Assert.Single(await _db.SensorReadings.FindAllAsync().ToListAsync());
    }

    // ─── Persistence across restart ──────────────────────────────────────────

    [Fact]
    public async Task InsertedDocuments_SurviveDbRestart()
    {
        await _db.SensorReadings.InsertAsync(new SensorReading { SensorId = "restart-test", Value = 3.14, Timestamp = DateTime.UtcNow });
        await _db.SaveChangesAsync();
        _db.Dispose();

        using var reopened = new TestDbContext(_dbPath);
        var results = await reopened.SensorReadings.FindAllAsync().ToListAsync();

        Assert.Single(results);
        Assert.Equal("restart-test", results[0].SensorId);
        Assert.Equal(3.14, results[0].Value, precision: 6);
    }
}
