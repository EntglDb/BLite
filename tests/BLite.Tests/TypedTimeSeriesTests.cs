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
    public void HasTimeSeries_PersistsIsTimeSeriesFlag_ToStorage()
    {
        // Metadata is written by DocumentDbContext during CreateCollection.
        // Verify it survives a full restart.
        _db.Dispose();
        using var reopened = new TestDbContext(_dbPath);

        // If metadata wasn't persisted the collection would throw on insert
        var ex = Record.Exception(() =>
        {
            reopened.SensorReadings.Insert(new SensorReading
            {
                SensorId = "s1",
                Value = 1.0,
                Timestamp = DateTime.UtcNow
            });
            reopened.SaveChanges();
        });

        Assert.Null(ex);
    }

    // ─── Insert + FindAll ────────────────────────────────────────────────────

    [Fact]
    public void Insert_Single_FindAll_ReturnsOneDocument()
    {
        _db.SensorReadings.Insert(new SensorReading { SensorId = "s1", Value = 42.5, Timestamp = DateTime.UtcNow });
        _db.SaveChanges();

        var results = _db.SensorReadings.FindAll().ToList();

        Assert.Single(results);
        Assert.Equal("s1", results[0].SensorId);
        Assert.Equal(42.5, results[0].Value, precision: 6);
    }

    [Fact]
    public void Insert_Multiple_FindAll_ReturnsAll()
    {
        const int count = 10;
        for (int i = 0; i < count; i++)
            _db.SensorReadings.Insert(new SensorReading { SensorId = $"s{i}", Value = i, Timestamp = DateTime.UtcNow.AddSeconds(-i) });
        _db.SaveChanges();

        var results = _db.SensorReadings.FindAll().ToList();
        Assert.Equal(count, results.Count);
    }

    [Fact]
    public void Insert_FindById_ReturnsCorrectDocument()
    {
        var reading = new SensorReading { SensorId = "probe-X1", Value = 98.6, Timestamp = DateTime.UtcNow };
        var id = _db.SensorReadings.Insert(reading);
        _db.SaveChanges();

        var found = _db.SensorReadings.FindById(id);

        Assert.NotNull(found);
        Assert.Equal("probe-X1", found!.SensorId);
        Assert.Equal(98.6, found.Value, precision: 6);
    }

    [Fact]
    public void Insert_PreservesTimestampField()
    {
        var ts = new DateTime(2025, 6, 15, 12, 0, 0, DateTimeKind.Utc);
        _db.SensorReadings.Insert(new SensorReading { SensorId = "clock", Value = 0, Timestamp = ts });
        _db.SaveChanges();

        var found = _db.SensorReadings.FindAll().Single();

        Assert.Equal(ts, found.Timestamp);
    }

    // ─── Pruning ─────────────────────────────────────────────────────────────

    [Fact]
    public void ForcePrune_AllExpired_FindAll_ReturnsEmpty()
    {
        var retention = TimeSpan.FromDays(7);
        var expiredTs = DateTime.UtcNow.Subtract(retention).AddDays(-1); // 1 day beyond retention

        for (int i = 0; i < 5; i++)
            _db.SensorReadings.Insert(new SensorReading { SensorId = $"old-{i}", Value = i, Timestamp = expiredTs });
        _db.SaveChanges();

        Assert.Equal(5, _db.SensorReadings.Count());

        _db.SensorReadings.ForcePrune();

        // Stale index entries pointing to freed pages are silently skipped (v1 known behaviour).
        Assert.Equal(0, _db.SensorReadings.FindAll().Count());
    }

    [Fact]
    public void ForcePrune_RecentDocuments_NotRemoved()
    {
        _db.SensorReadings.Insert(new SensorReading { SensorId = "recent", Value = 1, Timestamp = DateTime.UtcNow });
        _db.SaveChanges();

        _db.SensorReadings.ForcePrune();

        // The page holds a recent document, so it must not be freed.
        Assert.Equal(1, _db.SensorReadings.FindAll().Count());
    }

    // ─── Persistence across restart ──────────────────────────────────────────

    [Fact]
    public void InsertedDocuments_SurviveDbRestart()
    {
        _db.SensorReadings.Insert(new SensorReading { SensorId = "restart-test", Value = 3.14, Timestamp = DateTime.UtcNow });
        _db.SaveChanges();
        _db.Dispose();

        using var reopened = new TestDbContext(_dbPath);
        var results = reopened.SensorReadings.FindAll().ToList();

        Assert.Single(results);
        Assert.Equal("restart-test", results[0].SensorId);
        Assert.Equal(3.14, results[0].Value, precision: 6);
    }
}
