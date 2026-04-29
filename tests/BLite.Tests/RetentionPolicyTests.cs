using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using BLite.Core.Retention;

namespace BLite.Tests;

/// <summary>
/// Tests for the generalized retention policy system for all collection types.
/// Covers: DynamicCollection (via BLiteEngine) and typed DocumentCollection (via DocumentDbContext).
/// </summary>
public class RetentionPolicyTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;

    public RetentionPolicyTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"retention_{Guid.NewGuid()}.db");
        _engine = new BLiteEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ─── Helper factories ─────────────────────────────────────────────────────

    private DynamicCollection GetCol(string name = "test")
        => _engine.GetOrCreateCollection(name);

    private BsonDocument MakeDoc(DynamicCollection col, string name, DateTime? createdAt = null)
    {
        if (createdAt.HasValue)
            return col.CreateDocument(["name", "createdat"], b => b
                .AddString("name", name)
                .AddDateTime("createdat", createdAt.Value));
        return col.CreateDocument(["name"], b => b
            .AddString("name", name));
    }

    // ─── SetRetentionPolicy ───────────────────────────────────────────────────

    [Fact]
    public void SetRetentionPolicy_PersistsToStorage()
    {
        var col = GetCol("persist_test");
        col.SetRetentionPolicy(p => p
            .MaxDocumentCount(100)
            .TriggerOn(RetentionTrigger.OnInsert));

        // Verify it can be re-read after engine restart
        _engine.Dispose();
        using var engine2 = new BLiteEngine(_dbPath);
        var meta = engine2.Storage.GetCollectionMetadata("persist_test");
        Assert.NotNull(meta?.GeneralRetentionPolicy);
        Assert.Equal(100, meta!.GeneralRetentionPolicy!.MaxDocumentCount);
        Assert.Equal(RetentionTrigger.OnInsert, meta.GeneralRetentionPolicy.Triggers);
    }

    [Fact]
    public void SetRetentionPolicy_WithAllProperties_Persists()
    {
        var col = GetCol("all_props");
        col.SetRetentionPolicy(p => p
            .MaxAge(TimeSpan.FromDays(30))
            .MaxDocumentCount(500)
            .MaxSizeBytes(1024 * 1024)
            .OnField("createdat")
            .ScheduleInterval(TimeSpan.FromMinutes(10))
            .TriggerOn(RetentionTrigger.OnInsert | RetentionTrigger.Scheduled));

        _engine.Dispose();
        using var engine2 = new BLiteEngine(_dbPath);
        var meta = engine2.Storage.GetCollectionMetadata("all_props");
        var rp = meta?.GeneralRetentionPolicy;

        Assert.NotNull(rp);
        Assert.Equal((long)TimeSpan.FromDays(30).TotalMilliseconds, rp!.MaxAgeMs);
        Assert.Equal(500, rp.MaxDocumentCount);
        Assert.Equal(1024 * 1024, rp.MaxSizeBytes);
        Assert.Equal("createdat", rp.TimestampField);
        Assert.Equal((long)TimeSpan.FromMinutes(10).TotalMilliseconds, rp.ScheduledIntervalMs);
        Assert.Equal(RetentionTrigger.OnInsert | RetentionTrigger.Scheduled, rp.Triggers);
    }

    // ─── MaxAge ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task MaxAge_DeletesDocumentsOlderThanCutoff()
    {
        var col = GetCol("max_age");
        col.SetRetentionPolicy(p => p
            .MaxAge(TimeSpan.FromHours(1))
            .OnField("createdat")
            .TriggerOn(RetentionTrigger.None));

        // Insert old docs (older than 1 hour) and new docs
        var old = DateTime.UtcNow.AddHours(-2);
        var now = DateTime.UtcNow;

        await col.InsertAsync(MakeDoc(col, "old1", old));
        await col.InsertAsync(MakeDoc(col, "old2", old));
        await col.InsertAsync(MakeDoc(col, "new1", now));
        await col.InsertAsync(MakeDoc(col, "new2", now));

        Assert.Equal(4, await col.CountAsync());

        await col.ForceApplyRetentionPolicyAsync();

        Assert.Equal(2, await col.CountAsync());

        var remaining = new List<BsonDocument>();
        await foreach (var doc in col.FindAllAsync())
            remaining.Add(doc);

        Assert.All(remaining, doc =>
        {
            Assert.True(doc.TryGetValue("name", out var n));
            Assert.StartsWith("new", n.AsString);
        });
    }

    [Fact]
    public async Task MaxAge_ExemptsDocumentsWithNoTimestampField()
    {
        var col = GetCol("max_age_exempt");
        col.SetRetentionPolicy(p => p
            .MaxAge(TimeSpan.FromHours(1))
            .OnField("createdat")
            .TriggerOn(RetentionTrigger.None));

        // Old document WITH timestamp → should be deleted
        var old = DateTime.UtcNow.AddHours(-2);
        await col.InsertAsync(MakeDoc(col, "old_with_ts", old));

        // Document WITHOUT timestamp → should be exempt
        await col.InsertAsync(MakeDoc(col, "no_ts_exempt"));

        Assert.Equal(2, await col.CountAsync());

        await col.ForceApplyRetentionPolicyAsync();

        // Only the exempt document should remain
        Assert.Equal(1, await col.CountAsync());
        var docs = new List<BsonDocument>();
        await foreach (var doc in col.FindAllAsync())
            docs.Add(doc);
        Assert.True(docs[0].TryGetValue("name", out var name));
        Assert.Equal("no_ts_exempt", name.AsString);
    }

    // ─── MaxDocumentCount ─────────────────────────────────────────────────────

    [Fact]
    public async Task MaxDocumentCount_DeletesOldestWhenLimitExceeded()
    {
        var col = GetCol("max_count");
        col.SetRetentionPolicy(p => p
            .MaxDocumentCount(3)
            .TriggerOn(RetentionTrigger.None));

        // Insert 5 documents
        for (int i = 1; i <= 5; i++)
            await col.InsertAsync(MakeDoc(col, $"doc{i}"));

        Assert.Equal(5, await col.CountAsync());

        await col.ForceApplyRetentionPolicyAsync();

        Assert.Equal(3, await col.CountAsync());
    }

    [Fact]
    public async Task MaxDocumentCount_WithTimestampField_DeletesOldestByTimestamp()
    {
        var col = GetCol("max_count_ts");
        col.SetRetentionPolicy(p => p
            .MaxDocumentCount(2)
            .OnField("createdat")
            .TriggerOn(RetentionTrigger.None));

        var baseTime = DateTime.UtcNow.AddDays(-5);
        // Insert in time order so "oldest" should be known
        await col.InsertAsync(MakeDoc(col, "oldest", baseTime.AddDays(0)));   // oldest
        await col.InsertAsync(MakeDoc(col, "middle", baseTime.AddDays(1)));
        await col.InsertAsync(MakeDoc(col, "newest1", baseTime.AddDays(2)));
        await col.InsertAsync(MakeDoc(col, "newest2", baseTime.AddDays(3)));  // newest

        Assert.Equal(4, await col.CountAsync());

        await col.ForceApplyRetentionPolicyAsync();

        Assert.Equal(2, await col.CountAsync());

        var remaining = new List<BsonDocument>();
        await foreach (var doc in col.FindAllAsync())
            remaining.Add(doc);

        // oldest and middle should be gone
        Assert.DoesNotContain(remaining, d =>
        {
            d.TryGetValue("name", out var n);
            return n.AsString is "oldest" or "middle";
        });
    }

    [Fact]
    public async Task MaxDocumentCount_BelowLimit_NoDeletion()
    {
        var col = GetCol("max_count_below");
        col.SetRetentionPolicy(p => p
            .MaxDocumentCount(10)
            .TriggerOn(RetentionTrigger.None));

        for (int i = 0; i < 5; i++)
            await col.InsertAsync(MakeDoc(col, $"doc{i}"));

        Assert.Equal(5, await col.CountAsync());

        await col.ForceApplyRetentionPolicyAsync();

        // No documents should be deleted
        Assert.Equal(5, await col.CountAsync());
    }

    // ─── OnInsert trigger ─────────────────────────────────────────────────────

    [Fact]
    public async Task OnInsert_TriggersRetentionOnEveryInsert()
    {
        var col = GetCol("on_insert");
        col.SetRetentionPolicy(p => p
            .MaxDocumentCount(3)
            .TriggerOn(RetentionTrigger.OnInsert));

        // Insert 5 documents - after each insert past the limit, retention fires
        for (int i = 1; i <= 5; i++)
            await col.InsertAsync(MakeDoc(col, $"doc{i}"));

        // Should be trimmed to 3
        Assert.Equal(3, await col.CountAsync());
    }

    // ─── MaxSizeBytes ─────────────────────────────────────────────────────────

    [Fact]
    public async Task MaxSizeBytes_DeletesOldestWhenSizeExceeded()
    {
        var col = GetCol("max_size");
        // Set a very small size limit so it will definitely be exceeded
        col.SetRetentionPolicy(p => p
            .MaxSizeBytes(1)  // 1 byte — always exceeded if there is data
            .TriggerOn(RetentionTrigger.None));

        for (int i = 0; i < 5; i++)
            await col.InsertAsync(MakeDoc(col, $"doc{i}"));

        Assert.Equal(5, await col.CountAsync());

        await col.ForceApplyRetentionPolicyAsync();

        // All documents should have been deleted (size limit = 1 byte)
        Assert.Equal(0, await col.CountAsync());
    }

    // ─── Scheduled trigger ───────────────────────────────────────────────────

    [Fact]
    public void ScheduledTrigger_DoesNotThrow_OnConfiguration()
    {
        var col = GetCol("scheduled");
        var ex = Record.Exception(() =>
            col.SetRetentionPolicy(p => p
                .MaxDocumentCount(100)
                .ScheduleInterval(TimeSpan.FromMinutes(1))
                .TriggerOn(RetentionTrigger.Scheduled)));

        Assert.Null(ex);
    }

    // ─── ForceApplyRetentionPolicy via BLiteEngine ────────────────────────────

    [Fact]
    public async Task BLiteEngine_ForceApplyRetentionPolicy_Works()
    {
        _engine.SetRetentionPolicy("eng_retention", p => p
            .MaxDocumentCount(2)
            .TriggerOn(RetentionTrigger.None));

        var col = GetCol("eng_retention");
        for (int i = 0; i < 5; i++)
            await col.InsertAsync(MakeDoc(col, $"doc{i}"));

        Assert.Equal(5, await col.CountAsync());

        await _engine.ForceApplyRetentionPolicyAsync("eng_retention");

        Assert.Equal(2, await col.CountAsync());
    }

    // ─── Policy survives restart ──────────────────────────────────────────────

    [Fact]
    public async Task RetentionPolicy_SurvivesEngineRestart()
    {
        // Configure policy and insert docs
        var col = GetCol("restart_test");
        col.SetRetentionPolicy(p => p
            .MaxDocumentCount(2)
            .TriggerOn(RetentionTrigger.None));

        for (int i = 0; i < 4; i++)
            await col.InsertAsync(MakeDoc(col, $"doc{i}"));

        _engine.Dispose();

        // Re-open and verify policy is loaded
        using var engine2 = new BLiteEngine(_dbPath);
        var col2 = engine2.GetOrCreateCollection("restart_test");

        Assert.Equal(4, await col2.CountAsync()); // not auto-applied, trigger=None

        await col2.ForceApplyRetentionPolicyAsync();

        Assert.Equal(2, await col2.CountAsync());
    }

    // ─── RetentionPolicyBuilder validation ───────────────────────────────────

    [Fact]
    public void RetentionPolicyBuilder_NegativeDocumentCount_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            var b = new RetentionPolicyBuilder();
            b.MaxDocumentCount(-1);
        });
    }

    [Fact]
    public void RetentionPolicyBuilder_NegativeSizeBytes_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            var b = new RetentionPolicyBuilder();
            b.MaxSizeBytes(-1);
        });
    }

    [Fact]
    public void RetentionPolicyBuilder_ZeroOrNegativeScheduleInterval_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            var b = new RetentionPolicyBuilder();
            b.ScheduleInterval(TimeSpan.Zero);
        });
    }
}

/// <summary>
/// Tests for the generalized retention policy on typed DocumentCollection
/// accessed via DocumentCollection.SetRetentionPolicy and ForceApplyRetentionPolicyAsync.
/// </summary>
public class TypedRetentionPolicyTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;

    public TypedRetentionPolicyTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"typed_retention_{Guid.NewGuid()}.db");
        _engine = new BLiteEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    private DocumentCollection<ObjectId, BLite.Shared.SensorReading> GetCollection()
        => new(
            _engine.Storage,
            _engine,
            new BLite.Tests.TestDbContext_TestDbContext_Mappers.BLite_Shared_SensorReadingMapper(),
            "sensor_typed_retention");

    [Fact]
    public async Task TypedCollection_MaxDocumentCount_DeletesOldest()
    {
        using var col = GetCollection();
        col.SetRetentionPolicy(new RetentionPolicy
        {
            MaxDocumentCount = 3,
            Triggers = RetentionTrigger.None
        });

        for (int i = 0; i < 5; i++)
            await col.InsertAsync(new BLite.Shared.SensorReading { SensorId = $"s{i}", Value = i, Timestamp = DateTime.UtcNow });

        Assert.Equal(5, await col.CountAsync());

        await col.ForceApplyRetentionPolicyAsync();

        Assert.Equal(3, await col.CountAsync());
    }

    [Fact]
    public async Task TypedCollection_MaxAge_DeletesOlderThanCutoff()
    {
        using var col = GetCollection();
        col.SetRetentionPolicy(new RetentionPolicy
        {
            MaxAgeMs = (long)TimeSpan.FromHours(1).TotalMilliseconds,
            TimestampField = "timestamp",
            Triggers = RetentionTrigger.None
        });

        var old = DateTime.UtcNow.AddHours(-2);
        var now = DateTime.UtcNow;

        await col.InsertAsync(new BLite.Shared.SensorReading { SensorId = "old1", Value = 1, Timestamp = old });
        await col.InsertAsync(new BLite.Shared.SensorReading { SensorId = "old2", Value = 2, Timestamp = old });
        await col.InsertAsync(new BLite.Shared.SensorReading { SensorId = "new1", Value = 3, Timestamp = now });

        Assert.Equal(3, await col.CountAsync());

        await col.ForceApplyRetentionPolicyAsync();

        Assert.Equal(1, await col.CountAsync());

        var results = await col.FindAllAsync().ToListAsync();
        Assert.Equal("new1", results[0].SensorId);
    }

    [Fact]
    public async Task TypedCollection_OnInsert_TriggersRetention()
    {
        using var col = GetCollection();
        col.SetRetentionPolicy(new RetentionPolicy
        {
            MaxDocumentCount = 2,
            Triggers = RetentionTrigger.OnInsert
        });

        for (int i = 0; i < 4; i++)
            await col.InsertAsync(new BLite.Shared.SensorReading { SensorId = $"s{i}", Value = i, Timestamp = DateTime.UtcNow });

        Assert.Equal(2, await col.CountAsync());
    }
}
