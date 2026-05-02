using BLite.Bson;
using BLite.Core;
using BLite.Core.GDPR;

namespace BLite.Tests.Gdpr;

/// <summary>
/// Integration tests for <see cref="GdprEngineExtensions.InspectDatabase"/>.
/// </summary>
public class DatabaseInspectionTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;

    public DatabaseInspectionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_gdpr_inspect_{Guid.NewGuid():N}.db");
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

    private BsonDocument MakeDoc(string collection, string fieldName, string value)
    {
        var col = _engine.GetOrCreateCollection(collection);
        return col.CreateDocument([fieldName], b => b.AddString(fieldName, value));
    }

    private BsonDocument MakeDocInt(string collection, string fieldName, int value)
    {
        var col = _engine.GetOrCreateCollection(collection);
        return col.CreateDocument([fieldName], b => b.AddInt32(fieldName, value));
    }

    // ── Basic inspection ───────────────────────────────────────────────────────

    [Fact]
    public void InspectDatabase_ReturnsReport_WithoutThrowing()
    {
        var report = _engine.InspectDatabase();
        Assert.NotNull(report);
    }

    [Fact]
    public void InspectDatabase_DatabasePath_MatchesEngineFile()
    {
        var report = _engine.InspectDatabase();
        Assert.Equal(_dbPath, report.DatabasePath);
    }

    [Fact]
    public void InspectDatabase_IsEncrypted_FalseForUnencryptedDb()
    {
        var report = _engine.InspectDatabase();
        Assert.False(report.IsEncrypted);
    }

    [Fact]
    public void InspectDatabase_IsMultiFileMode_FalseForSingleFile()
    {
        var report = _engine.InspectDatabase();
        Assert.False(report.IsMultiFileMode);
    }

    [Fact]
    public void InspectDatabase_IsAuditEnabled_FalseByDefault()
    {
        var report = _engine.InspectDatabase();
        Assert.False(report.IsAuditEnabled);
    }

    // ── Collection discovery ───────────────────────────────────────────────────

    [Fact]
    public async Task InspectDatabase_ListsRegisteredCollections()
    {
        await _engine.InsertAsync("customers", MakeDoc("customers", "name", "Alice"));
        await _engine.InsertAsync("invoices",  MakeDocInt("invoices", "amount", 100));

        var report = _engine.InspectDatabase();
        var names = report.Collections.Select(c => c.Name).ToList();

        Assert.Contains("customers", names);
        Assert.Contains("invoices", names);
    }

    [Fact]
    public async Task InspectDatabase_Collections_NotNullOrEmpty_AfterInsert()
    {
        await _engine.InsertAsync("products", MakeDoc("products", "sku", "X1"));

        var report = _engine.InspectDatabase();
        Assert.NotNull(report.Collections);
        Assert.NotEmpty(report.Collections);
    }

    // ── PersonalDataFields on collection ──────────────────────────────────────

    [Fact]
    public async Task InspectDatabase_PersonalDataFields_EmptyForUnannotatedCollection()
    {
        await _engine.InsertAsync("logs", MakeDocInt("logs", "ts", 1));

        var report = _engine.InspectDatabase();
        var logs = report.Collections.FirstOrDefault(c => c.Name == "logs");
        Assert.NotNull(logs);

        // Dynamic collections have no entity type → empty list, never null.
        Assert.NotNull(logs!.PersonalDataFields);
        Assert.Empty(logs.PersonalDataFields);
    }

    // ── Indexes ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task InspectDatabase_IndexList_NotNull_WhenNoIndexes()
    {
        await _engine.InsertAsync("noindex", MakeDocInt("noindex", "x", 1));

        var report = _engine.InspectDatabase();
        var col = report.Collections.FirstOrDefault(c => c.Name == "noindex");
        Assert.NotNull(col);
        Assert.NotNull(col!.Indexes);
    }

    // ── RetentionPolicy ───────────────────────────────────────────────────────

    [Fact]
    public async Task InspectDatabase_RetentionPolicy_IsNullWhenNotConfigured()
    {
        await _engine.InsertAsync("events", MakeDocInt("events", "ts", 1));

        var report = _engine.InspectDatabase();
        var events = report.Collections.FirstOrDefault(c => c.Name == "events");
        Assert.NotNull(events);
        Assert.Null(events!.RetentionPolicy);
    }

    // ── Encrypted database ────────────────────────────────────────────────────

    [Fact]
    public void InspectDatabase_EncryptedEngine_ReportsIsEncryptedTrue()
    {
        var encPath = Path.Combine(Path.GetTempPath(), $"blite_enc_inspect_{Guid.NewGuid():N}.db");
        try
        {
        var crypto = new BLite.Core.Encryption.CryptoOptions("test-passphrase-gdpr");
            using var encEngine = new BLiteEngine(encPath, crypto);
            var report = encEngine.InspectDatabase();
            Assert.True(report.IsEncrypted,
                "Engine opened with CryptoOptions should report IsEncrypted = true.");
        }
        finally
        {
            CleanupDb(encPath);
        }
    }

    // ── SubjectDataReport.WriteToFileAsync ────────────────────────────────────

    [Fact]
    public async Task WriteToFileAsync_WritesJsonFile()
    {
        await _engine.InsertAsync("users", MakeDoc("users", "email", "alice@example.com"));

        var query = new SubjectQuery
        {
            FieldName  = "email",
            FieldValue = BsonValue.FromString("alice@example.com"),
        };
        await using var report = await _engine.ExportSubjectDataAsync(query);

        var outPath = Path.Combine(Path.GetTempPath(), $"gdpr_export_{Guid.NewGuid():N}.json");
        try
        {
            await report.WriteToFileAsync(outPath, SubjectExportFormat.Json);
            Assert.True(File.Exists(outPath));
            var content = await File.ReadAllTextAsync(outPath);
            Assert.Contains("generatedAt", content);
            Assert.Contains("alice@example.com", content);
        }
        finally
        {
            if (File.Exists(outPath)) File.Delete(outPath);
        }
    }

    // ── RetentionPolicyInfo projection ─────────────────────────────────────────

    [Fact]
    public void RetentionPolicyInfo_From_ReturnsNull_WhenPolicyIsNull()
    {
        var info = RetentionPolicyInfo.From(null);
        Assert.Null(info);
    }

    [Fact]
    public void RetentionPolicyInfo_From_ProjectsMaxAge()
    {
        var policy = new BLite.Core.Retention.RetentionPolicy
        {
            MaxAgeMs = 86_400_000L, // 1 day
        };
        var info = RetentionPolicyInfo.From(policy);
        Assert.NotNull(info);
        Assert.Equal(TimeSpan.FromDays(1), info!.MaxAge);
    }

    [Fact]
    public void RetentionPolicyInfo_From_NullMaxAge_WhenZero()
    {
        var policy = new BLite.Core.Retention.RetentionPolicy { MaxAgeMs = 0 };
        var info   = RetentionPolicyInfo.From(policy);
        Assert.NotNull(info);
        Assert.Null(info!.MaxAge);
    }
}
