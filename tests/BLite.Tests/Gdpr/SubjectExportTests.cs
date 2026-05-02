using System.Text;
using System.Text.Json;
using BLite.Bson;
using BLite.Core;
using BLite.Core.GDPR;

namespace BLite.Tests.Gdpr;

/// <summary>
/// Integration tests for <see cref="GdprEngineExtensions.ExportSubjectDataAsync"/>
/// and the <see cref="SubjectDataReport"/> export methods.
/// </summary>
public class SubjectExportTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;

    public SubjectExportTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_gdpr_export_{Guid.NewGuid():N}.db");
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

    private BsonDocument MakeUserDoc(string email, string name)
    {
        var col = _engine.GetOrCreateCollection("users");
        return col.CreateDocument(
            ["email", "name"],
            b => b.AddString("email", email).AddString("name", name));
    }

    private BsonDocument MakeDoc(string collection, string fieldName, string fieldValue)
    {
        var col = _engine.GetOrCreateCollection(collection);
        return col.CreateDocument([fieldName], b => b.AddString(fieldName, fieldValue));
    }

    private BsonDocument MakeDocInt(string collection, string fieldName, int fieldValue)
    {
        var col = _engine.GetOrCreateCollection(collection);
        return col.CreateDocument([fieldName], b => b.AddInt32(fieldName, fieldValue));
    }

    // ── ExportSubjectDataAsync basic matching ──────────────────────────────────

    [Fact]
    public async Task ExportSubjectData_ReturnsMatchingDocuments()
    {
        await _engine.InsertAsync("users", MakeUserDoc("alice@example.com", "Alice"));
        await _engine.InsertAsync("users", MakeUserDoc("bob@example.com", "Bob"));

        var query = new SubjectQuery
        {
            FieldName  = "email",
            FieldValue = BsonValue.FromString("alice@example.com"),
        };
        await using var report = await _engine.ExportSubjectDataAsync(query);

        Assert.True(report.DataByCollection.TryGetValue("users", out var docs));
        Assert.Single(docs);
        Assert.True(docs[0].TryGetValue("name", out var nameVal));
        Assert.Equal("Alice", nameVal.AsString);
    }

    [Fact]
    public async Task ExportSubjectData_EmptyReport_WhenNoMatchFound()
    {
        await _engine.InsertAsync("users", MakeDoc("users", "email", "bob@example.com"));

        var query = new SubjectQuery
        {
            FieldName  = "email",
            FieldValue = BsonValue.FromString("nobody@example.com"),
        };
        await using var report = await _engine.ExportSubjectDataAsync(query);

        Assert.True(report.DataByCollection.ContainsKey("users"));
        Assert.Empty(report.DataByCollection["users"]);
    }

    [Fact]
    public async Task ExportSubjectData_UnrelatedCollections_AppearWithEmptyLists()
    {
        await _engine.InsertAsync("users",   MakeDoc("users",   "email",    "alice@example.com"));
        await _engine.InsertAsync("orders",  MakeDocInt("orders",  "orderId", 1));
        await _engine.InsertAsync("reviews", MakeDocInt("reviews", "reviewId", 1));

        var query = new SubjectQuery
        {
            FieldName  = "email",
            FieldValue = BsonValue.FromString("alice@example.com"),
        };
        await using var report = await _engine.ExportSubjectDataAsync(query);

        // Collections without the field appear with empty lists — never omitted.
        Assert.True(report.DataByCollection.ContainsKey("orders"));
        Assert.Empty(report.DataByCollection["orders"]);
        Assert.True(report.DataByCollection.ContainsKey("reviews"));
        Assert.Empty(report.DataByCollection["reviews"]);
    }

    [Fact]
    public async Task ExportSubjectData_RestrictToSpecifiedCollections()
    {
        await _engine.InsertAsync("users",  MakeDoc("users",  "email", "alice@example.com"));
        await _engine.InsertAsync("orders", MakeDoc("orders", "email", "alice@example.com"));

        var query = new SubjectQuery
        {
            FieldName   = "email",
            FieldValue  = BsonValue.FromString("alice@example.com"),
            Collections = new[] { "users" },
        };
        await using var report = await _engine.ExportSubjectDataAsync(query);

        // Only "users" was requested; "orders" should not appear.
        Assert.True(report.DataByCollection.ContainsKey("users"));
        Assert.False(report.DataByCollection.ContainsKey("orders"));
    }

    [Fact]
    public async Task ExportSubjectData_SubjectId_EqualsFieldValue()
    {
        await _engine.InsertAsync("users", MakeDoc("users", "email", "alice@example.com"));

        var fieldValue = BsonValue.FromString("alice@example.com");
        var query = new SubjectQuery { FieldName = "email", FieldValue = fieldValue };
        await using var report = await _engine.ExportSubjectDataAsync(query);

        Assert.Equal(fieldValue, report.SubjectId);
    }

    [Fact]
    public async Task ExportSubjectData_GeneratedAt_IsRecentUtc()
    {
        var query = new SubjectQuery
        {
            FieldName  = "email",
            FieldValue = BsonValue.FromString("nobody@example.com"),
        };
        var before = DateTimeOffset.UtcNow.AddSeconds(-1);
        await using var report = await _engine.ExportSubjectDataAsync(query);
        var after  = DateTimeOffset.UtcNow.AddSeconds(1);

        Assert.True(report.GeneratedAt >= before && report.GeneratedAt <= after,
            $"GeneratedAt {report.GeneratedAt} should be between {before} and {after}.");
    }

    // ── CancellationToken ─────────────────────────────────────────────────────

    [Fact]
    public async Task ExportSubjectData_HonoursCancellationToken()
    {
        // Insert enough documents to give cancellation a chance to fire.
        var col = _engine.GetOrCreateCollection("items");
        for (int i = 0; i < 50; i++)
            await _engine.InsertAsync("items",
                col.CreateDocument(["userId"], b => b.AddInt32("userId", i)));

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately.

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => _engine.ExportSubjectDataAsync(
                new SubjectQuery { FieldName = "userId", FieldValue = BsonValue.FromInt32(0) },
                cts.Token));
    }

    // ── ExportAsJsonAsync ──────────────────────────────────────────────────────

    [Fact]
    public async Task ExportAsJson_ProducesValidUtf8Json()
    {
        var col = _engine.GetOrCreateCollection("users");
        await _engine.InsertAsync("users",
            col.CreateDocument(["email", "age"],
                b => b.AddString("email", "alice@example.com").AddInt32("age", 30)));

        var query = new SubjectQuery { FieldName = "email", FieldValue = BsonValue.FromString("alice@example.com") };
        await using var report = await _engine.ExportSubjectDataAsync(query);

        using var ms = new MemoryStream();
        await report.ExportAsJsonAsync(ms);
        ms.Position = 0;

        using var doc = JsonDocument.Parse(ms);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("generatedAt", out _));
        Assert.True(root.TryGetProperty("subjectId", out _));
        Assert.True(root.TryGetProperty("data", out var data));
        Assert.True(data.TryGetProperty("users", out var usersArr));
        Assert.Equal(1, usersArr.GetArrayLength());
    }

    [Fact]
    public async Task ExportAsJson_EmptyCollections_StillPresent()
    {
        // Force the collection to exist with no matching data.
        var col = _engine.GetOrCreateCollection("logs");
        await _engine.InsertAsync("logs", col.CreateDocument(["ts"], b => b.AddInt32("ts", 1)));

        var query = new SubjectQuery { FieldName = "email", FieldValue = BsonValue.FromString("nobody") };
        await using var report = await _engine.ExportSubjectDataAsync(query);

        using var ms = new MemoryStream();
        await report.ExportAsJsonAsync(ms);
        ms.Position = 0;

        using var doc = JsonDocument.Parse(ms);
        var data = doc.RootElement.GetProperty("data");
        Assert.True(data.TryGetProperty("logs", out var arr));
        Assert.Equal(0, arr.GetArrayLength());
    }

    // ── ExportAsCsvAsync ──────────────────────────────────────────────────────

    [Fact]
    public async Task ExportAsCsv_EmitsHeaderAndDataRows()
    {
        await _engine.InsertAsync("users", MakeUserDoc("alice@example.com", "Alice"));

        var query = new SubjectQuery { FieldName = "email", FieldValue = BsonValue.FromString("alice@example.com") };
        await using var report = await _engine.ExportSubjectDataAsync(query);

        using var ms = new MemoryStream();
        await report.ExportAsCsvAsync(ms, "users");
        var csv = Encoding.UTF8.GetString(ms.ToArray());

        var lines = csv.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.True(lines.Length >= 2, "Expected header row + at least one data row.");
        // Header row must contain field names
        Assert.Contains("email", lines[0]);
    }

    // ── ExportAsBsonAsync ─────────────────────────────────────────────────────

    [Fact]
    public async Task ExportAsBson_RoundTrips_DocumentsCorrectly()
    {
        var col = _engine.GetOrCreateCollection("users");
        var original = col.CreateDocument(
            ["email", "age"],
            b => b.AddString("email", "alice@example.com").AddInt32("age", 30));
        await _engine.InsertAsync("users", original);

        var query = new SubjectQuery { FieldName = "email", FieldValue = BsonValue.FromString("alice@example.com") };
        await using var report = await _engine.ExportSubjectDataAsync(query);

        using var ms = new MemoryStream();
        await report.ExportAsBsonAsync(ms);

        // The stream must be non-empty for a non-empty result set.
        Assert.True(ms.Length > 0, "BSON export of a matched document should produce bytes.");

        ms.Position = 0;
        // Standard BSON: first 4 bytes = total document size (little-endian, size includes itself).
        var sizeBuffer = new byte[4];
        int bytesRead = ms.Read(sizeBuffer, 0, 4);
        Assert.Equal(4, bytesRead);
        int bsonDocSize = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(sizeBuffer);
        Assert.True(bsonDocSize > 4, "BSON document size must be > 4 (includes the 4-byte size field).");

        // Read the full document from the start (size is inclusive).
        ms.Position = 0;
        var bsonBytes = new byte[bsonDocSize];
        int docBytesRead = ms.Read(bsonBytes, 0, bsonDocSize);
        Assert.Equal(bsonDocSize, docBytesRead);

        // Verify: the raw BSON data should contain the email value (as UTF-8 bytes)
        var emailUtf8 = System.Text.Encoding.UTF8.GetBytes("alice@example.com");
        bool containsEmail = ContainsBytes(bsonBytes, emailUtf8);
        Assert.True(containsEmail, "Serialised BSON should contain the email string bytes.");
    }

    private static bool ContainsBytes(byte[] source, byte[] pattern)
    {
        for (int i = 0; i <= source.Length - pattern.Length; i++)
        {
            bool found = true;
            for (int j = 0; j < pattern.Length; j++)
            {
                if (source[i + j] != pattern[j]) { found = false; break; }
            }
            if (found) return true;
        }
        return false;
    }
}
