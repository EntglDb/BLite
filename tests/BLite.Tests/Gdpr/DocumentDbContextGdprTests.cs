using BLite.Bson;
using BLite.Core;
using BLite.Core.GDPR;

namespace BLite.Tests.Gdpr;

/// <summary>
/// Symmetry tests verifying that <see cref="GdprDocumentDbContextExtensions"/> exposes
/// the same GDPR surface as <see cref="GdprEngineExtensions"/> and produces equivalent
/// reports when both are pointed at the same physical database.
/// </summary>
/// <remarks>
/// All seeding goes through the typed <see cref="MinimalDbContext"/> (whose
/// <see cref="DocumentCollection{TId,T}.InsertAsync"/> calls go through the
/// source-generator-produced mapper).  We never mix raw <see cref="BLiteEngine"/> writes
/// with a <see cref="DocumentDbContext"/> open on the same file, because
/// <c>DocumentDbContext.DropOrphanCollections</c> deletes any collection not registered
/// in the typed model — that would silently wipe an engine-seeded catalog.
/// The engine side of every symmetry test is read-only (<see cref="BLiteEngine"/> does
/// not drop on open), so it observes whatever the typed context persisted.
/// </remarks>
public class DocumentDbContextGdprTests : IDisposable
{
    private readonly string _dbPath;

    public DocumentDbContextGdprTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_gdpr_ctx_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    private static SubjectQuery NameQuery(string value) => new()
    {
        FieldName  = "name",
        FieldValue = BsonValue.FromString(value),
    };

    // ── ExportSubjectDataAsync — DocumentDbContext path ──────────────────────

    [Fact]
    public async Task ExportSubjectData_OnContext_FindsMatchingDocument()
    {
        using (var db = new MinimalDbContext(_dbPath))
        {
            await db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
            await db.Users.InsertAsync(new User { Name = "Bob",   Age = 40 });
            await db.SaveChangesAsync();
        }

        using var ctx = new MinimalDbContext(_dbPath);
        await using var report = await ctx.ExportSubjectDataAsync(NameQuery("Alice"));

        Assert.True(report.DataByCollection.TryGetValue("Users", out var docs));
        Assert.Single(docs);
        Assert.True(docs[0].TryGetValue("name", out var name));
        Assert.Equal("Alice", name.AsString);
    }

    [Fact]
    public async Task ExportSubjectData_OnContext_AndOnEngine_AreEquivalent()
    {
        // Seed via the typed context.
        using (var db = new MinimalDbContext(_dbPath))
        {
            await db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
            await db.Users.InsertAsync(new User { Name = "Bob",   Age = 40 });
            await db.SaveChangesAsync();
        }

        var query = NameQuery("Alice");

        // Engine read-only on the same file (does NOT drop orphan collections).
        SubjectDataReport engineReport;
        using (var engine = new BLiteEngine(_dbPath))
        {
            engineReport = await engine.ExportSubjectDataAsync(query);
        }

        // Context read-only on the same model — orphan-drop is a no-op because
        // every collection on disk is registered in MinimalDbContext.
        SubjectDataReport ctxReport;
        using (var ctx = new MinimalDbContext(_dbPath))
        {
            ctxReport = await ctx.ExportSubjectDataAsync(query);
        }

        try
        {
            Assert.Equal(
                engineReport.DataByCollection.Keys.OrderBy(k => k, StringComparer.OrdinalIgnoreCase),
                ctxReport.DataByCollection.Keys.OrderBy(k => k, StringComparer.OrdinalIgnoreCase));

            foreach (var key in engineReport.DataByCollection.Keys)
            {
                Assert.Equal(
                    engineReport.DataByCollection[key].Count,
                    ctxReport.DataByCollection[key].Count);
            }

            Assert.Equal(engineReport.SubjectId, ctxReport.SubjectId);
        }
        finally
        {
            await engineReport.DisposeAsync();
            await ctxReport.DisposeAsync();
        }
    }

    // ── InspectDatabase — DocumentDbContext path ─────────────────────────────

    [Fact]
    public async Task InspectDatabase_OnContext_ReturnsReport()
    {
        using (var db = new MinimalDbContext(_dbPath))
        {
            await db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
            await db.SaveChangesAsync();
        }

        using var ctx = new MinimalDbContext(_dbPath);
        var report = ctx.InspectDatabase();

        Assert.NotNull(report);
        Assert.Equal(_dbPath, report.DatabasePath);
        Assert.False(report.IsEncrypted);
        Assert.False(report.IsMultiFileMode);
        Assert.Contains(report.Collections, c => string.Equals(c.Name, "Users", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task InspectDatabase_OnContext_AndOnEngine_AreEquivalent()
    {
        using (var db = new MinimalDbContext(_dbPath))
        {
            await db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
            await db.Users.InsertAsync(new User { Name = "Bob",   Age = 40 });
            await db.SaveChangesAsync();
        }

        DatabaseInspectionReport engineReport;
        using (var engine = new BLiteEngine(_dbPath))
        {
            engineReport = engine.InspectDatabase();
        }

        DatabaseInspectionReport ctxReport;
        using (var ctx = new MinimalDbContext(_dbPath))
        {
            ctxReport = ctx.InspectDatabase();
        }

        Assert.Equal(engineReport.DatabasePath,    ctxReport.DatabasePath);
        Assert.Equal(engineReport.IsEncrypted,     ctxReport.IsEncrypted);
        Assert.Equal(engineReport.IsAuditEnabled,  ctxReport.IsAuditEnabled);
        Assert.Equal(engineReport.IsMultiFileMode, ctxReport.IsMultiFileMode);
        Assert.Equal(
            engineReport.Collections.Select(c => c.Name).OrderBy(n => n, StringComparer.OrdinalIgnoreCase),
            ctxReport.Collections.Select(c => c.Name).OrderBy(n => n, StringComparer.OrdinalIgnoreCase));
    }
}
