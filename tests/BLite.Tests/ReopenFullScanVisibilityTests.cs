using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Covers read visibility after a reopen without an explicit ForceCheckpoint: WAL recovery must make
/// uncheckpointed writes visible to point lookups and to full scans (AsQueryable, FindAllAsync) alike.
/// </summary>
public class ReopenFullScanVisibilityTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;

    public ReopenFullScanVisibilityTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"docdb_reopen_scan_{Guid.NewGuid()}.db");
        _walPath = Path.ChangeExtension(_dbPath, ".wal");
    }

    [Fact]
    public async Task Reopen_WithoutCheckpoint_PointLookupFindsRow()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            await db.StringEntities.InsertAsync(new StringEntity { Id = "node1", Value = "v1" });
            await db.SaveChangesAsync();
        }

        using (var db = new TestDbContext(_dbPath))
        {
            var found = await db.StringEntities.FindByIdAsync("node1");
            Assert.NotNull(found);
        }
    }

    [Fact]
    public async Task Reopen_WithoutCheckpoint_AsQueryableSeesRow()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            await db.StringEntities.InsertAsync(new StringEntity { Id = "node1", Value = "v1" });
            await db.SaveChangesAsync();
        }

        using (var db = new TestDbContext(_dbPath))
        {
            var all = await db.StringEntities.AsQueryable().ToListAsync();
            Assert.Contains(all, e => e.Id == "node1");
        }
    }

    [Fact]
    public async Task Reopen_WithoutCheckpoint_FindAllAsyncSeesRow()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            await db.StringEntities.InsertAsync(new StringEntity { Id = "node1", Value = "v1" });
            await db.SaveChangesAsync();
        }

        using (var db = new TestDbContext(_dbPath))
        {
            var all = new List<StringEntity>();
            await foreach (var e in db.StringEntities.FindAllAsync())
                all.Add(e);
            Assert.Contains(all, e => e.Id == "node1");
        }
    }

    [Fact]
    public async Task Reopen_AfterForceCheckpoint_AsQueryableSeesRow()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            await db.StringEntities.InsertAsync(new StringEntity { Id = "node1", Value = "v1" });
            await db.SaveChangesAsync();
            db.ForceCheckpoint();
        }

        using (var db = new TestDbContext(_dbPath))
        {
            var all = await db.StringEntities.AsQueryable().ToListAsync();
            Assert.Contains(all, e => e.Id == "node1");
        }
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);
    }
}
