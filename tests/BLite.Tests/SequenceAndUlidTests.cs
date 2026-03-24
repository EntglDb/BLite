using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Verifies auto-generation of <c>int</c> / <c>long</c> IDs via the per-collection
/// sequence counter, and <c>string</c> IDs via ULID.
/// </summary>
public class SequenceAndUlidTests : IDisposable
{
    private readonly string _dbPath;

    public SequenceAndUlidTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"seq_ulid_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ── int sequence ─────────────────────────────────────────────────────────

    [Fact]
    public async Task Int_AutoIncrement_Generates_Sequential_Ids()
    {
        using var db = new TestDbContext(_dbPath);

        var id1 = await db.IntEntities.InsertAsync(new IntEntity { Name = "A" });
        var id2 = await db.IntEntities.InsertAsync(new IntEntity { Name = "B" });
        var id3 = await db.IntEntities.InsertAsync(new IntEntity { Name = "C" });
        await db.SaveChangesAsync();

        Assert.Equal(1, id1);
        Assert.Equal(2, id2);
        Assert.Equal(3, id3);
    }

    [Fact]
    public async Task Int_AutoIncrement_Ids_Are_Stored_On_Entity()
    {
        using var db = new TestDbContext(_dbPath);

        var entity = new IntEntity { Name = "Auto" };
        await db.IntEntities.InsertAsync(entity);
        await db.SaveChangesAsync();

        Assert.Equal(1, entity.Id);
    }

    [Fact]
    public async Task Int_Explicit_Id_Is_Not_Overwritten()
    {
        using var db = new TestDbContext(_dbPath);

        var entity = new IntEntity { Id = 42, Name = "Manual" };
        var id = await db.IntEntities.InsertAsync(entity);
        await db.SaveChangesAsync();

        Assert.Equal(42, id);
        var found = await db.IntEntities.FindByIdAsync(42);
        Assert.NotNull(found);
        Assert.Equal("Manual", found.Name);
    }

    [Fact]
    public async Task Int_Sequence_Persists_Across_DbContext_Reopens()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            await db.IntEntities.InsertAsync(new IntEntity { Name = "First" });
            await db.IntEntities.InsertAsync(new IntEntity { Name = "Second" });
            await db.SaveChangesAsync();
        }

        // Reopen the same database file
        using (var db2 = new TestDbContext(_dbPath))
        {
            var id3 = await db2.IntEntities.InsertAsync(new IntEntity { Name = "Third" });
            await db2.SaveChangesAsync();

            // Counter should continue from 3, not restart at 1
            Assert.Equal(3, id3);
        }
    }

    // ── long sequence ────────────────────────────────────────────────────────

    [Fact]
    public async Task Long_AutoIncrement_Generates_Sequential_Ids()
    {
        using var db = new TestDbContext(_dbPath);

        var id1 = await db.LongEntities.InsertAsync(new LongEntity { Name = "X" });
        var id2 = await db.LongEntities.InsertAsync(new LongEntity { Name = "Y" });
        await db.SaveChangesAsync();

        Assert.Equal(1L, id1);
        Assert.Equal(2L, id2);
    }

    [Fact]
    public async Task Long_Explicit_Id_Is_Not_Overwritten()
    {
        using var db = new TestDbContext(_dbPath);

        var entity = new LongEntity { Id = 999L, Name = "Manual" };
        var id = await db.LongEntities.InsertAsync(entity);
        await db.SaveChangesAsync();

        Assert.Equal(999L, id);
    }

    // ── string ULID ──────────────────────────────────────────────────────────

    [Fact]
    public async Task String_AutoGenerate_Produces_Valid_Ulid()
    {
        using var db = new TestDbContext(_dbPath);

        // Id = null! triggers auto-generation
        var entity = new StringEntity { Id = null!, Value = "test" };
        var id = await db.StringEntities.InsertAsync(entity);
        await db.SaveChangesAsync();

        Assert.NotNull(id);
        // ULID string is always 26 characters (Crockford base32)
        Assert.Equal(26, id.Length);
    }

    [Fact]
    public async Task String_AutoGenerate_Produces_Unique_Ids()
    {
        using var db = new TestDbContext(_dbPath);

        var id1 = await db.StringEntities.InsertAsync(new StringEntity { Id = null!, Value = "a" });
        var id2 = await db.StringEntities.InsertAsync(new StringEntity { Id = null!, Value = "b" });
        var id3 = await db.StringEntities.InsertAsync(new StringEntity { Id = null!, Value = "c" });
        await db.SaveChangesAsync();

        Assert.NotEqual(id1, id2);
        Assert.NotEqual(id2, id3);
        Assert.NotEqual(id1, id3);
    }

    [Fact]
    public async Task String_AutoGenerate_Ids_Are_Lexicographically_Sortable()
    {
        using var db = new TestDbContext(_dbPath);

        var ids = new List<string>();
        for (int i = 0; i < 5; i++)
        {
            ids.Add(await db.StringEntities.InsertAsync(new StringEntity { Id = null!, Value = $"item_{i}" }));
            // ULIDs are only guaranteed to be time-ordered across distinct milliseconds
            Thread.Sleep(2);
        }
        await db.SaveChangesAsync();
        // ULIDs generated in monotonic time order must already be sorted
        var sorted = ids.OrderBy(x => x).ToList();
        Assert.Equal(ids, sorted);
    }

    [Fact]
    public async Task String_Explicit_Id_Is_Not_Overwritten()
    {
        using var db = new TestDbContext(_dbPath);

        var entity = new StringEntity { Id = "my-custom-key", Value = "explicit" };
        var id = await db.StringEntities.InsertAsync(entity);
        await db.SaveChangesAsync();

        Assert.Equal("my-custom-key", id);
        var found = await db.StringEntities.FindByIdAsync("my-custom-key");
        Assert.NotNull(found);
        Assert.Equal("explicit", found.Value);
    }

    [Fact]
    public async Task String_AutoGenerator_Entity_Round_Trips_Via_FindById()
    {
        using var db = new TestDbContext(_dbPath);

        var entity = new StringEntity { Id = null!, Value = "round-trip" };
        var id = await db.StringEntities.InsertAsync(entity);
        await db.SaveChangesAsync();

        var found = await db.StringEntities.FindByIdAsync(id);
        Assert.NotNull(found);
        Assert.Equal(id, found.Id);
        Assert.Equal("round-trip", found.Value);
    }
}
