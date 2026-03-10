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
    public void Int_AutoIncrement_Generates_Sequential_Ids()
    {
        using var db = new TestDbContext(_dbPath);

        var id1 = db.IntEntities.Insert(new IntEntity { Name = "A" });
        var id2 = db.IntEntities.Insert(new IntEntity { Name = "B" });
        var id3 = db.IntEntities.Insert(new IntEntity { Name = "C" });
        db.SaveChanges();

        Assert.Equal(1, id1);
        Assert.Equal(2, id2);
        Assert.Equal(3, id3);
    }

    [Fact]
    public void Int_AutoIncrement_Ids_Are_Stored_On_Entity()
    {
        using var db = new TestDbContext(_dbPath);

        var entity = new IntEntity { Name = "Auto" };
        db.IntEntities.Insert(entity);
        db.SaveChanges();

        Assert.Equal(1, entity.Id);
    }

    [Fact]
    public void Int_Explicit_Id_Is_Not_Overwritten()
    {
        using var db = new TestDbContext(_dbPath);

        var entity = new IntEntity { Id = 42, Name = "Manual" };
        var id = db.IntEntities.Insert(entity);
        db.SaveChanges();

        Assert.Equal(42, id);
        var found = db.IntEntities.FindById(42);
        Assert.NotNull(found);
        Assert.Equal("Manual", found.Name);
    }

    [Fact]
    public void Int_Sequence_Persists_Across_DbContext_Reopens()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            db.IntEntities.Insert(new IntEntity { Name = "First" });
            db.IntEntities.Insert(new IntEntity { Name = "Second" });
            db.SaveChanges();
        }

        // Reopen the same database file
        using (var db2 = new TestDbContext(_dbPath))
        {
            var id3 = db2.IntEntities.Insert(new IntEntity { Name = "Third" });
            db2.SaveChanges();

            // Counter should continue from 3, not restart at 1
            Assert.Equal(3, id3);
        }
    }

    // ── long sequence ────────────────────────────────────────────────────────

    [Fact]
    public void Long_AutoIncrement_Generates_Sequential_Ids()
    {
        using var db = new TestDbContext(_dbPath);

        var id1 = db.LongEntities.Insert(new LongEntity { Name = "X" });
        var id2 = db.LongEntities.Insert(new LongEntity { Name = "Y" });
        db.SaveChanges();

        Assert.Equal(1L, id1);
        Assert.Equal(2L, id2);
    }

    [Fact]
    public void Long_Explicit_Id_Is_Not_Overwritten()
    {
        using var db = new TestDbContext(_dbPath);

        var entity = new LongEntity { Id = 999L, Name = "Manual" };
        var id = db.LongEntities.Insert(entity);
        db.SaveChanges();

        Assert.Equal(999L, id);
    }

    // ── string ULID ──────────────────────────────────────────────────────────

    [Fact]
    public void String_AutoGenerate_Produces_Valid_Ulid()
    {
        using var db = new TestDbContext(_dbPath);

        // Id = null! triggers auto-generation
        var entity = new StringEntity { Id = null!, Value = "test" };
        var id = db.StringEntities.Insert(entity);
        db.SaveChanges();

        Assert.NotNull(id);
        // ULID string is always 26 characters (Crockford base32)
        Assert.Equal(26, id.Length);
    }

    [Fact]
    public void String_AutoGenerate_Produces_Unique_Ids()
    {
        using var db = new TestDbContext(_dbPath);

        var id1 = db.StringEntities.Insert(new StringEntity { Id = null!, Value = "a" });
        var id2 = db.StringEntities.Insert(new StringEntity { Id = null!, Value = "b" });
        var id3 = db.StringEntities.Insert(new StringEntity { Id = null!, Value = "c" });
        db.SaveChanges();

        Assert.NotEqual(id1, id2);
        Assert.NotEqual(id2, id3);
        Assert.NotEqual(id1, id3);
    }

    [Fact]
    public void String_AutoGenerate_Ids_Are_Lexicographically_Sortable()
    {
        using var db = new TestDbContext(_dbPath);

        var ids = new List<string>();
        for (int i = 0; i < 5; i++)
        {
            ids.Add(db.StringEntities.Insert(new StringEntity { Id = null!, Value = $"item_{i}" }));
            // ULIDs are only guaranteed to be time-ordered across distinct milliseconds
            Thread.Sleep(2);
        }
        db.SaveChanges();

        // ULIDs generated in monotonic time order must already be sorted
        var sorted = ids.OrderBy(x => x).ToList();
        Assert.Equal(ids, sorted);
    }

    [Fact]
    public void String_Explicit_Id_Is_Not_Overwritten()
    {
        using var db = new TestDbContext(_dbPath);

        var entity = new StringEntity { Id = "my-custom-key", Value = "explicit" };
        var id = db.StringEntities.Insert(entity);
        db.SaveChanges();

        Assert.Equal("my-custom-key", id);
        var found = db.StringEntities.FindById("my-custom-key");
        Assert.NotNull(found);
        Assert.Equal("explicit", found.Value);
    }

    [Fact]
    public void String_AutoGenerator_Entity_Round_Trips_Via_FindById()
    {
        using var db = new TestDbContext(_dbPath);

        var entity = new StringEntity { Id = null!, Value = "round-trip" };
        var id = db.StringEntities.Insert(entity);
        db.SaveChanges();

        var found = db.StringEntities.FindById(id);
        Assert.NotNull(found);
        Assert.Equal(id, found.Id);
        Assert.Equal("round-trip", found.Value);
    }
}
