using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Regression tests for the null non-nullable string serialization / deserialization bug.
///
/// Problem: a property declared as <c>public string Foo { get; set; }</c> (no <c>?</c>)
/// that is never assigned remains <c>null</c> at runtime.
/// The write path correctly serializes it as BSON Null, but the read path was missing
/// the null-type guard and called <c>reader.ReadString()</c> unconditionally, throwing
/// an exception when it encountered a Null BSON element.
/// </summary>
public class NullStringSerializationTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public NullStringSerializationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_nullstr_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var walPath = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(walPath)) File.Delete(walPath);
    }

    // ------------------------------------------------------------------
    // Scenario 1 – string property left unassigned (null) at insert time
    // ------------------------------------------------------------------

    [Fact]
    public async Task NonNullable_String_LeftNull_RoundTrips_Without_Exception()
    {
        // Arrange: Title is declared as non-nullable but never assigned → null at runtime
        var entity = new EntityWithUnassignedString
        {
            Number = 42
            // Title intentionally NOT assigned → remains null
        };

        // Act
        var id = await _db.UnassignedStringEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();
        var found = await _db.UnassignedStringEntities.FindByIdAsync(id);

        // Assert: deserialization must not throw; null is preserved
        Assert.NotNull(found);
        Assert.Null(found.Title);
        Assert.Equal(42, found.Number);
    }

    // ------------------------------------------------------------------
    // Scenario 2 – string property explicitly set to null
    // ------------------------------------------------------------------

    [Fact]
    public async Task NonNullable_String_ExplicitlyNull_RoundTrips_Without_Exception()
    {
        // Arrange
        var entity = new EntityWithUnassignedString
        {
            Title = null!,   // explicit null via null-forgiving operator
            Number = 7
        };

        // Act
        var id = await _db.UnassignedStringEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();
        var found = await _db.UnassignedStringEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(found);
        Assert.Null(found.Title);
        Assert.Equal(7, found.Number);
    }

    // ------------------------------------------------------------------
    // Scenario 3 – non-null string still round-trips correctly after the fix
    // ------------------------------------------------------------------

    [Fact]
    public async Task NonNullable_String_WithValue_RoundTrips_Correctly()
    {
        // Arrange
        var entity = new EntityWithUnassignedString
        {
            Title = "Hello World",
            Number = 99
        };

        // Act
        var id = await _db.UnassignedStringEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();
        var found = await _db.UnassignedStringEntities.FindByIdAsync(id);

        // Assert
        Assert.NotNull(found);
        Assert.Equal("Hello World", found.Title);
        Assert.Equal(99, found.Number);
    }
}
