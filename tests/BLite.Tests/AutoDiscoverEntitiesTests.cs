using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Tests that the source generator auto-discovers entities from DocumentCollection
/// properties when OnModelCreating is not overridden, matching the Quick-Start scenario.
/// </summary>
public class AutoDiscoverEntitiesTests : IDisposable
{
    private const string DbPath = "auto_discover.db";

    public AutoDiscoverEntitiesTests()
    {
        if (File.Exists(DbPath)) File.Delete(DbPath);
    }

    public void Dispose()
    {
        if (File.Exists(DbPath)) File.Delete(DbPath);
    }

    [Fact]
    public void Collections_Are_Initialized_Without_OnModelCreating()
    {
        using var db = new MinimalDbContext(DbPath);

        // Verify collection is not null (initialized by generated method without OnModelCreating)
        Assert.NotNull(db.Users);
    }

    [Fact]
    public async Task Can_Insert_And_Query_Without_OnModelCreating()
    {
        using var db = new MinimalDbContext(DbPath);

        // This replicates the Quick-Start scenario from the README
        await db.Users.InsertAsync(new User { Name = "Alice" });
        await db.Users.InsertAsync(new User { Name = "Bob" });
        await db.SaveChangesAsync();

        var results = db.Users.AsQueryable()
            .Where(u => u.Name.StartsWith("A"))
            .AsEnumerable()
            .ToList();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }
}
