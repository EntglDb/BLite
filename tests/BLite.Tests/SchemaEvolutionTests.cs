using BLite.Bson;
using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Regression tests for #153: inserting into a collection that did not exist when the
/// file was created must work on an existing database file.
/// </summary>
public class SchemaEvolutionTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"blite_evo_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var f in new[] { _path, Path.ChangeExtension(_path, ".wal") })
            if (File.Exists(f)) File.Delete(f);
    }

    [Fact]
    public async Task NewCollectionAddedInV2_KeepsExistingDataAndAcceptsInserts()
    {
        using (var v1 = new EvoV1DbContext(_path))
        {
            await v1.A.InsertAsync(new EvoA { Name = "one" });
            await v1.SaveChangesAsync();
        }

        using (var v2 = new EvoV2DbContext(_path))
        {
            await v2.B.InsertAsync(new EvoB { Name = "two" });
            await v2.SaveChangesAsync();
        }

        using var check = new EvoV2DbContext(_path);
        Assert.Equal("one", Assert.Single(await check.A.AsQueryable().ToListAsync()).Name);
        Assert.Equal("two", Assert.Single(await check.B.AsQueryable().ToListAsync()).Name);
    }

    [Fact]
    public async Task InsertAfterOrphanCollectionIsDropped_DoesNotRouteToFreedPages()
    {
        using (var v1 = new EvoV1DbContext(_path))
        {
            await v1.A.InsertAsync(new EvoA { Name = "one" });
            await v1.SaveChangesAsync();
        }

        // This context does not declare A, so A is dropped as an orphan at construction.
        // The free-space index must not keep advertising A's (now freed) data page.
        using (var onlyB = new EvoOnlyBDbContext(_path))
        {
            await onlyB.B.InsertAsync(new EvoB { Name = "two" });
            await onlyB.SaveChangesAsync();
        }

        using var check = new EvoOnlyBDbContext(_path);
        Assert.Equal("two", Assert.Single(await check.B.AsQueryable().ToListAsync()).Name);
    }
}
