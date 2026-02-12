using Xunit;
using BLite.Core.Indexing;
using System.IO;
using System.Linq;
using BLite.Core;

namespace BLite.Tests;

public class GeospatialTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public GeospatialTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_geo_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    [Fact]
    public void Can_Insert_And_Search_Within()
    {
        // Setup: Insert some points
        var p1 = new GeoEntity { Name = "Point 1", Location = (45.0, 9.0) };
        var p2 = new GeoEntity { Name = "Point 2", Location = (46.0, 10.0) };
        var p3 = new GeoEntity { Name = "Point 3", Location = (50.0, 50.0) }; // Far away

        _db.GeoItems.Insert(p1);
        _db.GeoItems.Insert(p2);
        _db.GeoItems.Insert(p3);

        // Search: Within box [44, 8] to [47, 11]
        var results = _db.GeoItems.Within("idx_spatial", (44.0, 8.0), (47.0, 11.0)).ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.Name == "Point 1");
        Assert.Contains(results, r => r.Name == "Point 2");
    }

    [Fact]
    public void Can_Search_Near_Proximity()
    {
        // Setup: Milan (roughly 45.46, 9.18)
        var milan = (45.4642, 9.1899);
        var rome = (41.9028, 12.4964);
        var ny = (40.7128, -74.0060);

        _db.GeoItems.Insert(new GeoEntity { Name = "Milan Office", Location = milan });
        _db.GeoItems.Insert(new GeoEntity { Name = "Rome Office", Location = rome });
        _db.GeoItems.Insert(new GeoEntity { Name = "New York Office", Location = ny });

        // Search near Milan (within 600km - should include Rome (~500km) but not NY)
        var results = _db.GeoItems.Near("idx_spatial", milan, 600.0).ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.Name == "Milan Office");
        Assert.Contains(results, r => r.Name == "Rome Office");
        Assert.DoesNotContain(results, r => r.Name == "New York Office");
    }

    [Fact]
    public void LINQ_Integration_Near_Works()
    {
        var milan = (45.4642, 9.1899);
        _db.GeoItems.Insert(new GeoEntity { Name = "Milan Office", Location = milan });

        // LINQ query using .Near() extension
        var query = from p in _db.GeoItems.AsQueryable()
                    where p.Location.Near(milan, 10.0)
                    select p;

        var results = query.ToList();

        Assert.Single(results);
        Assert.Equal("Milan Office", results[0].Name);
    }

    [Fact]
    public void LINQ_Integration_Within_Works()
    {
         var milan = (45.4642, 9.1899);
        _db.GeoItems.Insert(new GeoEntity { Name = "Milan Office", Location = milan });

        var min = (45.0, 9.0);
        var max = (46.0, 10.0);

        // LINQ query using .Within() extension
        var results = _db.GeoItems.AsQueryable()
                        .Where(p => p.Location.Within(min, max))
                        .ToList();

        Assert.Single(results);
        Assert.Equal("Milan Office", results[0].Name);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }
}
