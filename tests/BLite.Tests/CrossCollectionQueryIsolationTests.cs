using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

public class CrossCollectionQueryIsolationTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public CrossCollectionQueryIsolationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"cross_collection_isolation_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    [Fact]
    public async Task OrElse_OnIndexedField_DoesNotReturnCrossCollectionRows()
    {
        await _db.IntEntities.EnsureIndexAsync(x => x.Name!, "idx_intentities_name", false);

        await _db.IntEntities.InsertAsync(new IntEntity { Id = 1, Name = "A" });
        await _db.IntEntities.InsertAsync(new IntEntity { Id = 2, Name = "B" });

        await _db.People.InsertAsync(new Person { Id = 101, Name = "A", Age = 20 });
        await _db.People.InsertAsync(new Person { Id = 102, Name = "B", Age = 30 });
        await _db.SaveChangesAsync();

        var count = _db.IntEntities.AsQueryable()
            .Where(x => x.Name == "A" || x.Name == "B")
            .Count();

        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Contains_OnIndexedField_DoesNotReturnCrossCollectionRows()
    {
        await _db.IntEntities.EnsureIndexAsync(x => x.Name!, "idx_intentities_name", false);

        await _db.IntEntities.InsertAsync(new IntEntity { Id = 1, Name = "A" });
        await _db.IntEntities.InsertAsync(new IntEntity { Id = 2, Name = "B" });

        await _db.People.InsertAsync(new Person { Id = 101, Name = "A", Age = 20 });
        await _db.People.InsertAsync(new Person { Id = 102, Name = "B", Age = 30 });
        await _db.SaveChangesAsync();

        var names = new[] { "A", "B" };
        var count = _db.IntEntities.AsQueryable()
            .Where(x => names.Contains(x.Name))
            .Count();

        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Contains_WithDuplicateValues_DoesNotDoubleCount()
    {
        await _db.IntEntities.EnsureIndexAsync(x => x.Name!, "idx_intentities_name", false);

        await _db.IntEntities.InsertAsync(new IntEntity { Id = 1, Name = "A" });
        await _db.IntEntities.InsertAsync(new IntEntity { Id = 2, Name = "B" });
        await _db.SaveChangesAsync();

        var names = new[] { "A", "A", "B", "B" };
        var count = _db.IntEntities.AsQueryable()
            .Where(x => names.Contains(x.Name))
            .Count();

        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Where_OnUnindexedSharedField_DoesNotReturnCrossCollectionRows()
    {
        await _db.IntEntities.InsertAsync(new IntEntity { Id = 1, Name = "Pranzo" });
        await _db.People.InsertAsync(new Person { Id = 101, Name = "Pranzo", Age = 20 });
        await _db.SaveChangesAsync();

        var results = await _db.IntEntities.AsQueryable()
            .Where(x => x.Name == "Pranzo")
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
        Assert.Equal("Pranzo", results[0].Name);
    }

    [Fact]
    public async Task CountAsync_OnUnindexedNumericField_DoesNotCountCrossCollectionRows()
    {
        await _db.Users.InsertAsync(new User { Name = "Alpha", Age = 30 });
        await _db.People.InsertAsync(new Person { Id = 101, Name = "Foreign", Age = 30 });
        await _db.SaveChangesAsync();

        var count = await _db.Users.AsQueryable()
            .CountAsync(x => x.Age == 30);

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task MaxAsync_FallbackScan_OnUnindexedNumericField_DoesNotReadCrossCollectionRows()
    {
        await _db.Users.InsertAsync(new User { Name = "Alpha", Age = 30 });
        await _db.People.InsertAsync(new Person { Id = 101, Name = "Foreign", Age = 99 });
        await _db.SaveChangesAsync();

        var plan = IndexMinMax.Scan(BsonAggregator.Max("age"));
        var maxAge = await _db.Users.AsQueryable().MaxAsync<int>(plan);

        Assert.Equal(30, maxAge);
    }

    [Fact]
    public async Task ParallelScan_OnSharedPage_OnlyReturnsCurrentCollectionRows()
    {
        await _db.Users.InsertAsync(new User { Name = "Pranzo", Age = 30 });
        await _db.People.InsertAsync(new Person { Id = 101, Name = "Pranzo", Age = 20 });
        await _db.SaveChangesAsync();

        var results = await _db.Users.ParallelScanAsync(reader =>
        {
            reader.ReadDocumentSize();
            while (reader.Remaining > 0)
            {
                var type = reader.ReadBsonType();
                if (type == 0) break;

                var fieldName = reader.ReadElementHeader();
                if (fieldName == "name")
                    return reader.ReadString() == "Pranzo";

                reader.SkipValue(type);
            }

            return false;
        }, degreeOfParallelism: 2).ToListAsync();

        Assert.Single(results);
        Assert.Equal("Pranzo", results[0].Name);
        Assert.Equal(30, results[0].Age);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var walPath = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(walPath)) File.Delete(walPath);
    }
}
