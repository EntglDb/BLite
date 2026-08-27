using BLite.Shared;
using System;
using System.IO;
using System.Threading.Tasks;

namespace BLite.Tests;

public class BytePropertyTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public BytePropertyTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_byte_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var walPath = _dbPath.Replace(".db", ".wal");
        if (File.Exists(walPath)) File.Delete(walPath);
    }

    [Fact]
    public async Task ByteProperty_RoundTrips()
    {
        var entity = new ByteEntity
        {
            Label = "primary",
            Value = 8,
            OptionalValue = 12
        };

        var id = await _db.ByteEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        var result = await _db.ByteEntities.FindByIdAsync(id);

        Assert.NotNull(result);
        Assert.Equal((byte)8, result!.Value);
        Assert.Equal((byte)12, result.OptionalValue);
    }
}
