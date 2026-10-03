using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using BLite.Core.Query;
using System.Collections.Concurrent;

using BLite.Shared;

namespace BLite.Tests;

public class Issue149_153_Repro
{
    private static string Tmp() => Path.Combine(Path.GetTempPath(), $"repro_{Guid.NewGuid()}.db");

    [Fact]
    public void Issue149_LargeJsonObject()
    {
        var km = new ConcurrentDictionary<string, ushort>(StringComparer.OrdinalIgnoreCase);
        var rk = new ConcurrentDictionary<ushort, string>();
        var big = string.Join(",", Enumerable.Range(0, 80).Select(i => $"\"k{i}\":\"value-number-{i}\""));
        var json = "{\"streams\":[{" + big + "}]}";
        ushort id = 1;
        foreach (var k in new[] { "streams" }.Concat(Enumerable.Range(0, 80).Select(i => $"k{i}"))) { km[k] = id; rk[id++] = k; }
        var doc = BsonJsonConverter.FromJson(json, km, rk);
        Assert.NotNull(doc);
    }

    [Fact]
    public async Task Issue153_NewCollectionOnExistingFile()
    {
        var p = Tmp();
        using (var db = new ReproADbContext(p)) { await db.A.InsertAsync(new ReproA { Name = "one" }); await db.SaveChangesAsync(); }
        using (var db = new ReproBDbContext(p)) { await db.B.InsertAsync(new ReproB { Name = "two" }); await db.SaveChangesAsync(); }
    }

    [Fact]
    public async Task Issue150_DecimalIndexEq()
    {
        var p = Tmp();
        decimal[] vals = [1234567890123456.77m, 1234567890123456.78m, 1234567890123456.79m, 1234567890123456.80m];
        var probe = 1234567890123456.78m;
        using var db = new ReproFigDbContext(p);
        foreach (var v in vals) await db.Indexed.InsertAsync(new ReproIdxFigure { Amount = v });
        var r = await db.Indexed.AsQueryable().Where(f => f.Amount == probe).ToListAsync();
        Assert.Single(r);
    }

    [Theory]
    [InlineData(64)]
    [InlineData(65)]
    public async Task Issue151_RollbackThenRead(int n)
    {
        var p = Tmp();
        using var db = new ReproFigDbContext(p);
        using (var txn = db.BeginTransaction())
        {
            for (var i = 0; i < n; i++) await db.Figures.InsertAsync(new ReproFigure { Amount = i }, txn);
            await txn.RollbackAsync();
        }
        var task = Task.Run(async () => await db.Figures.AsQueryable().ToListAsync());
        Assert.True(await Task.WhenAny(task, Task.Delay(15000)) == task, "read hangs");
        Assert.Empty(await task);
    }

    [Fact]
    public async Task Issue151B_ReadOutsideOpenTxn()
    {
        var p = Tmp();
        using var db = new ReproFigDbContext(p);
        await db.Figures.InsertAsync(new ReproFigure { Amount = -1 });
        using var txn = db.BeginTransaction();
        for (var i = 0; i < 64; i++) await db.Figures.InsertAsync(new ReproFigure { Amount = i }, txn);
        var task = Task.Run(async () => await db.Figures.AsQueryable().ToListAsync());
        Assert.True(await Task.WhenAny(task, Task.Delay(15000)) == task, "read hangs");
        Assert.Single(await task);
    }
}
