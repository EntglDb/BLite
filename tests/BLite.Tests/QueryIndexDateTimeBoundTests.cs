using BLite.Bson;
using BLite.Core;
using BLite.Core.Query.Blql;

namespace BLite.Tests;

/// <summary>
/// <see cref="DynamicCollection.QueryIndexAsync"/> takes <see cref="DateTime"/> and
/// <see cref="DateTimeOffset"/> bounds (#270). The index keys date values as Unix milliseconds,
/// but the bound conversion had no case for either type and threw
/// "Cannot create IndexKey from type DateTime"; only a <c>long</c> of milliseconds worked.
/// </summary>
public sealed class QueryIndexDateTimeBoundTests : IDisposable
{
    private static readonly DateTime T0 = new(2026, 10, 17, 8, 0, 0, DateTimeKind.Utc);

    private readonly string _dir;

    public QueryIndexDateTimeBoundTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"blite_qidx_date_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    private async Task<(BLiteEngine Engine, DynamicCollection Col)> OpenAsync()
    {
        var engine = new BLiteEngine(Path.Combine(_dir, "db.blite"));
        var col = engine.GetOrCreateCollection("events");
        await col.CreateIndexAsync("when");
        foreach (var m in new[] { 0, 5, 10, 20 })
            await col.InsertAsync(col.CreateDocument(["n", "when"], b => b.AddString("n", $"m{m}").AddDateTime("when", T0.AddMinutes(m))));
        return (engine, col);
    }

    private static async Task<List<string>> NamesAsync(IAsyncEnumerable<BsonDocument> docs) =>
        (await docs.ToListAsync()).Select(d => d.TryGetString("n", out var n) ? n : "?").ToList();

    [Fact]
    public async Task DateTime_Bounds_Select_The_Range()
    {
        var (engine, col) = await OpenAsync();
        using (engine)
        {
            Assert.Equal(new[] { "m0", "m5" }, await NamesAsync(col.QueryIndexAsync("idx_when", T0, T0.AddMinutes(5))));
            Assert.Equal(new[] { "m10", "m20" }, await NamesAsync(col.QueryIndexAsync("idx_when", T0.AddMinutes(10), null)));
            Assert.Equal(new[] { "m5", "m0" }, await NamesAsync(col.QueryIndexAsync("idx_when", null, T0.AddMinutes(5), ascending: false)));

            // The same instant as local time selects the same documents.
            Assert.Equal(new[] { "m0", "m5" }, await NamesAsync(col.QueryIndexAsync("idx_when", T0.ToLocalTime(), T0.AddMinutes(5).ToLocalTime())));

            // Agrees with BLQL on the same index, which converts its bounds itself.
            var blql = col.Query(BlqlFilter.Lte("when", T0.AddMinutes(5))).ToList()
                .Select(d => d.TryGetString("n", out var n) ? n : "?").OrderBy(n => n);
            Assert.Equal(new[] { "m0", "m5" }, blql);
        }
    }

    [Fact]
    public async Task DateTimeOffset_Bounds_Compare_By_Instant()
    {
        var (engine, col) = await OpenAsync();
        using (engine)
        {
            var from = new DateTimeOffset(T0.AddMinutes(5)).ToOffset(TimeSpan.FromHours(2));
            var to = new DateTimeOffset(T0.AddMinutes(10)).ToOffset(TimeSpan.FromHours(-5));
            Assert.Equal(new[] { "m5", "m10" }, await NamesAsync(col.QueryIndexAsync("idx_when", from, to)));
        }
    }
}
