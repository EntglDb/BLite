using BLite.Bson;
using BLite.Core;
using BLite.Core.Query.Blql;

namespace BLite.Tests;

/// <summary>
/// BLQL <c>First</c>, <c>FirstOrDefault</c> and their async forms return the first document of the
/// query as <c>ToList</c> returns it (#266). They used to return the first match in scan order,
/// ignoring <c>OrderBy</c>/<c>Sort</c> and <c>Skip</c>. <c>Any</c>, <c>None</c> and their async forms
/// honour <c>Skip</c> and <c>Take</c> the same way.
/// </summary>
public sealed class BlqlFirstSortSkipTests : IDisposable
{
    private readonly string _dir;

    public BlqlFirstSortSkipTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"blite_blql_first_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    private static string Name(BsonDocument? d) => d != null && d.TryGetString("n", out var n) ? n : "<null>";

    private async Task<(BLiteEngine Engine, DynamicCollection Col)> OpenAsync(bool indexed)
    {
        var engine = new BLiteEngine(Path.Combine(_dir, indexed ? "indexed.db" : "plain.db"));
        var col = engine.GetOrCreateCollection("items");
        if (indexed) await col.CreateIndexAsync("v");
        // Inserted a, b, c, d; ascending v: b, c, a, d.
        foreach (var (n, v) in new[] { ("a", 3), ("b", 1), ("c", 2), ("d", 4) })
            await col.InsertAsync(col.CreateDocument(["n", "v"], b => b.AddString("n", n).AddInt32("v", v)));
        return (engine, col);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task First_Honours_Sort_And_Skip(bool indexed)
    {
        var (engine, col) = await OpenAsync(indexed);
        using (engine)
        {
            Assert.Equal(new[] { "b", "c", "a", "d" }, col.Query().OrderBy("v").ToList().Select(d => Name(d)));

            Assert.Equal("b", Name(col.Query().OrderBy("v").FirstOrDefault()));
            Assert.Equal("b", Name(await col.Query().OrderBy("v").FirstOrDefaultAsync()));
            Assert.Equal("d", Name(col.Query().OrderByDescending("v").First()));
            Assert.Equal("a", Name(col.Query().OrderBy("v").Skip(2).First()));
            Assert.Equal("a", Name(await col.Query().OrderBy("v").Skip(2).FirstAsync()));
            // Without a sort the scan order is the collection's; First still agrees with ToList.
            Assert.Equal(Name(col.Query().Skip(1).ToList().FirstOrDefault()), Name(col.Query().Skip(1).FirstOrDefault()));
            Assert.NotEqual(Name(col.Query().FirstOrDefault()), Name(col.Query().Skip(1).FirstOrDefault()));
            Assert.Equal("<null>", Name(col.Query().OrderBy("v").Skip(4).FirstOrDefault()));
            Assert.Throws<InvalidOperationException>(() => col.Query().OrderBy("v").Skip(4).First());

            // A filter, then a sort on another field.
            Assert.Equal("a", Name(col.Query(BlqlFilter.Gt("v", 1)).OrderBy("n").FirstOrDefault()));
            Assert.Equal("d", Name(col.Query(BlqlFilter.Gt("v", 1)).OrderByDescending("n").FirstOrDefault()));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Any_Honours_Skip_And_Take(bool indexed)
    {
        var (engine, col) = await OpenAsync(indexed);
        using (engine)
        {
            Assert.True(col.Query().Skip(3).Any());
            Assert.False(col.Query().Skip(4).Any());
            Assert.False(await col.Query().OrderBy("v").Skip(4).AnyAsync());
            Assert.True(col.Query().Skip(4).None());
            Assert.True(await col.Query().Skip(4).NoneAsync());
            Assert.False(col.Query().Take(0).Any());
            Assert.True(col.Query().Skip(3).Take(1).Any());

            // A filter served from the index when there is one: v > 1 matches a, c, d.
            Assert.True(col.Query(BlqlFilter.Gt("v", 1)).Skip(2).Any());
            Assert.False(col.Query(BlqlFilter.Gt("v", 1)).Skip(3).Any());
            Assert.Equal(col.Query(BlqlFilter.Gt("v", 1)).Skip(3).ToList().Count > 0, col.Query(BlqlFilter.Gt("v", 1)).Skip(3).Any());
        }
    }
}
