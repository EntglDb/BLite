using System.Linq.Expressions;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Query;

namespace BLite.Tests;

/// <summary>
/// Regression tests for issue #248: <c>x.Prop != constant</c> dropped the documents whose property
/// is null or absent, because the BSON predicate returned false for every operator on a stored
/// <c>BsonType.Null</c> and on a missing field. In C# <c>(int?)null != 5</c> is true, while
/// <c>(int?)null &lt; 5</c> is false; each query below must agree with LINQ-to-Objects over the
/// same documents, on a typed collection with and without indexes on the compared properties.
/// </summary>
public class NullComparisonTests : IAsyncLifetime
{
    private readonly string _indexedPath = Path.Combine(Path.GetTempPath(), $"nc_indexed_{Guid.NewGuid():N}.db");
    private readonly string _plainPath = Path.Combine(Path.GetTempPath(), $"nc_plain_{Guid.NewGuid():N}.db");
    private readonly string _dynamicPath = Path.Combine(Path.GetTempPath(), $"nc_dynamic_{Guid.NewGuid():N}.db");
    private NcIndexedContext _indexed = null!;
    private NcPlainContext _plain = null!;

    public async Task InitializeAsync()
    {
        // Written through the older entity shape first: these documents have none of the
        // nullable fields, which a query must treat as null.
        foreach (var path in new[] { _indexedPath, _plainPath })
        {
            using var v1 = new NcV1Context(path);
            await v1.Docs.InsertBulkAsync([new NcDocV1 { Name = "absent1" }, new NcDocV1 { Name = "absent2" }]);
        }

        _indexed = new NcIndexedContext(_indexedPath);
        _plain = new NcPlainContext(_plainPath);
        foreach (var docs in new[] { _indexed.Docs, _plain.Docs })
        {
            await docs.InsertBulkAsync(
            [
                new NcDoc { Name = "n1", MaybeCount = null, Nick = null, State = null, Kind = NcState.Open },
                new NcDoc { Name = "n2", MaybeCount = 5, Nick = "ab", State = NcState.Open, Kind = NcState.Closed },
                new NcDoc { Name = "n3", MaybeCount = 7, Nick = "a", State = NcState.Closed, Kind = NcState.Open },
                new NcDoc { Name = "n4", MaybeCount = 5, Nick = "b", State = null, Kind = NcState.Archived },
            ]);
        }
    }

    public Task DisposeAsync()
    {
        _indexed.Dispose();
        _plain.Dispose();
        foreach (var path in new[] { _indexedPath, _plainPath, _dynamicPath })
        {
            if (File.Exists(path)) File.Delete(path);
            var wal = Path.ChangeExtension(path, ".wal");
            if (File.Exists(wal)) File.Delete(wal);
        }
        return Task.CompletedTask;
    }

    // ─── != on a null or absent property ─────────────────────────────────────

    [Fact]
    public async Task NotEqual_NullableInt_Includes_Null_And_Absent()
        => await AssertAgreesAsync(x => x.MaybeCount != 5, "absent1", "absent2", "n1", "n3");

    [Fact]
    public async Task NotEqual_NullableInt_Captured_And_Flipped()
    {
        int? five = 5;
        await AssertAgreesAsync(x => x.MaybeCount != five, "absent1", "absent2", "n1", "n3");
        await AssertAgreesAsync(x => 5 != x.MaybeCount, "absent1", "absent2", "n1", "n3");
    }

    [Fact]
    public async Task NotEqual_NullableString_Includes_Null_And_Absent()
        => await AssertAgreesAsync(x => x.Nick != "ab", "absent1", "absent2", "n1", "n3", "n4");

    [Fact]
    public async Task NotEqual_NullableEnum_Includes_Null_And_Absent()
        => await AssertAgreesAsync(x => x.State != NcState.Open, "absent1", "absent2", "n1", "n3", "n4");

    [Fact]
    public async Task NotEqual_Enum_Includes_Absent()
        // An absent non-nullable enum reads back as default(NcState), which is not Open.
        => await AssertAgreesAsync(x => x.Kind != NcState.Open, "absent1", "absent2", "n2", "n4");

    [Fact]
    public async Task Negated_NotEqual_Excludes_Null_And_Absent()
        => await AssertAgreesAsync(x => !(x.Nick != "ab"), "n2");

    [Fact]
    public async Task NotEqual_In_And_Or()
    {
        await AssertAgreesAsync(x => x.MaybeCount != 5 && x.Nick != "a", "absent1", "absent2", "n1");
        await AssertAgreesAsync(x => x.MaybeCount != 7 || x.Nick == "a", "absent1", "absent2", "n1", "n2", "n3", "n4");
    }

    [Fact]
    public async Task Count_NotEqual_Includes_Null_And_Absent()
    {
        foreach (var docs in new[] { _indexed.Docs, _plain.Docs })
            Assert.Equal(4, await docs.AsQueryable().CountAsync(x => x.MaybeCount != 5));
    }

    // ─── Shapes that were already right and must stay so ─────────────────────

    [Fact]
    public async Task NullChecks_Unchanged()
    {
        await AssertAgreesAsync(x => x.MaybeCount != null, "n2", "n3", "n4");
        await AssertAgreesAsync(x => x.Nick != null, "n2", "n3", "n4");
    }

    [Fact]
    public async Task Equal_Constant_Excludes_Null_And_Absent()
    {
        await AssertAgreesAsync(x => x.MaybeCount == 5, "n2", "n4");
        await AssertAgreesAsync(x => x.Nick == "ab", "n2");
        await AssertAgreesAsync(x => x.State == NcState.Open, "n2");
    }

    [Fact]
    public async Task Ordering_Comparisons_With_Null_Stay_False()
    {
        await AssertAgreesAsync(x => x.MaybeCount < 6, "n2", "n4");
        await AssertAgreesAsync(x => x.MaybeCount <= 5, "n2", "n4");
        await AssertAgreesAsync(x => x.MaybeCount > 5, "n3");
        await AssertAgreesAsync(x => x.MaybeCount >= 5, "n2", "n3", "n4");
        await AssertAgreesAsync(x => !(x.MaybeCount < 6), "absent1", "absent2", "n1", "n3");
    }

    // ─── DynamicCollection ───────────────────────────────────────────────────

    [Fact]
    public async Task Dynamic_NotEqual_Predicate_Includes_Null_And_Absent()
    {
        using var engine = new BLiteEngine(_dynamicPath);
        var col = engine.GetOrCreateCollection("nc_dyn");
        await col.InsertAsync(col.CreateDocument(["name", "count", "nick"], b => b.AddString("name", "d1").AddNull("count").AddNull("nick")));
        await col.InsertAsync(col.CreateDocument(["name", "count", "nick"], b => b.AddString("name", "d2").AddInt32("count", 5).AddString("nick", "ab")));
        await col.InsertAsync(col.CreateDocument(["name", "count", "nick"], b => b.AddString("name", "d3").AddInt32("count", 7).AddString("nick", "a")));
        await col.InsertAsync(col.CreateDocument(["name"], b => b.AddString("name", "d4")));

        async Task<string[]> Scan(BsonReaderPredicate predicate)
        {
            var names = new List<string>();
            await foreach (var doc in col.ScanAsync(predicate))
                names.Add(doc.TryGetString("name", out var name) ? name : "?");
            return names.Order(StringComparer.Ordinal).ToArray();
        }

        Assert.Equal(["d1", "d3", "d4"], await Scan(BsonExpressionEvaluator.CreatePredicate("count", 5, ExpressionType.NotEqual)!));
        Assert.Equal(["d1", "d3", "d4"], await Scan(BsonExpressionEvaluator.CreatePredicate("nick", "ab", ExpressionType.NotEqual)!));
        Assert.Equal(["d2"], await Scan(BsonExpressionEvaluator.CreatePredicate("count", 6, ExpressionType.LessThan)!));
        Assert.Equal(["d2"], await Scan(BsonExpressionEvaluator.CreatePredicate("count", 5, ExpressionType.Equal)!));
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Runs <paramref name="predicate"/> on the indexed and the plain collection and checks both
    /// against LINQ-to-Objects over the documents as read back, and against <paramref name="expected"/>.
    /// </summary>
    private async Task AssertAgreesAsync(Expression<Func<NcDoc, bool>> predicate, params string[] expected)
    {
        foreach (var (label, docs) in new[] { ("indexed", _indexed.Docs), ("plain", _plain.Docs) })
        {
            var all = await docs.FindAllAsync().ToListAsync();
            var reference = Names(all.Where(predicate.Compile()));
            Assert.Equal(expected, reference);

            var actual = Names(await docs.AsQueryable().Where(predicate).ToListAsync());
            Assert.True(reference.SequenceEqual(actual),
                $"{label}: {predicate} returned [{string.Join(", ", actual)}], LINQ-to-Objects [{string.Join(", ", reference)}]");
        }
    }

    private static string[] Names(IEnumerable<NcDoc> docs)
        => docs.Select(d => d.Name).Order(StringComparer.Ordinal).ToArray();
}
