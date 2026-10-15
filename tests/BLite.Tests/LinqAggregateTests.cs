using System.Linq.Expressions;
using System.Reflection;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// LINQ <c>Sum</c>, <c>Average</c>, <c>Min</c> and <c>Max</c> computed from index keys or raw BSON
/// return exactly what LINQ returns over the materialised entities, or throw the same exception.
/// </summary>
public class LinqAggregateTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"agg_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var f in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
            try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
    }

    private static readonly string[] Operators = ["Sum", "Average", "Min", "Max"];
    private static readonly string[] Indexed = ["Count", "Total", "MaybeCount"];
    private static readonly string[] Properties =
        ["Count", "Total", "Units", "Ratio", "Weight", "Price", "MaybeCount", "MaybeTotal", "MaybeRatio", "MaybeWeight", "MaybePrice"];

    private static Reading Make(int i) => new()
    {
        Tag = "abc"[i % 3].ToString(),
        Count = (i * 7) % 13 - 4,
        Total = i * 1_000_000_007L - 5_000_000_000L,
        Units = i * 3 - 50,
        Ratio = Math.Sin(i) * 100 / 3,
        Weight = (float)(i / 7.0),
        // Same values at different scales: 1.5 / 1.50 / 1.500.
        Price = (i % 3) switch { 0 => i * 1.5m, 1 => i * 1.50m, _ => i * 1.500m },
        MaybeCount = i % 4 == 0 ? null : i - 20,
        MaybeTotal = i % 4 == 1 ? null : i * 3L,
        MaybeRatio = i % 4 == 2 ? null : 1.0 / (i + 1),
        MaybeWeight = i % 4 == 3 ? null : (float)(i * 0.1),
        MaybePrice = i % 4 == 0 ? null : i * 0.10m,
        Level = i,
        Score = 100 + i,
    };

    private static readonly (string Name, Expression<Func<Reading, bool>>? Where)[] Filters =
    [
        ("none", null),
        ("tag", r => r.Tag == "b"),
        ("count range", r => r.Count >= -1 && r.Count < 5),
        ("count range and tag", r => r.Count > 0 && r.Tag == "c"),
        ("no match", r => r.Tag == "zzz"),
        ("empty count range", r => r.Count > 1000),
    ];

    /// <summary>Whether the aggregate should be computed without materialising the entities.</summary>
    private static bool ExpectPushedDown(string property, string filter) => filter switch
    {
        "none" or "tag" or "no match" => true,               // index keys, or raw BSON without/with a predicate
        "count range" or "empty count range" => property == "Count", // the Count index covers the filter
        _ => false,                                             // another index serves the filter
    };

    [Fact]
    public async Task EveryOperatorPropertyAndFilter_MatchesLinqOverTheEntities()
    {
        using var db = new AggregateDbContext(_dbPath);
        for (int i = 0; i < 40; i++)
            await db.Readings.InsertAsync(Make(i));
        var entities = db.Readings.AsQueryable().ToList();

        var failures = new List<string>();
        foreach (var op in Operators)
        foreach (var property in Properties)
        foreach (var (filter, where) in Filters)
        {
            var selector = Selector(property);
            var label = $"{op}({property}) where {filter}";

            // LINQ over the matching entities, in the order the collection returns them for the filter
            // (primary-key order, or index order when an index serves it).
            var matches = where == null ? entities : db.Readings.AsQueryable().Where((Expression<Func<Reading, bool>>)where).ToList();
            var expected = Run(() => Execute(matches.AsQueryable(), op, selector, null));
            var actual = Run(() => Execute(db.Readings.AsQueryable(), op, selector, where));
            if (!Same(expected, actual))
                failures.Add($"{label}: expected {Show(expected)}, got {Show(actual)}");

            var resultType = AggregateCall(entities.AsQueryable(), op, selector, where).Type;
            var direct = await RunAsync(() => db.Readings.TryAggregateAsync(Enum.Parse<AggregateOperator>(op), selector, where, resultType));
            bool handled = direct.Error != null || direct.Value.Handled;
            if (handled != ExpectPushedDown(property, filter))
                failures.Add($"{label}: pushed down = {handled}, expected {ExpectPushedDown(property, filter)}");
            else if (handled && !Same(expected, (direct.Value.Value, direct.Error)))
                failures.Add($"{label} (pushed down): expected {Show(expected)}, got {Show((direct.Value.Value, direct.Error))}");
        }

        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
    }

    [Theory]
    [InlineData(new[] { 0.0, -0.0, double.NaN, 1.0, -0.0 })]
    [InlineData(new[] { -0.0, 0.0, 2.5, double.NaN })]
    [InlineData(new[] { double.NaN, double.NaN })]
    [InlineData(new[] { double.PositiveInfinity, double.NegativeInfinity, 1.0 })]
    [InlineData(new[] { 1e308, 1e308, -1e308 })]
    [InlineData(new[] { 447711.6875, 7923489.5, 16777216.0 })] // float Average: (float)(sum / n) != (float)sum / n
    public async Task FloatingPointEdgeCases_MatchLinq(double[] values)
    {
        using var db = new AggregateDbContext(_dbPath);
        foreach (var v in values)
            await db.Readings.InsertAsync(new Reading { Ratio = v, MaybeRatio = v, Weight = (float)v, MaybeWeight = (float)v });
        var entities = db.Readings.AsQueryable().ToList();

        foreach (var op in Operators)
        foreach (var property in new[] { "Ratio", "MaybeRatio", "Weight", "MaybeWeight" })
        {
            var selector = Selector(property);
            var expected = Run(() => Execute(entities.AsQueryable(), op, selector, null));
            var direct = await db.Readings.TryAggregateAsync(Enum.Parse<AggregateOperator>(op), selector, null, AggregateCall(entities.AsQueryable(), op, selector, null).Type);

            Assert.True(direct.Handled, $"{op}({property})");
            Assert.True(Same(expected, (direct.Value, null)), $"{op}({property}): expected {Show(expected)}, got {Show((direct.Value, null))}");
        }
    }

    [Fact]
    public async Task DecimalScale_MatchesLinq()
    {
        using var db = new AggregateDbContext(_dbPath);
        foreach (var price in new[] { 1.00m, 1.0m, 2.50m, 1m })
            await db.Readings.InsertAsync(new Reading { Price = price, MaybePrice = price });

        var entities = db.Readings.AsQueryable().ToList();
        string Text(decimal? d) => d?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "null";

        Assert.Equal(Text(entities.Sum(r => r.Price)), Text(db.Readings.AsQueryable().Sum(r => r.Price)));
        Assert.Equal(Text(entities.Min(r => r.Price)), Text(db.Readings.AsQueryable().Min(r => r.Price))); // first of the equal minima
        Assert.Equal(Text(entities.Max(r => r.MaybePrice)), Text(db.Readings.AsQueryable().Max(r => r.MaybePrice)));
        Assert.Equal(Text(entities.Average(r => r.MaybePrice)), Text(db.Readings.AsQueryable().Average(r => r.MaybePrice)));
    }

    [Fact]
    public async Task IntegerOverflow_ThrowsLikeLinq()
    {
        using var db = new AggregateDbContext(_dbPath);
        await db.Readings.InsertAsync(new Reading { Units = int.MaxValue, Count = int.MaxValue, Total = long.MaxValue });
        await db.Readings.InsertAsync(new Reading { Units = 1, Count = 1, Total = 1 });

        Assert.Throws<OverflowException>(() => db.Readings.AsQueryable().Sum(r => r.Units)); // raw BSON
        Assert.Throws<OverflowException>(() => db.Readings.AsQueryable().Sum(r => r.Count)); // index keys
        Assert.Throws<OverflowException>(() => db.Readings.AsQueryable().Sum(r => r.Total));
        Assert.Throws<OverflowException>(() => db.Readings.AsQueryable().Average(r => r.Total));
        Assert.Equal((int.MaxValue + 1.0) / 2, db.Readings.AsQueryable().Average(r => r.Units));
        Assert.Equal((int.MaxValue + 1.0) / 2, db.Readings.AsQueryable().Average(r => r.Count));
    }

    [Theory]
    // In primary-key order. LINQ overflows on a running total although the total fits.
    [InlineData(new[] { int.MaxValue, 1, -2 }, true)]
    [InlineData(new[] { int.MinValue, -1, 5 }, true)]
    // The positive or negative values alone overflow, but no running total in this order does.
    [InlineData(new[] { 5, int.MinValue, -1 }, false)]
    [InlineData(new[] { -2, int.MaxValue, 1 }, false)]
    public async Task IndexKeySums_OverflowWhereLinqDoes(int[] values, bool linqOverflows)
    {
        // Index keys come in value order, not the order LINQ sees the entities in, and whether LINQ
        // overflows depends on that order: such sums must not be taken from the keys.
        using var db = new AggregateDbContext(_dbPath);
        for (int i = 0; i < values.Length; i++)
        {
            long total = values[i] switch { int.MaxValue => long.MaxValue, int.MinValue => long.MinValue, var v => v };
            await db.Readings.InsertAsync(new Reading { Id = new ObjectId(i + 1, 0), Count = values[i], Total = total });
        }

        var entities = db.Readings.AsQueryable().ToList();
        Assert.Equal(values, entities.Select(r => r.Count)); // primary-key order
        Assert.Equal(linqOverflows, Run(() => entities.Sum(r => r.Count)).Error == typeof(OverflowException));

        var q = db.Readings.AsQueryable();
        Assert.True(Same(Run(() => entities.Sum(r => r.Count)), Run(() => q.Sum(r => r.Count))));
        Assert.True(Same(Run(() => entities.Sum(r => r.Total)), Run(() => q.Sum(r => r.Total))));
        Assert.True(Same(Run(() => entities.Average(r => r.Total)), Run(() => q.Average(r => r.Total))));
        Assert.Equal(entities.Average(r => r.Count), q.Average(r => r.Count));

        // With a filter the index covers, the entities come in index order.
        var filtered = q.Where(r => r.Count < 100).ToList();
        Assert.True(Same(Run(() => filtered.Sum(r => r.Count)), Run(() => q.Where(r => r.Count < 100).Sum(r => r.Count))));
        var filteredTotal = q.Where(r => r.Total < 100).ToList();
        Assert.True(Same(Run(() => filteredTotal.Sum(r => r.Total)), Run(() => q.Where(r => r.Total < 100).Sum(r => r.Total))));
    }

    [Fact]
    public async Task IndexOnAConvertedProperty_IsNotDecodedAsThePropertyType()
    {
        // The keys of this index are longs although its property path is the int Units.
        using var db = new AggregateDbContext(_dbPath);
        await db.Readings.CreateIndexAsync(r => (long)r.Units);
        for (int i = 0; i < 10; i++)
            await db.Readings.InsertAsync(Make(i));

        var entities = db.Readings.AsQueryable().ToList();
        var q = db.Readings.AsQueryable();
        Assert.Equal(entities.Sum(r => r.Units), q.Sum(r => r.Units));
        Assert.Equal(entities.Min(r => r.Units), q.Min(r => r.Units));
        Assert.Equal(entities.Max(r => r.Units), q.Max(r => r.Units));
    }

    [Fact]
    public async Task FilterBoundOfAnotherType_IsNotUsedAsAKeyRange()
    {
        // These bounds are a double and a long against the int Count index; encoded as keys of
        // their own type they would bound the wrong range (#222).
        using var db = new AggregateDbContext(_dbPath);
        for (int i = 0; i < 20; i++)
            await db.Readings.InsertAsync(new Reading { Id = new ObjectId(i + 1, 0), Count = i, Total = i });

        var entities = db.Readings.AsQueryable().ToList();
        var q = db.Readings.AsQueryable();
        Assert.Equal(entities.Where(r => r.Count < 9.5).Max(r => r.Count), q.Where(r => r.Count < 9.5).Max(r => r.Count));
        Assert.Equal(entities.Where(r => r.Count > 4L).Sum(r => r.Count), q.Where(r => r.Count > 4L).Sum(r => r.Count));
        Assert.Equal(entities.Where(r => r.Count > 4).Sum(r => r.Count), q.Where(r => r.Count > 4).Sum(r => r.Count));
    }

    [Fact]
    public async Task FilterWithASubqueryOverTheCollection_MatchesLinq()
    {
        using var db = new AggregateDbContext(_dbPath);
        for (int i = 0; i < 20; i++)
            await db.Readings.InsertAsync(new Reading { Id = new ObjectId(i + 1, 0), Count = i % 7, Total = i });

        var entities = db.Readings.AsQueryable().ToList();
        var q = db.Readings.AsQueryable();
        Assert.Equal(
            entities.Where(r => entities.Any(o => o.Count == r.Count + 1)).Sum(r => r.Count),
            q.Where(r => q.Any(o => o.Count == r.Count + 1)).Sum(r => r.Count));
        Assert.Equal(
            entities.Where(r => r.Count > 2 && entities.Count(o => o.Total < r.Total) > 3).Max(r => r.Total),
            q.Where(r => r.Count > 2 && q.Count(o => o.Total < r.Total) > 3).Max(r => r.Total));
    }

    [Fact]
    public async Task IndexKeySums_ThatCannotOverflow_AreTakenFromTheKeys()
    {
        // The positive values sum to exactly int.MaxValue and the negative ones to exactly
        // int.MinValue, so no running total overflows in any order and the sum is taken from the
        // keys. (One more below int.MinValue and the result would depend on the order: see
        // IndexKeySums_OverflowWhereLinqDoes.) Explicit ids keep the document order fixed.
        int[] counts = [int.MaxValue - 10, 4, -7, int.MinValue + 7, 6];
        Assert.Equal(int.MaxValue, counts.Where(c => c > 0).Sum(c => (long)c));
        Assert.Equal(int.MinValue, counts.Where(c => c < 0).Sum(c => (long)c));

        using var db = new AggregateDbContext(_dbPath);
        for (int i = 0; i < counts.Length; i++)
            await db.Readings.InsertAsync(new Reading { Id = new ObjectId(i + 1, 0), Count = counts[i], Total = counts[i] });

        var entities = db.Readings.AsQueryable().ToList();
        var sum = await db.Readings.TryAggregateAsync(AggregateOperator.Sum, Selector("Count"), null, typeof(int));
        Assert.True(sum.Handled);
        Assert.Equal(entities.Sum(r => r.Count), sum.Value);
        var average = await db.Readings.TryAggregateAsync(AggregateOperator.Average, Selector("Total"), null, typeof(double));
        Assert.True(average.Handled);
        Assert.Equal(entities.Average(r => r.Total), average.Value);
    }

    [Fact]
    public async Task DocumentsWithoutTheField_UseTheEntityValue()
    {
        using (var v1 = new AggregateDbContext(_dbPath))
        {
            for (int i = 0; i < 5; i++)
                await v1.Readings.InsertAsync(Make(i));
        }

        // Whatever value the mapper gives a missing property, the aggregate must use the same one.
        using var v2 = new AggregateV2DbContext(_dbPath);
        var entities = v2.Readings.AsQueryable().ToList();

        Assert.False((await v2.Readings.TryAggregateAsync(AggregateOperator.Sum, (Expression<Func<ReadingV2, int>>)(r => r.Bonus), null, typeof(int))).Handled);
        Assert.False((await v2.Readings.TryAggregateAsync(AggregateOperator.Max, (Expression<Func<ReadingV2, double?>>)(r => r.Factor), null, typeof(double?))).Handled);
        Assert.Equal(entities.Sum(r => r.Bonus), v2.Readings.AsQueryable().Sum(r => r.Bonus));
        Assert.Equal(entities.Max(r => r.Factor), v2.Readings.AsQueryable().Max(r => r.Factor));
        Assert.Equal(entities.Sum(r => r.Count), v2.Readings.AsQueryable().Sum(r => r.Count));
    }

    [Theory]
    [InlineData("units", "null")]   // null in a non-nullable int
    [InlineData("units", "int64")]  // an Int64 in an int field
    [InlineData("ratio", "null")]
    [InlineData("ratio", "int32")]  // an Int32 in a double field
    public async Task FieldsWithAnotherBsonType_UseTheEntityValue(string field, string kind)
    {
        using (var db = new AggregateDbContext(_dbPath))
        {
            for (int i = 0; i < 4; i++)
                await db.Readings.InsertAsync(Make(i));
        }

        // Written through the schemaless API, which stores whatever it is given.
        using (var engine = new BLiteEngine(_dbPath))
        {
            var col = engine.GetOrCreateCollection("readings");
            await col.InsertAsync(col.CreateDocument(["_id", "tag", field], b =>
            {
                b.AddObjectId("_id", ObjectId.NewObjectId()).AddString("tag", "x");
                switch (kind)
                {
                    case "null": b.AddNull(field); break;
                    case "int64": b.AddInt64(field, 5L); break;
                    default: b.AddInt32(field, 2); break;
                }
            }));
        }

        using var reopened = new AggregateDbContext(_dbPath);
        var property = field == "units" ? "Units" : "Ratio";
        var resultType = field == "units" ? typeof(int) : typeof(double);

        // A null the entity cannot hold makes materialising throw; the aggregate must throw alike.
        var entities = Run(() => reopened.Readings.AsQueryable().ToList());
        var expected = entities.Error != null
            ? entities
            : Run(() => Execute(((List<Reading>)entities.Value!).AsQueryable(), "Sum", Selector(property), null));

        Assert.False((await reopened.Readings.TryAggregateAsync(AggregateOperator.Sum, Selector(property), null, resultType)).Handled);
        Assert.True(Same(expected, Run(() => Execute(reopened.Readings.AsQueryable(), "Sum", Selector(property), null))), Show(expected));
    }

    [Fact]
    public async Task RenamedProperty_IsReadFromItsOwnField()
    {
        using var db = new AggregateDbContext(_dbPath);
        for (int i = 0; i < 5; i++)
            await db.Readings.InsertAsync(Make(i));

        // Level is stored as "score" and Score as "level".
        var level = await db.Readings.TryAggregateAsync(AggregateOperator.Sum, Selector("Level"), null, typeof(int));
        var score = await db.Readings.TryAggregateAsync(AggregateOperator.Sum, Selector("Score"), null, typeof(int));
        Assert.Equal((true, (object?)10), level);
        Assert.Equal((true, (object?)510), score);
        Assert.Equal(10, db.Readings.AsQueryable().Sum(r => r.Level));
        Assert.Equal(510, db.Readings.AsQueryable().Sum(r => r.Score));
    }

    [Fact]
    public async Task FieldAfterOthers_InDocumentsWithoutOffsetTable_IsFound()
    {
        using (var db = new AggregateDbContext(_dbPath))
        {
            for (int i = 0; i < 4; i++)
                await db.Readings.InsertAsync(Make(i));
        }

        // The schemaless API writes no offset table, so the field is looked up by name, after the
        // fields before it.
        using (var engine = new BLiteEngine(_dbPath))
        {
            var col = engine.GetOrCreateCollection("readings");
            for (int i = 0; i < 3; i++)
            {
                var units = 1000 * (i + 1);
                await col.InsertAsync(col.CreateDocument(["_id", "tag", "units"], b =>
                    b.AddObjectId("_id", ObjectId.NewObjectId()).AddString("tag", "x").AddInt32("units", units)));
            }
        }

        using var reopened = new AggregateDbContext(_dbPath);
        var entities = reopened.Readings.AsQueryable().ToList();
        Assert.Equal(7, entities.Count);

        var sum = await reopened.Readings.TryAggregateAsync(AggregateOperator.Sum, Selector("Units"), null, typeof(int));
        Assert.True(sum.Handled);
        Assert.Equal(entities.Sum(r => r.Units), sum.Value);
        Expression<Func<Reading, bool>> tagX = r => r.Tag == "x";
        var filtered = await reopened.Readings.TryAggregateAsync(AggregateOperator.Max, Selector("Units"), tagX, typeof(int));
        Assert.Equal((true, (object?)3000), filtered);
    }

    /// <summary>Writes five <see cref="ShapeV1"/> documents and reopens them as <see cref="ShapeV2"/>.</summary>
    private async Task<ShapeV2DbContext> ShapesWrittenBeforeTheyWereIgnoredAsync()
    {
        using (var v1 = new ShapeV1DbContext(_dbPath))
        {
            for (int i = 0; i < 5; i++)
                await v1.Shapes.InsertAsync(new ShapeV1 { Count = i, Hidden = 100 + i, Unmapped = 200 + i, Gone = 1000 + i });
        }
        return new ShapeV2DbContext(_dbPath);
    }

    [Fact]
    public async Task IgnoredProperty_OverDocumentsThatStillHoldTheField_IsMaterialised()
    {
        using var db = await ShapesWrittenBeforeTheyWereIgnoredAsync();
        var shapes = db.Shapes.AsQueryable().ToList();
        var q = db.Shapes.AsQueryable();

        Assert.Equal(0, shapes.Sum(s => s.Hidden));
        Assert.Equal(shapes.Sum(s => s.Hidden), q.Sum(s => s.Hidden));     // [BsonIgnore]
        Assert.Equal(shapes.Max(s => s.Hidden), q.Max(s => s.Hidden));
        Assert.Equal(shapes.Sum(s => s.Unmapped), q.Sum(s => s.Unmapped)); // [NotMapped]
        Assert.Equal(shapes.Sum(s => s.Count), q.Sum(s => s.Count));
    }

    [Fact]
    public async Task ComputedProperty_OverDocumentsThatStoredIt_IsMaterialised()
    {
        using var db = await ShapesWrittenBeforeTheyWereIgnoredAsync();
        var shapes = db.Shapes.AsQueryable().ToList();
        var q = db.Shapes.AsQueryable();

        Assert.Equal(100, shapes.Sum(s => s.Gone));
        Assert.Equal(shapes.Sum(s => s.Gone), q.Sum(s => s.Gone));
        Assert.Equal(shapes.Min(s => s.Gone), q.Min(s => s.Gone));
        Assert.Equal(shapes.Average(s => s.Gone), q.Average(s => s.Gone));
    }

    [Fact]
    public async Task AccessorsWithLogic_AreMaterialised()
    {
        using var db = new VerbatimDbContext(_dbPath);
        for (int i = 1; i <= 4; i++)
            await db.Doubled.InsertAsync(new Doubled { G = i * 5, H = i * 7, Plain = i });

        var doubled = db.Doubled.AsQueryable().ToList();
        var q = db.Doubled.AsQueryable();

        // G: getter with logic, indexed (index keys) and filtered on another property (raw BSON).
        Assert.Equal(doubled.Sum(d => d.G), q.Sum(d => d.G));
        Assert.Equal(doubled.Min(d => d.G), q.Min(d => d.G));
        Assert.Equal(doubled.Max(d => d.G), q.Max(d => d.G));
        Assert.Equal(doubled.Average(d => d.G), q.Average(d => d.G));
        Assert.Equal(doubled.Where(d => d.Plain > 1).Sum(d => d.G), q.Where(d => d.Plain > 1).Sum(d => d.G));
        Assert.Equal(doubled.Where(d => d.Plain > 1).Max(d => d.G), q.Where(d => d.Plain > 1).Max(d => d.G));
        // The index on G holds the stored values, so the filter matches what the index says (as on
        // main); the aggregate must still use the materialised values of those documents.
        Assert.Equal(q.Where(d => d.G > 15).ToList().Sum(d => d.G), q.Where(d => d.G > 15).Sum(d => d.G));

        // H: setter with logic, indexed long.
        Assert.Equal(doubled.Sum(d => d.H), q.Sum(d => d.H));
        Assert.Equal(doubled.Min(d => d.H), q.Min(d => d.H));
        Assert.Equal(doubled.Max(d => d.H), q.Max(d => d.H));
        Assert.Equal(doubled.Average(d => d.H), q.Average(d => d.H));

        // A plain property next to them is still aggregated without materialising.
        var plain = await db.Doubled.TryAggregateAsync(AggregateOperator.Sum, (Expression<Func<Doubled, int>>)(d => d.Plain), null, typeof(int));
        Assert.Equal((true, (object?)10), plain);
    }

    [Fact]
    public async Task PropertyNamedId_ThatIsNotTheKey_IsReadFromItsOwnField()
    {
        using var db = new VerbatimDbContext(_dbPath);
        for (int i = 1; i <= 4; i++)
            await db.Coded.InsertAsync(new Coded { Code = i, Id = i * 100 });

        var coded = db.Coded.AsQueryable().ToList();
        Assert.Equal(1000, coded.Sum(c => c.Id));
        Assert.Equal(coded.Sum(c => c.Id), db.Coded.AsQueryable().Sum(c => c.Id));
        Assert.Equal(coded.Max(c => c.Code), db.Coded.AsQueryable().Max(c => c.Code));

        // Stored as "id", not "_id": still read from raw BSON.
        var id = await db.Coded.TryAggregateAsync(AggregateOperator.Sum, (Expression<Func<Coded, int>>)(c => c.Id), null, typeof(int));
        Assert.Equal((true, (object?)1000), id);
    }

    [Fact]
    public async Task PropertySetThroughTheConstructor_IsMaterialised()
    {
        using var db = new VerbatimDbContext(_dbPath);
        for (int i = 1; i <= 4; i++)
            await db.Scaled.InsertAsync(new Scaled(i));

        var scaled = db.Scaled.AsQueryable().ToList();
        Assert.Equal(scaled.Sum(s => s.Amount), db.Scaled.AsQueryable().Sum(s => s.Amount));
        Assert.Equal(scaled.Max(s => s.Amount), db.Scaled.AsQueryable().Max(s => s.Amount));
    }

    [Fact]
    public void GeneratedMappers_NameTheFieldOnlyForPropertiesStoredVerbatim()
    {
        var reading = MapperOf<ObjectId, Reading>();
        Assert.Equal("_id", reading.GetVerbatimFieldName("Id"));
        Assert.Equal("count", reading.GetVerbatimFieldName("Count"));
        Assert.Equal("maybeprice", reading.GetVerbatimFieldName("MaybePrice"));
        Assert.Equal("score", reading.GetVerbatimFieldName("Level")); // [Column("score")]
        Assert.Equal("level", reading.GetVerbatimFieldName("Score"));
        Assert.Null(reading.GetVerbatimFieldName("NoSuchProperty"));

        var shape = MapperOf<ObjectId, ShapeV2>();
        Assert.Equal("count", shape.GetVerbatimFieldName("Count"));
        Assert.Null(shape.GetVerbatimFieldName("Hidden"));   // [BsonIgnore]
        Assert.Null(shape.GetVerbatimFieldName("Unmapped")); // [NotMapped]
        Assert.Null(shape.GetVerbatimFieldName("Gone"));     // computed

        var doubled = MapperOf<ObjectId, Doubled>();
        Assert.Null(doubled.GetVerbatimFieldName("G")); // getter with logic
        Assert.Null(doubled.GetVerbatimFieldName("H")); // setter with logic
        Assert.Equal("plain", doubled.GetVerbatimFieldName("Plain"));

        var coded = MapperOf<int, Coded>();
        Assert.Equal("_id", coded.GetVerbatimFieldName("Code"));
        Assert.Equal("id", coded.GetVerbatimFieldName("Id"));

        Assert.Null(MapperOf<ObjectId, Scaled>().GetVerbatimFieldName("Amount")); // set through the constructor
    }

    private static DocumentMapperBase<TId, TEntity> MapperOf<TId, TEntity>() where TEntity : class
    {
        var type = typeof(VerbatimDbContext).Assembly.GetTypes()
            .First(t => !t.IsAbstract && typeof(DocumentMapperBase<TId, TEntity>).IsAssignableFrom(t));
        return (DocumentMapperBase<TId, TEntity>)Activator.CreateInstance(type)!;
    }

    [Fact]
    public async Task OtherOperatorsBeforeTheAggregate_AreApplied()
    {
        using var db = new AggregateDbContext(_dbPath);
        for (int i = 0; i < 20; i++)
            await db.Readings.InsertAsync(Make(i));
        var entities = db.Readings.AsQueryable().ToList();
        var q = db.Readings.AsQueryable();

        Assert.Equal(entities.Take(3).Sum(r => r.Count), q.Take(3).Sum(r => r.Count));
        Assert.Equal(entities.Skip(5).Max(r => r.Count), q.Skip(5).Max(r => r.Count));
        Assert.Equal(entities.OrderBy(r => r.Ratio).Take(4).Sum(r => r.Ratio), q.OrderBy(r => r.Ratio).Take(4).Sum(r => r.Ratio));
        Assert.Equal(entities.Where(r => r.Tag == "a").Where(r => r.Units > -40).Sum(r => r.Units),
                     q.Where(r => r.Tag == "a").Where(r => r.Units > -40).Sum(r => r.Units));
        Assert.Equal(entities.Where(r => r.Count > 0).Sum(r => r.Count), await q.Where(r => r.Count > 0).SumAsync(r => r.Count));
        Assert.Equal(entities.Average(r => r.Units), await q.AverageAsync(r => r.Units));
    }

    [Fact]
    public async Task IndexAggregates_FollowUpdatesAndDeletes()
    {
        using var db = new AggregateDbContext(_dbPath);
        var readings = Enumerable.Range(0, 10).Select(Make).ToList();
        foreach (var r in readings) await db.Readings.InsertAsync(r);

        readings[3].Count = 500;
        await db.Readings.UpdateAsync(readings[3]);
        await db.Readings.DeleteAsync(readings[4].Id);
        readings.RemoveAt(4);

        Assert.Equal(readings.Sum(r => r.Count), db.Readings.AsQueryable().Sum(r => r.Count));
        Assert.Equal(500, db.Readings.AsQueryable().Max(r => r.Count));
        Assert.Equal(readings.Min(r => r.Count), db.Readings.AsQueryable().Min(r => r.Count));
        Assert.Equal(readings.Where(r => r.Count < 500).Max(r => r.Count), db.Readings.AsQueryable().Where(r => r.Count < 500).Max(r => r.Count));
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private static LambdaExpression Selector(string property)
    {
        var p = Expression.Parameter(typeof(Reading), "r");
        return Expression.Lambda(Expression.Property(p, property), p);
    }

    private static MethodCallExpression AggregateCall<TEntity>(IQueryable<TEntity> source, string op, LambdaExpression selector, LambdaExpression? where)
    {
        var src = where == null
            ? source.Expression
            : Expression.Call(typeof(Queryable), nameof(Queryable.Where), [typeof(TEntity)], source.Expression, Expression.Quote(where));
        Type[] typeArgs = op is "Min" or "Max" ? [typeof(TEntity), selector.ReturnType] : [typeof(TEntity)];
        return Expression.Call(typeof(Queryable), op, typeArgs, src, Expression.Quote(selector));
    }

    private static readonly MethodInfo s_execute = typeof(IQueryProvider).GetMethods().Single(m => m.Name == "Execute" && m.IsGenericMethodDefinition);

    private static object? Execute<TEntity>(IQueryable<TEntity> source, string op, LambdaExpression selector, LambdaExpression? where)
    {
        var call = AggregateCall(source, op, selector, where);
        return s_execute.MakeGenericMethod(call.Type).Invoke(source.Provider, [call]);
    }

    private static (object? Value, Type? Error) Run(Func<object?> action)
    {
        try { return (action(), null); }
        catch (Exception ex) { return (null, Unwrap(ex).GetType()); }
    }

    private static async Task<((bool Handled, object? Value) Value, Type? Error)> RunAsync(Func<Task<(bool, object?)>> action)
    {
        try { return (await action(), null); }
        catch (Exception ex) { return (default, Unwrap(ex).GetType()); }
    }

    private static Exception Unwrap(Exception ex)
    {
        while (ex is TargetInvocationException or AggregateException && ex.InnerException != null)
            ex = ex.InnerException!;
        return ex;
    }

    /// <summary>Same exception type, or values of the same type with identical bits.</summary>
    private static bool Same((object? Value, Type? Error) expected, (object? Value, Type? Error) actual)
    {
        if (expected.Error != null || actual.Error != null) return expected.Error == actual.Error;
        return (expected.Value, actual.Value) switch
        {
            (null, null) => true,
            (double x, double y) => BitConverter.DoubleToInt64Bits(x) == BitConverter.DoubleToInt64Bits(y),
            (float x, float y) => BitConverter.SingleToInt32Bits(x) == BitConverter.SingleToInt32Bits(y),
            (decimal x, decimal y) => decimal.GetBits(x).SequenceEqual(decimal.GetBits(y)),
            var (x, y) => x?.GetType() == y?.GetType() && Equals(x, y),
        };
    }

    private static string Show((object? Value, Type? Error) result)
        => result.Error != null ? result.Error.Name
         : result.Value switch
         {
             null => "null",
             double d => d.ToString("R") + (BitConverter.DoubleToInt64Bits(d) < 0 ? " (neg)" : ""),
             float f => f.ToString("R"),
             var v => $"{v} ({v.GetType().Name})",
         };
}
