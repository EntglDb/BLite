using BLite.Bson;
using BLite.Core;
using BLite.Core.Query.Blql;

namespace BLite.Tests;

/// <summary>
/// Unit tests for BLQL new filter operators:
///   $startsWith, $endsWith, $contains,
///   $elemMatch, $size, $all,
///   $mod,
///   $not (field-level),
///   $geoWithin, $geoNear,
///   $nearVector
/// Covers both fluent API and JSON parser paths.
/// </summary>
public class BlqlNewFiltersTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;
    private readonly DynamicCollection _col;

    public BlqlNewFiltersTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blql_new_{Guid.NewGuid()}.db");
        _engine = new BLiteEngine(_dbPath);
        _col = _engine.GetOrCreateCollection("items");

        SeedData();
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ── Seed ───────────────────────────────────────────────────────────────

    private void SeedData()
    {
        // doc1: Alice — tags=["urgent","reviewed"], scores=[85,92,78], qty=12
        InsertWithArrays("Alice", 30, "alice@gmail.com",
            ["urgent", "reviewed"], [85, 92, 78], 12,
            41.9028, 12.4964);   // Rome

        // doc2: Bob — tags=["pending"], scores=[60,55], qty=7
        InsertWithArrays("Bob", 25, "bob@outlook.com",
            ["pending"], [60, 55], 7,
            48.8566, 2.3522);    // Paris

        // doc3: Charlie — tags=["urgent","closed"], scores=[90,88,95], qty=20
        InsertWithArrays("Charlie", 35, "charlie@gmail.com",
            ["urgent", "closed"], [90, 88, 95], 20,
            51.5074, -0.1278);   // London

        // doc4: Diana — tags=["reviewed","pending","urgent"], scores=[70], qty=4
        InsertWithArrays("Diana", 28, "diana@yahoo.com",
            ["reviewed", "pending", "urgent"], [70], 4,
            40.7128, -74.0060);  // New York

        // doc5: Eve — tags=[], scores=[100], qty=15
        InsertWithArrays("Eve", 22, "eve@gmail.com",
            [], [100], 15,
            35.6762, 139.6503);  // Tokyo
    }

    private void InsertWithArrays(string name, int age, string email,
        string[] tags, int[] scores, int qty,
        double lat, double lon)
    {
        var tagList = tags.Select(BsonValue.FromString).ToList();
        var scoreList = scores.Select(BsonValue.FromInt32).ToList();

        var doc = _col.CreateDocument(
            ["_id", "name", "age", "email", "tags", "scores", "qty", "location"],
            b =>
            {
                b.AddString("name", name)
                 .AddInt32("age", age)
                 .AddString("email", email)
                 .Add("tags", BsonValue.FromArray(tagList))
                 .Add("scores", BsonValue.FromArray(scoreList))
                 .AddInt32("qty", qty)
                 .AddCoordinates("location", (lat, lon));
            });
        _col.Insert(doc);
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static string GetName(BsonDocument doc)
    {
        doc.TryGetString("name", out var n);
        return n ?? "";
    }

    private List<string> QueryNames(BlqlFilter filter)
        => _col.Query(filter).ToList().Select(GetName).OrderBy(n => n).ToList();

    private List<string> QueryNames(string json)
        => QueryNames(BlqlFilterParser.Parse(json));

    // ══════════════════════════════════════════════════════════════════════════
    //  $startsWith
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void StartsWith_Fluent_MatchesPrefix()
    {
        var names = QueryNames(BlqlFilter.StartsWith("name", "Al"));
        Assert.Equal(["Alice"], names);
    }

    [Fact]
    public void StartsWith_Fluent_NoMatch()
    {
        var names = QueryNames(BlqlFilter.StartsWith("name", "Zz"));
        Assert.Empty(names);
    }

    [Fact]
    public void StartsWith_Parser()
    {
        var names = QueryNames("""{ "name": { "$startsWith": "Ch" } }""");
        Assert.Equal(["Charlie"], names);
    }

    [Fact]
    public void StartsWith_CaseSensitive()
    {
        // Must NOT match lowercase
        var names = QueryNames("""{ "name": { "$startsWith": "al" } }""");
        Assert.Empty(names);
    }

    [Fact]
    public void StartsWith_Parser_NonString_Throws()
    {
        Assert.Throws<FormatException>(() =>
            BlqlFilterParser.Parse("""{ "name": { "$startsWith": 42 } }"""));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  $endsWith
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void EndsWith_Fluent()
    {
        var names = QueryNames(BlqlFilter.EndsWith("email", "@gmail.com"));
        Assert.Equal(["Alice", "Charlie", "Eve"], names);
    }

    [Fact]
    public void EndsWith_Parser()
    {
        var names = QueryNames("""{ "email": { "$endsWith": "@yahoo.com" } }""");
        Assert.Equal(["Diana"], names);
    }

    [Fact]
    public void EndsWith_NoMatch()
    {
        var names = QueryNames(BlqlFilter.EndsWith("email", "@nonexistent.org"));
        Assert.Empty(names);
    }

    [Fact]
    public void EndsWith_Parser_NonString_Throws()
    {
        Assert.Throws<FormatException>(() =>
            BlqlFilterParser.Parse("""{ "email": { "$endsWith": 123 } }"""));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  $contains
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Contains_Fluent()
    {
        var names = QueryNames(BlqlFilter.Contains("email", "outlook"));
        Assert.Equal(["Bob"], names);
    }

    [Fact]
    public void Contains_Parser()
    {
        var names = QueryNames("""{ "email": { "$contains": "gmail" } }""");
        Assert.Equal(["Alice", "Charlie", "Eve"], names);
    }

    [Fact]
    public void Contains_CaseSensitive()
    {
        var names = QueryNames("""{ "email": { "$contains": "Gmail" } }""");
        Assert.Empty(names);
    }

    [Fact]
    public void Contains_Parser_NonString_Throws()
    {
        Assert.Throws<FormatException>(() =>
            BlqlFilterParser.Parse("""{ "name": { "$contains": true } }"""));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  $size
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Size_Fluent_Match()
    {
        // Alice has 2 tags, Charlie has 2
        var names = QueryNames(BlqlFilter.Size("tags", 2));
        Assert.Equal(["Alice", "Charlie"], names);
    }

    [Fact]
    public void Size_Fluent_EmptyArray()
    {
        // Eve has tags=[]
        var names = QueryNames(BlqlFilter.Size("tags", 0));
        Assert.Equal(["Eve"], names);
    }

    [Fact]
    public void Size_Parser()
    {
        // Diana has 3 tags
        var names = QueryNames("""{ "tags": { "$size": 3 } }""");
        Assert.Equal(["Diana"], names);
    }

    [Fact]
    public void Size_NonArrayField_NoMatch()
    {
        // "name" is a string, not an array → should not match any $size
        var names = QueryNames(BlqlFilter.Size("name", 5));
        Assert.Empty(names);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  $all
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void All_Fluent_Match()
    {
        // Alice & Diana have both "urgent" and "reviewed"
        var names = QueryNames(BlqlFilter.All("tags", "urgent", "reviewed"));
        Assert.Equal(["Alice", "Diana"], names);
    }

    [Fact]
    public void All_Fluent_SingleValue()
    {
        var names = QueryNames(BlqlFilter.All("tags", "pending"));
        Assert.Equal(["Bob", "Diana"], names);
    }

    [Fact]
    public void All_Fluent_NoMatch()
    {
        var names = QueryNames(BlqlFilter.All("tags", "urgent", "nonexistent"));
        Assert.Empty(names);
    }

    [Fact]
    public void All_Parser()
    {
        var names = QueryNames("""{ "tags": { "$all": ["urgent", "closed"] } }""");
        Assert.Equal(["Charlie"], names);
    }

    [Fact]
    public void All_EmptyArray_NoMatch()
    {
        // Eve has tags=[] — $all with any value should not match
        var names = QueryNames("""{ "tags": { "$all": ["urgent"] } }""");
        Assert.DoesNotContain("Eve", names);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  $elemMatch (scalar arrays)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void ElemMatch_Scalar_Gte_Fluent()
    {
        // scores containing at least one element >= 95
        var filter = BlqlFilter.ElemMatch("scores",
            BlqlFilter.Gte("scores", BsonValue.FromInt32(95)));
        var names = QueryNames(filter);
        Assert.Equal(["Charlie", "Eve"], names);
    }

    [Fact]
    public void ElemMatch_Scalar_Range_Fluent()
    {
        // scores containing an element >= 80 AND < 90
        var filter = BlqlFilter.ElemMatch("scores",
            BlqlFilter.And(
                BlqlFilter.Gte("scores", BsonValue.FromInt32(80)),
                BlqlFilter.Lt("scores", BsonValue.FromInt32(90))));
        var names = QueryNames(filter);
        // Alice: 85 ✓, 92 ✗, 78 ✗ → matches Alice
        // Charlie: 90 ✗ (not < 90), 88 ✓, 95 ✗ → matches Charlie
        Assert.Equal(["Alice", "Charlie"], names);
    }

    [Fact]
    public void ElemMatch_Scalar_Parser()
    {
        // { "scores": { "$elemMatch": { "$gte": 95 } } }
        var names = QueryNames("""{ "scores": { "$elemMatch": { "$gte": 95 } } }""");
        Assert.Equal(["Charlie", "Eve"], names);
    }

    [Fact]
    public void ElemMatch_Scalar_Range_Parser()
    {
        var names = QueryNames("""{ "scores": { "$elemMatch": { "$gte": 80, "$lt": 90 } } }""");
        Assert.Equal(["Alice", "Charlie"], names);
    }

    [Fact]
    public void ElemMatch_Parser_NonObject_Throws()
    {
        Assert.Throws<FormatException>(() =>
            BlqlFilterParser.Parse("""{ "scores": { "$elemMatch": 42 } }"""));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  $mod
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Mod_Fluent_EvenQty()
    {
        // qty % 2 == 0 → Alice(12), Charlie(20), Diana(4)
        var names = QueryNames(BlqlFilter.Mod("qty", 2, 0));
        Assert.Equal(["Alice", "Charlie", "Diana"], names);
    }

    [Fact]
    public void Mod_Fluent_OddQty()
    {
        // qty % 2 == 1 → Bob(7), Eve(15)
        var names = QueryNames(BlqlFilter.Mod("qty", 2, 1));
        Assert.Equal(["Bob", "Eve"], names);
    }

    [Fact]
    public void Mod_Fluent_Mod5()
    {
        // qty % 5 == 0 → Charlie(20), Eve(15)
        var names = QueryNames(BlqlFilter.Mod("qty", 5, 0));
        Assert.Equal(["Charlie", "Eve"], names);
    }

    [Fact]
    public void Mod_Parser()
    {
        var names = QueryNames("""{ "qty": { "$mod": [4, 0] } }""");
        // qty % 4 == 0 → Alice(12), Charlie(20), Diana(4)
        Assert.Equal(["Alice", "Charlie", "Diana"], names);
    }

    [Fact]
    public void Mod_NonNumericField_NoMatch()
    {
        // "name" is a string — should not match
        var names = QueryNames(BlqlFilter.Mod("name", 2, 0));
        Assert.Empty(names);
    }

    [Fact]
    public void Mod_Parser_NotArray_Throws()
    {
        Assert.Throws<FormatException>(() =>
            BlqlFilterParser.Parse("""{ "qty": { "$mod": 5 } }"""));
    }

    [Fact]
    public void Mod_Parser_WrongArrayLength_Throws()
    {
        Assert.Throws<FormatException>(() =>
            BlqlFilterParser.Parse("""{ "qty": { "$mod": [5] } }"""));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  $not (field-level negation)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void FieldNot_Fluent()
    {
        // age NOT > 30 → age <= 30
        var filter = BlqlFilter.Not(BlqlFilter.Gt("age", 30));
        var names = QueryNames(filter);
        Assert.Equal(["Alice", "Bob", "Diana", "Eve"], names);
    }

    [Fact]
    public void FieldNot_Parser()
    {
        // { "age": { "$not": { "$gt": 30 } } } → age <= 30
        var names = QueryNames("""{ "age": { "$not": { "$gt": 30 } } }""");
        Assert.Equal(["Alice", "Bob", "Diana", "Eve"], names);
    }

    [Fact]
    public void FieldNot_Combined_Parser()
    {
        // { "qty": { "$not": { "$gte": 10, "$lte": 20 } } }
        // NOT (qty >= 10 AND qty <= 20) → qty < 10 OR qty > 20
        var names = QueryNames("""{ "qty": { "$not": { "$gte": 10, "$lte": 20 } } }""");
        // Bob(7) and Diana(4)
        Assert.Equal(["Bob", "Diana"], names);
    }

    [Fact]
    public void FieldNot_Parser_NonObject_Throws()
    {
        Assert.Throws<FormatException>(() =>
            BlqlFilterParser.Parse("""{ "age": { "$not": 30 } }"""));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  $geoWithin
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void GeoWithin_Fluent_Europe()
    {
        // Bounding box covering Europe (roughly)
        // minLon=-10, minLat=35, maxLon=25, maxLat=60
        var filter = BlqlFilter.GeoWithin("location", -10, 35, 25, 60);
        var names = QueryNames(filter);
        // Rome (41.9, 12.5), Paris (48.8, 2.3), London (51.5, -0.1) → all in box
        Assert.Equal(["Alice", "Bob", "Charlie"], names);
    }

    [Fact]
    public void GeoWithin_Parser()
    {
        // Only covers Italy-ish
        var names = QueryNames("""{ "location": { "$geoWithin": { "$box": [[10, 40], [15, 44]] } } }""");
        // Rome (41.9, 12.5) fits
        Assert.Equal(["Alice"], names);
    }

    [Fact]
    public void GeoWithin_NoMatch()
    {
        // Box in the middle of the Pacific
        var filter = BlqlFilter.GeoWithin("location", 170, -10, 180, 10);
        var names = QueryNames(filter);
        Assert.Empty(names);
    }

    [Fact]
    public void GeoWithin_Parser_InvalidBox_Throws()
    {
        Assert.Throws<FormatException>(() =>
            BlqlFilterParser.Parse("""{ "location": { "$geoWithin": { "$box": [1, 2] } } }"""));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  $geoNear
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void GeoNear_Fluent_NearRome()
    {
        // 100 km around Rome — should only match Alice (Rome)
        var filter = BlqlFilter.GeoNear("location", 12.4964, 41.9028, 100);
        var names = QueryNames(filter);
        Assert.Equal(["Alice"], names);
    }

    [Fact]
    public void GeoNear_Fluent_LargeRadius()
    {
        // 2000 km around Paris — should include Rome, Paris, London
        var filter = BlqlFilter.GeoNear("location", 2.3522, 48.8566, 2000);
        var names = QueryNames(filter);
        Assert.Contains("Alice", names);   // Rome ~1100km
        Assert.Contains("Bob", names);     // Paris 0km
        Assert.Contains("Charlie", names); // London ~340km
        Assert.DoesNotContain("Diana", names); // New York ~5800km
        Assert.DoesNotContain("Eve", names);   // Tokyo ~9700km
    }

    [Fact]
    public void GeoNear_Parser()
    {
        // 500 km around London
        var names = QueryNames("""{ "location": { "$geoNear": { "$center": [-0.1278, 51.5074], "$maxDistance": 500 } } }""");
        // London(0km) ✓, Paris(~340km) ✓, Rome(~1400km) ✗
        Assert.Contains("Bob", names);
        Assert.Contains("Charlie", names);
    }

    [Fact]
    public void GeoNear_Parser_MissingCenter_Throws()
    {
        Assert.Throws<FormatException>(() =>
            BlqlFilterParser.Parse("""{ "location": { "$geoNear": { "$maxDistance": 100 } } }"""));
    }

    [Fact]
    public void GeoNear_Parser_MissingMaxDistance_Throws()
    {
        Assert.Throws<FormatException>(() =>
            BlqlFilterParser.Parse("""{ "location": { "$geoNear": { "$center": [0, 0] } } }"""));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  $nearVector
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void NearVector_Fluent_AlwaysMatchesPostHoc()
    {
        // $nearVector is index-accelerated; Matches() returns true for any document
        var filter = BlqlFilter.NearVector("embedding", [0.1f, 0.2f, 0.3f], 5);
        var names = QueryNames(filter);
        Assert.Equal(5, names.Count); // all 5 docs match
    }

    [Fact]
    public void NearVector_Parser()
    {
        var filter = BlqlFilterParser.Parse(
            """{ "embedding": { "$nearVector": { "$vector": [0.1, 0.2, 0.3], "$k": 3, "$metric": "cosine" } } }""");
        // Just verify it parses without error and Matches returns true
        var names = QueryNames(filter);
        Assert.Equal(5, names.Count);
    }

    [Fact]
    public void NearVector_Parser_DefaultMetric()
    {
        // Omit $metric — should default to "cosine"
        var filter = BlqlFilterParser.Parse(
            """{ "embedding": { "$nearVector": { "$vector": [1.0, 2.0], "$k": 2 } } }""");
        Assert.NotNull(filter);
    }

    [Fact]
    public void NearVector_Parser_MissingVector_Throws()
    {
        Assert.Throws<FormatException>(() =>
            BlqlFilterParser.Parse("""{ "embedding": { "$nearVector": { "$k": 5 } } }"""));
    }

    [Fact]
    public void NearVector_Parser_NonObject_Throws()
    {
        Assert.Throws<FormatException>(() =>
            BlqlFilterParser.Parse("""{ "embedding": { "$nearVector": [0.1, 0.2] } }"""));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Combined filters (new operators with existing ones)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Combined_StartsWithAndMod()
    {
        // name starts with letter before 'D' AND qty is even
        var filter = BlqlFilter.And(
            BlqlFilter.StartsWith("email", "alice"),
            BlqlFilter.Mod("qty", 2, 0));
        var names = QueryNames(filter);
        Assert.Equal(["Alice"], names);
    }

    [Fact]
    public void Combined_AllAndSize()
    {
        // tags contains both "urgent" and "reviewed" AND tags has 3 elements
        var json = """{ "$and": [ { "tags": { "$all": ["urgent", "reviewed"] } }, { "tags": { "$size": 3 } } ] }""";
        var names = QueryNames(json);
        // Diana has ["reviewed","pending","urgent"] → size=3 and all match
        Assert.Equal(["Diana"], names);
    }

    [Fact]
    public void Combined_GeoNear_And_Contains()
    {
        // Near Rome AND email contains "gmail"
        var json = """{ "$and": [ { "location": { "$geoNear": { "$center": [12.4964, 41.9028], "$maxDistance": 100 } } }, { "email": { "$contains": "gmail" } } ] }""";
        var names = QueryNames(json);
        Assert.Equal(["Alice"], names);
    }

    [Fact]
    public void Combined_FieldNot_And_ElemMatch()
    {
        // scores has element >= 90 AND qty is NOT odd
        var json = """{ "$and": [ { "scores": { "$elemMatch": { "$gte": 90 } } }, { "qty": { "$not": { "$mod": [2, 1] } } } ] }""";
        // Alice: scores has 92 ✓, qty=12 (even) ✓
        // Charlie: scores has 90,95 ✓, qty=20 (even) ✓
        // Eve: scores has 100 ✓, qty=15 (odd) ✗
        var names = QueryNames(json);
        Assert.Equal(["Alice", "Charlie"], names);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Edge cases
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void StartsWith_EmptyPrefix_MatchesAll()
    {
        var names = QueryNames(BlqlFilter.StartsWith("name", ""));
        Assert.Equal(5, names.Count);
    }

    [Fact]
    public void EndsWith_EmptyPostfix_MatchesAll()
    {
        var names = QueryNames(BlqlFilter.EndsWith("email", ""));
        Assert.Equal(5, names.Count);
    }

    [Fact]
    public void Contains_EmptySubstring_MatchesAll()
    {
        var names = QueryNames(BlqlFilter.Contains("name", ""));
        Assert.Equal(5, names.Count);
    }

    [Fact]
    public void Size_MissingField_NoMatch()
    {
        var names = QueryNames(BlqlFilter.Size("nonexistent", 0));
        Assert.Empty(names);
    }

    [Fact]
    public void All_EmptyValuesArray_MatchesAllDocs()
    {
        // $all with no required values — vacuously true for any array
        var filter = BlqlFilter.All("tags", Array.Empty<BsonValue>());
        var names = QueryNames(filter);
        Assert.Equal(5, names.Count);
    }

    [Fact]
    public void Mod_FieldAbsent_NoMatch()
    {
        var names = QueryNames(BlqlFilter.Mod("nonexistent", 2, 0));
        Assert.Empty(names);
    }

    [Fact]
    public void GeoWithin_NonCoordinateField_NoMatch()
    {
        var filter = BlqlFilter.GeoWithin("name", -180, -90, 180, 90);
        var names = QueryNames(filter);
        Assert.Empty(names);
    }

    [Fact]
    public void GeoNear_NonCoordinateField_NoMatch()
    {
        var filter = BlqlFilter.GeoNear("name", 0, 0, 10000);
        var names = QueryNames(filter);
        Assert.Empty(names);
    }
}
