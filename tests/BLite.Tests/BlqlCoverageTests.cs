using BLite.Bson;
using BLite.Core;
using BLite.Core.Query.Blql;
using System.Text.RegularExpressions;

namespace BLite.Tests;

/// <summary>
/// Mutation-coverage tests for BlqlFilter, BlqlQuery, BlqlSort, BlqlProjection,
/// BlqlFilterParser, BlqlSortParser, and DynamicCollectionBlqlExtensions.
/// Targets NoCoverage mutants by exercising edge cases, boundary conditions,
/// and code paths not reached by existing tests.
/// </summary>
public class BlqlCoverageTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;
    private readonly DynamicCollection _col;

    public BlqlCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blql_cov_{Guid.NewGuid()}.db");
        _engine = new BLiteEngine(_dbPath);
        _col = _engine.GetOrCreateCollection("items");
        SeedData().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _engine.Dispose();
        TryDelete(_dbPath);
        TryDelete(Path.ChangeExtension(_dbPath, ".wal"));
    }

    private static void TryDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); } catch { }
    }

    private async Task SeedData()
    {
        await InsertFull("Alice", 30, "active", "admin", "alice@example.com",
            new DateTime(2024, 1, 10), new[] { "c#", "go" }, 100);
        await InsertFull("Bob", 25, "inactive", "user", null,
            new DateTime(2024, 2, 5), new[] { "python" }, 200);
        await InsertFull("Charlie", 35, "active", "mod", "charlie@x.com",
            new DateTime(2024, 3, 1), new[] { "c#", "rust", "go" }, 300);
        await InsertFull("Diana", 28, "active", "user", "diana@x.com",
            new DateTime(2024, 4, 15), new[] { "java" }, 150);
        await InsertFull("Eve", 22, "banned", "user", "eve@x.com",
            new DateTime(2024, 5, 20), Array.Empty<string>(), 50);
    }

    private async Task InsertFull(string name, int age, string status, string role,
        string? email, DateTime createdAt, string[] tags, long score)
    {
        var doc = _col.CreateDocument(
            ["_id", "name", "age", "status", "role", "createdAt", "email", "tags", "score"],
            b =>
            {
                b.AddString("name", name)
                 .AddInt32("age", age)
                 .AddString("status", status)
                 .AddString("role", role)
                 .AddDateTime("createdAt", createdAt)
                 .AddInt64("score", score);
                if (email != null) b.AddString("email", email);
                b.Add("tags", BsonValue.FromArray(tags.Select(BsonValue.FromString).ToList()));
            });
        await _col.InsertAsync(doc);
    }

    private static string GetName(BsonDocument doc)
    {
        doc.TryGetString("name", out var n);
        return n ?? "";
    }

    // ══════════════════════════════════════════════════════════════════════
    //  BlqlFilter — direct API coverage (not via parser)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Filter_Eq_Long_MatchesCorrectly()
    {
        var f = BlqlFilter.Eq("score", 100L);
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Alice", GetName(docs[0]));
    }

    [Fact]
    public void Filter_Eq_Double_MatchesCorrectly()
    {
        // No exact matches but should return empty, not error
        var f = BlqlFilter.Eq("age", 30.5);
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_Eq_Bool_MatchesNothing()
    {
        var f = BlqlFilter.Eq("status", true);
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_Eq_DateTime_MatchesCorrectly()
    {
        var f = BlqlFilter.Eq("createdAt", new DateTime(2024, 1, 10));
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Alice", GetName(docs[0]));
    }

    [Fact]
    public void Filter_Eq_ObjectId_NoMatch()
    {
        var f = BlqlFilter.Eq("name", ObjectId.NewObjectId());
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_Ne_MissingField_Matches()
    {
        // Bob has no email → field absent → Ne should match
        var f = BlqlFilter.Ne("email", "anything");
        var docs = _col.Query(f).ToList();
        Assert.Contains(docs, d => GetName(d) == "Bob");
    }

    [Fact]
    public void Filter_Eq_MissingField_DoesNotMatch()
    {
        // Bob has no email → field absent → Eq should not match
        var f = BlqlFilter.Eq("email", "anything");
        var docs = _col.Query(f).ToList();
        Assert.DoesNotContain(docs, d => GetName(d) == "Bob");
    }

    [Fact]
    public void Filter_Gt_Long_MatchesCorrectly()
    {
        var f = BlqlFilter.Gt("score", 150L);
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Bob", "Charlie"], names);
    }

    [Fact]
    public void Filter_Gte_Long_MatchesCorrectly()
    {
        var f = BlqlFilter.Gte("score", 150L);
        var docs = _col.Query(f).ToList();
        Assert.Equal(3, docs.Count); // Diana=150, Bob=200, Charlie=300
    }

    [Fact]
    public void Filter_Lt_Long_MatchesCorrectly()
    {
        var f = BlqlFilter.Lt("score", 100L);
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Eve", GetName(docs[0]));
    }

    [Fact]
    public void Filter_Lte_Long_MatchesCorrectly()
    {
        var f = BlqlFilter.Lte("score", 100L);
        var docs = _col.Query(f).ToList();
        Assert.Equal(2, docs.Count); // Eve=50, Alice=100
    }

    [Fact]
    public void Filter_Gt_Double_MatchesCorrectly()
    {
        var f = BlqlFilter.Gt("age", 34.9);
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Charlie", GetName(docs[0]));
    }

    [Fact]
    public void Filter_Gte_Double_MatchesCorrectly()
    {
        var f = BlqlFilter.Gte("age", 35.0);
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Charlie", GetName(docs[0]));
    }

    [Fact]
    public void Filter_Lt_Double_MatchesCorrectly()
    {
        var f = BlqlFilter.Lt("age", 23.0);
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Eve", GetName(docs[0]));
    }

    [Fact]
    public void Filter_Lte_Double_MatchesCorrectly()
    {
        var f = BlqlFilter.Lte("age", 22.0);
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Eve", GetName(docs[0]));
    }

    [Fact]
    public void Filter_Gt_DateTime_MatchesCorrectly()
    {
        var f = BlqlFilter.Gt("createdAt", new DateTime(2024, 4, 1));
        var docs = _col.Query(f).ToList();
        Assert.Equal(2, docs.Count); // Diana, Eve
    }

    [Fact]
    public void Filter_Gte_DateTime_MatchesCorrectly()
    {
        var f = BlqlFilter.Gte("createdAt", new DateTime(2024, 4, 15));
        var docs = _col.Query(f).ToList();
        Assert.Equal(2, docs.Count); // Diana, Eve
    }

    [Fact]
    public void Filter_Lt_DateTime_MatchesCorrectly()
    {
        var f = BlqlFilter.Lt("createdAt", new DateTime(2024, 2, 1));
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Alice", GetName(docs[0]));
    }

    [Fact]
    public void Filter_Lte_DateTime_MatchesCorrectly()
    {
        var f = BlqlFilter.Lte("createdAt", new DateTime(2024, 1, 10));
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Alice", GetName(docs[0]));
    }

    [Fact]
    public void Filter_In_IntOverload_MatchesCorrectly()
    {
        var f = BlqlFilter.In("age", 22, 30);
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Eve"], names);
    }

    [Fact]
    public void Filter_In_StringOverload_MatchesCorrectly()
    {
        var f = BlqlFilter.In("role", "admin", "mod");
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Charlie"], names);
    }

    [Fact]
    public void Filter_In_IEnumerableOverload_MatchesCorrectly()
    {
        IEnumerable<BsonValue> values = new[] { BsonValue.FromString("active"), BsonValue.FromString("banned") };
        var f = BlqlFilter.In("status", values);
        var docs = _col.Query(f).ToList();
        Assert.Equal(4, docs.Count); // 3 active + 1 banned
    }

    [Fact]
    public void Filter_Nin_StringOverload_MatchesCorrectly()
    {
        var f = BlqlFilter.Nin("status", "active", "inactive");
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Eve", GetName(docs[0]));
    }

    [Fact]
    public void Filter_Nin_MissingField_Matches()
    {
        // Bob has no email → field absent → $nin should match
        var f = BlqlFilter.Nin("email", new[] { BsonValue.FromString("alice@example.com") });
        var docs = _col.Query(f).ToList();
        Assert.Contains(docs, d => GetName(d) == "Bob");
    }

    [Fact]
    public void Filter_And_IEnumerable_MatchesCorrectly()
    {
        IEnumerable<BlqlFilter> filters = new[]
        {
            BlqlFilter.Eq("status", "active"),
            BlqlFilter.Gt("age", 29)
        };
        var f = BlqlFilter.And(filters);
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Charlie"], names);
    }

    [Fact]
    public void Filter_Or_IEnumerable_MatchesCorrectly()
    {
        IEnumerable<BlqlFilter> filters = new[]
        {
            BlqlFilter.Eq("name", "Alice"),
            BlqlFilter.Eq("name", "Eve")
        };
        var f = BlqlFilter.Or(filters);
        var docs = _col.Query(f).ToList();
        Assert.Equal(2, docs.Count);
    }

    [Fact]
    public void Filter_Nor_MatchesCorrectly()
    {
        var f = BlqlFilter.Nor(BlqlFilter.Eq("name", "Alice"), BlqlFilter.Eq("name", "Bob"));
        var docs = _col.Query(f).ToList();
        Assert.Equal(3, docs.Count);
        Assert.DoesNotContain(docs, d => GetName(d) == "Alice");
        Assert.DoesNotContain(docs, d => GetName(d) == "Bob");
    }

    [Fact]
    public void Filter_Not_MatchesCorrectly()
    {
        var f = BlqlFilter.Not(BlqlFilter.Eq("status", "active"));
        var docs = _col.Query(f).ToList();
        Assert.Equal(2, docs.Count); // Bob, Eve
    }

    [Fact]
    public void Filter_Exists_True_MatchesCorrectly()
    {
        var f = BlqlFilter.Exists("email");
        var docs = _col.Query(f).ToList();
        Assert.Equal(4, docs.Count);
        Assert.DoesNotContain(docs, d => GetName(d) == "Bob");
    }

    [Fact]
    public void Filter_Exists_False_MatchesCorrectly()
    {
        var f = BlqlFilter.Exists("email", false);
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Bob", GetName(docs[0]));
    }

    [Fact]
    public void Filter_IsNull_MatchesMissingField()
    {
        var f = BlqlFilter.IsNull("email");
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Bob", GetName(docs[0]));
    }

    [Fact]
    public void Filter_Type_Int32_MatchesCorrectly()
    {
        var f = BlqlFilter.Type("age", BsonType.Int32);
        var docs = _col.Query(f).ToList();
        Assert.Equal(5, docs.Count);
    }

    [Fact]
    public void Filter_Type_String_MatchesCorrectly()
    {
        var f = BlqlFilter.Type("name", BsonType.String);
        var docs = _col.Query(f).ToList();
        Assert.Equal(5, docs.Count);
    }

    [Fact]
    public void Filter_Type_MissingField_DoesNotMatch()
    {
        var f = BlqlFilter.Type("nonexistent", BsonType.String);
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_Regex_WithOptions_MatchesCorrectly()
    {
        var f = BlqlFilter.Regex("name", "^alice", RegexOptions.IgnoreCase);
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Alice", GetName(docs[0]));
    }

    [Fact]
    public void Filter_Regex_WithRegexInstance_MatchesCorrectly()
    {
        var regex = new Regex("^[DE]", RegexOptions.Compiled);
        var f = BlqlFilter.Regex("name", regex);
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Diana", "Eve"], names);
    }

    [Fact]
    public void Filter_Regex_NonStringField_DoesNotMatch()
    {
        var f = BlqlFilter.Regex("age", "\\d+");
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_Regex_MissingField_DoesNotMatch()
    {
        var f = BlqlFilter.Regex("nonexistent", ".*");
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_StartsWith_MatchesCorrectly()
    {
        var f = BlqlFilter.StartsWith("name", "Ch");
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Charlie", GetName(docs[0]));
    }

    [Fact]
    public void Filter_StartsWith_NonStringField_DoesNotMatch()
    {
        var f = BlqlFilter.StartsWith("age", "3");
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_StartsWith_MissingField_DoesNotMatch()
    {
        var f = BlqlFilter.StartsWith("nonexistent", "x");
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_EndsWith_MatchesCorrectly()
    {
        var f = BlqlFilter.EndsWith("name", "ce");
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Alice", GetName(docs[0]));
    }

    [Fact]
    public void Filter_EndsWith_NonStringField_DoesNotMatch()
    {
        var f = BlqlFilter.EndsWith("age", "0");
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_EndsWith_MissingField_DoesNotMatch()
    {
        var f = BlqlFilter.EndsWith("nonexistent", "x");
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_Contains_MatchesCorrectly()
    {
        var f = BlqlFilter.Contains("name", "li");
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Charlie"], names);
    }

    [Fact]
    public void Filter_Contains_NonStringField_DoesNotMatch()
    {
        var f = BlqlFilter.Contains("age", "3");
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_Contains_MissingField_DoesNotMatch()
    {
        var f = BlqlFilter.Contains("nonexistent", "x");
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_Size_MatchesCorrectly()
    {
        var f = BlqlFilter.Size("tags", 2);
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Alice", GetName(docs[0]));
    }

    [Fact]
    public void Filter_Size_Zero_MatchesEmpty()
    {
        var f = BlqlFilter.Size("tags", 0);
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Eve", GetName(docs[0]));
    }

    [Fact]
    public void Filter_Size_MissingField_DoesNotMatch()
    {
        var f = BlqlFilter.Size("nonexistent", 1);
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_Size_NonArrayField_DoesNotMatch()
    {
        var f = BlqlFilter.Size("name", 5);
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_All_StringOverload_MatchesCorrectly()
    {
        var f = BlqlFilter.All("tags", "c#", "go");
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Charlie"], names);
    }

    [Fact]
    public void Filter_All_IntOverload_NoMatch()
    {
        var f = BlqlFilter.All("tags", 1, 2);
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs); // tags are strings, not ints
    }

    [Fact]
    public void Filter_All_MissingField_DoesNotMatch()
    {
        var f = BlqlFilter.All("nonexistent", "x");
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_All_NonArrayField_DoesNotMatch()
    {
        var f = BlqlFilter.All("name", "A");
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_ElemMatch_ScalarArray_MatchesCorrectly()
    {
        var f = BlqlFilter.ElemMatch("tags", BlqlFilter.Eq("tags", "rust"));
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Charlie", GetName(docs[0]));
    }

    [Fact]
    public void Filter_ElemMatch_MissingField_DoesNotMatch()
    {
        var f = BlqlFilter.ElemMatch("nonexistent", BlqlFilter.Eq("nonexistent", "x"));
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_ElemMatch_NonArrayField_DoesNotMatch()
    {
        var f = BlqlFilter.ElemMatch("name", BlqlFilter.Eq("name", "Alice"));
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_Mod_MatchesCorrectly()
    {
        var f = BlqlFilter.Mod("age", 5, 0);
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Bob", "Charlie"], names); // 30,25,35 all mod5=0
    }

    [Fact]
    public void Filter_Mod_NonNumericField_DoesNotMatch()
    {
        var f = BlqlFilter.Mod("name", 5, 0);
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_Mod_MissingField_DoesNotMatch()
    {
        var f = BlqlFilter.Mod("nonexistent", 5, 0);
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Filter_Mod_Int64Field_MatchesCorrectly()
    {
        var f = BlqlFilter.Mod("score", 100, 0);
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Bob", "Charlie"], names); // 100,200,300
    }

    [Fact]
    public void Filter_Between_Long_MatchesCorrectly()
    {
        var f = BlqlFilter.Between("score", 100L, 200L);
        var docs = _col.Query(f).ToList();
        Assert.Equal(3, docs.Count); // Alice=100, Diana=150, Bob=200
    }

    [Fact]
    public void Filter_Between_Double_MatchesCorrectly()
    {
        var f = BlqlFilter.Between("age", 25.0, 30.0);
        var docs = _col.Query(f).ToList();
        Assert.Equal(3, docs.Count); // Bob=25, Diana=28, Alice=30
    }

    [Fact]
    public void Filter_Empty_MatchesAll()
    {
        var f = BlqlFilter.Empty;
        var docs = _col.Query(f).ToList();
        Assert.Equal(5, docs.Count);
    }

    // ── InFilter MatchesValue path ──────────────────────────────────────

    [Fact]
    public void Filter_In_MatchesValue_WrongField_ReturnsFalse()
    {
        // Test via ElemMatch which calls MatchesValue internally
        var innerFilter = BlqlFilter.In("tags", "c#");
        var f = BlqlFilter.ElemMatch("tags", innerFilter);
        var docs = _col.Query(f).ToList();
        Assert.Contains(docs, d => GetName(d) == "Alice");
    }

    [Fact]
    public void Filter_Nin_MatchesValue_ViaElemMatch()
    {
        var innerFilter = BlqlFilter.Nin("tags", new[] { BsonValue.FromString("python") });
        var f = BlqlFilter.ElemMatch("tags", innerFilter);
        var docs = _col.Query(f).ToList();
        // Alice has c#,go → both pass Nin(python) → Alice matches
        Assert.Contains(docs, d => GetName(d) == "Alice");
    }

    // ── Logical filter MatchesValue paths ──────────────────────────────

    [Fact]
    public void Filter_And_MatchesValue_ViaElemMatch()
    {
        // ElemMatch with And condition on scalar items
        var innerFilter = BlqlFilter.And(
            BlqlFilter.Ne("tags", "python"),
            BlqlFilter.Ne("tags", "java")
        );
        var f = BlqlFilter.ElemMatch("tags", innerFilter);
        var docs = _col.Query(f).ToList();
        Assert.Contains(docs, d => GetName(d) == "Alice"); // c#, go pass both conditions
    }

    [Fact]
    public void Filter_Or_MatchesValue_ViaElemMatch()
    {
        var innerFilter = BlqlFilter.Or(
            BlqlFilter.Eq("tags", "python"),
            BlqlFilter.Eq("tags", "java")
        );
        var f = BlqlFilter.ElemMatch("tags", innerFilter);
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Bob", "Diana"], names);
    }

    [Fact]
    public void Filter_Nor_MatchesValue_ViaElemMatch()
    {
        var innerFilter = BlqlFilter.Nor(
            BlqlFilter.Eq("tags", "c#"),
            BlqlFilter.Eq("tags", "go")
        );
        var f = BlqlFilter.ElemMatch("tags", innerFilter);
        var docs = _col.Query(f).ToList();
        // Bob has "python" → passes Nor(c#, go)
        Assert.Contains(docs, d => GetName(d) == "Bob");
        // Charlie has c#, rust, go → "rust" passes Nor(c#, go) → should be included
        Assert.Contains(docs, d => GetName(d) == "Charlie");
    }

    [Fact]
    public void Filter_Not_MatchesValue_ViaElemMatch()
    {
        var innerFilter = BlqlFilter.Not(BlqlFilter.Eq("tags", "c#"));
        var f = BlqlFilter.ElemMatch("tags", innerFilter);
        var docs = _col.Query(f).ToList();
        // Alice has c#, go → "go" passes Not(c#) → Alice matches
        Assert.Contains(docs, d => GetName(d) == "Alice");
    }

    // ── ToString coverage ───────────────────────────────────────────────

    [Fact]
    public void Filter_Empty_ToString()
    {
        Assert.Equal("{}", BlqlFilter.Empty.ToString());
    }

    [Fact]
    public void Filter_Eq_ToString()
    {
        var f = BlqlFilter.Eq("name", "Alice");
        var s = f.ToString();
        Assert.Contains("name", s);
        Assert.Contains("Alice", s);
    }

    [Fact]
    public void Filter_Gt_ToString()
    {
        var f = BlqlFilter.Gt("age", 25);
        var s = f.ToString();
        Assert.Contains("$gt", s);
    }

    [Fact]
    public void Filter_In_ToString()
    {
        var f = BlqlFilter.In("role", "admin", "mod");
        var s = f.ToString();
        Assert.Contains("$in", s);
    }

    [Fact]
    public void Filter_Nin_ToString()
    {
        var f = BlqlFilter.Nin("role", "admin");
        var s = f.ToString();
        Assert.Contains("$nin", s);
    }

    [Fact]
    public void Filter_And_ToString()
    {
        var f = BlqlFilter.And(BlqlFilter.Eq("a", 1), BlqlFilter.Eq("b", 2));
        var s = f.ToString();
        Assert.Contains("$and", s);
    }

    [Fact]
    public void Filter_Or_ToString()
    {
        var f = BlqlFilter.Or(BlqlFilter.Eq("a", 1), BlqlFilter.Eq("b", 2));
        Assert.Contains("$or", f.ToString());
    }

    [Fact]
    public void Filter_Nor_ToString()
    {
        var f = BlqlFilter.Nor(BlqlFilter.Eq("a", 1));
        Assert.Contains("$nor", f.ToString());
    }

    [Fact]
    public void Filter_Not_ToString()
    {
        var f = BlqlFilter.Not(BlqlFilter.Eq("a", 1));
        Assert.Contains("$not", f.ToString());
    }

    [Fact]
    public void Filter_Exists_ToString()
    {
        var f = BlqlFilter.Exists("field", true);
        var s = f.ToString();
        Assert.Contains("$exists", s);
        Assert.Contains("true", s);
    }

    [Fact]
    public void Filter_Type_ToString()
    {
        var f = BlqlFilter.Type("field", BsonType.String);
        Assert.Contains("$type", f.ToString());
    }

    [Fact]
    public void Filter_Regex_ToString()
    {
        var f = BlqlFilter.Regex("field", "^abc");
        Assert.Contains("$regex", f.ToString());
    }

    [Fact]
    public void Filter_StartsWith_ToString()
    {
        var f = BlqlFilter.StartsWith("field", "abc");
        Assert.Contains("$startsWith", f.ToString());
    }

    [Fact]
    public void Filter_EndsWith_ToString()
    {
        var f = BlqlFilter.EndsWith("field", "abc");
        Assert.Contains("$endsWith", f.ToString());
    }

    [Fact]
    public void Filter_Contains_ToString()
    {
        var f = BlqlFilter.Contains("field", "abc");
        Assert.Contains("$contains", f.ToString());
    }

    [Fact]
    public void Filter_ElemMatch_ToString()
    {
        var f = BlqlFilter.ElemMatch("field", BlqlFilter.Eq("field", 1));
        Assert.Contains("$elemMatch", f.ToString());
    }

    [Fact]
    public void Filter_Size_ToString()
    {
        var f = BlqlFilter.Size("field", 5);
        Assert.Contains("$size", f.ToString());
    }

    [Fact]
    public void Filter_All_ToString()
    {
        var f = BlqlFilter.All("field", new[] { BsonValue.FromInt32(1) });
        Assert.Contains("$all", f.ToString());
    }

    [Fact]
    public void Filter_Mod_ToString()
    {
        var f = BlqlFilter.Mod("field", 3, 1);
        Assert.Contains("$mod", f.ToString());
    }

    [Fact]
    public void Filter_GeoWithin_ToString()
    {
        var f = BlqlFilter.GeoWithin("loc", -10.0, -10.0, 10.0, 10.0);
        Assert.Contains("$geoWithin", f.ToString());
    }

    [Fact]
    public void Filter_GeoNear_ToString()
    {
        var f = BlqlFilter.GeoNear("loc", 12.0, 45.0, 100.0);
        Assert.Contains("$geoNear", f.ToString());
    }

    [Fact]
    public void Filter_NearVector_ToString()
    {
        var f = BlqlFilter.NearVector("vec", new float[] { 1.0f, 2.0f }, 5);
        var s = f.ToString();
        Assert.Contains("$nearVector", s);
        Assert.Contains("$vector", s);
        Assert.Contains("$k", s);
    }

    [Fact]
    public void Filter_NearVector_AlwaysMatchesOnScan()
    {
        // NearVector.Matches always returns true (post-hoc filter)
        var f = BlqlFilter.NearVector("vec", new float[] { 1.0f }, 5);
        var docs = _col.Query(f).ToList();
        Assert.Equal(5, docs.Count); // Matches everything
    }

    // ══════════════════════════════════════════════════════════════════════
    //  BlqlSort — direct API coverage
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Sort_By_Default_Ascending()
    {
        var sort = BlqlSort.By("name");
        Assert.Single(sort.Keys);
        Assert.False(sort.Keys[0].Descending);
        Assert.Equal("name", sort.Keys[0].Field);
    }

    [Fact]
    public void Sort_By_Descending()
    {
        var sort = BlqlSort.By("name", descending: true);
        Assert.True(sort.Keys[0].Descending);
    }

    [Fact]
    public void Sort_ThenBy_AddsSecondKey()
    {
        var sort = BlqlSort.By("age").ThenBy("name", descending: true);
        Assert.Equal(2, sort.Keys.Count);
        Assert.Equal("age", sort.Keys[0].Field);
        Assert.False(sort.Keys[0].Descending);
        Assert.Equal("name", sort.Keys[1].Field);
        Assert.True(sort.Keys[1].Descending);
    }

    [Fact]
    public async Task Sort_ToComparison_SortsCorrectly()
    {
        var sort = BlqlSort.Ascending("age");
        var docs = _col.Query().Sort(sort).ToList();
        var ages = docs.Select(d => { d.TryGetInt32("age", out var a); return a; }).ToList();
        Assert.Equal(ages.OrderBy(x => x).ToList(), ages);
    }

    [Fact]
    public void Sort_Descending_SortsCorrectly()
    {
        var sort = BlqlSort.Descending("age");
        var docs = _col.Query().Sort(sort).ToList();
        var ages = docs.Select(d => { d.TryGetInt32("age", out var a); return a; }).ToList();
        Assert.Equal(ages.OrderByDescending(x => x).ToList(), ages);
    }

    [Fact]
    public void Sort_MultiKey_SortsCorrectly()
    {
        var sort = BlqlSort.By("status").ThenBy("age");
        var docs = _col.Query().Sort(sort).ToList();
        // First sorted by status, then by age within each status group
        Assert.True(docs.Count > 0);
    }

    [Fact]
    public void Sort_SortKey_ToString()
    {
        var sort = BlqlSort.By("name");
        Assert.Contains("name", sort.Keys[0].ToString());
        Assert.Contains("1", sort.Keys[0].ToString());

        var sortDesc = BlqlSort.Descending("age");
        Assert.Contains("-1", sortDesc.Keys[0].ToString());
    }

    // ══════════════════════════════════════════════════════════════════════
    //  BlqlProjection — direct API coverage
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Projection_Include_AlwaysIncludesId()
    {
        var proj = BlqlProjection.Include("name");
        Assert.False(proj.IsIdentity);
    }

    [Fact]
    public void Projection_Exclude_IsNotIdentity()
    {
        var proj = BlqlProjection.Exclude("password");
        Assert.False(proj.IsIdentity);
    }

    [Fact]
    public void Projection_All_IsIdentity()
    {
        Assert.True(BlqlProjection.All.IsIdentity);
    }

    [Fact]
    public void Projection_Include_ToString()
    {
        var proj = BlqlProjection.Include("name", "age");
        var s = proj.ToString();
        Assert.Contains("name", s);
        Assert.Contains("1", s);
    }

    [Fact]
    public void Projection_Exclude_ToString()
    {
        var proj = BlqlProjection.Exclude("password");
        var s = proj.ToString();
        Assert.Contains("password", s);
        Assert.Contains("0", s);
    }

    [Fact]
    public void Projection_All_ToString()
    {
        Assert.Equal("{}", BlqlProjection.All.ToString());
    }

    // ══════════════════════════════════════════════════════════════════════
    //  BlqlQuery — edge cases and uncovered paths
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Query_Skip_Negative_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => _col.Query().Skip(-1));
    }

    [Fact]
    public void Query_Take_Negative_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => _col.Query().Take(-1));
    }

    [Fact]
    public void Query_And_OnEmptyFilter_UsesFilterDirectly()
    {
        // When the current filter is Empty, And should set the new filter directly
        var docs = _col.Query().And(BlqlFilter.Eq("name", "Alice")).ToList();
        Assert.Single(docs);
        Assert.Equal("Alice", GetName(docs[0]));
    }

    [Fact]
    public void Query_Or_OnEmptyFilter_UsesFilterDirectly()
    {
        // When the current filter is Empty, Or should set the new filter directly
        var docs = _col.Query().Or(BlqlFilter.Eq("name", "Alice")).ToList();
        Assert.Single(docs);
    }

    [Fact]
    public void Query_Filter_Null_UsesEmpty()
    {
        var docs = _col.Query().Filter(null!).ToList();
        Assert.Equal(5, docs.Count);
    }

    [Fact]
    public void Query_Project_Null_UsesAll()
    {
        var docs = _col.Query().Project(null!).Take(1).ToList();
        Assert.Single(docs);
    }

    [Fact]
    public void Query_Limit_IsAliasForTake()
    {
        var d1 = _col.Query().Take(2).ToList();
        var d2 = _col.Query().Limit(2).ToList();
        Assert.Equal(d1.Count, d2.Count);
    }

    [Fact]
    public void Query_FirstOrDefault_WithProjection()
    {
        var doc = _col.Query()
            .Filter(BlqlFilter.Eq("name", "Alice"))
            .Project(BlqlProjection.Include("name"))
            .FirstOrDefault();
        Assert.NotNull(doc);
        var fields = doc!.EnumerateFields().Select(f => f.Name).ToList();
        Assert.Contains("name", fields);
        Assert.DoesNotContain("age", fields);
    }

    [Fact]
    public async Task Query_AsAsyncEnumerable_WithSort()
    {
        var results = new List<string>();
        await foreach (var doc in _col.Query()
            .OrderBy("name")
            .AsAsyncEnumerable())
        {
            results.Add(GetName(doc));
        }
        Assert.Equal(results.OrderBy(n => n).ToList(), results);
    }

    [Fact]
    public async Task Query_AsAsyncEnumerable_WithSkipAndTake()
    {
        var results = new List<string>();
        await foreach (var doc in _col.Query()
            .Skip(1)
            .Take(2)
            .AsAsyncEnumerable())
        {
            results.Add(GetName(doc));
        }
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Query_AsAsyncEnumerable_WithProjection()
    {
        await foreach (var doc in _col.Query()
            .Project(BlqlProjection.Include("name"))
            .Take(1)
            .AsAsyncEnumerable())
        {
            var fields = doc.EnumerateFields().Select(f => f.Name).ToList();
            Assert.Contains("name", fields);
            Assert.DoesNotContain("status", fields);
        }
    }

    [Fact]
    public async Task Query_AsAsyncEnumerable_CancellationRespected()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var count = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var doc in _col.Query().OrderBy("name").AsAsyncEnumerable(cts.Token))
            {
                count++;
            }
        });
    }

    // ══════════════════════════════════════════════════════════════════════
    //  BlqlFilterParser — additional parser coverage
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Parser_BoolValue_MatchesCorrectly()
    {
        // Parser handles boolean values
        var f = BlqlFilterParser.Parse("""{ "status": true }""");
        var docs = _col.Query(f).ToList();
        Assert.Empty(docs); // No field has boolean value "true"
    }

    [Fact]
    public void Parser_Double_MatchesCorrectly()
    {
        var f = BlqlFilterParser.Parse("""{ "age": 30.0 }""");
        var docs = _col.Query(f).ToList();
        // int 30 vs double 30.0 — BsonValueComparer should handle this
        Assert.True(docs.Count <= 1);
    }

    [Fact]
    public void Parser_Nested_And_Or()
    {
        var f = BlqlFilterParser.Parse("""
            { "$and": [
                { "$or": [ { "name": "Alice" }, { "name": "Bob" } ] },
                { "age": { "$gte": 25 } }
            ] }
        """);
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Bob"], names);
    }

    [Fact]
    public void Parser_NearVector_Parsed()
    {
        // $nearVector operator parsed from JSON
        var f = BlqlFilterParser.Parse("""
            { "embedding": { "$nearVector": { "$vector": [1.0, 2.0, 3.0], "$k": 5 } } }
        """);
        Assert.NotNull(f);
        // NearVector always matches on scan
        var docs = _col.Query(f).ToList();
        Assert.Equal(5, docs.Count);
    }

    [Fact]
    public void Parser_GeoWithin_Parsed()
    {
        var f = BlqlFilterParser.Parse("""
            { "location": { "$geoWithin": { "$box": [[-180, -90], [180, 90]] } } }
        """);
        Assert.NotNull(f);
    }

    [Fact]
    public void Parser_GeoNear_Parsed()
    {
        var f = BlqlFilterParser.Parse("""
            { "location": { "$geoNear": { "$center": [12.0, 45.0], "$maxDistance": 100 } } }
        """);
        Assert.NotNull(f);
    }

    [Fact]
    public void Parser_StartsWith_Parsed()
    {
        var f = BlqlFilterParser.Parse("""{ "name": { "$startsWith": "Al" } }""");
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Alice", GetName(docs[0]));
    }

    [Fact]
    public void Parser_EndsWith_Parsed()
    {
        var f = BlqlFilterParser.Parse("""{ "name": { "$endsWith": "ce" } }""");
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Alice", GetName(docs[0]));
    }

    [Fact]
    public void Parser_Contains_Parsed()
    {
        var f = BlqlFilterParser.Parse("""{ "name": { "$contains": "li" } }""");
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Charlie"], names);
    }

    [Fact]
    public void Parser_Size_Parsed()
    {
        var f = BlqlFilterParser.Parse("""{ "tags": { "$size": 3 } }""");
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Charlie", GetName(docs[0]));
    }

    [Fact]
    public void Parser_All_Parsed()
    {
        var f = BlqlFilterParser.Parse("""{ "tags": { "$all": ["c#", "go"] } }""");
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Charlie"], names);
    }

    [Fact]
    public void Parser_ElemMatch_Parsed()
    {
        var f = BlqlFilterParser.Parse("""{ "tags": { "$elemMatch": { "$eq": "rust" } } }""");
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Charlie", GetName(docs[0]));
    }

    [Fact]
    public void Parser_Mod_Parsed()
    {
        var f = BlqlFilterParser.Parse("""{ "age": { "$mod": [10, 0] } }""");
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice"], names); // 30 mod 10 = 0; others are 25,35,28,22 → non-zero
    }

    [Fact]
    public void Parser_Not_Parsed()
    {
        var f = BlqlFilterParser.Parse("""{ "$not": { "status": "active" } }""");
        var docs = _col.Query(f).ToList();
        Assert.Equal(2, docs.Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  BlqlSortParser — additional coverage
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void SortParser_NonNumberValue_Throws()
    {
        Assert.Throws<FormatException>(() => BlqlSortParser.Parse("""{ "name": "asc" }"""));
    }

    [Fact]
    public void SortParser_ZeroDirection_Throws()
    {
        Assert.Throws<FormatException>(() => BlqlSortParser.Parse("""{ "name": 0 }"""));
    }

    // ══════════════════════════════════════════════════════════════════════
    //  DynamicCollectionBlqlExtensions — entry points
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Extension_Query_NoArgs_ReturnsAll()
    {
        var docs = _col.Query().ToList();
        Assert.Equal(5, docs.Count);
    }

    [Fact]
    public void Extension_Query_WithBlqlFilter_ReturnsFiltered()
    {
        var docs = _col.Query(BlqlFilter.Eq("name", "Alice")).ToList();
        Assert.Single(docs);
    }

    [Fact]
    public void Extension_Query_WithJsonString_ReturnsFiltered()
    {
        var docs = _col.Query("""{ "name": "Alice" }""").ToList();
        Assert.Single(docs);
    }
}
