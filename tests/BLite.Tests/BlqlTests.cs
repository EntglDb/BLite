using BLite.Bson;
using BLite.Core;
using BLite.Core.Query.Blql;

namespace BLite.Tests;

/// <summary>
/// Unit tests for BLQL — the BLite Query Language.
/// Covers filter parsing from JSON strings, filter evaluation, sorting,
/// projection, paging and the full query pipeline on a real DynamicCollection.
/// </summary>
public class BlqlTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;
    private readonly DynamicCollection _col;

    public BlqlTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blql_test_{Guid.NewGuid()}.db");
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
        // 10 documents to exercise all query scenarios
        Insert("Alice",   30, "active",  "admin",    "alice@example.com", new DateTime(2024, 1, 10));
        Insert("Bob",     25, "inactive","user",     null,                 new DateTime(2024, 2, 5));
        Insert("Charlie", 35, "active",  "mod",      "charlie@x.com",     new DateTime(2024, 3, 1));
        Insert("Diana",   28, "active",  "user",     "diana@x.com",       new DateTime(2024, 4, 15));
        Insert("Eve",     22, "banned",  "user",     "eve@x.com",         new DateTime(2024, 5, 20));
        Insert("Frank",   40, "active",  "admin",    "frank@x.com",       new DateTime(2024, 6, 10));
        Insert("Grace",   18, "active",  "user",     "grace@x.com",       new DateTime(2024, 7, 7));
        Insert("Hank",    55, "inactive","superadmin","hank@x.com",       new DateTime(2024, 8, 1));
        Insert("Iris",    31, "active",  "mod",      "iris@x.com",        new DateTime(2024, 9, 9));
        Insert("Jack",    19, "active",  "user",     "jack@x.com",        new DateTime(2024, 10, 3));
    }

    private void Insert(string name, int age, string status, string role, string? email, DateTime createdAt)
    {
        // Register all possible field names up-front in the key map
        var doc = _col.CreateDocument(
            ["_id", "name", "age", "status", "role", "createdAt", "email"],
            b =>
            {
                b.AddString("name", name)
                 .AddInt32("age", age)
                 .AddString("status", status)
                 .AddString("role", role)
                 .AddDateTime("createdAt", createdAt);
                if (email != null) b.AddString("email", email);
            });
        _col.Insert(doc);
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static string GetName(BsonDocument doc)
    {
        doc.TryGetString("name", out var n);
        return n ?? "";
    }

    private static int GetAge(BsonDocument doc)
    {
        doc.TryGetInt32("age", out var a);
        return a;
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  BlqlFilterParser — parsing correctness
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Parser_EmptyJson_ReturnsEmptyFilter()
    {
        var f = BlqlFilterParser.Parse("{}");
        Assert.Same(BlqlFilter.Empty, f);
    }

    [Fact]
    public void Parser_Equality_String()
    {
        var f = BlqlFilterParser.Parse("""{ "status": "active" }""");
        var docs = _col.Query(f).ToList();
        Assert.Equal(7, docs.Count);
        Assert.All(docs, d => { d.TryGetString("status", out var s); Assert.Equal("active", s); });
    }

    [Fact]
    public void Parser_Equality_Int()
    {
        var f = BlqlFilterParser.Parse("""{ "age": 30 }""");
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Alice", GetName(docs[0]));
    }

    [Fact]
    public void Parser_GT_Operator()
    {
        var f = BlqlFilterParser.Parse("""{ "age": { "$gt": 35 } }""");
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Frank", "Hank"], names);
    }

    [Fact]
    public void Parser_GTE_Operator()
    {
        var f = BlqlFilterParser.Parse("""{ "age": { "$gte": 35 } }""");
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Charlie", "Frank", "Hank"], names);
    }

    [Fact]
    public void Parser_LT_Operator()
    {
        var f = BlqlFilterParser.Parse("""{ "age": { "$lt": 23 } }""");
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Eve", "Grace", "Jack"], names);
    }

    [Fact]
    public void Parser_LTE_Operator()
    {
        var f = BlqlFilterParser.Parse("""{ "age": { "$lte": 22 } }""");
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Eve", "Grace", "Jack"], names);
    }

    [Fact]
    public void Parser_NE_Operator()
    {
        var f = BlqlFilterParser.Parse("""{ "status": { "$ne": "active" } }""");
        var docs = _col.Query(f).ToList();
        Assert.Equal(3, docs.Count);
        Assert.All(docs, d => { d.TryGetString("status", out var s); Assert.NotEqual("active", s); });
    }

    [Fact]
    public void Parser_In_Operator()
    {
        var f = BlqlFilterParser.Parse("""{ "role": { "$in": ["admin", "superadmin"] } }""");
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Frank", "Hank"], names);
    }

    [Fact]
    public void Parser_Nin_Operator()
    {
        var f = BlqlFilterParser.Parse("""{ "status": { "$nin": ["banned", "inactive"] } }""");
        var docs = _col.Query(f).ToList();
        Assert.Equal(7, docs.Count);
        Assert.All(docs, d => { d.TryGetString("status", out var s); Assert.Equal("active", s); });
    }

    [Fact]
    public void Parser_Exists_True()
    {
        var f = BlqlFilterParser.Parse("""{ "email": { "$exists": true } }""");
        var docs = _col.Query(f).ToList();
        // Bob has no email
        Assert.Equal(9, docs.Count);
        Assert.DoesNotContain(docs, d => GetName(d) == "Bob");
    }

    [Fact]
    public void Parser_Exists_False()
    {
        var f = BlqlFilterParser.Parse("""{ "email": { "$exists": false } }""");
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Bob", GetName(docs[0]));
    }

    [Fact]
    public void Parser_And_Operator()
    {
        var f = BlqlFilterParser.Parse("""
            { "$and": [
                { "status": "active" },
                { "age": { "$gt": 28 } }
            ] }
            """);
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Charlie", "Frank", "Iris"], names);
    }

    [Fact]
    public void Parser_Or_Operator()
    {
        var f = BlqlFilterParser.Parse("""
            { "$or": [
                { "role": "admin" },
                { "age": { "$lt": 20 } }
            ] }
            """);
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Frank", "Grace", "Jack"], names);
    }

    [Fact]
    public void Parser_Nor_Operator()
    {
        var f = BlqlFilterParser.Parse("""
            { "$nor": [
                { "status": "banned" },
                { "status": "inactive" }
            ] }
            """);
        var docs = _col.Query(f).ToList();
        Assert.Equal(7, docs.Count);
        Assert.All(docs, d => { d.TryGetString("status", out var s); Assert.Equal("active", s); });
    }

    [Fact]
    public void Parser_Not_Operator()
    {
        var f = BlqlFilterParser.Parse("""{ "$not": { "status": "active" } }""");
        var docs = _col.Query(f).ToList();
        Assert.Equal(3, docs.Count);
    }

    [Fact]
    public void Parser_ImplicitAnd_MultipleTopLevelFields()
    {
        // Multiple top-level keys → implicit AND
        var f = BlqlFilterParser.Parse("""{ "status": "active", "role": "admin" }""");
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Frank"], names);
    }

    [Fact]
    public void Parser_Range_GteAndLte_Combined()
    {
        // { "age": { "$gte": 25, "$lte": 31 } } → implicit AND of the two operators
        var f = BlqlFilterParser.Parse("""{ "age": { "$gte": 25, "$lte": 31 } }""");
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Bob", "Diana", "Iris"], names);
    }

    [Fact]
    public void Parser_Regex_Operator()
    {
        var f = BlqlFilterParser.Parse("""{ "name": { "$regex": "^[AE]" } }""");
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Eve"], names);
    }

    [Fact]
    public void Parser_Type_Operator()
    {
        // BsonType.Int32 = 0x10 = 16
        var f = BlqlFilterParser.Parse("""{ "age": { "$type": 16 } }""");
        var docs = _col.Query(f).ToList();
        Assert.Equal(10, docs.Count); // all have int32 age
    }

    [Fact]
    public void Parser_NullEquality()
    {
        var f = BlqlFilterParser.Parse("""{ "email": null }""");
        var docs = _col.Query(f).ToList();
        Assert.Single(docs);
        Assert.Equal("Bob", GetName(docs[0]));
    }

    [Fact]
    public void Parser_UnknownOperator_ThrowsFormatException()
    {
        Assert.Throws<FormatException>(() =>
            BlqlFilterParser.Parse("""{ "age": { "$unknownOp": 5 } }"""));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  String-based Query entry point
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void StringQuery_ReturnsFilteredResults()
    {
        var docs = _col.Query("""{ "status": "active", "age": { "$gte": 30 } }""").ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Charlie", "Frank", "Iris"], names);
    }

    [Fact]
    public void StringQuery_EmptyFilter_ReturnsAll()
    {
        var docs = _col.Query("{}").ToList();
        Assert.Equal(10, docs.Count);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  BlqlSortParser
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void SortParser_Ascending()
    {
        var sort = BlqlSortParser.Parse("""{ "name": 1 }""");
        Assert.NotNull(sort);
        Assert.Single(sort!.Keys);
        Assert.Equal("name", sort.Keys[0].Field);
        Assert.False(sort.Keys[0].Descending);
    }

    [Fact]
    public void SortParser_Descending()
    {
        var sort = BlqlSortParser.Parse("""{ "age": -1 }""");
        Assert.NotNull(sort);
        Assert.True(sort!.Keys[0].Descending);
    }

    [Fact]
    public void SortParser_MultiKey()
    {
        var sort = BlqlSortParser.Parse("""{ "status": 1, "age": -1 }""");
        Assert.NotNull(sort);
        Assert.Equal(2, sort!.Keys.Count);
        Assert.Equal("status", sort.Keys[0].Field);
        Assert.Equal("age", sort.Keys[1].Field);
        Assert.True(sort.Keys[1].Descending);
    }

    [Fact]
    public void SortParser_Empty_ReturnsNull()
    {
        Assert.Null(BlqlSortParser.Parse("{}"));
        Assert.Null(BlqlSortParser.Parse(""));
        Assert.Null(BlqlSortParser.Parse(null));
    }

    [Fact]
    public void SortParser_InvalidDirection_Throws()
    {
        Assert.Throws<FormatException>(() => BlqlSortParser.Parse("""{ "name": 2 }"""));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Sorting via Query pipeline
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Query_OrderBy_Ascending()
    {
        var docs = _col.Query().OrderBy("age").ToList();
        var ages = docs.Select(GetAge).ToList();
        Assert.Equal(ages.OrderBy(x => x).ToList(), ages);
    }

    [Fact]
    public void Query_OrderByDescending()
    {
        var docs = _col.Query().OrderByDescending("age").ToList();
        var ages = docs.Select(GetAge).ToList();
        Assert.Equal(ages.OrderByDescending(x => x).ToList(), ages);
    }

    [Fact]
    public void Query_Sort_StringJson()
    {
        var docs = _col.Query().Sort("""{ "name": 1 }""").ToList();
        var names = docs.Select(GetName).ToList();
        Assert.Equal(names.OrderBy(n => n, StringComparer.Ordinal).ToList(), names);
    }

    [Fact]
    public void Query_FilterAndSort()
    {
        var docs = _col.Query("""{ "status": "active" }""")
            .Sort("""{ "age": -1 }""")
            .ToList();

        var ages = docs.Select(GetAge).ToList();
        Assert.Equal(ages.OrderByDescending(x => x).ToList(), ages);
        Assert.All(docs, d => { d.TryGetString("status", out var s); Assert.Equal("active", s); });
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Skip / Take
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Query_Take_LimitsResults()
    {
        var docs = _col.Query().Take(3).ToList();
        Assert.Equal(3, docs.Count);
    }

    [Fact]
    public void Query_Skip_OffsetsResults()
    {
        var all  = _col.Query().OrderBy("name").ToList();
        var page = _col.Query().OrderBy("name").Skip(3).ToList();
        Assert.Equal(all.Skip(3).Select(GetName).ToList(), page.Select(GetName).ToList());
    }

    [Fact]
    public void Query_Skip_And_Take_Pagination()
    {
        var allByName = _col.Query().OrderBy("name").ToList();

        var page1 = _col.Query().OrderBy("name").Skip(0).Take(3).ToList();
        var page2 = _col.Query().OrderBy("name").Skip(3).Take(3).ToList();
        var page3 = _col.Query().OrderBy("name").Skip(6).Take(3).ToList();

        Assert.Equal(3, page1.Count);
        Assert.Equal(3, page2.Count);
        Assert.Equal(3, page3.Count);

        var merged = page1.Concat(page2).Concat(page3).Select(GetName).ToList();
        Assert.Equal(allByName.Take(9).Select(GetName).ToList(), merged);
    }

    [Fact]
    public void Query_Take_Zero_ReturnsEmpty()
    {
        var docs = _col.Query().Take(0).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Query_Limit_IsAliasForTake()
    {
        var docs = _col.Query().Limit(5).ToList();
        Assert.Equal(5, docs.Count);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Projection
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Projection_Include_OnlyRequestedFields()
    {
        var docs = _col.Query()
            .Filter(BlqlFilter.Eq("name", "Alice"))
            .Project(BlqlProjection.Include("name", "age"))
            .ToList();

        Assert.Single(docs);
        var fields = docs[0].EnumerateFields().Select(f => f.Name).ToList();
        Assert.Contains("_id", fields);
        Assert.Contains("name", fields);
        Assert.Contains("age", fields);
        Assert.DoesNotContain("status", fields);
        Assert.DoesNotContain("role", fields);
    }

    [Fact]
    public void Projection_Exclude_RemovesFields()
    {
        var docs = _col.Query()
            .Filter(BlqlFilter.Eq("name", "Alice"))
            .Project(BlqlProjection.Exclude("status", "role", "createdAt", "email"))
            .ToList();

        Assert.Single(docs);
        var fields = docs[0].EnumerateFields().Select(f => f.Name).ToList();
        Assert.Contains("name", fields);
        Assert.Contains("age", fields);
        Assert.DoesNotContain("status", fields);
        Assert.DoesNotContain("role", fields);
    }

    [Fact]
    public void Projection_All_IsIdentity()
    {
        Assert.True(BlqlProjection.All.IsIdentity);
        var docs = _col.Query().Project(BlqlProjection.All).Take(1).ToList();
        Assert.Single(docs);
        // All 10 fields should be present (including email for Alice)
        var fields = docs[0].EnumerateFields().Select(f => f.Name).ToList();
        Assert.Contains("name", fields);
        Assert.Contains("age", fields);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Terminal operators: Count, FirstOrDefault, First, Any, None
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Query_Count_NoFilter()
    {
        Assert.Equal(10, _col.Query().Count());
    }

    [Fact]
    public void Query_Count_WithFilter()
    {
        int count = _col.Query("""{ "status": "active" }""").Count();
        Assert.Equal(7, count);
    }

    [Fact]
    public void Query_FirstOrDefault_Found()
    {
        var doc = _col.Query(BlqlFilter.Eq("name", "Charlie")).FirstOrDefault();
        Assert.NotNull(doc);
        Assert.Equal("Charlie", GetName(doc!));
    }

    [Fact]
    public void Query_FirstOrDefault_NotFound_ReturnsNull()
    {
        var doc = _col.Query(BlqlFilter.Eq("name", "Nobody")).FirstOrDefault();
        Assert.Null(doc);
    }

    [Fact]
    public void Query_First_ThrowsWhenNotFound()
    {
        Assert.Throws<InvalidOperationException>(() =>
            _col.Query(BlqlFilter.Eq("name", "Nobody")).First());
    }

    [Fact]
    public void Query_Any_True()
    {
        Assert.True(_col.Query("""{ "role": "admin" }""").Any());
    }

    [Fact]
    public void Query_Any_False()
    {
        Assert.False(_col.Query("""{ "role": "ghost" }""").Any());
    }

    [Fact]
    public void Query_None_True()
    {
        Assert.True(_col.Query("""{ "role": "ghost" }""").None());
    }

    [Fact]
    public void Query_None_False()
    {
        Assert.False(_col.Query("""{ "role": "admin" }""").None());
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Fluent filter combinators (AndAlso / OrElse)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Filter_AndAlso_CombinesFilters()
    {
        var f = BlqlFilter.Eq("status", "active").AndAlso(BlqlFilter.Gt("age", 30));
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Charlie", "Frank", "Iris"], names);
    }

    [Fact]
    public void Filter_OrElse_CombinesFilters()
    {
        var f = BlqlFilter.Eq("role", "admin").OrElse(BlqlFilter.Eq("role", "superadmin"));
        var docs = _col.Query(f).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Frank", "Hank"], names);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  BlqlQuery.And / Or shorthand chaining on the query builder
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void BlqlQuery_And_Chaining()
    {
        var docs = _col.Query()
            .Filter(BlqlFilter.Eq("status", "active"))
            .And(BlqlFilter.Lt("age", 25))
            .ToList();

        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Grace", "Jack"], names);
    }

    [Fact]
    public void BlqlQuery_Or_Chaining()
    {
        var docs = _col.Query()
            .Filter(BlqlFilter.Eq("role", "admin"))
            .Or(BlqlFilter.Eq("role", "superadmin"))
            .ToList();

        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Frank", "Hank"], names);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  BlqlSort — BlqlFilter.Between helper
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Filter_Between_Int()
    {
        var docs = _col.Query(BlqlFilter.Between("age", 25, 30)).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Bob", "Diana"], names);
    }

    [Fact]
    public void Filter_Between_DateTime()
    {
        var from = new DateTime(2024, 3, 1);
        var to   = new DateTime(2024, 6, 10);
        var docs = _col.Query(BlqlFilter.Between("createdAt", from, to)).ToList();
        var names = docs.Select(GetName).OrderBy(n => n).ToList();
        Assert.Equal(["Charlie", "Diana", "Eve", "Frank"], names);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Async enumerable
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_AsAsyncEnumerable_WithFilter()
    {
        var results = new List<string>();
        await foreach (var doc in _col.Query("""{ "status": "active", "age": { "$lt": 25 } }""").AsAsyncEnumerable())
            results.Add(GetName(doc));

        results.Sort();
        Assert.Equal(["Grace", "Jack"], results);
    }
}
