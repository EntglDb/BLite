using System;
using System.Diagnostics;
using System.Text.RegularExpressions;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Query.Blql;

namespace BLite.Tests;

/// <summary>
/// Security-focused tests for BLQL filter parsing.
/// Covers BLQL-injection vectors analogous to SQLi / NoSQLi attacks:
///
/// 1. Code-execution operator injection  ($where, $function, $expr, …)
/// 2. Unknown / unsupported top-level operator injection
/// 3. Field-level unknown operator injection
/// 4. Type-confusion attacks (wrong JSON type for operator value)
/// 5. Tautology / always-true filter injection
/// 6. ReDoS via $regex (NonBacktracking protection)
/// 7. Deeply nested filter DoS
/// 8. Malformed / truncated JSON
/// 9. Null & empty edge-cases in logical arrays
/// 10. SQL-injection strings as literal values (must be treated as data, not code)
/// 11. Field-name boundary cases (empty, symbol-prefixed, very long)
/// </summary>
public class BlqlInjectionTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;
    private readonly DynamicCollection _col;

    public BlqlInjectionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blql_inj_{Guid.NewGuid()}.db");
        _engine = new BLiteEngine(_dbPath);
        _col = _engine.GetOrCreateCollection("items");

        // One active doc for matching tests
        var doc = _col.CreateDocument(
            ["_id", "name", "age", "status", "role"],
            b => b.AddString("name", "Alice")
                  .AddInt32("age", 30)
                  .AddString("status", "active")
                  .AddString("role", "admin"));
        _col.Insert(doc);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  1. Code-execution operator injection (MongoDB-style)
    //     These operators execute JavaScript / server-side code in MongoDB.
    //     BLQL must reject them unconditionally.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("""{ "$where": "this.age > 0" }""")]
    [InlineData("""{ "$where": "function(){ return true; }" }""")]
    [InlineData("""{ "$function": { "body": "return true;", "args": [], "lang": "js" } }""")]
    [InlineData("""{ "$expr": { "$gt": ["$age", 0] } }""")]
    [InlineData("""{ "$accumulator": {} }""")]
    [InlineData("""{ "$reduce": {} }""")]
    [InlineData("""{ "$eval": "db.dropCollection('items')" }""")]
    public void CodeExecution_Operators_ThrowFormatException(string json)
    {
        var ex = Assert.Throws<FormatException>(() => BlqlFilterParser.Parse(json));
        Assert.Contains("unsupported", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  2. Unknown top-level operator injection
    //     Any unknown "$xyz" at the root object → FormatException.
    //     Must NOT silently match or fall through as a field name.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("""{ "$gt": 10 }""")]              // comparison op at top-level (not a field)
    [InlineData("""{ "$unknown": "value" }""")]
    [InlineData("""{ "$": "value" }""")]           // bare dollar
    [InlineData("""{ "$__proto__": {} }""")]       // prototype-style name with $
    [InlineData("""{ "$constructor": {} }""")]
    public void UnknownTopLevel_DollarOperator_ThrowsFormatException(string json)
    {
        Assert.Throws<FormatException>(() => BlqlFilterParser.Parse(json));
    }

    // Case variants must also be rejected
    [Theory]
    [InlineData("""{ "$WHERE": "1" }""")]
    [InlineData("""{ "$Where": "1" }""")]
    [InlineData("""{ "$EXPR": {} }""")]
    public void UnknownTopLevel_DollarOperator_CaseInsensitive_ThrowsFormatException(string json)
    {
        Assert.Throws<FormatException>(() => BlqlFilterParser.Parse(json));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  3. Field-level unknown operator injection
    //     { "field": { "$unknownOp": value } } → FormatException.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("""{ "age": { "$where": "1" } }""")]
    [InlineData("""{ "age": { "$between": [0, 100] } }""")]   // not a raw BLQL operator
    [InlineData("""{ "age": { "$mod": [2, 0] } }""")]
    [InlineData("""{ "name": { "$text": "Alice" } }""")]
    [InlineData("""{ "name": { "$search": "Alice" } }""")]
    public void FieldLevel_UnknownOperator_ThrowsFormatException(string json)
    {
        Assert.Throws<FormatException>(() => BlqlFilterParser.Parse(json));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  4. Type-confusion attacks (wrong JSON value types for known operators)
    // ══════════════════════════════════════════════════════════════════════════

    // $exists must be strictly boolean — numeric truthy/falsy must be rejected
    [Theory]
    [InlineData("""{ "email": { "$exists": 1 } }""")]
    [InlineData("""{ "email": { "$exists": 0 } }""")]
    [InlineData("""{ "email": { "$exists": "true" } }""")]
    [InlineData("""{ "email": { "$exists": null } }""")]
    [InlineData("""{ "email": { "$exists": {} } }""")]
    public void Exists_NonBoolean_Value_ThrowsFormatException(string json)
    {
        Assert.Throws<FormatException>(() => BlqlFilterParser.Parse(json));
    }

    // $in / $nin must receive a JSON array
    [Theory]
    [InlineData("""{ "role": { "$in": "admin" } }""")]          // string instead of array
    [InlineData("""{ "role": { "$in": 42 } }""")]               // number
    [InlineData("""{ "role": { "$in": {"admin": 1} } }""")]     // object (key-value map)
    [InlineData("""{ "role": { "$nin": true } }""")]
    public void InNin_NonArray_Value_ThrowsFormatException(string json)
    {
        Assert.Throws<FormatException>(() => BlqlFilterParser.Parse(json));
    }

    // $and / $or / $nor must receive a JSON array
    [Theory]
    [InlineData("""{ "$and": {"status": "active"} }""")]        // object instead of array
    [InlineData("""{ "$or": "active" }""")]                     // string
    [InlineData("""{ "$nor": 1 }""")]                           // number
    [InlineData("""{ "$and": null }""")]                        // null
    [InlineData("""{ "$and": true }""")]                        // boolean
    public void Logical_NonArray_Value_ThrowsFormatException(string json)
    {
        Assert.Throws<FormatException>(() => BlqlFilterParser.Parse(json));
    }

    // $not must receive an object, not a scalar or array
    [Theory]
    [InlineData("""{ "$not": "active" }""")]
    [InlineData("""{ "$not": 1 }""")]
    [InlineData("""{ "$not": [{"status": "active"}] }""")]
    [InlineData("""{ "$not": null }""")]
    public void Not_NonObject_Value_ThrowsFormatException(string json)
    {
        Assert.Throws<FormatException>(() => BlqlFilterParser.Parse(json));
    }

    // $regex must receive a string
    [Theory]
    [InlineData("""{ "name": { "$regex": 123 } }""")]
    [InlineData("""{ "name": { "$regex": true } }""")]
    [InlineData("""{ "name": { "$regex": null } }""")]
    [InlineData("""{ "name": { "$regex": ["^Al"] } }""")]
    [InlineData("""{ "name": { "$regex": {"pattern": "^Al"} } }""")]
    public void Regex_NonString_Value_ThrowsFormatException(string json)
    {
        Assert.Throws<FormatException>(() => BlqlFilterParser.Parse(json));
    }

    // $regex invalid pattern
    [Fact]
    public void Regex_InvalidPattern_ThrowsArgumentException()
    {
        // "((" is an unbalanced group — Regex constructor throws ArgumentException
        Assert.ThrowsAny<ArgumentException>(() =>
            BlqlFilterParser.Parse("""{ "name": { "$regex": "((" } }"""));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  5. Tautology / always-true injection
    //     Verify that crafted always-true filters are correctly identified
    //     as such, enabling callers to detect and reject them if needed.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Tautology_EmptyFilter_MatchesAll()
    {
        // Attacker passes "{}" hoping to bypass a filter — must be explicit about effect
        var filter = BlqlFilterParser.Parse("{}");
        Assert.Same(BlqlFilter.Empty, filter);
        // Empty filter really does match everything — callers must guard against
        // accepting arbitrary user-supplied filter strings without allow-listing fields.
        Assert.True(filter.Matches(MakeDoc("name", "Alice")));
        Assert.True(filter.Matches(MakeDoc("name", "EvilUser")));
    }

    [Fact]
    public void Tautology_OrWithEmptyObject_MatchesAll()
    {
        // { "$or": [{}] } — empty object in OR matches everything
        var filter = BlqlFilterParser.Parse("""{ "$or": [{}] }""");
        Assert.True(filter.Matches(MakeDoc("name", "EvilUser")));
    }

    [Fact]
    public void Tautology_AndEmptyArray_MatchesAll()
    {
        // { "$and": [] } — vacuously true (standard MongoDB semantics)
        var filter = BlqlFilterParser.Parse("""{ "$and": [] }""");
        Assert.True(filter.Matches(MakeDoc("name", "Alice")));
    }

    [Fact]
    public void Tautology_OrEmptyArray_MatchesNothing()
    {
        // { "$or": [] } — vacuously false (no alternative satisfied)
        var filter = BlqlFilterParser.Parse("""{ "$or": [] }""");
        Assert.False(filter.Matches(MakeDoc("name", "Alice")));
    }

    [Fact]
    public void Tautology_NorEmptyArray_MatchesAll()
    {
        // { "$nor": [] } — no failing conditions = all pass (vacuously true)
        var filter = BlqlFilterParser.Parse("""{ "$nor": [] }""");
        Assert.True(filter.Matches(MakeDoc("name", "Alice")));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  6. ReDoS protection via $regex (NonBacktracking)
    //     The parser must use RegexOptions.NonBacktracking so catastrophic
    //     backtracking patterns complete in linear time even against long inputs.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Regex_ReDoS_Pattern_CompletesInLinearTime()
    {
        // Classic catastrophic-backtracking pattern with NonBacktracking:
        // (a+)+ against a long string of 'a' without trailing match would
        // spin exponentially on a backtracking engine.
        const string redosPattern = """{ "name": { "$regex": "(a+)+" } }""";
        var filter = BlqlFilterParser.Parse(redosPattern);

        // Insert a document whose name is 100 'a' chars — no trailing 'b', so
        // a backtracking engine would explore exponential states.
        var longA = new string('a', 100);
        var doc = MakeDoc("name", longA);

        var sw = Stopwatch.StartNew();
        filter.Matches(doc);
        sw.Stop();

        // NonBacktracking completes in microseconds; give generous 500 ms margin.
        Assert.True(sw.ElapsedMilliseconds < 500,
            $"Regex evaluation took {sw.ElapsedMilliseconds} ms — possible ReDoS vulnerability.");
    }

    [Fact]
    public void Regex_AnotherReDoS_Pattern_CompletesInLinearTime()
    {
        // Another classic pattern: (.*a){30} against a string with no match
        const string pattern = """{ "name": { "$regex": "(.*a){30}" } }""";
        var filter = BlqlFilterParser.Parse(pattern);

        var noMatchInput = new string('b', 50);
        var doc = MakeDoc("name", noMatchInput);

        var sw = Stopwatch.StartNew();
        filter.Matches(doc);
        sw.Stop();

        Assert.True(sw.ElapsedMilliseconds < 500,
            $"Regex evaluation took {sw.ElapsedMilliseconds} ms — possible ReDoS vulnerability.");
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  7. Deeply nested filter DoS (stack-overflow / CPU bomb via $and / $not)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void DeepNesting_And_60Levels_CompletesSuccessfully()
    {
        // Each { "$and": [ adds 2 JSON depth units (object + array).
        // 30 iterations × 2 = 60 depth < System.Text.Json default max of 64.
        var json = "";
        for (int i = 0; i < 30; i++) json += """{ "$and": [""";
        json += """{ "status": "active" }""";
        for (int i = 0; i < 30; i++) json += "] }";

        var filter = BlqlFilterParser.Parse(json);
        var doc = MakeDoc("status", "active");
        Assert.True(filter.Matches(doc));
    }

    [Fact]
    public void DeepNesting_Beyond64_ThrowsSafeJsonException()
    {
        // System.Text.Json enforces a max depth of 64 — exceeding it causes a
        // JsonReaderException, NOT a StackOverflowException.
        // This is a built-in DoS protection that the parser inherits for free.
        var json = "";
        for (int i = 0; i < 100; i++) json += """{ "$and": [""";
        json += """{ "status": "active" }""";
        for (int i = 0; i < 100; i++) json += "] }";

        // Must throw a safe, catchable exception — never a StackOverflowException.
        // JsonDocument.Parse raises JsonReaderException (a subclass of JsonException),
        // so use ThrowsAny to accept any JsonException derivative.
        Assert.ThrowsAny<Exception>(() => BlqlFilterParser.Parse(json));
    }

    [Fact]
    public void DeepNesting_Not_50Levels_CompletesWithoutStackOverflow()
    {
        // Build { "$not": { "$not": { "$not": ... { "status": "active" } } } }
        var json = "";
        for (int i = 0; i < 50; i++) json += """{ "$not": """;
        json += """{ "status": "active" }""";
        for (int i = 0; i < 50; i++) json += " }";

        var filter = BlqlFilterParser.Parse(json);
        var doc = MakeDoc("status", "active");

        // 50 $not wraps → even number → matches
        Assert.True(filter.Matches(doc));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  8. Malformed / truncated JSON
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("{")]                                           // unclosed
    [InlineData("""{ "age": }""")]                             // missing value
    [InlineData("""{ "age": { "$gt": }""")]                    // truncated operator
    [InlineData("""{ "age": { "$gt": 10""")]                   // truncated object
    [InlineData("[1,2,3]")]                                    // array at root
    [InlineData("null")]                                       // null at root
    [InlineData("42")]                                         // number at root
    [InlineData("""{ "age": { "$gt": 10 }, }""")]             // trailing comma
    public void MalformedJson_ThrowsJsonOrFormatException(string json)
    {
        Assert.ThrowsAny<Exception>(() => BlqlFilterParser.Parse(json));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  9. Empty $in/$nin – null comparisons in set operators
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void In_EmptyArray_MatchesNoDocument()
    {
        var filter = BlqlFilterParser.Parse("""{ "role": { "$in": [] } }""");
        var docs = _col.Query(filter).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void Nin_EmptyArray_MatchesAllDocuments()
    {
        var filter = BlqlFilterParser.Parse("""{ "role": { "$nin": [] } }""");
        var docs = _col.Query(filter).ToList();
        Assert.Single(docs); // one doc in the collection
    }

    [Fact]
    public void In_ContainsNull_DoesNotMatchExistingField()
    {
        // Checking that null inside $in is handled (not matched against "admin")
        var filter = BlqlFilterParser.Parse("""{ "role": { "$in": [null, "guest"] } }""");
        var docs = _col.Query(filter).ToList();
        Assert.Empty(docs);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  10. SQL-injection style strings as literal values
    //      These must be treated as plain string comparisons, never interpreted.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("""{ "name": "1 OR 1=1" }""")]
    [InlineData("""{ "name": "'; DROP TABLE items; --" }""")]
    [InlineData("""{ "name": "\" OR \"1\"=\"1" }""")]
    [InlineData("""{ "name": "admin'--" }""")]
    [InlineData("""{ "name": "0; SELECT * FROM users" }""")]
    public void SqlInjection_StringValues_MatchNobody(string json)
    {
        // None of these strings match the document whose name is "Alice"
        var filter = BlqlFilterParser.Parse(json);
        var docs = _col.Query(filter).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void SqlInjection_StringValue_CanStillMatchExact()
    {
        // A filter whose VALUE is an injection string must match only documents
        // that literally have that value stored — it cannot affect other docs.
        var col = _engine.GetOrCreateCollection("evil");
        var doc = col.CreateDocument(
            ["_id", "name"],
            b => b.AddString("name", "'; DROP TABLE items; --"));
        col.Insert(doc);

        var filter = BlqlFilterParser.Parse("""{ "name": "'; DROP TABLE items; --" }""");
        var docs = col.Query(filter).ToList();
        Assert.Single(docs); // matches only the doc with that literal name
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  11. Field-name boundary cases
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void FieldName_EmptyString_MatchesNobody()
    {
        // Querying on an empty field name must not throw and must match no doc
        var filter = BlqlFilterParser.Parse("""{ "": "Alice" }""");
        var docs = _col.Query(filter).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void FieldName_VeryLong_DoesNotThrow()
    {
        var longField = new string('x', 10_000);
        var json = $"{{ \"{longField}\": \"value\" }}";
        var filter = BlqlFilterParser.Parse(json);
        var docs = _col.Query(filter).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void FieldName_UnicodeAndSpecialChars_DoesNotThrow()
    {
        var filter = BlqlFilterParser.Parse("""{ "名前": "Alice" }""");
        var docs = _col.Query(filter).ToList();
        Assert.Empty(docs);
    }

    [Fact]
    public void FieldName_WithDotNotation_TreatedAsLiteral()
    {
        // BLQL does not support dot-notation path traversal — "a.b" is a literal field name,
        // NOT a nested path. Must not match unless a field literally named "a.b" exists.
        var filter = BlqlFilterParser.Parse("""{ "role.admin": "true" }""");
        var docs = _col.Query(filter).ToList();
        Assert.Empty(docs);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  12. Large $in array (DoS resilience)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Large_In_Array_CompletesWithoutError()
    {
        // Build $in with 50,000 entries — must not exhaust memory or CPU unreasonably
        var values = string.Join(",", Enumerable.Range(0, 50_000).Select(i => $"\"item{i}\""));
        var json = $"{{ \"role\": {{ \"$in\": [{values}] }} }}";

        var sw = Stopwatch.StartNew();
        var filter = BlqlFilterParser.Parse(json);
        var docs = _col.Query(filter).ToList();
        sw.Stop();

        Assert.Empty(docs);
        Assert.True(sw.ElapsedMilliseconds < 5_000,
            $"Large $in query took {sw.ElapsedMilliseconds} ms");
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Helpers
    // ══════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Creates a minimal BsonDocument using the collection's pre-registered key map.
    /// All field names used here must be registered by SeedData() first.
    /// </summary>
    private BsonDocument MakeDoc(string field, string value)
        => _engine.CreateDocument([field], b => b.AddString(field, value));
}
