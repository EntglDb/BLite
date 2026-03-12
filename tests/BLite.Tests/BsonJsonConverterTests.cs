using BLite.Bson;
using System.Collections.Concurrent;
using System.Text.Json;

namespace BLite.Tests;

/// <summary>
/// Tests for JSON <-> BSON type-preservation fixes in BsonJsonConverter.
///
/// Root issue: BsonJsonConverter.JsonNumberToBsonValue previously tried TryGetInt32 first,
/// so whole-number JSON literals like 600 were stored as BSON Int32 (4 bytes) even when
/// the target C# property is double. Generated mappers then called ReadDouble() (8 bytes),
/// causing byte-level deserialization corruption for all subsequent fields.
///
/// Fixes applied:
///   1. JsonNumberToBsonValue: inspect raw JSON text -- if it contains '.' or exponent
///      characters, store as Double (skip Int32/Int64 attempt entirely).
///   2. WriteValue for IsDouble: serialize whole-number doubles as "600.0" (not "600")
///      so that round-trip parsing restores them as Double, not Int32.
/// </summary>
public class BsonJsonConverterTests
{
    private readonly ConcurrentDictionary<string, ushort> _keyMap = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<ushort, string> _reverseKeyMap = new();

    public BsonJsonConverterTests()
    {
        // Keys must be pre-registered -- BsonSpanWriter requires them in the map.
        ushort id = 1;
        foreach (var key in new[] { "value", "price", "qty", "stock", "rating", "views" })
        {
            _keyMap[key] = id;
            _reverseKeyMap[id++] = key;
        }
    }

    private BsonDocument Parse(string json) =>
        BsonJsonConverter.FromJson(json, _keyMap, _reverseKeyMap);

    // -- Input: JSON -> BSON type selection ------------------------------------

    [Fact]
    public void FromJson_Integer_StoresAsBsonInt32()
    {
        var doc = Parse("""{"value": 42}""");

        var value = doc.GetValue("value");
        Assert.True(value.IsInt32, $"Expected Int32 but got: IsDouble={value.IsDouble}, IsInt64={value.IsInt64}");
        Assert.Equal(42, value.AsInt32);
    }

    [Fact]
    public void FromJson_LargeInteger_StoresAsBsonInt64()
    {
        long bigNum = (long)int.MaxValue + 1;
        var doc = Parse($$"""{"value": {{bigNum}}}""");

        var value = doc.GetValue("value");
        Assert.True(value.IsInt64, $"Expected Int64 but got: IsInt32={value.IsInt32}, IsDouble={value.IsDouble}");
        Assert.Equal(bigNum, value.AsInt64);
    }

    [Fact]
    public void FromJson_WholeNumberWithDecimalPoint_StoresAsBsonDouble()
    {
        // "600.0" must NOT be stored as Int32 -- the decimal point signals floating-point
        var doc = Parse("""{"price": 600.0}""");

        var price = doc.GetValue("price");
        Assert.True(price.IsDouble, $"Expected Double but got: IsInt32={price.IsInt32}, IsInt64={price.IsInt64}");
        Assert.Equal(600.0, price.AsDouble);
    }

    [Fact]
    public void FromJson_DecimalNumber_StoresAsBsonDouble()
    {
        var doc = Parse("""{"price": 29.99}""");

        var price = doc.GetValue("price");
        Assert.True(price.IsDouble, $"Expected Double but got: IsInt32={price.IsInt32}");
        Assert.Equal(29.99, price.AsDouble);
    }

    [Fact]
    public void FromJson_ExponentNotation_StoresAsBsonDouble()
    {
        var doc = Parse("""{"value": 1.5e2}""");

        var value = doc.GetValue("value");
        Assert.True(value.IsDouble, $"Expected Double but got: IsInt32={value.IsInt32}");
        Assert.Equal(150.0, value.AsDouble);
    }

    [Fact]
    public void FromJson_NegativeInteger_StoresAsBsonInt32()
    {
        var doc = Parse("""{"value": -5}""");

        var value = doc.GetValue("value");
        Assert.True(value.IsInt32);
        Assert.Equal(-5, value.AsInt32);
    }

    [Fact]
    public void FromJson_NegativeDecimal_StoresAsBsonDouble()
    {
        var doc = Parse("""{"value": -3.14}""");

        var value = doc.GetValue("value");
        Assert.True(value.IsDouble);
        Assert.Equal(-3.14, value.AsDouble);
    }

    // -- Output: BSON Double -> JSON serialization -----------------------------

    [Fact]
    public void ToJson_WholeNumberDouble_IncludesDecimalPoint()
    {
        // A BSON Double 600.0 must serialize with ".0" so that round-trip parses it
        // back as Double (not Int32).
        var doc = BsonDocument.Create(_keyMap, _reverseKeyMap,
            b => b.Add("price", BsonValue.FromDouble(600.0)));

        var json = BsonJsonConverter.ToJson(doc);
        using var parsed = JsonDocument.Parse(json);

        var raw = parsed.RootElement.GetProperty("price").GetRawText();
        Assert.True(raw.Contains('.') || raw.Contains('e') || raw.Contains('E'),
            $"Expected decimal point or exponent in JSON output for double 600.0, but got: {raw}");
    }

    [Fact]
    public void ToJson_FractionalDouble_Preserved()
    {
        var doc = BsonDocument.Create(_keyMap, _reverseKeyMap,
            b => b.Add("price", BsonValue.FromDouble(29.99)));

        var json = BsonJsonConverter.ToJson(doc);
        using var parsed = JsonDocument.Parse(json);

        var raw = parsed.RootElement.GetProperty("price").GetRawText();
        Assert.Contains(".", raw);
    }

    [Fact]
    public void ToJson_Int32_NoDecimalPoint()
    {
        // Int32 values must NOT gain a decimal point
        var doc = BsonDocument.Create(_keyMap, _reverseKeyMap,
            b => b.Add("qty", BsonValue.FromInt32(10)));

        var json = BsonJsonConverter.ToJson(doc);
        using var parsed = JsonDocument.Parse(json);

        var raw = parsed.RootElement.GetProperty("qty").GetRawText();
        Assert.DoesNotContain(".", raw);
        Assert.Equal(10, parsed.RootElement.GetProperty("qty").GetInt32());
    }

    // -- Round-trip: BSON Double survives JSON -> parse -> BSON ---------------

    [Fact]
    public void RoundTrip_BsonDoubleRemainsDouble()
    {
        // Arrange: document with a BSON Double
        var original = BsonDocument.Create(_keyMap, _reverseKeyMap,
            b => b.Add("price", BsonValue.FromDouble(600.0)));

        // Act: serialize to JSON, then parse back
        var json = BsonJsonConverter.ToJson(original);
        var restored = Parse(json);

        // Assert: still a Double (not Int32)
        var price = restored.GetValue("price");
        Assert.True(price.IsDouble, $"Round-trip changed BSON type. IsInt32={price.IsInt32}, got raw JSON: {json}");
        Assert.Equal(600.0, price.AsDouble);
    }

    [Fact]
    public void RoundTrip_BsonInt32RemainsInt32()
    {
        var original = BsonDocument.Create(_keyMap, _reverseKeyMap,
            b => b.Add("qty", BsonValue.FromInt32(42)));

        var json = BsonJsonConverter.ToJson(original);
        var restored = Parse(json);

        var qty = restored.GetValue("qty");
        Assert.True(qty.IsInt32, $"Round-trip changed BSON type. IsDouble={qty.IsDouble}, got raw JSON: {json}");
        Assert.Equal(42, qty.AsInt32);
    }

    [Fact]
    public void RoundTrip_MultipleNumericTypes_PreservedCorrectly()
    {
        var original = BsonDocument.Create(_keyMap, _reverseKeyMap, b =>
        {
            b.Add("price", BsonValue.FromDouble(1999.0));
            b.Add("stock", BsonValue.FromInt32(50));
            b.Add("rating", BsonValue.FromDouble(4.5));
            b.Add("views", BsonValue.FromInt64(100_000L));
        });

        var json = BsonJsonConverter.ToJson(original);
        var restored = Parse(json);

        var price = restored.GetValue("price");
        Assert.True(price.IsDouble, $"price should be Double. JSON={json}");
        Assert.Equal(1999.0, price.AsDouble);

        var stock = restored.GetValue("stock");
        Assert.True(stock.IsInt32, $"stock should be Int32. JSON={json}");
        Assert.Equal(50, stock.AsInt32);

        var rating = restored.GetValue("rating");
        Assert.True(rating.IsDouble, $"rating should be Double. JSON={json}");
        Assert.Equal(4.5, rating.AsDouble);

        var views = restored.GetValue("views");
        Assert.True(views.IsInt64 || views.IsInt32,
            $"views should be Int64 or Int32. IsDouble={views.IsDouble}");
    }
}