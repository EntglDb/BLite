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

    // -- FromJson: null, boolean, string --------------------------------------

    [Fact]
    public void FromJson_Null_StoresAsNull()
    {
        var doc = Parse("""{"value": null}""");
        Assert.True(doc.GetValue("value").IsNull);
    }

    [Fact]
    public void FromJson_True_StoresAsBooleanTrue()
    {
        var doc = Parse("""{"value": true}""");
        var v = doc.GetValue("value");
        Assert.True(v.IsBoolean);
        Assert.True(v.AsBoolean);
    }

    [Fact]
    public void FromJson_False_StoresAsBooleanFalse()
    {
        var doc = Parse("""{"value": false}""");
        var v = doc.GetValue("value");
        Assert.True(v.IsBoolean);
        Assert.False(v.AsBoolean);
    }

    [Fact]
    public void FromJson_String_StoresAsString()
    {
        var doc = Parse("""{"value": "hello"}""");
        var v = doc.GetValue("value");
        Assert.True(v.IsString);
        Assert.Equal("hello", v.AsString);
    }

    [Fact]
    public void FromJson_EmptyString_StoresAsString()
    {
        var doc = Parse("""{"value": ""}""");
        var v = doc.GetValue("value");
        Assert.True(v.IsString);
        Assert.Equal("", v.AsString);
    }

    // -- FromJson: arrays -----------------------------------------------------

    [Fact]
    public void FromJson_ArrayOfInts_StoresAsBsonArray()
    {
        var doc = Parse("""{"price": [1, 2, 3]}""");
        var v = doc.GetValue("price");
        Assert.True(v.IsArray);
        var arr = v.AsArray;
        Assert.Equal(3, arr.Count);
        Assert.Equal(1, arr[0].AsInt32);
        Assert.Equal(2, arr[1].AsInt32);
        Assert.Equal(3, arr[2].AsInt32);
    }

    [Fact]
    public void FromJson_EmptyArray_StoresAsEmptyBsonArray()
    {
        var doc = Parse("""{"price": []}""");
        var v = doc.GetValue("price");
        Assert.True(v.IsArray);
        Assert.Empty(v.AsArray);
    }

    // -- FromJson: throws on non-object root ----------------------------------

    [Fact]
    public void FromJson_ArrayRoot_ThrowsJsonException()
    {
        Assert.Throws<System.Text.Json.JsonException>(() => Parse("""[1, 2, 3]"""));
    }

    [Fact]
    public void FromJson_NumberRoot_ThrowsJsonException()
    {
        Assert.Throws<System.Text.Json.JsonException>(() => Parse("42"));
    }

    // -- ToJson: scalar BSON types --------------------------------------------

    [Fact]
    public void ToJson_Null_OutputsJsonNull()
    {
        var doc = BsonDocument.Create(_keyMap, _reverseKeyMap,
            b => b.Add("value", BsonValue.Null));
        var json = BsonJsonConverter.ToJson(doc);
        using var parsed = JsonDocument.Parse(json);
        Assert.Equal(JsonValueKind.Null, parsed.RootElement.GetProperty("value").ValueKind);
    }

    [Fact]
    public void ToJson_BoolTrue_OutputsTrue()
    {
        var doc = BsonDocument.Create(_keyMap, _reverseKeyMap,
            b => b.Add("value", BsonValue.FromBoolean(true)));
        var json = BsonJsonConverter.ToJson(doc);
        using var parsed = JsonDocument.Parse(json);
        Assert.True(parsed.RootElement.GetProperty("value").GetBoolean());
    }

    [Fact]
    public void ToJson_BoolFalse_OutputsFalse()
    {
        var doc = BsonDocument.Create(_keyMap, _reverseKeyMap,
            b => b.Add("value", BsonValue.FromBoolean(false)));
        var json = BsonJsonConverter.ToJson(doc);
        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.GetProperty("value").GetBoolean());
    }

    [Fact]
    public void ToJson_String_OutputsQuotedString()
    {
        var doc = BsonDocument.Create(_keyMap, _reverseKeyMap,
            b => b.Add("value", BsonValue.FromString("hello")));
        var json = BsonJsonConverter.ToJson(doc);
        using var parsed = JsonDocument.Parse(json);
        Assert.Equal("hello", parsed.RootElement.GetProperty("value").GetString());
    }

    // -- ToJson round-trip: string, bool, null --------------------------------

    [Fact]
    public void RoundTrip_BoolTrue_RemainsBool()
    {
        var original = BsonDocument.Create(_keyMap, _reverseKeyMap,
            b => b.Add("value", BsonValue.FromBoolean(true)));
        var json = BsonJsonConverter.ToJson(original);
        var restored = Parse(json);
        var v = restored.GetValue("value");
        Assert.True(v.IsBoolean);
        Assert.True(v.AsBoolean);
    }

    [Fact]
    public void RoundTrip_String_RemainsString()
    {
        var original = BsonDocument.Create(_keyMap, _reverseKeyMap,
            b => b.Add("value", BsonValue.FromString("test")));
        var json = BsonJsonConverter.ToJson(original);
        var restored = Parse(json);
        var v = restored.GetValue("value");
        Assert.True(v.IsString);
        Assert.Equal("test", v.AsString);
    }

    [Fact]
    public void RoundTrip_Null_RemainsNull()
    {
        var original = BsonDocument.Create(_keyMap, _reverseKeyMap,
            b => b.Add("value", BsonValue.Null));
        var json = BsonJsonConverter.ToJson(original);
        var restored = Parse(json);
        Assert.True(restored.GetValue("value").IsNull);
    }

    // -- ToJson(BsonValue) overload -------------------------------------------

    [Fact]
    public void ToJsonValue_Int32_ReturnsNumberString()
    {
        var json = BsonJsonConverter.ToJson(BsonValue.FromInt32(99));
        Assert.Equal("99", json.Trim());
    }

    [Fact]
    public void ToJsonValue_String_ReturnsQuotedString()
    {
        var json = BsonJsonConverter.ToJson(BsonValue.FromString("hi"));
        Assert.Equal("\"hi\"", json.Trim());
    }
}
