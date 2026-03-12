using BLite.Bson;
using System.Collections.Concurrent;

namespace BLite.Tests;

/// <summary>
/// Tests for the coerced numeric read methods added to BsonSpanReader:
///   ReadDoubleCoerced(BsonType) — reads Int32/Int64/Double BSON fields as double
///   ReadInt32Coerced(BsonType)  — reads Int64/Double/Int32 BSON fields as int
///   ReadInt64Coerced(BsonType)  — reads Int32/Double/Int64 BSON fields as long
///
/// These methods allow generated mappers to tolerate BSON type mismatches that occur
/// when a schema-less tool (e.g. BLite Studio) stores a JSON integer literal like 600
/// as BSON Int32, while the typed C# model declares the property as double.
/// Without coercion, ReadDouble() reads 8 bytes from a 4-byte Int32 field, causing
/// byte-level corruption for the current field and all subsequent fields.
/// </summary>
public class BsonSpanReaderCoercedTests
{
    private readonly ConcurrentDictionary<string, ushort> _keyMap;
    private readonly ConcurrentDictionary<ushort, string> _keys;

    public BsonSpanReaderCoercedTests()
    {
        _keyMap = new(StringComparer.OrdinalIgnoreCase);
        _keys = new();
        ushort id = 1;
        foreach (var key in new[] { "price", "stock", "rating", "views", "value" })
        {
            _keyMap[key] = id;
            _keys[id++] = key;
        }
    }

    // ── ReadDoubleCoerced ──────────────────────────────────────────────────────

    [Fact]
    public void ReadDoubleCoerced_FromBsonDouble_ReturnsValue()
    {
        var buf = new byte[64];
        var w = new BsonSpanWriter(buf, _keyMap);
        w.WriteDouble("price", 29.99);

        var r = new BsonSpanReader(buf, _keys);
        var bsonType = r.ReadBsonType();
        r.ReadElementHeader();
        var value = r.ReadDoubleCoerced(bsonType);

        Assert.Equal(BsonType.Double, bsonType);
        Assert.Equal(29.99, value, precision: 10);
    }

    [Fact]
    public void ReadDoubleCoerced_FromBsonInt32_ReturnsCoercedValue()
    {
        // This is the key scenario: Studio stores "600" as Int32, typed model wants double
        var buf = new byte[64];
        var w = new BsonSpanWriter(buf, _keyMap);
        w.WriteInt32("price", 600);

        var r = new BsonSpanReader(buf, _keys);
        var bsonType = r.ReadBsonType();
        r.ReadElementHeader();
        var value = r.ReadDoubleCoerced(bsonType);

        Assert.Equal(BsonType.Int32, bsonType);
        Assert.Equal(600.0, value);
    }

    [Fact]
    public void ReadDoubleCoerced_FromBsonInt64_ReturnsCoercedValue()
    {
        var buf = new byte[64];
        var w = new BsonSpanWriter(buf, _keyMap);
        w.WriteInt64("price", 1_000_000L);

        var r = new BsonSpanReader(buf, _keys);
        var bsonType = r.ReadBsonType();
        r.ReadElementHeader();
        var value = r.ReadDoubleCoerced(bsonType);

        Assert.Equal(BsonType.Int64, bsonType);
        Assert.Equal(1_000_000.0, value);
    }

    [Fact]
    public void ReadDoubleCoerced_Int32_DoesNotConsumeExtraBytes()
    {
        // Most important invariant: reading a 4-byte Int32 field as double must
        // consume exactly 4 bytes — not 8 — so the next field's position is correct.
        var buf = new byte[128];
        var w = new BsonSpanWriter(buf, _keyMap);
        w.WriteInt32("price", 600);
        w.WriteInt32("stock", 99);

        var r = new BsonSpanReader(buf, _keys);

        var type1 = r.ReadBsonType();
        r.ReadElementHeader();
        var price = r.ReadDoubleCoerced(type1);

        var type2 = r.ReadBsonType();
        var name2 = r.ReadElementHeader();
        var stock = r.ReadInt32();

        Assert.Equal(600.0, price);
        Assert.Equal(BsonType.Int32, type2);
        Assert.Equal("stock", name2);
        Assert.Equal(99, stock);
    }

    // ── ReadInt32Coerced ───────────────────────────────────────────────────────

    [Fact]
    public void ReadInt32Coerced_FromBsonInt32_ReturnsValue()
    {
        var buf = new byte[64];
        var w = new BsonSpanWriter(buf, _keyMap);
        w.WriteInt32("stock", 42);

        var r = new BsonSpanReader(buf, _keys);
        var bsonType = r.ReadBsonType();
        r.ReadElementHeader();
        var value = r.ReadInt32Coerced(bsonType);

        Assert.Equal(BsonType.Int32, bsonType);
        Assert.Equal(42, value);
    }

    [Fact]
    public void ReadInt32Coerced_FromBsonInt64_ReturnsCoercedValue()
    {
        var buf = new byte[64];
        var w = new BsonSpanWriter(buf, _keyMap);
        w.WriteInt64("stock", 100L);

        var r = new BsonSpanReader(buf, _keys);
        var bsonType = r.ReadBsonType();
        r.ReadElementHeader();
        var value = r.ReadInt32Coerced(bsonType);

        Assert.Equal(BsonType.Int64, bsonType);
        Assert.Equal(100, value);
    }

    [Fact]
    public void ReadInt32Coerced_FromBsonDouble_ReturnsCoercedValue()
    {
        var buf = new byte[64];
        var w = new BsonSpanWriter(buf, _keyMap);
        w.WriteDouble("stock", 7.0);

        var r = new BsonSpanReader(buf, _keys);
        var bsonType = r.ReadBsonType();
        r.ReadElementHeader();
        var value = r.ReadInt32Coerced(bsonType);

        Assert.Equal(BsonType.Double, bsonType);
        Assert.Equal(7, value);
    }

    [Fact]
    public void ReadInt32Coerced_Int64_DoesNotConsumeExtraBytes()
    {
        var buf = new byte[128];
        var w = new BsonSpanWriter(buf, _keyMap);
        w.WriteInt64("stock", 50L);
        w.WriteInt32("views", 999);

        var r = new BsonSpanReader(buf, _keys);

        var type1 = r.ReadBsonType();
        r.ReadElementHeader();
        var stock = r.ReadInt32Coerced(type1);

        var type2 = r.ReadBsonType();
        var name2 = r.ReadElementHeader();
        var views = r.ReadInt32();

        Assert.Equal(50, stock);
        Assert.Equal(BsonType.Int32, type2);
        Assert.Equal("views", name2);
        Assert.Equal(999, views);
    }

    // ── ReadInt64Coerced ───────────────────────────────────────────────────────

    [Fact]
    public void ReadInt64Coerced_FromBsonInt64_ReturnsValue()
    {
        var buf = new byte[64];
        var w = new BsonSpanWriter(buf, _keyMap);
        w.WriteInt64("views", long.MaxValue);

        var r = new BsonSpanReader(buf, _keys);
        var bsonType = r.ReadBsonType();
        r.ReadElementHeader();
        var value = r.ReadInt64Coerced(bsonType);

        Assert.Equal(BsonType.Int64, bsonType);
        Assert.Equal(long.MaxValue, value);
    }

    [Fact]
    public void ReadInt64Coerced_FromBsonInt32_ReturnsCoercedValue()
    {
        var buf = new byte[64];
        var w = new BsonSpanWriter(buf, _keyMap);
        w.WriteInt32("views", 1234);

        var r = new BsonSpanReader(buf, _keys);
        var bsonType = r.ReadBsonType();
        r.ReadElementHeader();
        var value = r.ReadInt64Coerced(bsonType);

        Assert.Equal(BsonType.Int32, bsonType);
        Assert.Equal(1234L, value);
    }

    [Fact]
    public void ReadInt64Coerced_FromBsonDouble_ReturnsCoercedValue()
    {
        var buf = new byte[64];
        var w = new BsonSpanWriter(buf, _keyMap);
        w.WriteDouble("views", 9.0);

        var r = new BsonSpanReader(buf, _keys);
        var bsonType = r.ReadBsonType();
        r.ReadElementHeader();
        var value = r.ReadInt64Coerced(bsonType);

        Assert.Equal(BsonType.Double, bsonType);
        Assert.Equal(9L, value);
    }

    // ── Integration: simulated Studio-edit payload deserialization ─────────────

    [Fact]
    public void Integration_AllNumericFieldsDeserializeCorrectly_WhenStoredAsMixedTypes()
    {
        // Simulate what happens when Studio saves:
        //   { "price": 600, "stock": 10, "rating": 4.5 }
        //   price → Int32(600), stock → Int32(10), rating → Double(4.5)
        // And the typed mapper expects price=double, stock=int, rating=double
        var buf = new byte[256];
        var w = new BsonSpanWriter(buf, _keyMap);
        var sizePos = w.BeginDocument();
        w.WriteInt32("price", 600);       // Studio stored as Int32 (bug scenario)
        w.WriteInt32("stock", 10);
        w.WriteDouble("rating", 4.5);
        w.EndDocument(sizePos);

        var r = new BsonSpanReader(buf, _keys);
        r.ReadDocumentSize();

        var t1 = r.ReadBsonType(); r.ReadElementHeader();
        var price = r.ReadDoubleCoerced(t1);  // mapper calls ReadDoubleCoerced

        var t2 = r.ReadBsonType(); r.ReadElementHeader();
        var stock = r.ReadInt32Coerced(t2);   // mapper calls ReadInt32Coerced

        var t3 = r.ReadBsonType(); r.ReadElementHeader();
        var rating = r.ReadDoubleCoerced(t3); // mapper calls ReadDoubleCoerced

        var tEnd = r.ReadBsonType();

        Assert.Equal(600.0, price);
        Assert.Equal(10, stock);
        Assert.Equal(4.5, rating, precision: 10);
        Assert.Equal(BsonType.EndOfDocument, tEnd);
    }
}
