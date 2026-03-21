using BLite.Bson;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace BLite.Tests;

/// <summary>
/// Unit tests for BsonBufferWriter — verifies that each write method emits
/// the correct raw bytes to the underlying IBufferWriter&lt;byte&gt;.
/// </summary>
public class BsonBufferWriterTests
{
    // ── Int32 ────────────────────────────────────────────────────────────────

    [Fact]
    public void WriteInt32_EmitsCorrectBytes()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);

        writer.WriteInt32("age", 42);

        var span = buf.WrittenSpan;
        // type byte + cstring "age\0" + 4-byte LE value
        Assert.Equal((byte)BsonType.Int32, span[0]);
        Assert.Equal("age\0"u8.ToArray(), span.Slice(1, 4).ToArray());
        Assert.Equal(42, BinaryPrimitives.ReadInt32LittleEndian(span.Slice(5, 4)));
        Assert.Equal(9, buf.WrittenCount);
    }

    [Fact]
    public void WriteInt32_NegativeValue_EncodesCorrectly()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);

        writer.WriteInt32("v", -1);

        var value = BinaryPrimitives.ReadInt32LittleEndian(buf.WrittenSpan.Slice(3, 4)); // "v\0" = 2 bytes
        Assert.Equal(-1, value);
    }

    [Fact]
    public void WriteInt32_ZeroValue_EncodesCorrectly()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);

        writer.WriteInt32("n", 0);

        var value = BinaryPrimitives.ReadInt32LittleEndian(buf.WrittenSpan.Slice(3, 4));
        Assert.Equal(0, value);
    }

    // ── Int64 ────────────────────────────────────────────────────────────────

    [Fact]
    public void WriteInt64_EmitsCorrectBytes()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);

        writer.WriteInt64("ts", long.MaxValue);

        var span = buf.WrittenSpan;
        Assert.Equal((byte)BsonType.Int64, span[0]);
        Assert.Equal("ts\0"u8.ToArray(), span.Slice(1, 3).ToArray());
        Assert.Equal(long.MaxValue, BinaryPrimitives.ReadInt64LittleEndian(span.Slice(4, 8)));
        Assert.Equal(12, buf.WrittenCount);
    }

    [Fact]
    public void WriteInt64_NegativeValue_EncodesCorrectly()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);

        writer.WriteInt64("v", long.MinValue);

        var value = BinaryPrimitives.ReadInt64LittleEndian(buf.WrittenSpan.Slice(3, 8));
        Assert.Equal(long.MinValue, value);
    }

    // ── Double ───────────────────────────────────────────────────────────────

    [Fact]
    public void WriteDouble_EmitsCorrectBytes()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);

        writer.WriteDouble("pi", 3.14159);

        var span = buf.WrittenSpan;
        Assert.Equal((byte)BsonType.Double, span[0]);
        Assert.Equal("pi\0"u8.ToArray(), span.Slice(1, 3).ToArray());
        var value = BinaryPrimitives.ReadDoubleLittleEndian(span.Slice(4, 8));
        Assert.Equal(3.14159, value, precision: 5);
        Assert.Equal(12, buf.WrittenCount);
    }

    [Fact]
    public void WriteDouble_Zero_EncodesCorrectly()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);

        writer.WriteDouble("x", 0.0);

        var value = BinaryPrimitives.ReadDoubleLittleEndian(buf.WrittenSpan.Slice(3, 8));
        Assert.Equal(0.0, value);
    }

    // ── Boolean ──────────────────────────────────────────────────────────────

    [Fact]
    public void WriteBoolean_True_EmitsByteOne()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);

        writer.WriteBoolean("ok", true);

        var span = buf.WrittenSpan;
        Assert.Equal((byte)BsonType.Boolean, span[0]);
        Assert.Equal("ok\0"u8.ToArray(), span.Slice(1, 3).ToArray());
        Assert.Equal(1, span[4]);
        Assert.Equal(5, buf.WrittenCount);
    }

    [Fact]
    public void WriteBoolean_False_EmitsByteZero()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);

        writer.WriteBoolean("ok", false);

        Assert.Equal(0, buf.WrittenSpan[4]);
    }

    // ── String ───────────────────────────────────────────────────────────────

    [Fact]
    public void WriteString_EmitsCorrectFormat()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);

        writer.WriteString("n", "hello");

        var span = buf.WrittenSpan;
        Assert.Equal((byte)BsonType.String, span[0]);
        Assert.Equal("n\0"u8.ToArray(), span.Slice(1, 2).ToArray());
        // Length (int32 LE) = 6 (5 chars + null terminator)
        Assert.Equal(6, BinaryPrimitives.ReadInt32LittleEndian(span.Slice(3, 4)));
        Assert.Equal("hello"u8.ToArray(), span.Slice(7, 5).ToArray());
        Assert.Equal(0, span[12]); // null terminator after value
    }

    [Fact]
    public void WriteString_EmptyString_EncodesCorrectly()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);

        writer.WriteString("s", "");

        // Length should be 1 (just the null terminator)
        var len = BinaryPrimitives.ReadInt32LittleEndian(buf.WrittenSpan.Slice(3, 4));
        Assert.Equal(1, len);
    }

    [Fact]
    public void WriteString_UnicodeChars_EncodesUtf8()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);
        const string text = "héllo";
        var expectedBytes = Encoding.UTF8.GetBytes(text);

        writer.WriteString("s", text);

        var span = buf.WrittenSpan;
        var len = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(3, 4));
        Assert.Equal(expectedBytes.Length + 1, len);
        Assert.Equal(expectedBytes, span.Slice(7, expectedBytes.Length).ToArray());
    }

    // ── Null ─────────────────────────────────────────────────────────────────

    [Fact]
    public void WriteNull_EmitsTypeByteAndName()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);

        writer.WriteNull("x");

        var span = buf.WrittenSpan;
        Assert.Equal((byte)BsonType.Null, span[0]);
        Assert.Equal("x\0"u8.ToArray(), span.Slice(1, 2).ToArray());
        Assert.Equal(3, buf.WrittenCount);
    }

    // ── ObjectId ─────────────────────────────────────────────────────────────

    [Fact]
    public void WriteObjectId_Emits12Bytes()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);
        var id = ObjectId.NewObjectId();

        writer.WriteObjectId("_id", id);

        var span = buf.WrittenSpan;
        Assert.Equal((byte)BsonType.ObjectId, span[0]);
        Assert.Equal("_id\0"u8.ToArray(), span.Slice(1, 4).ToArray());
        // 12 bytes for ObjectId
        Assert.Equal(id.ToByteArray(), span.Slice(5, 12).ToArray());
        Assert.Equal(17, buf.WrittenCount);
    }

    // ── Binary ───────────────────────────────────────────────────────────────

    [Fact]
    public void WriteBinary_EmitsLengthSubtypeAndData()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);
        byte[] data = [0xAA, 0xBB, 0xCC];

        writer.WriteBinary("b", data);

        var span = buf.WrittenSpan;
        Assert.Equal((byte)BsonType.Binary, span[0]);
        Assert.Equal("b\0"u8.ToArray(), span.Slice(1, 2).ToArray());
        // Length int32 = 3
        Assert.Equal(3, BinaryPrimitives.ReadInt32LittleEndian(span.Slice(3, 4)));
        Assert.Equal(0, span[7]); // subtype generic = 0
        Assert.Equal(data, span.Slice(8, 3).ToArray());
    }

    // ── DateTime ─────────────────────────────────────────────────────────────

    [Fact]
    public void WriteDateTime_EmitsMillisecondsSinceEpoch()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);
        var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        writer.WriteDateTime("t", epoch);

        var span = buf.WrittenSpan;
        Assert.Equal((byte)BsonType.DateTime, span[0]);
        var ms = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(3, 8));
        Assert.Equal(0L, ms); // epoch = 0ms
    }

    [Fact]
    public void WriteDateTime_NonEpoch_EncodesPositiveMilliseconds()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);
        var dt = new DateTime(1970, 1, 1, 0, 0, 1, DateTimeKind.Utc); // 1 second after epoch

        writer.WriteDateTime("t", dt);

        var ms = BinaryPrimitives.ReadInt64LittleEndian(buf.WrittenSpan.Slice(3, 8));
        Assert.Equal(1000L, ms);
    }

    // ── Document / Array structure ────────────────────────────────────────────

    [Fact]
    public void BeginDocument_WritesZeroPlaceholder()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);

        var pos = writer.BeginDocument();

        Assert.Equal(0, pos);
        Assert.Equal(4, buf.WrittenCount); // 4-byte size placeholder
        Assert.Equal(0, BinaryPrimitives.ReadInt32LittleEndian(buf.WrittenSpan));
    }

    [Fact]
    public void EndDocument_WritesZeroTerminator()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);
        var pos = writer.BeginDocument();

        writer.EndDocument(pos);

        Assert.Equal(5, buf.WrittenCount); // 4-byte size + 1-byte terminator
        Assert.Equal(0, buf.WrittenSpan[4]);
    }

    [Fact]
    public void Position_TracksAllBytesWritten()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);

        Assert.Equal(0, writer.Position);
        writer.WriteInt32("x", 1);
        var after = writer.Position;
        Assert.Equal(buf.WrittenCount, after);
        Assert.True(after > 0);
    }

    [Fact]
    public void MultipleWrites_AppendSequentially()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new BsonBufferWriter(buf);

        writer.WriteInt32("a", 1);
        var afterFirst = buf.WrittenCount;
        writer.WriteInt32("b", 2);
        var afterSecond = buf.WrittenCount;

        // Both fields append; second starts right after first and both are identical size
        Assert.True(afterSecond > afterFirst);
        Assert.Equal(afterFirst * 2, afterSecond);
    }
}
