using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using BLite.Bson;

namespace BLite.NetStandard21.Tests;

public class BsonWriterNetStandardTests
{
    private static (byte[] buffer, int docStart) WriteDoubleDocument(string fieldName, double value)
    {
        var bufferWriter = new ArrayBufferWriter<byte>(256);
        var writer = new BsonBufferWriter(bufferWriter);
        var sizePos = writer.BeginDocument();
        writer.WriteDouble(fieldName, value);
        writer.EndDocument(sizePos);

        var bytes = bufferWriter.WrittenMemory.ToArray();
        // Patch the 4-byte document size at sizePos
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(sizePos, 4), bytes.Length - sizePos);
        return (bytes, sizePos);
    }

    private static double ReadBackDouble(byte[] bytes, int docStart)
    {
        var span = bytes.AsSpan(docStart);
        var reader = new BsonSpanReader(span, new ConcurrentDictionary<ushort, string>());
        reader.ReadDocumentSize();
        reader.ReadBsonType();
        reader.ReadCString();
        return reader.ReadDouble();
    }

    [Fact]
    public void BsonWriter_WriteDouble_RoundTrip()
    {
        const double value = 3.14159265358979;
        var (bytes, docStart) = WriteDoubleDocument("v", value);
        var result = ReadBackDouble(bytes, docStart);
        Assert.Equal(value, result);
    }

    [Fact]
    public void BsonWriter_WriteDouble_SpecialValues()
    {
        var specials = new[]
        {
            double.NaN,
            double.PositiveInfinity,
            double.NegativeInfinity,
            double.MaxValue,
            double.MinValue
        };

        foreach (var value in specials)
        {
            var (bytes, docStart) = WriteDoubleDocument("v", value);
            var result = ReadBackDouble(bytes, docStart);
            // Use bit comparison for NaN since NaN != NaN by definition
            Assert.Equal(
                BitConverter.DoubleToInt64Bits(value),
                BitConverter.DoubleToInt64Bits(result));
        }
    }
}
