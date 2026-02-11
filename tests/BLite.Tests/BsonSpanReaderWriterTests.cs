using BLite.Bson;
using Xunit;

namespace BLite.Tests;

public class BsonSpanReaderWriterTests
{
    [Fact]
    public void WriteAndRead_SimpleDocument()
    {
        Span<byte> buffer = stackalloc byte[256];
        var writer = new BsonSpanWriter(buffer);
        
        var sizePos = writer.BeginDocument();
        writer.WriteString("name", "John");
        writer.WriteInt32("age", 30);
        writer.WriteBoolean("active", true);
        writer.EndDocument(sizePos);
        
        var documentBytes = buffer[..writer.Position];
        
        var reader = new BsonSpanReader(documentBytes);
        var size = reader.ReadDocumentSize();
        
        Assert.Equal(writer.Position, size);
        
        var type1 = reader.ReadBsonType();
        var name1 = reader.ReadCString();
        var value1 = reader.ReadString();
        
        Assert.Equal(BsonType.String, type1);
        Assert.Equal("name", name1);
        Assert.Equal("John", value1);
        
        var type2 = reader.ReadBsonType();
        var name2 = reader.ReadCString();
        var value2 = reader.ReadInt32();
        
        Assert.Equal(BsonType.Int32, type2);
        Assert.Equal("age", name2);
        Assert.Equal(30, value2);
        
        var type3 = reader.ReadBsonType();
        var name3 = reader.ReadCString();
        var value3 = reader.ReadBoolean();
        
        Assert.Equal(BsonType.Boolean, type3);
        Assert.Equal("active", name3);
        Assert.True(value3);
    }

    [Fact]
    public void WriteAndRead_ObjectId()
    {
        Span<byte> buffer = stackalloc byte[256];
        var writer = new BsonSpanWriter(buffer);
        
        var oid = ObjectId.NewObjectId();
        
        var sizePos = writer.BeginDocument();
        writer.WriteObjectId("_id", oid);
        writer.EndDocument(sizePos);
        
        var documentBytes = buffer[..writer.Position];
        var reader = new BsonSpanReader(documentBytes);
        
        reader.ReadDocumentSize();
        var type = reader.ReadBsonType();
        var name = reader.ReadCString();
        var readOid = reader.ReadObjectId();
        
        Assert.Equal(BsonType.ObjectId, type);
        Assert.Equal("_id", name);
        Assert.Equal(oid, readOid);
    }

    [Fact]
    public void WriteAndRead_DateTime()
    {
        Span<byte> buffer = stackalloc byte[256];
        var writer = new BsonSpanWriter(buffer);
        
        var now = DateTime.UtcNow;
        // Round to milliseconds as BSON only stores millisecond precision
        var expectedTime = new DateTime(now.Year, now.Month, now.Day, 
            now.Hour, now.Minute, now.Second, now.Millisecond, DateTimeKind.Utc);
        
        var sizePos = writer.BeginDocument();
        writer.WriteDateTime("timestamp", expectedTime);
        writer.EndDocument(sizePos);
        
        var documentBytes = buffer[..writer.Position];
        var reader = new BsonSpanReader(documentBytes);
        
        reader.ReadDocumentSize();
        var type = reader.ReadBsonType();
        var name = reader.ReadCString();
        var readTime = reader.ReadDateTime();
        
        Assert.Equal(BsonType.DateTime, type);
        Assert.Equal("timestamp", name);
        Assert.Equal(expectedTime, readTime);
    }

    [Fact]
    public void WriteAndRead_NumericTypes()
    {
        Span<byte> buffer = stackalloc byte[256];
        var writer = new BsonSpanWriter(buffer);
        
        var sizePos = writer.BeginDocument();
        writer.WriteInt32("int32", int.MaxValue);
        writer.WriteInt64("int64", long.MaxValue);
        writer.WriteDouble("double", 3.14159);
        writer.EndDocument(sizePos);
        
        var documentBytes = buffer[..writer.Position];
        var reader = new BsonSpanReader(documentBytes);
        
        reader.ReadDocumentSize();
        
        reader.ReadBsonType();
        reader.ReadCString();
        Assert.Equal(int.MaxValue, reader.ReadInt32());
        
        reader.ReadBsonType();
        reader.ReadCString();
        Assert.Equal(long.MaxValue, reader.ReadInt64());
        
        reader.ReadBsonType();
        reader.ReadCString();
        Assert.Equal(3.14159, reader.ReadDouble(), precision: 5);
    }

    [Fact]
    public void WriteAndRead_Binary()
    {
        Span<byte> buffer = stackalloc byte[256];
        var writer = new BsonSpanWriter(buffer);
        
        byte[] testData = { 1, 2, 3, 4, 5 };
        
        var sizePos = writer.BeginDocument();
        writer.WriteBinary("data", testData);
        writer.EndDocument(sizePos);
        
        var documentBytes = buffer[..writer.Position];
        var reader = new BsonSpanReader(documentBytes);
        
        reader.ReadDocumentSize();
        var type = reader.ReadBsonType();
        var name = reader.ReadCString();
        var readData = reader.ReadBinary(out var subtype);
        
        Assert.Equal(BsonType.Binary, type);
        Assert.Equal("data", name);
        Assert.Equal((byte)0, subtype);
        Assert.True(testData.AsSpan().SequenceEqual(readData));
    }

    [Fact]
    public void WriteAndRead_NestedDocument()
    {
        Span<byte> buffer = stackalloc byte[512];
        var writer = new BsonSpanWriter(buffer);
        
        var rootSizePos = writer.BeginDocument();
        writer.WriteString("name", "Parent");
        
        var childSizePos = writer.BeginDocument("child");
        writer.WriteString("name", "Child");
        writer.WriteInt32("value", 42);
        writer.EndDocument(childSizePos);
        
        writer.EndDocument(rootSizePos);
        
        var documentBytes = buffer[..writer.Position];
        var rootSize = BitConverter.ToInt32(documentBytes[..4]);
        
        Assert.Equal(writer.Position, rootSize);
        Assert.True(rootSize > 0);
    }
}
