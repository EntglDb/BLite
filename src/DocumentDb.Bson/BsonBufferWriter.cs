using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace DocumentDb.Bson;

/// <summary>
/// BSON writer that serializes to an IBufferWriter, enabling streaming serialization
/// without fixed buffer size limits.
/// </summary>
public ref struct BsonBufferWriter
{
    private IBufferWriter<byte> _writer;
    private int _totalBytesWritten;
    
    public BsonBufferWriter(IBufferWriter<byte> writer)
    {
        _writer = writer;
        _totalBytesWritten = 0;
    }
    
    public int Position => _totalBytesWritten;
    
    private void WriteBytes(ReadOnlySpan<byte> data)
    {
        var destination = _writer.GetSpan(data.Length);
        data.CopyTo(destination);
        _writer.Advance(data.Length);
        _totalBytesWritten += data.Length;
    }
    
    private void WriteByte(byte value)
    {
        var span = _writer.GetSpan(1);
        span[0] = value;
        _writer.Advance(1);
        _totalBytesWritten++;
    }
    
    public void WriteDateTime(string name, DateTime value)
    {
        WriteByte((byte)BsonType.DateTime);
        WriteCString(name);
        // BSON DateTime: milliseconds since Unix epoch (UTC)
        var unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var milliseconds = (long)(value.ToUniversalTime() - unixEpoch).TotalMilliseconds;
        WriteInt64Internal(milliseconds);
    }
    
    public int BeginDocument()
    {
        // Write placeholder for size (4 bytes)
        var sizePosition = _totalBytesWritten;
        var span = _writer.GetSpan(4);
        // Initialize with default value (will be patched later)
        span[0] = 0; span[1] = 0; span[2] = 0; span[3] = 0;
        _writer.Advance(4);
        _totalBytesWritten += 4;
        return sizePosition;
    }
    
    public void EndDocument(int sizePosition)
    {
        // Write document terminator
        WriteByte(0);
        
        // Note: Size patching must be done by caller after accessing WrittenSpan
        // from ArrayBufferWriter (or equivalent)
    }
    
    // Private helper methods

    private void WriteInt32Internal(int value)
    {
        var span = _writer.GetSpan(4);
        BinaryPrimitives.WriteInt32LittleEndian(span, value);
        _writer.Advance(4);
        _totalBytesWritten += 4;
    }
    
    private void WriteInt64Internal(long value)
    {
        var span = _writer.GetSpan(8);
        BinaryPrimitives.WriteInt64LittleEndian(span, value);
        _writer.Advance(8);
        _totalBytesWritten += 8;
    }
    
    public void WriteObjectId(string name, ObjectId value)
    {
        WriteByte((byte)BsonType.ObjectId);
        WriteCString(name);
        WriteBytes(value.ToByteArray());
    }

    public void WriteString(string name, string value)
    {
        WriteByte((byte)BsonType.String);
        WriteCString(name);
        WriteStringValue(value);
    }
    
    public void WriteBoolean(string name, bool value)
    {
        WriteByte((byte)BsonType.Boolean);
        WriteCString(name);
        WriteByte((byte)(value ? 1 : 0));
    }
    
    public void WriteNull(string name)
    {
        WriteByte((byte)BsonType.Null);
        WriteCString(name);
    }

    private void WriteStringValue(string value)
    {
        // String: length (int32) + UTF8 bytes + null terminator
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteInt32Internal(bytes.Length + 1); // +1 for null terminator
        WriteBytes(bytes);
        WriteByte(0);
    }
    
    private void WriteDoubleInternal(double value)
    {
        var span = _writer.GetSpan(8);
        BinaryPrimitives.WriteDoubleLittleEndian(span, value);
        _writer.Advance(8);
        _totalBytesWritten += 8;
    }
    
    public void WriteBinary(string name, ReadOnlySpan<byte> data)
    {
        WriteByte((byte)BsonType.Binary);
        WriteCString(name);
        WriteInt32Internal(data.Length);
        WriteByte(0); // Binary subtype: Generic
        WriteBytes(data);
    }
    
    public void WriteInt64(string name, long value)
    {
        WriteByte((byte)BsonType.Int64);
        WriteCString(name);
        WriteInt64Internal(value);
    }
    
    public void WriteDouble(string name, double value)
    {
        WriteByte((byte)BsonType.Double);
        WriteCString(name);
        WriteDoubleInternal(value);
    }
    
    private void WriteCString(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteBytes(bytes);
        WriteByte(0); // Null terminator
    }

    public void WriteInt32(string name, int value)
    {
        WriteByte((byte)BsonType.Int32);
        WriteCString(name);
        WriteInt32Internal(value);
    }
}
