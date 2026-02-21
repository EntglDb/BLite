using System;
using System.Buffers.Binary;
using System.Text;

namespace BLite.Bson;

/// <summary>
/// Zero-allocation BSON writer using Span&lt;byte&gt;.
/// Implemented as ref struct to ensure stack-only allocation.
/// </summary>
public ref struct BsonSpanWriter
{
    private Span<byte> _buffer;
    private int _position;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, ushort> _keyMap;

    public BsonSpanWriter(Span<byte> buffer, System.Collections.Concurrent.ConcurrentDictionary<string, ushort> keyMap)
    {
        _buffer = buffer;
        _keyMap = keyMap;
        _position = 0;
    }

    public int Position => _position;
    public int Remaining => _buffer.Length - _position;

    /// <summary>
    /// Writes document size placeholder and returns the position to patch later
    /// </summary>
    public int WriteDocumentSizePlaceholder()
    {
        var sizePosition = _position;
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position, 4), 0);
        _position += 4;
        return sizePosition;
    }

    /// <summary>
    /// Patches the document size at the given position
    /// </summary>
    public void PatchDocumentSize(int sizePosition)
    {
        var size = _position - sizePosition;
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(sizePosition, 4), size);
    }

    /// <summary>
    /// Writes a BSON element header (type + name)
    /// </summary>
    public void WriteElementHeader(BsonType type, string name)
    {
        _buffer[_position] = (byte)type;
        _position++;

        if (!_keyMap.TryGetValue(name, out var id))
        {
            throw new InvalidOperationException($"BSON Key '{name}' not found in dictionary cache. Ensure all keys are registered before serialization.");
        }

        BinaryPrimitives.WriteUInt16LittleEndian(_buffer.Slice(_position, 2), id);
        _position += 2;
    }

    /// <summary>
    /// Writes a C-style null-terminated string
    /// </summary>
    private void WriteCString(string value)
    {
        var bytesWritten = Encoding.UTF8.GetBytes(value, _buffer[_position..]);
        _position += bytesWritten;
        _buffer[_position] = 0; // Null terminator
        _position++;
    }

    /// <summary>
    /// Writes end-of-document marker
    /// </summary>
    public void WriteEndOfDocument()
    {
        _buffer[_position] = 0;
        _position++;
    }

    /// <summary>
    /// Writes a BSON string element
    /// </summary>
    public void WriteString(string name, string value)
    {
        WriteElementHeader(BsonType.String, name);

        var valueBytes = Encoding.UTF8.GetByteCount(value);
        var stringLength = valueBytes + 1; // Include null terminator

        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position, 4), stringLength);
        _position += 4;

        Encoding.UTF8.GetBytes(value, _buffer[_position..]);
        _position += valueBytes;

        _buffer[_position] = 0; // Null terminator
        _position++;
    }

    public void WriteInt32(string name, int value)
    {
        WriteElementHeader(BsonType.Int32, name);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position, 4), value);
        _position += 4;
    }

    public void WriteInt64(string name, long value)
    {
        WriteElementHeader(BsonType.Int64, name);
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.Slice(_position, 8), value);
        _position += 8;
    }

    public void WriteDouble(string name, double value)
    {
        WriteElementHeader(BsonType.Double, name);
        BinaryPrimitives.WriteDoubleLittleEndian(_buffer.Slice(_position, 8), value);
        _position += 8;
    }

    /// <summary>
    /// Writes spatial coordinates as a BSON array [X, Y].
    /// Optimized for (double, double) tuples.
    /// </summary>
    public void WriteCoordinates(string name, (double, double) coordinates)
    {
        WriteElementHeader(BsonType.Array, name);
        
        var startPos = _position;
        _position += 4; // Placeholder for array size

        // Element 0: X
        _buffer[_position++] = (byte)BsonType.Double;
        _buffer[_position++] = 0x30; // '0'
        _buffer[_position++] = 0x00; // Null
        BinaryPrimitives.WriteDoubleLittleEndian(_buffer.Slice(_position, 8), coordinates.Item1);
        _position += 8;

        // Element 1: Y
        _buffer[_position++] = (byte)BsonType.Double;
        _buffer[_position++] = 0x31; // '1'
        _buffer[_position++] = 0x00; // Null
        BinaryPrimitives.WriteDoubleLittleEndian(_buffer.Slice(_position, 8), coordinates.Item2);
        _position += 8;

        _buffer[_position++] = 0x00; // End of array marker

        // Patch array size
        var size = _position - startPos;
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(startPos, 4), size);
    }

    public void WriteDecimal128(string name, decimal value)
    {
        WriteElementHeader(BsonType.Decimal128, name);
        // Note: usage of C# decimal bits for round-trip fidelity within BLite.
        // This makes it compatible with BLite Reader but strictly speaking not standard IEEE 754-2008 Decimal128.
        var bits = decimal.GetBits(value);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position, 4), bits[0]);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position + 4, 4), bits[1]);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position + 8, 4), bits[2]);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position + 12, 4), bits[3]);
        _position += 16;
    }

    public void WriteBoolean(string name, bool value)
    {
        WriteElementHeader(BsonType.Boolean, name);
        _buffer[_position] = (byte)(value ? 1 : 0);
        _position++;
    }

    public void WriteDateTime(string name, DateTime value)
    {
        WriteElementHeader(BsonType.DateTime, name);
        var milliseconds = new DateTimeOffset(value.ToUniversalTime()).ToUnixTimeMilliseconds();
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.Slice(_position, 8), milliseconds);
        _position += 8;
    }

    public void WriteDateTimeOffset(string name, DateTimeOffset value)
    {
        WriteElementHeader(BsonType.DateTime, name);
        var milliseconds = value.ToUnixTimeMilliseconds();
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.Slice(_position, 8), milliseconds);
        _position += 8;
    }

    public void WriteTimeSpan(string name, TimeSpan value)
    {
        WriteElementHeader(BsonType.Int64, name);
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.Slice(_position, 8), value.Ticks);
        _position += 8;
    }

    public void WriteDateOnly(string name, DateOnly value)
    {
        WriteElementHeader(BsonType.Int32, name);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position, 4), value.DayNumber);
        _position += 4;
    }

    public void WriteTimeOnly(string name, TimeOnly value)
    {
        WriteElementHeader(BsonType.Int64, name);
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.Slice(_position, 8), value.Ticks);
        _position += 8;
    }

    public void WriteGuid(string name, Guid value)
    {
        WriteString(name, value.ToString());
    }

    public void WriteObjectId(string name, ObjectId value)
    {
        WriteElementHeader(BsonType.ObjectId, name);
        value.WriteTo(_buffer.Slice(_position, 12));
        _position += 12;
    }

    public void WriteNull(string name)
    {
        WriteElementHeader(BsonType.Null, name);
        // No value to write for null
    }

    /// <summary>
    /// Writes binary data
    /// </summary>
    public void WriteBinary(string name, ReadOnlySpan<byte> data, byte subtype = 0)
    {
        WriteElementHeader(BsonType.Binary, name);
        
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position, 4), data.Length);
        _position += 4;
        
        _buffer[_position] = subtype;
        _position++;
        
        data.CopyTo(_buffer[_position..]);
        _position += data.Length;
    }

    /// <summary>
    /// Begins writing a subdocument and returns the size position to patch later
    /// </summary>
    public int BeginDocument(string name)
    {
        WriteElementHeader(BsonType.Document, name);
        return WriteDocumentSizePlaceholder();
    }

    /// <summary>
    /// Begins writing the root document and returns the size position to patch later
    /// </summary>
    public int BeginDocument()
    {
        return WriteDocumentSizePlaceholder();
    }

    /// <summary>
    /// Ends the current document
    /// </summary>
    public void EndDocument(int sizePosition)
    {
        WriteEndOfDocument();
        PatchDocumentSize(sizePosition);
    }

    /// <summary>
    /// Begins writing a BSON array and returns the size position to patch later
    /// </summary>
    public int BeginArray(string name)
    {
        WriteElementHeader(BsonType.Array, name);
        return WriteDocumentSizePlaceholder();
    }

    /// <summary>
    /// Writes a double element inside a BSON array using a raw positional uint16 index.
    /// Does NOT use the key dictionary — index is stored as a raw ushort, not a keymap ID.
    /// Must be used between <see cref="BeginArray"/> and <see cref="EndArray"/>.
    /// The reader side must use <see cref="BsonSpanReader.SkipArrayKey"/> to consume the index.
    /// </summary>
    public void WriteArrayDouble(int index, double value)
    {
        _buffer[_position++] = (byte)BsonType.Double;
        BinaryPrimitives.WriteUInt16LittleEndian(_buffer.Slice(_position, 2), (ushort)index);
        _position += 2;
        BinaryPrimitives.WriteDoubleLittleEndian(_buffer.Slice(_position, 8), value);
        _position += 8;
    }

    /// <summary>
    /// Ends the current BSON array
    /// </summary>
    public void EndArray(int sizePosition)
    {
        WriteEndOfDocument();
        PatchDocumentSize(sizePosition);
    }
}
