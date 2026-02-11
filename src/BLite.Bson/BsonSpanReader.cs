using System.Buffers.Binary;
using System.Text;

namespace BLite.Bson;

/// <summary>
/// Zero-allocation BSON reader using ReadOnlySpan&lt;byte&gt;.
/// Implemented as ref struct to ensure stack-only allocation.
/// </summary>
public ref struct BsonSpanReader
{
    private ReadOnlySpan<byte> _buffer;
    private int _position;

    public BsonSpanReader(ReadOnlySpan<byte> buffer)
    {
        _buffer = buffer;
        _position = 0;
    }

    public int Position => _position;
    public int Remaining => _buffer.Length - _position;

    /// <summary>
    /// Reads the document size (first 4 bytes of a BSON document)
    /// </summary>
    public int ReadDocumentSize()
    {
        if (Remaining < 4)
            throw new InvalidOperationException("Not enough bytes to read document size");

        var size = BinaryPrimitives.ReadInt32LittleEndian(_buffer.Slice(_position, 4));
        _position += 4;
        return size;
    }

    /// <summary>
    /// Reads a BSON element type
    /// </summary>
    public BsonType ReadBsonType()
    {
        if (Remaining < 1)
            throw new InvalidOperationException("Not enough bytes to read BSON type");

        var type = (BsonType)_buffer[_position];
        _position++;
        return type;
    }

    /// <summary>
    /// Reads a C-style null-terminated string (e-name in BSON spec)
    /// </summary>
    public string ReadCString()
    {
        var start = _position;
        while (_position < _buffer.Length && _buffer[_position] != 0)
            _position++;

        if (_position >= _buffer.Length)
            throw new InvalidOperationException("Unterminated C-string");

        var nameBytes = _buffer.Slice(start, _position - start);
        _position++; // Skip null terminator

        return Encoding.UTF8.GetString(nameBytes);
    }

    /// <summary>
    /// Reads a C-string into a destination span. Returns the number of bytes written.
    /// </summary>
    public int ReadCString(Span<char> destination)
    {
        var start = _position;
        while (_position < _buffer.Length && _buffer[_position] != 0)
            _position++;

        if (_position >= _buffer.Length)
            throw new InvalidOperationException("Unterminated C-string");

        var nameBytes = _buffer.Slice(start, _position - start);
        _position++; // Skip null terminator

        return Encoding.UTF8.GetChars(nameBytes, destination);
    }

    /// <summary>
    /// Reads a BSON string (4-byte length + UTF-8 bytes + null terminator)
    /// </summary>
    public string ReadString()
    {
        var length = BinaryPrimitives.ReadInt32LittleEndian(_buffer.Slice(_position, 4));
        _position += 4;

        if (length < 1)
            throw new InvalidOperationException("Invalid string length");

        var stringBytes = _buffer.Slice(_position, length - 1); // Exclude null terminator
        _position += length;

        return Encoding.UTF8.GetString(stringBytes);
    }

    public int ReadInt32()
    {
        if (Remaining < 4)
            throw new InvalidOperationException("Not enough bytes to read Int32");

        var value = BinaryPrimitives.ReadInt32LittleEndian(_buffer.Slice(_position, 4));
        _position += 4;
        return value;
    }

    public long ReadInt64()
    {
        if (Remaining < 8)
            throw new InvalidOperationException("Not enough bytes to read Int64");

        var value = BinaryPrimitives.ReadInt64LittleEndian(_buffer.Slice(_position, 8));
        _position += 8;
        return value;
    }

    public double ReadDouble()
    {
        if (Remaining < 8)
            throw new InvalidOperationException("Not enough bytes to read Double");

        var value = BinaryPrimitives.ReadDoubleLittleEndian(_buffer.Slice(_position, 8));
        _position += 8;
        return value;
    }

    public decimal ReadDecimal128()
    {
        if (Remaining < 16)
            throw new InvalidOperationException("Not enough bytes to read Decimal128");

        var bits = new int[4];
        bits[0] = BinaryPrimitives.ReadInt32LittleEndian(_buffer.Slice(_position, 4));
        bits[1] = BinaryPrimitives.ReadInt32LittleEndian(_buffer.Slice(_position + 4, 4));
        bits[2] = BinaryPrimitives.ReadInt32LittleEndian(_buffer.Slice(_position + 8, 4));
        bits[3] = BinaryPrimitives.ReadInt32LittleEndian(_buffer.Slice(_position + 12, 4));
        _position += 16;

        return new decimal(bits);
    }

    public bool ReadBoolean()
    {
        if (Remaining < 1)
            throw new InvalidOperationException("Not enough bytes to read Boolean");

        var value = _buffer[_position] != 0;
        _position++;
        return value;
    }

    /// <summary>
    /// Reads a BSON DateTime (UTC milliseconds since Unix epoch)
    /// </summary>
    public DateTime ReadDateTime()
    {
        var milliseconds = ReadInt64();
        return DateTimeOffset.FromUnixTimeMilliseconds(milliseconds).UtcDateTime;
    }

    /// <summary>
    /// Reads a BSON ObjectId (12 bytes)
    /// </summary>
    public ObjectId ReadObjectId()
    {
        if (Remaining < 12)
            throw new InvalidOperationException("Not enough bytes to read ObjectId");

        var oidBytes = _buffer.Slice(_position, 12);
        _position += 12;
        return new ObjectId(oidBytes);
    }

    /// <summary>
    /// Reads binary data (subtype + length + bytes)
    /// </summary>
    public ReadOnlySpan<byte> ReadBinary(out byte subtype)
    {
        var length = ReadInt32();
        
        if (Remaining < 1)
            throw new InvalidOperationException("Not enough bytes to read binary subtype");
        
        subtype = _buffer[_position];
        _position++;

        if (Remaining < length)
            throw new InvalidOperationException("Not enough bytes to read binary data");

        var data = _buffer.Slice(_position, length);
        _position += length;
        return data;
    }

    /// <summary>
    /// Skips the current value based on type
    /// </summary>
    public void SkipValue(BsonType type)
    {
        switch (type)
        {
            case BsonType.Double:
                _position += 8;
                break;
            case BsonType.String:
                var stringLength = ReadInt32();
                _position += stringLength;
                break;
            case BsonType.Document:
            case BsonType.Array:
                var docLength = BinaryPrimitives.ReadInt32LittleEndian(_buffer.Slice(_position, 4));
                _position += docLength;
                break;
            case BsonType.Binary:
                var binaryLength = ReadInt32();
                _position += 1 + binaryLength; // subtype + data
                break;
            case BsonType.ObjectId:
                _position += 12;
                break;
            case BsonType.Boolean:
                _position += 1;
                break;
            case BsonType.DateTime:
            case BsonType.Int64:
            case BsonType.Timestamp:
                _position += 8;
                break;
            case BsonType.Decimal128:
                _position += 16;
                break;
            case BsonType.Int32:
                _position += 4;
                break;
            case BsonType.Null:
                // No data
                break;
            default:
                throw new NotSupportedException($"Skipping type {type} not supported");
        }
    }

    public byte ReadByte()
    {
        if (Remaining < 1)
            throw new InvalidOperationException("Not enough bytes to read byte");
        var value = _buffer[_position];
        _position++;
        return value;
    }

    public int PeekInt32()
    {
        if (Remaining < 4)
            throw new InvalidOperationException("Not enough bytes to peek Int32");
        return BinaryPrimitives.ReadInt32LittleEndian(_buffer.Slice(_position, 4));
    }

    public string ReadElementHeader()
    {
        return ReadCString();
    }

    public ReadOnlySpan<byte> RemainingBytes() => _buffer[_position..];
}
