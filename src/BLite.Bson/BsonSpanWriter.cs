using System;
using System.Buffers.Binary;
using System.Collections.Generic;
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
    private readonly IReadOnlyDictionary<string, ushort> _keyMap;

    // ── C-BSON v2 offset table state ────────────────────────────────────────────
    // _offsetTableBasePos: absolute position in _buffer of offset[0].
    //   -1 = no offset table active for this writer instance.
    // _offsetTableFieldIdMin: first fieldId in the contiguous range; 0xFFFF = not yet discovered.
    // _offsetTableCount: number of entries reserved in the table.
    // _offsetPatchingDepth: 0 = no table / disabled; 1 = at root level (patch enabled);
    //   >1 = inside a nested sub-document (patching suspended).
    private int _offsetTableBasePos;
    private ushort _offsetTableFieldIdMin;
    private byte _offsetTableCount;
    private int _offsetPatchingDepth;

    public BsonSpanWriter(Span<byte> buffer, IReadOnlyDictionary<string, ushort> keyMap)
    {
        _buffer = buffer;
        _keyMap = keyMap;
        _position = 0;
        _offsetTableBasePos = -1;
        _offsetTableFieldIdMin = 0xFFFF;
        _offsetTableCount = 0;
        _offsetPatchingDepth = 0;
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
    /// Begins a root document with a C-BSON v2 offset table header.
    /// The offset table allows O(1) seeks to top-level fields during predicate evaluation,
    /// avoiding a sequential scan through preceding fields.
    /// </summary>
    /// <param name="fieldCount">
    /// Number of top-level fields that will be written into this document.
    /// Must equal the number of <see cref="WriteElementHeader"/> calls at the root level.
    /// </param>
    /// <returns>The size-placeholder position to pass to <see cref="EndDocument"/>.</returns>
    public int BeginDocumentWithOffsets(byte fieldCount)
    {
        var docSizePos = WriteDocumentSizePlaceholder();

        // Write offset table header: [flag:1][fieldCount:1][fieldIdMin:2]
        _buffer[_position++] = 0xCB; // C-BSON v2 magic
        _buffer[_position++] = fieldCount;
        // fieldIdMin is not yet known — placeholder 0xFFFF, patched on first WriteElementHeader.
        BinaryPrimitives.WriteUInt16LittleEndian(_buffer.Slice(_position, 2), 0xFFFF);
        _position += 2;

        // Reserve offset entries, all initialized to 0xFFFF (= absent).
        _offsetTableBasePos = _position;
        _offsetTableCount = fieldCount;
        _offsetTableFieldIdMin = 0xFFFF;
        for (int i = 0; i < fieldCount; i++)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(_buffer.Slice(_position, 2), 0xFFFF);
            _position += 2;
        }

        _offsetPatchingDepth = 1; // root level — patching is active
        return docSizePos;
    }

    /// <summary>
    /// Writes a BSON element header (type + name)
    /// </summary>
    public void WriteElementHeader(BsonType type, string name)
    {
        if (!_keyMap.TryGetValue(name, out var id))
        {
            throw new InvalidOperationException($"BSON Key '{name}' not found in dictionary cache. Ensure all keys are registered before serialization.");
        }

        // Patch the offset table if we are at root level (depth == 1).
        if (_offsetPatchingDepth == 1)
        {
            if (_offsetTableFieldIdMin == 0xFFFF)
            {
                // First field written: discover fieldIdMin and back-patch it.
                _offsetTableFieldIdMin = id;
                // fieldIdMin lives 2 bytes before _offsetTableBasePos.
                BinaryPrimitives.WriteUInt16LittleEndian(
                    _buffer.Slice(_offsetTableBasePos - 2, 2), id);
            }

            int idx = (int)id - (int)_offsetTableFieldIdMin;
            if ((uint)idx < (uint)_offsetTableCount && _position <= ushort.MaxValue)
            {
                // Store the absolute buffer position of this element's type byte.
                // Only written when position fits in a ushort; entries beyond 64 KB
                // stay at their zero-initialised sentinel so the reader skips them.
                BinaryPrimitives.WriteUInt16LittleEndian(
                    _buffer.Slice(_offsetTableBasePos + idx * 2, 2),
                    (ushort)_position);
            }
        }

        _buffer[_position] = (byte)type;
        _position++;

        BinaryPrimitives.WriteUInt16LittleEndian(_buffer.Slice(_position, 2), id);
        _position += 2;
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

#if NET7_0_OR_GREATER
        // Single-pass: encode directly into the buffer, then back-patch the 4-byte length prefix.
        // Avoids the extra GetByteCount scan over the string characters.
        var sizePos = _position;
        _position += 4; // reserve space for the length prefix
        if (!Encoding.UTF8.TryGetBytes(value.AsSpan(), _buffer[_position..], out var bytesWritten))
            throw new ArgumentException($"Buffer too small to encode string field '{name}'.");
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(sizePos, 4), bytesWritten + 1); // +1 null terminator
        _position += bytesWritten;
#else
        var valueBytes = Encoding.UTF8.GetByteCount(value);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position, 4), valueBytes + 1);
        _position += 4;
        Encoding.UTF8.GetBytes(value, _buffer[_position..]);
        _position += valueBytes;
#endif

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
        WriteDoubleLE(_position, value);
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
        WriteDoubleLE(_position, coordinates.Item1);
        _position += 8;

        // Element 1: Y
        _buffer[_position++] = (byte)BsonType.Double;
        _buffer[_position++] = 0x31; // '1'
        _buffer[_position++] = 0x00; // Null
        WriteDoubleLE(_position, coordinates.Item2);
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

#if NET6_0_OR_GREATER
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
#endif

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
        // Suspend offset patching while inside this sub-document.
        if (_offsetPatchingDepth > 0) _offsetPatchingDepth++;
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
        // Restore patching depth when returning from a sub-document.
        if (_offsetPatchingDepth > 0) _offsetPatchingDepth--;
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
    /// Writes a BSON array element header using a raw positional uint16 index.
    /// Does NOT use the key dictionary — the index is stored directly as a ushort.
    /// The reader side must use <see cref="BsonSpanReader.SkipArrayKey"/> to consume it.
    /// </summary>
    public void WriteArrayElementHeader(BsonType type, int index)
    {
        _buffer[_position++] = (byte)type;
        BinaryPrimitives.WriteUInt16LittleEndian(_buffer.Slice(_position, 2), (ushort)index);
        _position += 2;
    }

    /// <summary>
    /// Begins a sub-document inside a BSON array using a raw positional index.
    /// Returns the size-placeholder position to patch later with <see cref="EndDocument"/>.
    /// </summary>
    public int BeginArrayDocument(int index)
    {
        WriteArrayElementHeader(BsonType.Document, index);
        // Suspend offset patching while inside this array-element sub-document.
        if (_offsetPatchingDepth > 0) _offsetPatchingDepth++;
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
        WriteArrayElementHeader(BsonType.Double, index);
        WriteDoubleLE(_position, value);
        _position += 8;
    }

    public void WriteArrayString(int index, string value)
    {
        WriteArrayElementHeader(BsonType.String, index);
        var valueBytes = Encoding.UTF8.GetByteCount(value);
        var stringLength = valueBytes + 1;
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position, 4), stringLength);
        _position += 4;
        Encoding.UTF8.GetBytes(value, _buffer[_position..]);
        _position += valueBytes;
        _buffer[_position++] = 0;
    }

    public void WriteArrayInt32(int index, int value)
    {
        WriteArrayElementHeader(BsonType.Int32, index);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position, 4), value);
        _position += 4;
    }

    public void WriteArrayInt64(int index, long value)
    {
        WriteArrayElementHeader(BsonType.Int64, index);
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.Slice(_position, 8), value);
        _position += 8;
    }

    public void WriteArrayBoolean(int index, bool value)
    {
        WriteArrayElementHeader(BsonType.Boolean, index);
        _buffer[_position++] = (byte)(value ? 1 : 0);
    }

    public void WriteArrayDecimal128(int index, decimal value)
    {
        WriteArrayElementHeader(BsonType.Decimal128, index);
        var bits = decimal.GetBits(value);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position, 4), bits[0]);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position + 4, 4), bits[1]);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position + 8, 4), bits[2]);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position + 12, 4), bits[3]);
        _position += 16;
    }

    public void WriteArrayDateTime(int index, DateTime value)
    {
        WriteArrayElementHeader(BsonType.DateTime, index);
        var milliseconds = new DateTimeOffset(value.ToUniversalTime()).ToUnixTimeMilliseconds();
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.Slice(_position, 8), milliseconds);
        _position += 8;
    }

    public void WriteArrayDateTimeOffset(int index, DateTimeOffset value)
    {
        WriteArrayElementHeader(BsonType.DateTime, index);
        var milliseconds = value.ToUnixTimeMilliseconds();
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.Slice(_position, 8), milliseconds);
        _position += 8;
    }

    public void WriteArrayTimeSpan(int index, TimeSpan value)
    {
        WriteArrayElementHeader(BsonType.Int64, index);
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.Slice(_position, 8), value.Ticks);
        _position += 8;
    }

#if NET6_0_OR_GREATER
    public void WriteArrayDateOnly(int index, DateOnly value)
    {
        WriteArrayElementHeader(BsonType.Int32, index);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position, 4), value.DayNumber);
        _position += 4;
    }

    public void WriteArrayTimeOnly(int index, TimeOnly value)
    {
        WriteArrayElementHeader(BsonType.Int64, index);
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.Slice(_position, 8), value.Ticks);
        _position += 8;
    }
#endif

    public void WriteArrayGuid(int index, Guid value)
    {
        WriteArrayString(index, value.ToString());
    }

    public void WriteArrayObjectId(int index, ObjectId value)
    {
        WriteArrayElementHeader(BsonType.ObjectId, index);
        value.WriteTo(_buffer.Slice(_position, 12));
        _position += 12;
    }

    public void WriteArrayNull(int index)
    {
        WriteArrayElementHeader(BsonType.Null, index);
    }

    /// <summary>
    /// Ends the current BSON array
    /// </summary>
    public void EndArray(int sizePosition)
    {
        WriteEndOfDocument();
        PatchDocumentSize(sizePosition);
    }

    // ── Compatibility helpers ──────────────────────────────────────────────────
    private void WriteDoubleLE(int position, double value)
    {
#if NET5_0_OR_GREATER
        BinaryPrimitives.WriteDoubleLittleEndian(_buffer.Slice(position, 8), value);
#else
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.Slice(position, 8), BitConverter.DoubleToInt64Bits(value));
#endif
    }
}
