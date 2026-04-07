using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace BLite.Bson;

/// <summary>
/// Represents an in-memory BSON document with lazy parsing.
/// Uses Memory&lt;byte&gt; to store raw BSON data for zero-copy operations.
/// 
/// Supports two modes:
/// - Raw mode: wraps existing C-BSON bytes (from storage) for zero-copy reads
/// - Dynamic mode: built via BsonDocumentBuilder with BsonValue fields (for server/API)
/// </summary>
public sealed class BsonDocument
{
    private readonly Memory<byte> _rawData;
    private readonly ConcurrentDictionary<ushort, string>? _keys;
    private readonly ConcurrentDictionary<string, ushort>? _forwardKeys;
    
    public BsonDocument(Memory<byte> rawBsonData, ConcurrentDictionary<ushort, string>? keys = null)
    {
        _rawData = rawBsonData;
        _keys = keys;
    }

    public BsonDocument(byte[] rawBsonData, ConcurrentDictionary<ushort, string>? keys = null)
    {
        _rawData = rawBsonData;
        _keys = keys;
    }

    public BsonDocument(Memory<byte> rawBsonData, ConcurrentDictionary<ushort, string>? keys, ConcurrentDictionary<string, ushort>? forwardKeys)
    {
        _rawData = rawBsonData;
        _keys = keys;
        _forwardKeys = forwardKeys;
    }

    public BsonDocument(byte[] rawBsonData, ConcurrentDictionary<ushort, string>? keys, ConcurrentDictionary<string, ushort>? forwardKeys)
    {
        _rawData = rawBsonData;
        _keys = keys;
        _forwardKeys = forwardKeys;
    }

    /// <summary>
    /// Gets the raw BSON bytes
    /// </summary>
    public ReadOnlyMemory<byte> RawData => _rawData;

    /// <summary>
    /// Gets the document size in bytes
    /// </summary>
    public int Size => BitConverter.ToInt32(_rawData.Span[..4]);

    /// <summary>
    /// Creates a reader for this document
    /// </summary>
    public BsonSpanReader GetReader() => new BsonSpanReader(_rawData.Span, _keys ?? new ConcurrentDictionary<ushort, string>());

    /// <summary>
    /// Attempts an O(1) field seek via the C-BSON v2 offset table.
    /// Returns <c>true</c> and the <paramref name="type"/> when the field is found;
    /// <c>false</c> when the fast path is unavailable and the caller should fall back
    /// to a sequential scan (reader position is then after <see cref="BsonSpanReader.ReadDocumentSize"/>).
    /// </summary>
    private bool TrySeekField(string fieldName, ref BsonSpanReader reader, out BsonType type)
    {
        type = default;
        reader.ReadDocumentSize();
        if (_forwardKeys != null && _forwardKeys.TryGetValue(fieldName, out var fieldId))
        {
            if (reader.TrySeekToField(fieldId, out type))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Tries to get a field value by name.
    /// Returns false if field not found.
    /// </summary>
    public bool TryGetString(string fieldName, out string? value)
    {
        value = null;
        var reader = GetReader();
        fieldName = fieldName.ToLowerInvariant();

        if (TrySeekField(fieldName, ref reader, out var seekType))
        {
            if (seekType == BsonType.String) { value = reader.ReadString(); return true; }
            return false;
        }

        while (reader.Remaining > 1)
        {
            var type = reader.ReadBsonType();
            if (type == BsonType.EndOfDocument)
                break;

            var name = reader.ReadElementHeader();
            
            if (name == fieldName && type == BsonType.String)
            {
                value = reader.ReadString();
                return true;
            }

            reader.SkipValue(type);
        }

        return false;
    }

    /// <summary>
    /// Tries to get an Int32 field value by name.
    /// </summary>
    public bool TryGetInt32(string fieldName, out int value)
    {
        value = 0;
        var reader = GetReader();
        fieldName = fieldName.ToLowerInvariant();

        if (TrySeekField(fieldName, ref reader, out var seekType))
        {
            if (seekType == BsonType.Int32) { value = reader.ReadInt32(); return true; }
            return false;
        }

        while (reader.Remaining > 1)
        {
            var type = reader.ReadBsonType();
            if (type == BsonType.EndOfDocument)
                break;

            var name = reader.ReadElementHeader();
            
            if (name == fieldName && type == BsonType.Int32)
            {
                value = reader.ReadInt32();
                return true;
            }

            reader.SkipValue(type);
        }

        return false;
    }

    /// <summary>
    /// Tries to get an ObjectId field value by name.
    /// </summary>
    public bool TryGetObjectId(string fieldName, out ObjectId value)
    {
        value = default;
        var reader = GetReader();
        fieldName = fieldName.ToLowerInvariant();

        if (TrySeekField(fieldName, ref reader, out var seekType))
        {
            if (seekType == BsonType.ObjectId) { value = reader.ReadObjectId(); return true; }
            return false;
        }

        while (reader.Remaining > 1)
        {
            var type = reader.ReadBsonType();
            if (type == BsonType.EndOfDocument)
                break;

            var name = reader.ReadElementHeader();
            
            if (name == fieldName && type == BsonType.ObjectId)
            {
                value = reader.ReadObjectId();
                return true;
            }

            reader.SkipValue(type);
        }

        return false;
    }

    /// <summary>
    /// Tries to get the document's _id field as a BsonId (supports any ID type).
    /// </summary>
    public bool TryGetId(out BsonId id)
    {
        id = default;
        var reader = GetReader();

        if (TrySeekField("_id", ref reader, out var seekType))
        {
            id = BsonId.ReadFrom(ref reader, seekType);
            return true;
        }

        while (reader.Remaining > 1)
        {
            var type = reader.ReadBsonType();
            if (type == BsonType.EndOfDocument) break;

            var name = reader.ReadElementHeader();
            if (name == "_id")
            {
                id = BsonId.ReadFrom(ref reader, type);
                return true;
            }
            reader.SkipValue(type);
        }
        return false;
    }

    /// <summary>
    /// Gets a field as a BsonValue by name.
    /// Returns BsonValue.Null if field not found.
    /// </summary>
    public BsonValue GetValue(string fieldName)
    {
        if (TryGetValue(fieldName, out var value))
            return value;
        return BsonValue.Null;
    }

    /// <summary>
    /// Tries to get a field as a BsonValue by name.
    /// </summary>
    public bool TryGetValue(string fieldName, out BsonValue value)
    {
        value = BsonValue.Null;
        var reader = GetReader();
        fieldName = fieldName.ToLowerInvariant();

        if (TrySeekField(fieldName, ref reader, out var seekType))
        {
            value = BsonValue.ReadFrom(ref reader, seekType);
            return true;
        }

        while (reader.Remaining > 1)
        {
            var type = reader.ReadBsonType();
            if (type == BsonType.EndOfDocument) break;

            var name = reader.ReadElementHeader();
            if (name == fieldName)
            {
                value = BsonValue.ReadFrom(ref reader, type);
                return true;
            }
            reader.SkipValue(type);
        }
        return false;
    }

    /// <summary>
    /// Enumerates all fields in this document as (name, BsonValue) pairs.
    /// </summary>
    public List<(string Name, BsonValue Value)> EnumerateFields()
    {
        var result = new List<(string Name, BsonValue Value)>();
        var reader = GetReader();
        reader.ReadDocumentSize();
        while (reader.Remaining > 1)
        {
            var type = reader.ReadBsonType();
            if (type == BsonType.EndOfDocument) break;
            var name = reader.ReadElementHeader();
            var value = BsonValue.ReadFrom(ref reader, type);
            result.Add((name, value));
        }
        return result;
    }

    /// <summary>
    /// Writes all fields (without document envelope) to a BsonSpanWriter.
    /// Used for embedding this document inside another document.
    /// </summary>
    internal void WriteFieldsTo(ref BsonSpanWriter writer)
    {
        var reader = GetReader();
        reader.ReadDocumentSize();
        while (reader.Remaining > 1)
        {
            var type = reader.ReadBsonType();
            if (type == BsonType.EndOfDocument) break;
            var name = reader.ReadElementHeader();
            var value = BsonValue.ReadFrom(ref reader, type);
            value.WriteTo(ref writer, name);
        }
    }

    /// <summary>
    /// Creates a new BsonDocument from field values using a builder pattern
    /// </summary>
    public static BsonDocument Create(IReadOnlyDictionary<string, ushort> keyMap, Action<BsonDocumentBuilder> buildAction)
    {
        var builder = new BsonDocumentBuilder(keyMap);
        buildAction(builder);
        return builder.Build();
    }

    /// <summary>
    /// Creates a new BsonDocument from field values using a builder pattern, with explicit reverse key map for reading.
    /// </summary>
    public static BsonDocument Create(IReadOnlyDictionary<string, ushort> keyMap, ConcurrentDictionary<ushort, string> reverseKeyMap, Action<BsonDocumentBuilder> buildAction)
    {
        var builder = new BsonDocumentBuilder(keyMap, reverseKeyMap);
        buildAction(builder);
        return builder.Build();
    }
}

/// <summary>
/// Builder for creating BSON documents.
/// Supports all BSON types including BsonId and BsonValue.
/// </summary>
public sealed class BsonDocumentBuilder
{
    private byte[] _buffer = new byte[1024]; // Start with 1KB
    private int _position;
    private readonly IReadOnlyDictionary<string, ushort> _keyMap;
    private readonly ConcurrentDictionary<ushort, string>? _reverseKeyMap;

    public BsonDocumentBuilder(IReadOnlyDictionary<string, ushort> keyMap)
    {
        _keyMap = keyMap;
        _reverseKeyMap = null;
        var writer = new BsonSpanWriter(_buffer, _keyMap);
        _position = writer.Position;
    }

    public BsonDocumentBuilder(IReadOnlyDictionary<string, ushort> keyMap, ConcurrentDictionary<ushort, string> reverseKeyMap)
    {
        _keyMap = keyMap;
        _reverseKeyMap = reverseKeyMap;
        var writer = new BsonSpanWriter(_buffer, _keyMap);
        _position = writer.Position;
    }

    /// <summary>
    /// Adds a BsonId field (polymorphic ID — ObjectId, int, long, string, Guid).
    /// </summary>
    public BsonDocumentBuilder AddId(BsonId id, string fieldName = "_id")
    {
        EnsureCapacity(256);
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        id.WriteTo(ref writer, fieldName);
        _position += writer.Position;
        return this;
    }

    /// <summary>
    /// Adds a BsonValue field (any supported BSON type).
    /// </summary>
    public BsonDocumentBuilder Add(string name, BsonValue value)
    {
        EnsureCapacity(1024);
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        value.WriteTo(ref writer, name);
        _position += writer.Position;
        return this;
    }

    public BsonDocumentBuilder AddString(string name, string value)
    {
        // 3 = element header (type byte + 2-byte key id)
        // 4 = int32 string length prefix
        // 1 = null terminator
        EnsureCapacity(3 + 4 + System.Text.Encoding.UTF8.GetByteCount(value) + 1);
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        writer.WriteString(name, value);
        _position += writer.Position;
        return this;
    }

    public BsonDocumentBuilder AddInt32(string name, int value)
    {
        EnsureCapacity(64);
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        writer.WriteInt32(name, value);
        _position += writer.Position;
        return this;
    }

    public BsonDocumentBuilder AddInt64(string name, long value)
    {
        EnsureCapacity(64);
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        writer.WriteInt64(name, value);
        _position += writer.Position;
        return this;
    }

    public BsonDocumentBuilder AddDouble(string name, double value)
    {
        EnsureCapacity(64);
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        writer.WriteDouble(name, value);
        _position += writer.Position;
        return this;
    }

    public BsonDocumentBuilder AddDecimal(string name, decimal value)
    {
        EnsureCapacity(64);
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        writer.WriteDecimal128(name, value);
        _position += writer.Position;
        return this;
    }

    public BsonDocumentBuilder AddBoolean(string name, bool value)
    {
        EnsureCapacity(64);
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        writer.WriteBoolean(name, value);
        _position += writer.Position;
        return this;
    }

    public BsonDocumentBuilder AddDateTime(string name, DateTime value)
    {
        EnsureCapacity(64);
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        writer.WriteDateTime(name, value);
        _position += writer.Position;
        return this;
    }

    public BsonDocumentBuilder AddObjectId(string name, ObjectId value)
    {
        EnsureCapacity(64);
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        writer.WriteObjectId(name, value);
        _position += writer.Position;
        return this;
    }

    public BsonDocumentBuilder AddGuid(string name, Guid value)
    {
        EnsureCapacity(128);
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        writer.WriteGuid(name, value);
        _position += writer.Position;
        return this;
    }

    public BsonDocumentBuilder AddNull(string name)
    {
        EnsureCapacity(64);
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        writer.WriteNull(name);
        _position += writer.Position;
        return this;
    }

    public BsonDocumentBuilder AddCoordinates(string name, (double, double) coordinates)
    {
        EnsureCapacity(128);
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        writer.WriteCoordinates(name, coordinates);
        _position += writer.Position;
        return this;
    }

    /// <summary>
    /// Adds a nested document field using a builder action.
    /// The inner builder shares the same key map for consistent field registration.
    /// </summary>
    public BsonDocumentBuilder AddDocument(string name, Action<BsonDocumentBuilder> buildAction)
    {
        // Create inner builder sharing the same key maps
        var innerBuilder = _reverseKeyMap != null
            ? new BsonDocumentBuilder(_keyMap, _reverseKeyMap)
            : new BsonDocumentBuilder(_keyMap);
        buildAction(innerBuilder);
        var innerDoc = innerBuilder.Build();
        var rawBytes = innerDoc.RawData;

        // elem type (1) + key (variable) + doc bytes
        EnsureCapacity(rawBytes.Length + name.Length + 16);

        // Write element header: type = 0x03 (Document)
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        writer.WriteElementHeader(BsonType.Document, name);
        _position += writer.Position;

        // Embed raw nested document bytes (includes size + fields + 0x00 end marker)
        rawBytes.Span.CopyTo(_buffer.AsSpan(_position));
        _position += rawBytes.Length;

        return this;
    }

    /// <summary>
    /// Writes a float array as a BSON array of double elements (same format as the source generator).
    /// The numeric index keys ("0", "1", ...) must have been pre-registered in the key map;
    /// they are typically registered by <c>CreateVectorIndex</c>.
    /// </summary>
    /// <summary>
    /// Writes a float array as a BSON array of double elements using raw positional uint16 keys.
    /// Does NOT register or read numeric index keys ("0", "1"…) in the key dictionary.
    /// Read back via <see cref="BsonValue.AsArray"/> which calls <see cref="BsonSpanReader.SkipArrayKey"/>.
    /// </summary>
    public BsonDocumentBuilder AddFloatArray(string name, float[] values)
    {
        EnsureCapacity(values.Length * 12 + 64);
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        var arrayPos = writer.BeginArray(name);
        for (int i = 0; i < values.Length; i++)
            writer.WriteArrayDouble(i, values[i]);
        writer.EndArray(arrayPos);
        _position += writer.Position;
        return this;
    }

    public BsonDocument Build()
    {
        // Final layout: [4-byte size] [accumulated fields] [0x00 end marker]
        var totalSize = 4 + _position + 1;
        var finalBuffer = new byte[totalSize];

        // Write size (total document size including the size field itself and end marker)
        BitConverter.TryWriteBytes(finalBuffer.AsSpan(0, 4), totalSize);

        // Copy accumulated fields after size header
        _buffer.AsSpan(0, _position).CopyTo(finalBuffer.AsSpan(4));

        // Write end-of-document marker
        finalBuffer[4 + _position] = 0x00;

        return new BsonDocument(finalBuffer, _reverseKeyMap);
    }

    private void EnsureCapacity(int additional)
    {
        if (_position + additional > _buffer.Length)
        {
            var newSize = Math.Max(_buffer.Length * 2, _position + additional);
            var newBuffer = new byte[newSize];
            _buffer.CopyTo(newBuffer, 0);
            _buffer = newBuffer;
        }
    }
}
