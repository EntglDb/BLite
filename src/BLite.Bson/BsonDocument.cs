using System;

namespace BLite.Bson;

/// <summary>
/// Represents an in-memory BSON document with lazy parsing.
/// Uses Memory&lt;byte&gt; to store raw BSON data for zero-copy operations.
/// </summary>
public sealed class BsonDocument
{
    private readonly Memory<byte> _rawData;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<ushort, string>? _keys;
    
    public BsonDocument(Memory<byte> rawBsonData, System.Collections.Concurrent.ConcurrentDictionary<ushort, string>? keys = null)
    {
        _rawData = rawBsonData;
        _keys = keys;
    }

    public BsonDocument(byte[] rawBsonData, System.Collections.Concurrent.ConcurrentDictionary<ushort, string>? keys = null)
    {
        _rawData = rawBsonData;
        _keys = keys;
    }

    /// <summary>
    /// Gets the raw BSON bytes
    /// </summary>
    public ReadOnlySpan<byte> RawData => _rawData.Span;

    /// <summary>
    /// Gets the document size in bytes
    /// </summary>
    public int Size => BitConverter.ToInt32(_rawData.Span[..4]);

    /// <summary>
    /// Creates a reader for this document
    /// </summary>
    public BsonSpanReader GetReader() => new BsonSpanReader(_rawData.Span, _keys ?? new System.Collections.Concurrent.ConcurrentDictionary<ushort, string>());

    /// <summary>
    /// Tries to get a field value by name.
    /// Returns false if field not found.
    /// </summary>
    public bool TryGetString(string fieldName, out string? value)
    {
        value = null;
        var reader = GetReader();
        fieldName = fieldName.ToLowerInvariant();
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
    /// Creates a new BsonDocument from field values using a builder pattern
    /// </summary>
    public static BsonDocument Create(System.Collections.Concurrent.ConcurrentDictionary<string, ushort> keyMap, Action<BsonDocumentBuilder> buildAction)
    {
        var builder = new BsonDocumentBuilder(keyMap);
        buildAction(builder);
        return builder.Build();
    }
}

/// <summary>
/// Builder for creating BSON documents
/// </summary>
public sealed class BsonDocumentBuilder
{
    private byte[] _buffer = new byte[1024]; // Start with 1KB
    private int _position;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, ushort> _keyMap;

    public BsonDocumentBuilder(System.Collections.Concurrent.ConcurrentDictionary<string, ushort> keyMap)
    {
        _keyMap = keyMap;
        var writer = new BsonSpanWriter(_buffer, _keyMap);
        _position = writer.Position;
    }

    public BsonDocumentBuilder AddString(string name, string value)
    {
        EnsureCapacity(256); // Conservative estimate
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

    public BsonDocumentBuilder AddBoolean(string name, bool value)
    {
        EnsureCapacity(64);
        var writer = new BsonSpanWriter(_buffer.AsSpan(_position..), _keyMap);
        writer.WriteBoolean(name, value);
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

    public BsonDocument Build()
    {
        var finalBuffer = new byte[_position + 5]; // Size header + content + end marker
        var writer = new BsonSpanWriter(finalBuffer, _keyMap);
        
        var sizePos = writer.BeginDocument();
        
        // Copy the accumulated fields
        _buffer.AsSpan(0, _position).CopyTo(finalBuffer.AsSpan(4));
        
        writer.EndDocument(sizePos);
        
        return new BsonDocument(finalBuffer[..writer.Position]);
    }

    private void EnsureCapacity(int additional)
    {
        if (_position + additional > _buffer.Length)
        {
            var newBuffer = new byte[_buffer.Length * 2];
            _buffer.CopyTo(newBuffer, 0);
            _buffer = newBuffer;
        }
    }
}
