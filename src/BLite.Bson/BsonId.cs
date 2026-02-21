using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;

namespace BLite.Bson;

/// <summary>
/// Represents a polymorphic document ID that can hold ObjectId, int, long, string, or Guid.
/// Designed for server/dynamic mode where the ID type is not known at compile-time.
/// Value-type semantics with no heap allocation for primitive types.
/// </summary>
public readonly struct BsonId : IEquatable<BsonId>, IComparable<BsonId>
{
    private readonly BsonIdType _type;
    private readonly ObjectId _objectId;
    private readonly long _intValue;       // Holds int (widened) or long
    private readonly Guid _guidValue;
    private readonly string? _stringValue;

    /// <summary>
    /// The underlying BSON type of this ID.
    /// </summary>
    public BsonIdType Type => _type;

    /// <summary>
    /// Returns true if this is a default/empty BsonId.
    /// </summary>
    public bool IsEmpty => _type == BsonIdType.None;

    #region Constructors

    private BsonId(BsonIdType type, ObjectId objectId = default, long intValue = 0, Guid guidValue = default, string? stringValue = null)
    {
        _type = type;
        _objectId = objectId;
        _intValue = intValue;
        _guidValue = guidValue;
        _stringValue = stringValue;
    }

    public BsonId(ObjectId value) : this(BsonIdType.ObjectId, objectId: value) { }
    public BsonId(int value) : this(BsonIdType.Int32, intValue: value) { }
    public BsonId(long value) : this(BsonIdType.Int64, intValue: value) { }
    public BsonId(string value) : this(BsonIdType.String, stringValue: value ?? throw new ArgumentNullException(nameof(value))) { }
    public BsonId(Guid value) : this(BsonIdType.Guid, guidValue: value) { }

    #endregion

    #region Implicit conversions

    public static implicit operator BsonId(ObjectId value) => new(value);
    public static implicit operator BsonId(int value) => new(value);
    public static implicit operator BsonId(long value) => new(value);
    public static implicit operator BsonId(string value) => new(value);
    public static implicit operator BsonId(Guid value) => new(value);

    #endregion

    #region Value accessors

    /// <summary>Gets the value as ObjectId. Throws if type mismatch.</summary>
    public ObjectId AsObjectId() => _type == BsonIdType.ObjectId
        ? _objectId
        : throw new InvalidOperationException($"BsonId is {_type}, not ObjectId");

    /// <summary>Gets the value as Int32. Throws if type mismatch.</summary>
    public int AsInt32() => _type == BsonIdType.Int32
        ? (int)_intValue
        : throw new InvalidOperationException($"BsonId is {_type}, not Int32");

    /// <summary>Gets the value as Int64. Accepts both Int32 and Int64.</summary>
    public long AsInt64() => _type is BsonIdType.Int32 or BsonIdType.Int64
        ? _intValue
        : throw new InvalidOperationException($"BsonId is {_type}, not Int64");

    /// <summary>Gets the value as string. Throws if type mismatch.</summary>
    public string AsString() => _type == BsonIdType.String
        ? _stringValue!
        : throw new InvalidOperationException($"BsonId is {_type}, not String");

    /// <summary>Gets the value as Guid. Throws if type mismatch.</summary>
    public Guid AsGuid() => _type == BsonIdType.Guid
        ? _guidValue
        : throw new InvalidOperationException($"BsonId is {_type}, not Guid");

    #endregion

    #region BSON I/O

    /// <summary>
    /// Writes this ID as a BSON "_id" field using the appropriate BSON type.
    /// </summary>
    public void WriteTo(ref BsonSpanWriter writer, string fieldName = "_id")
    {
        switch (_type)
        {
            case BsonIdType.ObjectId:
                writer.WriteObjectId(fieldName, _objectId);
                break;
            case BsonIdType.Int32:
                writer.WriteInt32(fieldName, (int)_intValue);
                break;
            case BsonIdType.Int64:
                writer.WriteInt64(fieldName, _intValue);
                break;
            case BsonIdType.String:
                writer.WriteString(fieldName, _stringValue!);
                break;
            case BsonIdType.Guid:
                writer.WriteGuid(fieldName, _guidValue);
                break;
            default:
                throw new InvalidOperationException("Cannot write an empty BsonId");
        }
    }

    /// <summary>
    /// Reads a BsonId from a BsonSpanReader given the BSON type of the field.
    /// </summary>
    public static BsonId ReadFrom(ref BsonSpanReader reader, BsonType bsonType)
    {
        return bsonType switch
        {
            BsonType.ObjectId => new BsonId(reader.ReadObjectId()),
            BsonType.Int32 => new BsonId(reader.ReadInt32()),
            BsonType.Int64 => new BsonId(reader.ReadInt64()),
            BsonType.String => ReadStringOrGuid(reader.ReadString()),
            _ => throw new NotSupportedException($"BsonType {bsonType} is not supported as a document ID")
        };
    }

    private static BsonId ReadStringOrGuid(string value)
    {
        // Guids are serialized as strings in C-BSON — try to detect and restore the Guid type
        if (Guid.TryParse(value, out var guid))
            return new BsonId(guid);
        return new BsonId(value);
    }

    /// <summary>
    /// Creates a new auto-generated BsonId of the specified type.
    /// Supported for ObjectId and Guid.
    /// </summary>
    public static BsonId NewId(BsonIdType type = BsonIdType.ObjectId)
    {
        return type switch
        {
            BsonIdType.ObjectId => new BsonId(ObjectId.NewObjectId()),
            BsonIdType.Guid => new BsonId(Guid.NewGuid()),
            _ => throw new InvalidOperationException($"Cannot auto-generate ID for type {type}. Provide an explicit value.")
        };
    }

    #endregion

    #region Raw bytes (for IndexKey interop in BLite.Core)

    /// <summary>
    /// Returns the raw bytes representation suitable for index key creation.
    /// Used by BLite.Core to bridge BsonId → IndexKey without coupling.
    /// </summary>
    public byte[] ToBytes()
    {
        return _type switch
        {
            BsonIdType.ObjectId => _objectId.ToByteArray(),
            BsonIdType.Int32 => BitConverter.GetBytes((int)_intValue),
            BsonIdType.Int64 => BitConverter.GetBytes(_intValue),
            BsonIdType.String => Encoding.UTF8.GetBytes(_stringValue!),
            BsonIdType.Guid => _guidValue.ToByteArray(),
            _ => throw new InvalidOperationException("Cannot convert empty BsonId to bytes")
        };
    }

    /// <summary>
    /// Creates a BsonId from raw bytes and a known type.
    /// Used by BLite.Core to bridge IndexKey → BsonId without coupling.
    /// </summary>
    public static BsonId FromBytes(ReadOnlySpan<byte> data, BsonIdType type)
    {
        return type switch
        {
            BsonIdType.ObjectId => new BsonId(new ObjectId(data)),
            BsonIdType.Int32 => new BsonId(BitConverter.ToInt32(data)),
            BsonIdType.Int64 => new BsonId(BitConverter.ToInt64(data)),
            BsonIdType.String => new BsonId(Encoding.UTF8.GetString(data)),
            BsonIdType.Guid => new BsonId(new Guid(data)),
            _ => throw new InvalidOperationException($"Cannot create BsonId of type {type} from bytes")
        };
    }

    #endregion

    #region Equality & Comparison

    public bool Equals(BsonId other)
    {
        if (_type != other._type) return false;
        return _type switch
        {
            BsonIdType.ObjectId => _objectId.Equals(other._objectId),
            BsonIdType.Int32 or BsonIdType.Int64 => _intValue == other._intValue,
            BsonIdType.String => string.Equals(_stringValue, other._stringValue, StringComparison.Ordinal),
            BsonIdType.Guid => _guidValue.Equals(other._guidValue),
            _ => true // Both None
        };
    }

    public int CompareTo(BsonId other)
    {
        if (_type != other._type)
            return ((byte)_type).CompareTo((byte)other._type);

        return _type switch
        {
            BsonIdType.ObjectId => _objectId.ToByteArray().AsSpan().SequenceCompareTo(other._objectId.ToByteArray()),
            BsonIdType.Int32 or BsonIdType.Int64 => _intValue.CompareTo(other._intValue),
            BsonIdType.String => string.Compare(_stringValue, other._stringValue, StringComparison.Ordinal),
            BsonIdType.Guid => _guidValue.CompareTo(other._guidValue),
            _ => 0
        };
    }

    public override bool Equals(object? obj) => obj is BsonId other && Equals(other);

    public override int GetHashCode()
    {
        return _type switch
        {
            BsonIdType.ObjectId => HashCode.Combine(_type, _objectId),
            BsonIdType.Int32 or BsonIdType.Int64 => HashCode.Combine(_type, _intValue),
            BsonIdType.String => HashCode.Combine(_type, _stringValue),
            BsonIdType.Guid => HashCode.Combine(_type, _guidValue),
            _ => 0
        };
    }

    public static bool operator ==(BsonId left, BsonId right) => left.Equals(right);
    public static bool operator !=(BsonId left, BsonId right) => !left.Equals(right);

    #endregion

    public override string ToString()
    {
        return _type switch
        {
            BsonIdType.ObjectId => _objectId.ToString(),
            BsonIdType.Int32 => ((int)_intValue).ToString(),
            BsonIdType.Int64 => _intValue.ToString(),
            BsonIdType.String => _stringValue!,
            BsonIdType.Guid => _guidValue.ToString(),
            _ => "(empty)"
        };
    }
}

/// <summary>
/// Identifies the underlying type of a BsonId.
/// </summary>
public enum BsonIdType : byte
{
    /// <summary>No ID assigned</summary>
    None = 0,
    /// <summary>12-byte MongoDB-compatible ObjectId</summary>
    ObjectId = 1,
    /// <summary>32-bit integer</summary>
    Int32 = 2,
    /// <summary>64-bit integer</summary>
    Int64 = 3,
    /// <summary>UTF-8 string</summary>
    String = 4,
    /// <summary>128-bit GUID</summary>
    Guid = 5
}
