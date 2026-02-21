using System;
using System.Collections.Generic;
using System.Text;

namespace BLite.Bson;

/// <summary>
/// Represents a polymorphic BSON value for dynamic/server mode.
/// Wraps any supported BSON type in a single struct without boxing for primitives.
/// </summary>
public readonly struct BsonValue : IEquatable<BsonValue>
{
    private readonly BsonType _type;
    private readonly double _numericValue;   // Holds int (widened), long (bits), double
    private readonly object? _refValue;      // Holds string, byte[], BsonDocument, List<BsonValue>, ObjectId (boxed), Guid (boxed), decimal (boxed)

    /// <summary>The BSON type of this value.</summary>
    public BsonType Type => _type;

    /// <summary>Returns true if this value is null or default.</summary>
    public bool IsNull => _type == BsonType.Null || _type == BsonType.EndOfDocument;

    #region Constructors

    private BsonValue(BsonType type, double numericValue = 0, object? refValue = null)
    {
        _type = type;
        _numericValue = numericValue;
        _refValue = refValue;
    }

    #endregion

    #region Factory methods

    public static BsonValue Null => new(BsonType.Null);

    public static BsonValue FromInt32(int value) => new(BsonType.Int32, value);
    public static BsonValue FromInt64(long value) => new(BsonType.Int64, BitConverter.Int64BitsToDouble(value));
    public static BsonValue FromDouble(double value) => new(BsonType.Double, value);
    public static BsonValue FromDecimal(decimal value) => new(BsonType.Decimal128, refValue: value);
    public static BsonValue FromString(string value) => new(BsonType.String, refValue: value ?? throw new ArgumentNullException(nameof(value)));
    public static BsonValue FromBoolean(bool value) => new(BsonType.Boolean, value ? 1 : 0);
    public static BsonValue FromObjectId(ObjectId value) => new(BsonType.ObjectId, refValue: value);
    public static BsonValue FromDateTime(DateTime value) => new(BsonType.DateTime, BitConverter.Int64BitsToDouble(new DateTimeOffset(value.ToUniversalTime()).ToUnixTimeMilliseconds()));
    public static BsonValue FromDateTimeOffset(DateTimeOffset value) => new(BsonType.DateTime, BitConverter.Int64BitsToDouble(value.ToUnixTimeMilliseconds()));
    public static BsonValue FromGuid(Guid value) => new(BsonType.String, refValue: value.ToString());
    public static BsonValue FromBinary(byte[] value) => new(BsonType.Binary, refValue: value ?? throw new ArgumentNullException(nameof(value)));
    public static BsonValue FromDocument(BsonDocument value) => new(BsonType.Document, refValue: value ?? throw new ArgumentNullException(nameof(value)));
    public static BsonValue FromArray(List<BsonValue> value) => new(BsonType.Array, refValue: value ?? throw new ArgumentNullException(nameof(value)));
    public static BsonValue FromCoordinates((double Lat, double Lon) value) => new(BsonType.Array, refValue: new double[] { value.Lat, value.Lon });

    #endregion

    #region Implicit conversions

    public static implicit operator BsonValue(int value) => FromInt32(value);
    public static implicit operator BsonValue(long value) => FromInt64(value);
    public static implicit operator BsonValue(double value) => FromDouble(value);
    public static implicit operator BsonValue(decimal value) => FromDecimal(value);
    public static implicit operator BsonValue(string value) => FromString(value);
    public static implicit operator BsonValue(bool value) => FromBoolean(value);
    public static implicit operator BsonValue(ObjectId value) => FromObjectId(value);
    public static implicit operator BsonValue(DateTime value) => FromDateTime(value);

    #endregion

    #region Value accessors

    public int AsInt32 => _type == BsonType.Int32
        ? (int)_numericValue
        : throw new InvalidOperationException($"BsonValue is {_type}, not Int32");

    public long AsInt64 => _type switch
    {
        BsonType.Int32 => (long)_numericValue,
        BsonType.Int64 => BitConverter.DoubleToInt64Bits(_numericValue),
        _ => throw new InvalidOperationException($"BsonValue is {_type}, not Int64")
    };

    public double AsDouble => _type switch
    {
        BsonType.Double => _numericValue,
        BsonType.Int32 => _numericValue,
        BsonType.Int64 => (double)BitConverter.DoubleToInt64Bits(_numericValue),
        _ => throw new InvalidOperationException($"BsonValue is {_type}, not Double")
    };

    public decimal AsDecimal => _type == BsonType.Decimal128 && _refValue is decimal d
        ? d
        : throw new InvalidOperationException($"BsonValue is {_type}, not Decimal128");

    public string AsString => _type == BsonType.String && _refValue is string s
        ? s
        : throw new InvalidOperationException($"BsonValue is {_type}, not String");

    public bool AsBoolean => _type == BsonType.Boolean
        ? _numericValue != 0
        : throw new InvalidOperationException($"BsonValue is {_type}, not Boolean");

    public ObjectId AsObjectId => _type == BsonType.ObjectId && _refValue is ObjectId oid
        ? oid
        : throw new InvalidOperationException($"BsonValue is {_type}, not ObjectId");

    public DateTime AsDateTime => _type == BsonType.DateTime
        ? DateTimeOffset.FromUnixTimeMilliseconds(BitConverter.DoubleToInt64Bits(_numericValue)).UtcDateTime
        : throw new InvalidOperationException($"BsonValue is {_type}, not DateTime");

    public DateTimeOffset AsDateTimeOffset => _type == BsonType.DateTime
        ? DateTimeOffset.FromUnixTimeMilliseconds(BitConverter.DoubleToInt64Bits(_numericValue))
        : throw new InvalidOperationException($"BsonValue is {_type}, not DateTime");

    public byte[] AsBinary => _type == BsonType.Binary && _refValue is byte[] b
        ? b
        : throw new InvalidOperationException($"BsonValue is {_type}, not Binary");

    public BsonDocument AsDocument => _type == BsonType.Document && _refValue is BsonDocument doc
        ? doc
        : throw new InvalidOperationException($"BsonValue is {_type}, not Document");

    public List<BsonValue> AsArray => _type == BsonType.Array && _refValue is List<BsonValue> arr
        ? arr
        : throw new InvalidOperationException($"BsonValue is {_type}, not Array");

    public (double Lat, double Lon) AsCoordinates
    {
        get
        {
            if (_type == BsonType.Array && _refValue is double[] arr && arr.Length == 2)
                return (arr[0], arr[1]);
            // Documents round-tripped through ReadArray store elements as List<BsonValue>
            if (_type == BsonType.Array && _refValue is List<BsonValue> list && list.Count == 2)
                return (list[0].AsDouble, list[1].AsDouble);
            throw new InvalidOperationException($"BsonValue is {_type}, not Coordinates");
        }
    }

    #endregion

    #region BSON I/O

    /// <summary>
    /// Writes this value as a named BSON element.
    /// </summary>
    public void WriteTo(ref BsonSpanWriter writer, string fieldName)
    {
        switch (_type)
        {
            case BsonType.Int32:
                writer.WriteInt32(fieldName, (int)_numericValue);
                break;
            case BsonType.Int64:
                writer.WriteInt64(fieldName, BitConverter.DoubleToInt64Bits(_numericValue));
                break;
            case BsonType.Double:
                writer.WriteDouble(fieldName, _numericValue);
                break;
            case BsonType.Decimal128:
                writer.WriteDecimal128(fieldName, (decimal)_refValue!);
                break;
            case BsonType.String:
                writer.WriteString(fieldName, (string)_refValue!);
                break;
            case BsonType.Boolean:
                writer.WriteBoolean(fieldName, _numericValue != 0);
                break;
            case BsonType.ObjectId:
                writer.WriteObjectId(fieldName, (ObjectId)_refValue!);
                break;
            case BsonType.DateTime:
                var dt = DateTimeOffset.FromUnixTimeMilliseconds(BitConverter.DoubleToInt64Bits(_numericValue)).UtcDateTime;
                writer.WriteDateTime(fieldName, dt);
                break;
            case BsonType.Binary:
                writer.WriteBinary(fieldName, (byte[])_refValue!);
                break;
            case BsonType.Null:
                writer.WriteNull(fieldName);
                break;
            case BsonType.Array when _refValue is (double lat, double lon):
                writer.WriteCoordinates(fieldName, (lat, lon));
                break;
            case BsonType.Array when _refValue is List<BsonValue> arr:
                var arrPos = writer.BeginArray(fieldName);
                for (int i = 0; i < arr.Count; i++)
                {
                    arr[i].WriteTo(ref writer, i.ToString());
                }
                writer.EndArray(arrPos);
                break;
            case BsonType.Document when _refValue is BsonDocument doc:
                // Nested document: write raw bytes
                var docPos = writer.BeginDocument(fieldName);
                doc.WriteFieldsTo(ref writer);
                writer.EndDocument(docPos);
                break;
            default:
                throw new NotSupportedException($"Cannot write BsonValue of type {_type}");
        }
    }

    /// <summary>
    /// Reads a BsonValue from a reader given the BSON type.
    /// </summary>
    public static BsonValue ReadFrom(ref BsonSpanReader reader, BsonType type)
    {
        return type switch
        {
            BsonType.Int32 => FromInt32(reader.ReadInt32()),
            BsonType.Int64 => FromInt64(reader.ReadInt64()),
            BsonType.Double => FromDouble(reader.ReadDouble()),
            BsonType.Decimal128 => FromDecimal(reader.ReadDecimal128()),
            BsonType.String => FromString(reader.ReadString()),
            BsonType.Boolean => FromBoolean(reader.ReadBoolean()),
            BsonType.ObjectId => FromObjectId(reader.ReadObjectId()),
            BsonType.DateTime => FromDateTimeOffset(reader.ReadDateTimeOffset()),
            BsonType.Null => Null,
            BsonType.Binary => FromBinary(reader.ReadBinary(out _).ToArray()),
            BsonType.Array => ReadArray(ref reader),
            BsonType.Document => ReadNestedDocument(ref reader),
            _ => throw new NotSupportedException($"BsonType {type} is not supported in BsonValue")
        };
    }

    private static BsonValue ReadArray(ref BsonSpanReader reader)
    {
        var list = new List<BsonValue>();
        reader.ReadDocumentSize(); // array size
        while (reader.Remaining > 0)
        {
            var elemType = reader.ReadBsonType();
            if (elemType == BsonType.EndOfDocument) break;
            reader.SkipArrayKey(); // positional index — value always discarded; works for both keymap and raw uint16 encoding
            list.Add(ReadFrom(ref reader, elemType));
        }
        return FromArray(list);
    }

    private static BsonValue ReadNestedDocument(ref BsonSpanReader reader)
    {
        var docSize = reader.PeekInt32();
        var docData = reader.RemainingBytes()[..docSize].ToArray();
        // Advance reader past the subdocument
        for (int i = 0; i < docSize; i++) reader.ReadByte();
        return FromDocument(new BsonDocument(docData));
    }

    #endregion

    #region Equality

    public bool Equals(BsonValue other)
    {
        if (_type != other._type) return false;
        return _type switch
        {
            BsonType.Int32 or BsonType.Int64 or BsonType.Double or BsonType.Boolean or BsonType.DateTime
                => _numericValue == other._numericValue,
            BsonType.String => string.Equals((string?)_refValue, (string?)other._refValue, StringComparison.Ordinal),
            BsonType.ObjectId => Equals(_refValue, other._refValue),
            BsonType.Null => true,
            _ => ReferenceEquals(_refValue, other._refValue) || Equals(_refValue, other._refValue)
        };
    }

    public override bool Equals(object? obj) => obj is BsonValue other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(_type, _numericValue, _refValue);

    public static bool operator ==(BsonValue left, BsonValue right) => left.Equals(right);
    public static bool operator !=(BsonValue left, BsonValue right) => !left.Equals(right);

    #endregion

    public override string ToString()
    {
        return _type switch
        {
            BsonType.Int32 => ((int)_numericValue).ToString(),
            BsonType.Int64 => BitConverter.DoubleToInt64Bits(_numericValue).ToString(),
            BsonType.Double => _numericValue.ToString(),
            BsonType.String => $"\"{_refValue}\"",
            BsonType.Boolean => (_numericValue != 0).ToString(),
            BsonType.ObjectId => _refValue?.ToString() ?? "(null)",
            BsonType.DateTime => AsDateTime.ToString("O"),
            BsonType.Null => "null",
            _ => $"({_type})"
        };
    }
}
