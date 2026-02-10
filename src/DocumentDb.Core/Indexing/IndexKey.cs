using DocumentDb.Bson;

namespace DocumentDb.Core.Indexing;

/// <summary>
/// Represents a key in an index.
/// Implemented as struct for efficient index operations.
/// Note: Contains byte array so cannot be readonly struct.
/// </summary>
public struct IndexKey : IEquatable<IndexKey>, IComparable<IndexKey>
{
    private readonly byte[] _data;
    private readonly int _hashCode;

    public static IndexKey MinKey => new IndexKey(Array.Empty<byte>());
    public static IndexKey MaxKey => new IndexKey(Enumerable.Repeat((byte)0xFF, 32).ToArray());

    public IndexKey(ReadOnlySpan<byte> data)
    {
        _data = data.ToArray();
        _hashCode = ComputeHashCode(data);
    }

    public IndexKey(ObjectId objectId)
    {
        _data = new byte[12];
        objectId.WriteTo(_data);
        _hashCode = ComputeHashCode(_data);
    }

    public IndexKey(int value)
    {
        _data = BitConverter.GetBytes(value);
        _hashCode = ComputeHashCode(_data);
    }

    public IndexKey(long value)
    {
        _data = BitConverter.GetBytes(value);
        _hashCode = ComputeHashCode(_data);
    }

    public IndexKey(string value)
    {
        _data = System.Text.Encoding.UTF8.GetBytes(value);
        _hashCode = ComputeHashCode(_data);
    }

    public IndexKey(Guid value)
    {
        _data = value.ToByteArray();
        _hashCode = ComputeHashCode(_data);
    }

    public readonly ReadOnlySpan<byte> Data => _data;

    public readonly int CompareTo(IndexKey other)
    {
        if (_data == null) return other._data == null ? 0 : -1;
        if (other._data == null) return 1;

        var minLength = Math.Min(_data.Length, other._data.Length);
        
        for (int i = 0; i < minLength; i++)
        {
            var cmp = _data[i].CompareTo(other._data[i]);
            if (cmp != 0)
                return cmp;
        }

        return _data.Length.CompareTo(other._data.Length);
    }

    public readonly bool Equals(IndexKey other)
    {
        if (_hashCode != other._hashCode)
            return false;

        if (_data == null) return other._data == null;
        if (other._data == null) return false;

        return _data.AsSpan().SequenceEqual(other._data);
    }

    public override readonly bool Equals(object? obj) => obj is IndexKey other && Equals(other);

    public override readonly int GetHashCode() => _hashCode;

    public static bool operator ==(IndexKey left, IndexKey right) => left.Equals(right);
    public static bool operator !=(IndexKey left, IndexKey right) => !left.Equals(right);
    public static bool operator <(IndexKey left, IndexKey right) => left.CompareTo(right) < 0;
    public static bool operator >(IndexKey left, IndexKey right) => left.CompareTo(right) > 0;
    public static bool operator <=(IndexKey left, IndexKey right) => left.CompareTo(right) <= 0;
    public static bool operator >=(IndexKey left, IndexKey right) => left.CompareTo(right) >= 0;

    private static int ComputeHashCode(ReadOnlySpan<byte> data)
    {
        var hash = new HashCode();
        hash.AddBytes(data);
        return hash.ToHashCode();
    }

    public static IndexKey Create<T>(T value)
    {
        if (value == null) return default;

        if (typeof(T) == typeof(ObjectId)) return new IndexKey((ObjectId)(object)value);
        if (typeof(T) == typeof(int)) return new IndexKey((int)(object)value);
        if (typeof(T) == typeof(long)) return new IndexKey((long)(object)value);
        if (typeof(T) == typeof(string)) return new IndexKey((string)(object)value);
        if (typeof(T) == typeof(Guid)) return new IndexKey((Guid)(object)value);
        if (typeof(T) == typeof(byte[])) return new IndexKey((byte[])(object)value);

        throw new NotSupportedException($"Type {typeof(T).Name} is not supported as an IndexKey. Provide a custom mapping.");
    }

    public readonly T As<T>()
    {
        if (_data == null) return default!;

        if (typeof(T) == typeof(ObjectId)) return (T)(object)new ObjectId(_data);
        if (typeof(T) == typeof(int)) return (T)(object)BitConverter.ToInt32(_data);
        if (typeof(T) == typeof(long)) return (T)(object)BitConverter.ToInt64(_data);
        if (typeof(T) == typeof(string)) return (T)(object)System.Text.Encoding.UTF8.GetString(_data);
        if (typeof(T) == typeof(Guid)) return (T)(object)new Guid(_data);
        if (typeof(T) == typeof(byte[])) return (T)(object)_data;

        throw new NotSupportedException($"Type {typeof(T).Name} cannot be extracted from IndexKey. Provide a custom mapping.");
    }
}
