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
}
