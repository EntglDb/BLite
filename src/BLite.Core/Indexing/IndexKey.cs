using BLite.Bson;
using System;

namespace BLite.Core.Indexing;

/// <summary>
/// Represents a key in an index.
/// Implemented as struct for efficient index operations.
/// Note: Contains byte array so cannot be readonly struct.
/// </summary>
public struct IndexKey : IEquatable<IndexKey>, IComparable<IndexKey>
{
    private readonly byte[] _data;
    private readonly int _hashCode;

    // Pre-allocated static sentinels — single allocation at type-init time.
    public static readonly IndexKey MinKey = new IndexKey(Array.Empty<byte>());
    public static readonly IndexKey MaxKey = new IndexKey(new byte[] {
        0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
        0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
        0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
        0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF });

    /// <summary>
    /// Sentinel key stored in the B-tree for BSON null values.
    /// Sorts after MinKey (0-byte array) and before all real data keys (DateTimeOffset, int, etc.
    /// encode with a leading byte ≥ 0x01).
    /// </summary>
    public static readonly IndexKey NullSentinel = new IndexKey(new byte[] { 0x00 });

    /// <summary>
    /// One step above NullSentinel — used as the open lower bound in range scans so that
    /// null-valued entries are excluded from inequality / range queries.
    /// </summary>
    public static readonly IndexKey NullSentinelNext = new IndexKey(new byte[] { 0x01 });

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
        // Big-endian + sign-bit flip: lexicographic byte order == numeric order (including negatives).
        uint u = (uint)value ^ 0x8000_0000u;
        _data = [(byte)(u >> 24), (byte)(u >> 16), (byte)(u >> 8), (byte)u];
        _hashCode = ComputeHashCode(_data);
    }

    public IndexKey(long value)
    {
        // Big-endian + sign-bit flip: lexicographic byte order == numeric order (including negatives).
        ulong u = (ulong)value ^ 0x8000_0000_0000_0000UL;
        _data = [(byte)(u >> 56), (byte)(u >> 48), (byte)(u >> 40), (byte)(u >> 32),
                 (byte)(u >> 24), (byte)(u >> 16), (byte)(u >>  8), (byte)u];
        _hashCode = ComputeHashCode(_data);
    }

    public IndexKey(double value)
    {
        // Sort-preserving IEEE 754 encoding:
        // Positive: flip sign bit only  → 1xxx…  (positive doubles sort after all negatives)
        // Negative: flip all bits        → sort from least-negative first
        // Result: lexicographic byte order == IEEE 754 numeric order for all finite and ±Inf.
        var bits = BitConverter.DoubleToInt64Bits(value);
        ulong u = bits < 0 ? ~(ulong)bits : (ulong)bits | 0x8000_0000_0000_0000UL;
        _data = [(byte)(u >> 56), (byte)(u >> 48), (byte)(u >> 40), (byte)(u >> 32),
                 (byte)(u >> 24), (byte)(u >> 16), (byte)(u >>  8), (byte)u];
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

    // ── Ownership-taking factory ───────────────────────────────────────────────
    // Used by CreateCompositeKey to avoid the extra .ToArray() copy that the
    // ReadOnlySpan<byte> ctor incurs (intermediate byte[] allocated by the caller
    // + second copy inside the ctor = 2 allocations → now 1).
    // The caller transfers ownership; the array must not be used afterwards.
    private IndexKey(byte[] ownedData, bool _)
    {
        _data = ownedData;
        _hashCode = ComputeHashCode(ownedData);
    }

    internal static IndexKey FromOwnedArray(byte[] ownedData)
        => new IndexKey(ownedData, true);

    // ── Zero-allocation span comparison ──────────────────────────────────────
    // Compares raw byte spans with the same semantics as IndexKey.CompareTo,
    // but without constructing an IndexKey (and without allocating a byte[]).
    // Used by BTreeIndex.Range and FindChildNode to eliminate per-entry allocations
    // during leaf/internal-node scans.
    internal static int CompareRaw(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        var minLen = Math.Min(a.Length, b.Length);
        for (int i = 0; i < minLen; i++)
        {
            int diff = a[i] - b[i];
            if (diff != 0) return diff;
        }
        return a.Length - b.Length;
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
#if NET8_0_OR_GREATER
        hash.AddBytes(data);
#else
        foreach (var b in data) hash.Add(b);
#endif
        return hash.ToHashCode();
    }

    public static IndexKey Create<T>(T value)
    {
        if (value == null) return default;

        if (typeof(T) == typeof(ObjectId)) return new IndexKey((ObjectId)(object)value);
        if (typeof(T) == typeof(int)) return new IndexKey((int)(object)value);
        if (typeof(T) == typeof(long)) return new IndexKey((long)(object)value);
        if (typeof(T) == typeof(double)) return new IndexKey((double)(object)value);
        if (typeof(T) == typeof(string)) return new IndexKey((string)(object)value);
        if (typeof(T) == typeof(Guid)) return new IndexKey((Guid)(object)value);
        if (typeof(T) == typeof(byte[])) return new IndexKey((byte[])(object)value);

        throw new NotSupportedException($"Type {typeof(T).Name} is not supported as an IndexKey. Provide a custom mapping.");
    }

    public readonly T As<T>()
    {
        if (_data == null) return default!;

        if (typeof(T) == typeof(ObjectId)) return (T)(object)new ObjectId(_data);
        if (typeof(T) == typeof(int))
        {
            uint u = ((uint)_data[0] << 24) | ((uint)_data[1] << 16) | ((uint)_data[2] << 8) | _data[3];
            return (T)(object)(int)(u ^ 0x8000_0000u);
        }
        if (typeof(T) == typeof(long))
        {
            ulong u = ((ulong)_data[0] << 56) | ((ulong)_data[1] << 48) | ((ulong)_data[2] << 40) | ((ulong)_data[3] << 32)
                    | ((ulong)_data[4] << 24) | ((ulong)_data[5] << 16) | ((ulong)_data[6] <<  8) | _data[7];
            return (T)(object)(long)(u ^ 0x8000_0000_0000_0000UL);
        }
        if (typeof(T) == typeof(double))
        {
            ulong u = ((ulong)_data[0] << 56) | ((ulong)_data[1] << 48) | ((ulong)_data[2] << 40) | ((ulong)_data[3] << 32)
                    | ((ulong)_data[4] << 24) | ((ulong)_data[5] << 16) | ((ulong)_data[6] <<  8) | _data[7];
            // Reverse encoding: if MSB was set (positive double), clear it; else flip all bits (negative double)
            long bits = u >= 0x8000_0000_0000_0000UL
                ? (long)(u ^ 0x8000_0000_0000_0000UL)
                : (long)~u;
            return (T)(object)BitConverter.Int64BitsToDouble(bits);
        }
        if (typeof(T) == typeof(string)) return (T)(object)System.Text.Encoding.UTF8.GetString(_data);
        if (typeof(T) == typeof(Guid)) return (T)(object)new Guid(_data);
        if (typeof(T) == typeof(byte[])) return (T)(object)_data;

        throw new NotSupportedException($"Type {typeof(T).Name} cannot be extracted from IndexKey. Provide a custom mapping.");
    }
}
