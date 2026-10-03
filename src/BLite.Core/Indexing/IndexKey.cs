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

    // Discriminator prefix prepended to every real (non-sentinel) key byte array.
    // Guarantees NullSentinel (0x00) and NullSentinelNext (0x01) sort strictly before
    // all encoded user values, including int.MinValue which encodes to { 0x00, 0x00, 0x00, 0x00 }
    // without the prefix.
    private const byte KeyPrefix = 0x02;

    // Prefix of decimal keys. Distinct from KeyPrefix so that an index whose keys were written by an
    // older version (decimal stored as double under KeyPrefix) can be told apart from a current one.
    // 0x03 sorts after every KeyPrefix key, so range scans must use an upper bound above both prefixes.
    internal const byte DecimalKeyPrefix = 0x03;

    // Prefix + class byte + exponent byte + 15 bytes (30 BCD nibbles, a decimal has at most 29 digits).
    private const int DecimalKeyLength = 18;

    // Pre-allocated static sentinels — single allocation at type-init time.
    // These use the raw (no-prefix) FromOwnedArray factory so the sentinel byte values
    // are preserved exactly as-is and not mistaken for real user keys.
    public static readonly IndexKey MinKey = FromOwnedArray(Array.Empty<byte>());
    public static readonly IndexKey MaxKey = FromOwnedArray(new byte[] {
        0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
        0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
        0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
        0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF });

    /// <summary>
    /// Sentinel key stored in the B-tree for BSON null values.
    /// Sorts after MinKey (empty array) and before all real data keys, which are
    /// prefixed with <c>KeyPrefix</c> (0x02).
    /// </summary>
    public static readonly IndexKey NullSentinel = FromOwnedArray(new byte[] { 0x00 });

    /// <summary>
    /// One step above NullSentinel — used as the open lower bound in range scans so that
    /// null-valued entries are excluded from inequality / range queries.
    /// All real data keys start with <c>KeyPrefix</c> (0x02), so this sentinel safely
    /// sorts before every encoded non-null value.
    /// </summary>
    public static readonly IndexKey NullSentinelNext = FromOwnedArray(new byte[] { 0x01 });

    public IndexKey(ReadOnlySpan<byte> data)
    {
        var buf = new byte[data.Length + 1];
        buf[0] = KeyPrefix;
        data.CopyTo(buf.AsSpan(1));
        _data = buf;
        _hashCode = ComputeHashCode(_data);
    }

    public IndexKey(ObjectId objectId)
    {
        _data = new byte[13];
        _data[0] = KeyPrefix;
        objectId.WriteTo(_data.AsSpan(1));
        _hashCode = ComputeHashCode(_data);
    }

    public IndexKey(int value)
    {
        // Big-endian + sign-bit flip: lexicographic byte order == numeric order (including negatives).
        uint u = (uint)value ^ 0x8000_0000u;
        _data = [KeyPrefix, (byte)(u >> 24), (byte)(u >> 16), (byte)(u >> 8), (byte)u];
        _hashCode = ComputeHashCode(_data);
    }

    public IndexKey(long value)
    {
        // Big-endian + sign-bit flip: lexicographic byte order == numeric order (including negatives).
        ulong u = (ulong)value ^ 0x8000_0000_0000_0000UL;
        _data = [KeyPrefix, (byte)(u >> 56), (byte)(u >> 48), (byte)(u >> 40), (byte)(u >> 32),
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
        _data = [KeyPrefix, (byte)(u >> 56), (byte)(u >> 48), (byte)(u >> 40), (byte)(u >> 32),
                 (byte)(u >> 24), (byte)(u >> 16), (byte)(u >>  8), (byte)u];
        _hashCode = ComputeHashCode(_data);
    }

    /// <summary>
    /// Order-preserving, fixed-length encoding of a <see cref="decimal"/> (exact, no double round-trip).
    /// Layout after <c>DecimalKeyPrefix</c>: <c>[class][exponent][15 bytes of packed BCD digits]</c> where
    /// value = 0.D1D2…Dn × 10^exponent and trailing zeros are stripped, so <c>5</c> and <c>5.00</c> yield the
    /// same key. Negative values store every byte after the prefix inverted so that byte order == numeric order.
    /// </summary>
    public IndexKey(decimal value)
    {
        var data = new byte[DecimalKeyLength];
        data[0] = DecimalKeyPrefix;
        EncodeDecimal(value, data);
        _data = data;
        _hashCode = ComputeHashCode(_data);
    }

    public IndexKey(string value)
    {
        var utf8 = System.Text.Encoding.UTF8.GetBytes(value);
        var buf = new byte[utf8.Length + 1];
        buf[0] = KeyPrefix;
        utf8.CopyTo(buf, 1);
        _data = buf;
        _hashCode = ComputeHashCode(_data);
    }

    public IndexKey(Guid value)
    {
        _data = new byte[17];
        _data[0] = KeyPrefix;
        value.ToByteArray().CopyTo(_data, 1);
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
        if (typeof(T) == typeof(decimal)) return new IndexKey((decimal)(object)value);
        if (typeof(T) == typeof(string)) return new IndexKey((string)(object)value);
        if (typeof(T) == typeof(Guid)) return new IndexKey((Guid)(object)value);
        if (typeof(T) == typeof(byte[])) return new IndexKey((byte[])(object)value);

        throw new NotSupportedException($"Type {typeof(T).Name} is not supported as an IndexKey. Provide a custom mapping.");
    }

    public readonly T As<T>()
    {
        if (_data == null) return default!;

        // Skip the KeyPrefix discriminator byte (index 0) that was prepended by the constructors.
        // Sentinel keys (NullSentinel, NullSentinelNext, MinKey, MaxKey) are never decoded via As<T>.
        var d = _data.AsSpan(1);

        if (typeof(T) == typeof(ObjectId)) return (T)(object)new ObjectId(d.ToArray());
        if (typeof(T) == typeof(int))
        {
            uint u = ((uint)d[0] << 24) | ((uint)d[1] << 16) | ((uint)d[2] << 8) | d[3];
            return (T)(object)(int)(u ^ 0x8000_0000u);
        }
        if (typeof(T) == typeof(long))
        {
            ulong u = ((ulong)d[0] << 56) | ((ulong)d[1] << 48) | ((ulong)d[2] << 40) | ((ulong)d[3] << 32)
                    | ((ulong)d[4] << 24) | ((ulong)d[5] << 16) | ((ulong)d[6] <<  8) | d[7];
            return (T)(object)(long)(u ^ 0x8000_0000_0000_0000UL);
        }
        if (typeof(T) == typeof(double))
        {
            ulong u = ((ulong)d[0] << 56) | ((ulong)d[1] << 48) | ((ulong)d[2] << 40) | ((ulong)d[3] << 32)
                    | ((ulong)d[4] << 24) | ((ulong)d[5] << 16) | ((ulong)d[6] <<  8) | d[7];
            // Reverse encoding: if MSB was set (positive double), clear it; else flip all bits (negative double)
            long bits = u >= 0x8000_0000_0000_0000UL
                ? (long)(u ^ 0x8000_0000_0000_0000UL)
                : (long)~u;
            return (T)(object)BitConverter.Int64BitsToDouble(bits);
        }
        if (typeof(T) == typeof(decimal)) return (T)(object)DecodeDecimal(d);
        if (typeof(T) == typeof(string)) return (T)(object)System.Text.Encoding.UTF8.GetString(d);
        if (typeof(T) == typeof(Guid)) return (T)(object)new Guid(d.ToArray());
        if (typeof(T) == typeof(byte[])) return (T)(object)d.ToArray();

        throw new NotSupportedException($"Type {typeof(T).Name} cannot be extracted from IndexKey. Provide a custom mapping.");
    }

    private static void EncodeDecimal(decimal value, byte[] data)
    {
        var bits = decimal.GetBits(value);
        uint lo = (uint)bits[0], mid = (uint)bits[1], hi = (uint)bits[2];
        bool negative = bits[3] < 0;
        int scale = (bits[3] >> 16) & 0xFF;

        if ((lo | mid | hi) == 0)
        {
            data[1] = 0x01; // zero (every zero, whatever its scale or sign, maps to the same key)
            return;
        }

        // Decimal digits, least significant first, by repeated division of the 96-bit mantissa by 10.
        Span<byte> digits = stackalloc byte[29];
        int count = 0;
        while ((lo | mid | hi) != 0)
        {
            ulong cur = hi;
            hi = (uint)(cur / 10);
            cur = ((cur % 10) << 32) | mid;
            mid = (uint)(cur / 10);
            cur = ((cur % 10) << 32) | lo;
            lo = (uint)(cur / 10);
            digits[count++] = (byte)(cur % 10);
        }

        int start = 0; // skip trailing zeros (least significant side) so 5 and 5.00 get the same key
        while (digits[start] == 0) start++;

        int exponent = count - scale; // value = 0.D1…Dn × 10^exponent, in [-27, 29]
        for (int i = 0; i < count - start; i++)
        {
            byte digit = digits[count - 1 - i];
            int pos = 3 + (i >> 1);
            data[pos] |= (i & 1) == 0 ? (byte)(digit << 4) : digit;
        }

        data[1] = negative ? (byte)0x00 : (byte)0x02;
        data[2] = (byte)(exponent + 64);
        if (negative)
            for (int i = 2; i < data.Length; i++)
                data[i] = (byte)~data[i];
    }

    private static decimal DecodeDecimal(ReadOnlySpan<byte> d)
    {
        // d = [class][exponent][15 BCD bytes] (the KeyPrefix/DecimalKeyPrefix byte is already skipped)
        if (d[0] == 0x01) return 0m;
        bool negative = d[0] == 0x00;
        int exponent = (negative ? (byte)~d[1] : d[1]) - 64;

        int n = 0;
        for (int i = 0; i < 30; i++)
        {
            byte b = negative ? (byte)~d[2 + (i >> 1)] : d[2 + (i >> 1)];
            if (((i & 1) == 0 ? b >> 4 : b & 0x0F) != 0) n = i + 1;
        }

        decimal mantissa = 0m;
        for (int i = 0; i < n; i++)
        {
            byte b = negative ? (byte)~d[2 + (i >> 1)] : d[2 + (i >> 1)];
            mantissa = mantissa * 10 + ((i & 1) == 0 ? b >> 4 : b & 0x0F);
        }

        int newScale = n - exponent;
        decimal result;
        if (newScale >= 0)
        {
            var mb = decimal.GetBits(mantissa);
            result = new decimal(mb[0], mb[1], mb[2], false, (byte)newScale);
        }
        else
        {
            result = mantissa;
            for (int i = 0; i < -newScale; i++) result *= 10;
        }
        return negative ? -result : result;
    }
}
