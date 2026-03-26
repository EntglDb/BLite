using System;
using System.Runtime.InteropServices;

namespace BLite.Bson;

/// <summary>
/// 12-byte ObjectId compatible with MongoDB ObjectId.
/// Implemented as readonly struct for zero allocation.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 12)]
public readonly struct ObjectId : IEquatable<ObjectId>, IComparable<ObjectId>, IComparable
{
    [FieldOffset(0)] private readonly int _timestamp;
    [FieldOffset(4)] private readonly long _randomAndCounter;

    /// <summary>
    /// Empty ObjectId (all zeros)
    /// </summary>
    public static readonly ObjectId Empty = new ObjectId(0, 0);

    /// <summary>
    /// Maximum ObjectId (all 0xFF bytes) - useful for range queries
    /// </summary>
    public static readonly ObjectId MaxValue = new ObjectId(int.MaxValue, long.MaxValue);

    public ObjectId(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 12)
            throw new ArgumentException("ObjectId must be exactly 12 bytes", nameof(bytes));

        _timestamp = BitConverter.ToInt32(bytes[..4]);
        _randomAndCounter = BitConverter.ToInt64(bytes[4..12]);
    }

    public ObjectId(int timestamp, long randomAndCounter)
    {
        _timestamp = timestamp;
        _randomAndCounter = randomAndCounter;
    }

    /// <summary>
    /// Creates a new ObjectId with current timestamp
    /// </summary>
    public static ObjectId NewObjectId()
    {
        var timestamp = (int)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
#if NET6_0_OR_GREATER
        var random = Random.Shared.NextInt64();
#else
        var random = (long)((ulong)new Random().Next() << 32 | (ulong)(uint)new Random().Next());
#endif
        return new ObjectId(timestamp, random);
    }

    /// <summary>
    /// Writes the ObjectId to the destination span (must be 12 bytes)
    /// </summary>
    public void WriteTo(Span<byte> destination)
    {
        if (destination.Length < 12)
            throw new ArgumentException("Destination must be at least 12 bytes", nameof(destination));

        BitConverter.TryWriteBytes(destination[..4], _timestamp);
        BitConverter.TryWriteBytes(destination[4..12], _randomAndCounter);
    }

    /// <summary>
    /// Converts ObjectId to byte array
    /// </summary>
    public byte[] ToByteArray()
    {
        var bytes = new byte[12];
        WriteTo(bytes);
        return bytes;
    }

    /// <summary>
    /// Gets timestamp portion as UTC DateTime
    /// </summary>
    public DateTime Timestamp => DateTimeOffset.FromUnixTimeSeconds(_timestamp).UtcDateTime;

    public bool Equals(ObjectId other) =>
        _timestamp == other._timestamp && _randomAndCounter == other._randomAndCounter;

    public override bool Equals(object? obj) => obj is ObjectId other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(_timestamp, _randomAndCounter);

    public static bool operator ==(ObjectId left, ObjectId right) => left.Equals(right);
    public static bool operator !=(ObjectId left, ObjectId right) => !left.Equals(right);
    public static bool operator < (ObjectId left, ObjectId right) => left.CompareTo(right) < 0;
    public static bool operator > (ObjectId left, ObjectId right) => left.CompareTo(right) > 0;
    public static bool operator <=(ObjectId left, ObjectId right) => left.CompareTo(right) <= 0;
    public static bool operator >=(ObjectId left, ObjectId right) => left.CompareTo(right) >= 0;

    /// <summary>
    /// Compares two ObjectIds. Ordering matches MongoDB: timestamp (seconds) ascending,
    /// then the remaining 8 bytes (machine id + pid + counter) treated as an unsigned integer.
    /// </summary>
    public int CompareTo(ObjectId other)
    {
        // Compare timestamp (big-endian seconds) as unsigned 32-bit integer.
        int tsCompare = ((uint)_timestamp).CompareTo((uint)other._timestamp);
        if (tsCompare != 0) return tsCompare;
        // Compare remaining 8 bytes as unsigned 64-bit integer.
        return ((ulong)_randomAndCounter).CompareTo((ulong)other._randomAndCounter);
    }

    public int CompareTo(object? obj)
    {
        if (obj is null) return 1;
        if (obj is ObjectId other) return CompareTo(other);
        throw new ArgumentException($"Object must be of type {nameof(ObjectId)}.", nameof(obj));
    }

    public override string ToString()
    {
        Span<byte> bytes = stackalloc byte[12];
        WriteTo(bytes);
#if NET5_0_OR_GREATER
        return Convert.ToHexString(bytes).ToLowerInvariant();
#else
        return BitConverter.ToString(bytes.ToArray()).Replace("-", "").ToLowerInvariant();
#endif
    }
}
