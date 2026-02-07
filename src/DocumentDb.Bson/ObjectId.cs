using System.Runtime.InteropServices;

namespace DocumentDb.Bson;

/// <summary>
/// 12-byte ObjectId compatible with MongoDB ObjectId.
/// Implemented as readonly struct for zero allocation.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 12)]
public readonly struct ObjectId : IEquatable<ObjectId>
{
    [FieldOffset(0)] private readonly int _timestamp;
    [FieldOffset(4)] private readonly long _randomAndCounter;

    /// <summary>
    /// Empty ObjectId (all zeros)
    /// </summary>
    public static readonly ObjectId Empty = new ObjectId(0, 0);

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
        var random = Random.Shared.NextInt64();
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

    public override string ToString()
    {
        Span<byte> bytes = stackalloc byte[12];
        WriteTo(bytes);
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }
}
