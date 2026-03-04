using System;
using System.Collections.Generic;
using BLite.Bson;

namespace BLite.NetStandard21.Tests;

public class ObjectIdNetStandardTests
{
    [Fact]
    public void ObjectId_NewObjectId_IsUnique()
    {
        var ids = new HashSet<string>();
        for (int i = 0; i < 1000; i++)
        {
            var id = ObjectId.NewObjectId();
            Assert.True(ids.Add(id.ToString()), $"Duplicate ObjectId found at iteration {i}");
        }
    }

    [Fact]
    public void ObjectId_NewObjectId_HasCurrentTimestamp()
    {
        var before = DateTimeOffset.UtcNow.AddSeconds(-2);
        var id = ObjectId.NewObjectId();
        var after = DateTimeOffset.UtcNow.AddSeconds(2);

        var ts = id.Timestamp;
        Assert.True(ts >= before.UtcDateTime, $"Timestamp {ts} is before {before.UtcDateTime}");
        Assert.True(ts <= after.UtcDateTime, $"Timestamp {ts} is after {after.UtcDateTime}");
    }

    [Fact]
    public void ObjectId_ToString_ReturnsHexString()
    {
        var id = ObjectId.NewObjectId();
        var str = id.ToString();

        Assert.Equal(24, str.Length);
        Assert.Equal(str.ToLowerInvariant(), str);
        foreach (var c in str)
            Assert.Contains(c, "0123456789abcdef");
    }

    [Fact]
    public void ObjectId_RoundTrip_ToString_Parse()
    {
        var id = ObjectId.NewObjectId();
        var hex = id.ToString();

        Assert.Equal(24, hex.Length);

        // Verify we can reconstruct the same ObjectId from the hex bytes
        var bytes = new byte[12];
        for (int i = 0; i < 12; i++)
            bytes[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);

        var restored = new ObjectId(bytes);
        Assert.Equal(id, restored);
    }

    [Fact]
    public void ObjectId_KnownValue_ToString_MatchesExpected()
    {
        // Build ObjectId from known bytes: 0x00..0x0B
        var bytes = new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B };
        var id = new ObjectId(bytes);
        var str = id.ToString();

        Assert.Equal("000102030405060708090a0b", str);
    }
}
