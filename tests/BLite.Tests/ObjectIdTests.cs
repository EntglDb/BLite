using BLite.Bson;
using Xunit;

namespace BLite.Tests;

public class ObjectIdTests
{
    [Fact]
    public void NewObjectId_ShouldCreate12ByteId()
    {
        var oid = ObjectId.NewObjectId();
        
        Span<byte> bytes = stackalloc byte[12];
        oid.WriteTo(bytes);
        
        Assert.Equal(12, bytes.Length);
    }

    [Fact]
    public void ObjectId_ShouldRoundTrip()
    {
        var original = ObjectId.NewObjectId();
        
        Span<byte> bytes = stackalloc byte[12];
        original.WriteTo(bytes);
        
        var restored = new ObjectId(bytes);
        
        Assert.Equal(original, restored);
    }

    [Fact]
    public void ObjectId_Equals_ShouldWork()
    {
        var oid1 = ObjectId.NewObjectId();
        var oid2 = oid1;
        var oid3 = ObjectId.NewObjectId();
        
        Assert.Equal(oid1, oid2);
        Assert.NotEqual(oid1, oid3);
    }

    [Fact]
    public void ObjectId_Timestamp_ShouldBeRecentUtc()
    {
        var oid = ObjectId.NewObjectId();
        var timestamp = oid.Timestamp;
        
        Assert.True(timestamp <= DateTime.UtcNow);
        Assert.True(timestamp >= DateTime.UtcNow.AddSeconds(-5));
    }
}
