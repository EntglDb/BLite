using BLite.Bson;
using BLite.Core.Indexing;

namespace BLite.Tests;

/// <summary>
/// Unit tests for <see cref="IndexKey"/>.
/// Covers constructor variants, As&lt;T&gt; round-trips, Create&lt;T&gt; factory,
/// lexicographic ordering, equality/comparison operators, and MinKey/MaxKey bounds.
/// </summary>
public class IndexKeyTests
{
    #region Constructor + As<T> round-trips

    [Fact]
    public void Int_RoundTrips()
    {
        var key = new IndexKey(42);
        Assert.Equal(42, key.As<int>());
    }

    [Fact]
    public void Int_Negative_RoundTrips()
    {
        Assert.Equal(-5, new IndexKey(-5).As<int>());
    }

    [Fact]
    public void Int_MinValue_RoundTrips()
    {
        Assert.Equal(int.MinValue, new IndexKey(int.MinValue).As<int>());
    }

    [Fact]
    public void Int_MaxValue_RoundTrips()
    {
        Assert.Equal(int.MaxValue, new IndexKey(int.MaxValue).As<int>());
    }

    [Fact]
    public void Int_Zero_RoundTrips()
    {
        Assert.Equal(0, new IndexKey(0).As<int>());
    }

    [Fact]
    public void Long_RoundTrips()
    {
        Assert.Equal(long.MaxValue, new IndexKey(long.MaxValue).As<long>());
    }

    [Fact]
    public void Long_Negative_RoundTrips()
    {
        Assert.Equal(-1000L, new IndexKey(-1000L).As<long>());
    }

    [Fact]
    public void Long_MinValue_RoundTrips()
    {
        Assert.Equal(long.MinValue, new IndexKey(long.MinValue).As<long>());
    }

    [Fact]
    public void Double_RoundTrips()
    {
        Assert.Equal(3.14, new IndexKey(3.14).As<double>());
    }

    [Fact]
    public void Double_Negative_RoundTrips()
    {
        Assert.Equal(-2.5, new IndexKey(-2.5).As<double>());
    }

    [Fact]
    public void Double_Zero_RoundTrips()
    {
        Assert.Equal(0.0, new IndexKey(0.0).As<double>());
    }

    [Fact]
    public void Double_MinValue_RoundTrips()
    {
        var val = double.MinValue;
        Assert.Equal(val, new IndexKey(val).As<double>());
    }

    [Fact]
    public void String_RoundTrips()
    {
        Assert.Equal("hello", new IndexKey("hello").As<string>());
    }

    [Fact]
    public void String_Empty_RoundTrips()
    {
        Assert.Equal("", new IndexKey("").As<string>());
    }

    [Fact]
    public void String_Unicode_RoundTrips()
    {
        const string unicode = "こんにちは";
        Assert.Equal(unicode, new IndexKey(unicode).As<string>());
    }

    [Fact]
    public void Guid_RoundTrips()
    {
        var guid = Guid.NewGuid();
        Assert.Equal(guid, new IndexKey(guid).As<Guid>());
    }

    [Fact]
    public void ObjectId_RoundTrips()
    {
        var oid = ObjectId.NewObjectId();
        Assert.Equal(oid, new IndexKey(oid).As<ObjectId>());
    }

    [Fact]
    public void Bytes_RoundTrips()
    {
        var bytes = new byte[] { 0xAB, 0xCD, 0xEF };
        var key = new IndexKey((ReadOnlySpan<byte>)bytes);
        Assert.Equal(bytes, key.As<byte[]>());
    }

    [Fact]
    public void Data_ReturnsCorrectLength_ForInt()
    {
        var key = new IndexKey(0);
        Assert.Equal(5, key.Data.Length); // prefix(1) + int(4) = 5 bytes
    }

    [Fact]
    public void Data_ReturnsCorrectLength_ForLong()
    {
        var key = new IndexKey(0L);
        Assert.Equal(9, key.Data.Length); // prefix(1) + long(8) = 9 bytes
    }

    #endregion

    #region Create<T> generic factory

    [Fact]
    public void Create_Int_ReturnsCorrectKey()
    {
        Assert.Equal(10, IndexKey.Create(10).As<int>());
    }

    [Fact]
    public void Create_Long_ReturnsCorrectKey()
    {
        Assert.Equal(999L, IndexKey.Create(999L).As<long>());
    }

    [Fact]
    public void Create_Double_ReturnsCorrectKey()
    {
        Assert.Equal(1.5, IndexKey.Create(1.5).As<double>());
    }

    [Fact]
    public void Create_String_ReturnsCorrectKey()
    {
        Assert.Equal("test", IndexKey.Create("test").As<string>());
    }

    [Fact]
    public void Create_Guid_ReturnsCorrectKey()
    {
        var guid = Guid.NewGuid();
        Assert.Equal(guid, IndexKey.Create(guid).As<Guid>());
    }

    [Fact]
    public void Create_ObjectId_ReturnsCorrectKey()
    {
        var oid = ObjectId.NewObjectId();
        Assert.Equal(oid, IndexKey.Create(oid).As<ObjectId>());
    }

    [Fact]
    public void Create_NullString_ReturnsDefault()
    {
        var key = IndexKey.Create<string>(null!);
        Assert.Equal(default, key);
    }

    [Fact]
    public void Create_UnsupportedType_ThrowsNotSupportedException()
    {
        Assert.Throws<NotSupportedException>(() => IndexKey.Create(DateTime.UtcNow));
    }

    [Fact]
    public void As_UnsupportedType_ThrowsNotSupportedException()
    {
        var key = new IndexKey(42);
        Assert.Throws<NotSupportedException>(() => key.As<DateTime>());
    }

    #endregion

    #region Lexicographic ordering

    [Fact]
    public void Int_NegativeSortBeforeZeroBeforePositive()
    {
        var neg = new IndexKey(-1);
        var zero = new IndexKey(0);
        var pos = new IndexKey(1);

        Assert.True(neg.CompareTo(zero) < 0);
        Assert.True(zero.CompareTo(pos) < 0);
        Assert.True(neg.CompareTo(pos) < 0);
    }

    [Fact]
    public void Int_SmallerSortBeforeLarger()
    {
        var a = new IndexKey(10);
        var b = new IndexKey(20);

        Assert.True(a.CompareTo(b) < 0);
        Assert.True(b.CompareTo(a) > 0);
        Assert.Equal(0, a.CompareTo(a));
    }

    [Fact]
    public void Int_MinValueBefore_MaxValue()
    {
        var min = new IndexKey(int.MinValue);
        var max = new IndexKey(int.MaxValue);
        Assert.True(min.CompareTo(max) < 0);
    }

    [Fact]
    public void Long_NegativeSortBeforePositive()
    {
        var neg = new IndexKey(-100L);
        var pos = new IndexKey(100L);
        Assert.True(neg.CompareTo(pos) < 0);
    }

    [Fact]
    public void Double_NegativeSortBeforePositive()
    {
        var neg = new IndexKey(-1.0);
        var pos = new IndexKey(1.0);
        Assert.True(neg.CompareTo(pos) < 0);
    }

    [Fact]
    public void String_LexicographicOrdering()
    {
        var a = new IndexKey("apple");
        var b = new IndexKey("banana");
        Assert.True(a.CompareTo(b) < 0);
        Assert.True(b.CompareTo(a) > 0);
    }

    [Fact]
    public void String_SameString_ZeroCompareTo()
    {
        var a = new IndexKey("same");
        var b = new IndexKey("same");
        Assert.Equal(0, a.CompareTo(b));
    }

    [Fact]
    public void String_EmptyStringSortBeforeNonEmpty()
    {
        var empty = new IndexKey("");
        var nonEmpty = new IndexKey("a");
        Assert.True(empty.CompareTo(nonEmpty) < 0);
    }

    [Fact]
    public void MinKey_IsLessThanSmallestInt()
    {
        var min = IndexKey.MinKey;
        var key = new IndexKey(int.MinValue);
        // MinKey has 0 bytes; real keys start with prefix 0x02 → MinKey < any real key
        Assert.True(min.CompareTo(key) < 0);
    }

    [Fact]
    public void MaxKey_IsGreaterThanLargestInt()
    {
        var max = IndexKey.MaxKey;
        var key = new IndexKey(int.MaxValue);
        // MaxKey starts with 0xFF; real keys start with the 0x02 prefix → MaxKey > key
        Assert.True(max.CompareTo(key) > 0);
    }

    [Fact]
    public void NullSentinel_IsLessThan_AllRealKeys()
    {
        // NullSentinel = {0x00}; all real keys are prefixed with 0x02, so null < any real key.
        var sentinel = IndexKey.NullSentinel;
        Assert.True(sentinel.CompareTo(new IndexKey(int.MinValue)) < 0);
        Assert.True(sentinel.CompareTo(new IndexKey(int.MaxValue)) < 0);
        Assert.True(sentinel.CompareTo(new IndexKey(long.MinValue)) < 0);
        Assert.True(sentinel.CompareTo(new IndexKey(0.0)) < 0);
        Assert.True(sentinel.CompareTo(new IndexKey("")) < 0);
    }

    [Fact]
    public void NullSentinelNext_IsLessThan_AllRealKeys()
    {
        // NullSentinelNext = {0x01}; all real keys start with 0x02, so sentinel < any real key.
        // This guarantees that using NullSentinelNext as a lower bound excludes only null entries
        // and never incorrectly skips negative numbers or other values encoding to 0x00/0x01 bytes.
        var sentinel = IndexKey.NullSentinelNext;
        Assert.True(sentinel.CompareTo(new IndexKey(int.MinValue)) < 0);
        Assert.True(sentinel.CompareTo(new IndexKey(int.MaxValue)) < 0);
        Assert.True(sentinel.CompareTo(new IndexKey(long.MinValue)) < 0);
        Assert.True(sentinel.CompareTo(new IndexKey(double.MinValue)) < 0);
        Assert.True(sentinel.CompareTo(new IndexKey("")) < 0);
    }

    #endregion

    #region Equality

    [Fact]
    public void Equal_SameInt_ReturnsTrue()
    {
        var a = new IndexKey(7);
        var b = new IndexKey(7);
        Assert.True(a == b);
        Assert.False(a != b);
        Assert.True(a.Equals(b));
    }

    [Fact]
    public void Equal_DifferentInt_ReturnsFalse()
    {
        Assert.False(new IndexKey(1) == new IndexKey(2));
        Assert.True(new IndexKey(1) != new IndexKey(2));
    }

    [Fact]
    public void Equal_SameString_ReturnsTrue()
    {
        Assert.True(new IndexKey("abc") == new IndexKey("abc"));
    }

    [Fact]
    public void Equals_Object_NonIndexKey_ReturnsFalse()
    {
        var key = new IndexKey(1);
        Assert.False(key.Equals("not-a-key"));
    }

    #endregion

    #region Comparison operators

    [Fact]
    public void LessThan_SmallerInt_IsCorrect()
    {
        var a = new IndexKey(1);
        var b = new IndexKey(2);
        Assert.True(a < b);
        Assert.True(a <= b);
        Assert.False(a > b);
        Assert.False(a >= b);
    }

    [Fact]
    public void GreaterThan_LargerInt_IsCorrect()
    {
        var a = new IndexKey(10);
        var b = new IndexKey(5);
        Assert.True(a > b);
        Assert.True(a >= b);
        Assert.False(a < b);
        Assert.False(a <= b);
    }

    [Fact]
    public void EqualInts_LessOrGreaterOrEqual_Correct()
    {
        var a = new IndexKey(5);
        var b = new IndexKey(5);
        Assert.True(a >= b);
        Assert.True(a <= b);
        Assert.False(a > b);
        Assert.False(a < b);
    }

    #endregion

    #region GetHashCode

    [Fact]
    public void GetHashCode_SameKeys_SameHashCode()
    {
        var a = new IndexKey(42);
        var b = new IndexKey(42);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void GetHashCode_DifferentKeys_DifferentHashCode()
    {
        // Not guaranteed but very likely for these specific values
        Assert.NotEqual(new IndexKey(1).GetHashCode(), new IndexKey(2).GetHashCode());
    }

    [Fact]
    public void GetHashCode_DefaultKey_DoesNotThrow()
    {
        var key = default(IndexKey);
        _ = key.GetHashCode(); // must not throw
    }

    #endregion

    #region Default/null _data edge cases

    [Fact]
    public void Default_CompareTo_Default_ReturnsZero()
    {
        var a = default(IndexKey);
        var b = default(IndexKey);
        Assert.Equal(0, a.CompareTo(b));
    }

    [Fact]
    public void Default_CompareTo_NonEmpty_ReturnsNegative()
    {
        var empty = default(IndexKey);
        var nonEmpty = new IndexKey(5);
        Assert.True(empty.CompareTo(nonEmpty) < 0);
    }

    [Fact]
    public void NonEmpty_CompareTo_Default_ReturnsPositive()
    {
        var nonEmpty = new IndexKey(5);
        var empty = default(IndexKey);
        Assert.True(nonEmpty.CompareTo(empty) > 0);
    }

    [Fact]
    public void Default_As_Int_ReturnsDefaultValue()
    {
        var key = default(IndexKey);
        // default has null _data, As<int> will return 0
        Assert.Equal(default(int), key.As<int>());
    }

    #endregion
}
