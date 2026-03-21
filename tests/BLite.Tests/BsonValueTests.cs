using BLite.Bson;

namespace BLite.Tests;

/// <summary>
/// Unit tests for <see cref="BsonValue"/>.
/// Covers factory methods, type predicates, value accessors, cross-type widening,
/// error cases, implicit conversions, equality, and ToString.
/// </summary>
public class BsonValueTests
{
    #region Null / default

    [Fact]
    public void Null_IsNull_ReturnsTrue()
    {
        var v = BsonValue.Null;
        Assert.True(v.IsNull);
        Assert.Equal(BsonType.Null, v.Type);
    }

    [Fact]
    public void Default_IsNull_ReturnsTrue()
    {
        // default(BsonValue) has BsonType.EndOfDocument which also satisfies IsNull
        var v = default(BsonValue);
        Assert.True(v.IsNull);
    }

    #endregion

    #region FromInt32

    [Fact]
    public void FromInt32_SetsType_IsInt32_AsInt32()
    {
        var v = BsonValue.FromInt32(42);
        Assert.Equal(BsonType.Int32, v.Type);
        Assert.True(v.IsInt32);
        Assert.False(v.IsString);
        Assert.Equal(42, v.AsInt32);
    }

    [Fact]
    public void FromInt32_Zero_RoundTrips()
    {
        Assert.Equal(0, BsonValue.FromInt32(0).AsInt32);
    }

    [Fact]
    public void FromInt32_Negative_RoundTrips()
    {
        Assert.Equal(-99, BsonValue.FromInt32(-99).AsInt32);
    }

    [Fact]
    public void FromInt32_MinMax_RoundTrips()
    {
        Assert.Equal(int.MinValue, BsonValue.FromInt32(int.MinValue).AsInt32);
        Assert.Equal(int.MaxValue, BsonValue.FromInt32(int.MaxValue).AsInt32);
    }

    #endregion

    #region FromInt64

    [Fact]
    public void FromInt64_SetsType_IsInt64_AsInt64()
    {
        var v = BsonValue.FromInt64(long.MaxValue);
        Assert.Equal(BsonType.Int64, v.Type);
        Assert.True(v.IsInt64);
        Assert.Equal(long.MaxValue, v.AsInt64);
    }

    [Fact]
    public void FromInt64_Negative_RoundTrips()
    {
        Assert.Equal(-1L, BsonValue.FromInt64(-1L).AsInt64);
    }

    #endregion

    #region FromDouble

    [Fact]
    public void FromDouble_SetsType_IsDouble_AsDouble()
    {
        var v = BsonValue.FromDouble(3.14);
        Assert.Equal(BsonType.Double, v.Type);
        Assert.True(v.IsDouble);
        Assert.Equal(3.14, v.AsDouble);
    }

    [Fact]
    public void FromDouble_NegativeInfinity_RoundTrips()
    {
        Assert.Equal(double.NegativeInfinity, BsonValue.FromDouble(double.NegativeInfinity).AsDouble);
    }

    [Fact]
    public void FromDouble_NaN_RoundTrips()
    {
        Assert.True(double.IsNaN(BsonValue.FromDouble(double.NaN).AsDouble));
    }

    #endregion

    #region FromDecimal

    [Fact]
    public void FromDecimal_SetsType_IsDecimal_AsDecimal()
    {
        var v = BsonValue.FromDecimal(1.23m);
        Assert.Equal(BsonType.Decimal128, v.Type);
        Assert.True(v.IsDecimal);
        Assert.Equal(1.23m, v.AsDecimal);
    }

    [Fact]
    public void FromDecimal_MaxValue_RoundTrips()
    {
        Assert.Equal(decimal.MaxValue, BsonValue.FromDecimal(decimal.MaxValue).AsDecimal);
    }

    #endregion

    #region FromString

    [Fact]
    public void FromString_SetsType_IsString_AsString()
    {
        var v = BsonValue.FromString("hello");
        Assert.Equal(BsonType.String, v.Type);
        Assert.True(v.IsString);
        Assert.Equal("hello", v.AsString);
    }

    [Fact]
    public void FromString_EmptyString_RoundTrips()
    {
        Assert.Equal("", BsonValue.FromString("").AsString);
    }

    [Fact]
    public void FromString_Null_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => BsonValue.FromString(null!));
    }

    #endregion

    #region FromBoolean

    [Fact]
    public void FromBoolean_True_SetsType_IsBoolean_AsBoolean()
    {
        var v = BsonValue.FromBoolean(true);
        Assert.Equal(BsonType.Boolean, v.Type);
        Assert.True(v.IsBoolean);
        Assert.True(v.AsBoolean);
    }

    [Fact]
    public void FromBoolean_False_AsBoolean_ReturnsFalse()
    {
        var v = BsonValue.FromBoolean(false);
        Assert.True(v.IsBoolean);
        Assert.False(v.AsBoolean);
    }

    #endregion

    #region FromObjectId

    [Fact]
    public void FromObjectId_SetsType_IsObjectId_AsObjectId()
    {
        var oid = ObjectId.NewObjectId();
        var v = BsonValue.FromObjectId(oid);
        Assert.Equal(BsonType.ObjectId, v.Type);
        Assert.True(v.IsObjectId);
        Assert.Equal(oid, v.AsObjectId);
    }

    #endregion

    #region FromDateTime / FromDateTimeOffset

    [Fact]
    public void FromDateTime_SetsType_IsDateTime_AsDateTime()
    {
        var dt = new DateTime(2025, 3, 1, 12, 0, 0, DateTimeKind.Utc);
        var v = BsonValue.FromDateTime(dt);
        Assert.Equal(BsonType.DateTime, v.Type);
        Assert.True(v.IsDateTime);
        Assert.Equal(dt, v.AsDateTime, TimeSpan.FromMilliseconds(1));
    }

    [Fact]
    public void FromDateTimeOffset_RoundTrips_ThroughUnixMs()
    {
        var dto = new DateTimeOffset(2025, 6, 15, 8, 30, 0, TimeSpan.Zero);
        var v = BsonValue.FromDateTimeOffset(dto);
        Assert.Equal(BsonType.DateTime, v.Type);
        Assert.Equal(dto, v.AsDateTimeOffset);
    }

    #endregion

    #region FromGuid

    [Fact]
    public void FromGuid_StoredAsString_IsGuid_AsGuid()
    {
        var guid = Guid.NewGuid();
        var v = BsonValue.FromGuid(guid);
        Assert.Equal(BsonType.String, v.Type); // guid stored as string
        Assert.True(v.IsGuid);
        Assert.Equal(guid, v.AsGuid);
    }

    [Fact]
    public void FromString_ValidGuid_IsGuid_ReturnsTrue()
    {
        var guid = Guid.NewGuid();
        var v = BsonValue.FromString(guid.ToString());
        Assert.True(v.IsGuid);
    }

    [Fact]
    public void FromString_NonGuid_IsGuid_ReturnsFalse()
    {
        var v = BsonValue.FromString("not-a-guid");
        Assert.False(v.IsGuid);
    }

    #endregion

    #region FromBinary

    [Fact]
    public void FromBinary_SetsType_IsBinary_AsBinary()
    {
        var bytes = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var v = BsonValue.FromBinary(bytes);
        Assert.Equal(BsonType.Binary, v.Type);
        Assert.True(v.IsBinary);
        Assert.Equal(bytes, v.AsBinary);
    }

    [Fact]
    public void FromBinary_Null_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => BsonValue.FromBinary(null!));
    }

    #endregion

    #region FromArray

    [Fact]
    public void FromArray_SetsType_IsArray_AsArray()
    {
        var arr = new List<BsonValue> { BsonValue.FromInt32(1), BsonValue.FromInt32(2) };
        var v = BsonValue.FromArray(arr);
        Assert.Equal(BsonType.Array, v.Type);
        Assert.True(v.IsArray);
        Assert.Equal(2, v.AsArray.Count);
        Assert.Equal(1, v.AsArray[0].AsInt32);
        Assert.Equal(2, v.AsArray[1].AsInt32);
    }

    [Fact]
    public void FromArray_Null_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => BsonValue.FromArray(null!));
    }

    [Fact]
    public void FromArray_Empty_IsArray_CountZero()
    {
        var v = BsonValue.FromArray([]);
        Assert.True(v.IsArray);
        Assert.Empty(v.AsArray);
    }

    #endregion

    #region FromCoordinates

    [Fact]
    public void FromCoordinates_SetsType_IsCoordinates_AsCoordinates()
    {
        var v = BsonValue.FromCoordinates((48.85, 2.35));
        Assert.Equal(BsonType.Array, v.Type);
        Assert.True(v.IsCoordinates);
        var (lat, lon) = v.AsCoordinates;
        Assert.Equal(48.85, lat);
        Assert.Equal(2.35, lon);
    }

    [Fact]
    public void IsCoordinates_NonCoordinatesArray_ReturnsFalse()
    {
        var arr = new List<BsonValue> { BsonValue.FromInt32(1) }; // 1-item list
        var v = BsonValue.FromArray(arr);
        Assert.False(v.IsCoordinates);
    }

    #endregion

    #region Cross-type widening accessors

    [Fact]
    public void AsInt64_FromInt32_WidensToLong()
    {
        var v = BsonValue.FromInt32(42);
        Assert.Equal(42L, v.AsInt64);
    }

    [Fact]
    public void AsDouble_FromInt32_WidensToDouble()
    {
        var v = BsonValue.FromInt32(10);
        Assert.Equal(10.0, v.AsDouble);
    }

    [Fact]
    public void AsDouble_FromInt64_WidensToDouble()
    {
        var v = BsonValue.FromInt64(50L);
        Assert.Equal(50.0, v.AsDouble, 10);
    }

    #endregion

    #region Wrong-type accessor throws

    [Fact]
    public void AsInt32_WrongType_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => BsonValue.FromString("x").AsInt32);
    }

    [Fact]
    public void AsString_WrongType_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => BsonValue.FromInt32(1).AsString);
    }

    [Fact]
    public void AsBoolean_WrongType_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => BsonValue.FromInt32(1).AsBoolean);
    }

    [Fact]
    public void AsDouble_WrongType_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => BsonValue.FromString("x").AsDouble);
    }

    [Fact]
    public void AsInt64_WrongType_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => BsonValue.FromString("x").AsInt64);
    }

    [Fact]
    public void AsDecimal_WrongType_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => BsonValue.FromInt32(1).AsDecimal);
    }

    [Fact]
    public void AsBinary_WrongType_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => BsonValue.FromInt32(1).AsBinary);
    }

    [Fact]
    public void AsObjectId_WrongType_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => BsonValue.FromInt32(1).AsObjectId);
    }

    [Fact]
    public void AsArray_WrongType_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => BsonValue.FromString("not-array").AsArray);
    }

    [Fact]
    public void AsDateTime_WrongType_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => BsonValue.FromString("not-dt").AsDateTime);
    }

    [Fact]
    public void AsDateTimeOffset_WrongType_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => BsonValue.FromString("not-dt").AsDateTimeOffset);
    }

    [Fact]
    public void AsGuid_InvalidGuidString_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => BsonValue.FromString("not-a-guid").AsGuid);
    }

    [Fact]
    public void AsNull_IsNull_NoThrow()
    {
        var v = BsonValue.Null;
        Assert.True(v.IsNull);
    }

    [Fact]
    public void AsCoordinates_WrongType_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => BsonValue.FromString("x").AsCoordinates);
    }

    #endregion

    #region Implicit conversions

    [Fact]
    public void ImplicitConversion_Int_CreatesInt32()
    {
        BsonValue v = 99;
        Assert.Equal(BsonType.Int32, v.Type);
        Assert.Equal(99, v.AsInt32);
    }

    [Fact]
    public void ImplicitConversion_Long_CreatesInt64()
    {
        BsonValue v = 100L;
        Assert.Equal(BsonType.Int64, v.Type);
        Assert.Equal(100L, v.AsInt64);
    }

    [Fact]
    public void ImplicitConversion_Double_CreatesDouble()
    {
        BsonValue v = 2.71828;
        Assert.Equal(BsonType.Double, v.Type);
        Assert.Equal(2.71828, v.AsDouble);
    }

    [Fact]
    public void ImplicitConversion_String_CreatesString()
    {
        BsonValue v = "world";
        Assert.Equal(BsonType.String, v.Type);
        Assert.Equal("world", v.AsString);
    }

    [Fact]
    public void ImplicitConversion_Bool_CreatesBoolean()
    {
        BsonValue t = true;
        BsonValue f = false;
        Assert.True(t.AsBoolean);
        Assert.False(f.AsBoolean);
    }

    [Fact]
    public void ImplicitConversion_Decimal_CreatesDecimal128()
    {
        BsonValue v = 9.99m;
        Assert.Equal(BsonType.Decimal128, v.Type);
        Assert.Equal(9.99m, v.AsDecimal);
    }

    #endregion

    #region Equality

    [Fact]
    public void Equal_SameInt32_ReturnsTrue()
    {
        var a = BsonValue.FromInt32(5);
        var b = BsonValue.FromInt32(5);
        Assert.True(a == b);
        Assert.False(a != b);
        Assert.True(a.Equals(b));
    }

    [Fact]
    public void Equal_DifferentInt32_ReturnsFalse()
    {
        Assert.False(BsonValue.FromInt32(1) == BsonValue.FromInt32(2));
        Assert.True(BsonValue.FromInt32(1) != BsonValue.FromInt32(2));
    }

    [Fact]
    public void Equal_DifferentTypes_ReturnsFalse()
    {
        Assert.False(BsonValue.FromInt32(5) == BsonValue.FromString("5"));
    }

    [Fact]
    public void Equal_TwoNulls_ReturnsTrue()
    {
        Assert.True(BsonValue.Null == BsonValue.Null);
    }

    [Fact]
    public void Equal_NullAndNonNull_ReturnsFalse()
    {
        Assert.False(BsonValue.Null == BsonValue.FromString("x"));
    }

    [Fact]
    public void Equal_SameString_ReturnsTrue()
    {
        Assert.True(BsonValue.FromString("abc") == BsonValue.FromString("abc"));
    }

    [Fact]
    public void Equal_DifferentStrings_ReturnsFalse()
    {
        Assert.False(BsonValue.FromString("abc") == BsonValue.FromString("def"));
    }

    [Fact]
    public void Equal_SameBooleans_CorrectEquality()
    {
        Assert.True(BsonValue.FromBoolean(true) == BsonValue.FromBoolean(true));
        Assert.True(BsonValue.FromBoolean(false) == BsonValue.FromBoolean(false));
        Assert.False(BsonValue.FromBoolean(true) == BsonValue.FromBoolean(false));
    }

    [Fact]
    public void Equal_SameDouble_ReturnsTrue()
    {
        Assert.True(BsonValue.FromDouble(3.14) == BsonValue.FromDouble(3.14));
    }

    [Fact]
    public void Equals_Object_Null_ReturnsFalse()
    {
        var v = BsonValue.FromInt32(1);
        Assert.False(v.Equals((object?)null));
    }

    [Fact]
    public void GetHashCode_SameValues_SameHashCode()
    {
        var a = BsonValue.FromInt32(42);
        var b = BsonValue.FromInt32(42);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void GetHashCode_Null_DoesNotThrow()
    {
        var v = BsonValue.Null;
        _ = v.GetHashCode(); // must not throw
    }

    #endregion

    #region ToString

    [Fact]
    public void ToString_Int32_ReturnsNumberString()
    {
        Assert.Equal("42", BsonValue.FromInt32(42).ToString());
    }

    [Fact]
    public void ToString_Int64_ReturnsNumberString()
    {
        Assert.Equal("1000", BsonValue.FromInt64(1000L).ToString());
    }

    [Fact]
    public void ToString_String_ReturnsQuotedValue()
    {
        Assert.Equal("\"hello\"", BsonValue.FromString("hello").ToString());
    }

    [Fact]
    public void ToString_Boolean_ReturnsExpectedString()
    {
        var s = BsonValue.FromBoolean(true).ToString();
        Assert.NotEmpty(s);
    }

    [Fact]
    public void ToString_Null_ReturnsNullLiteral()
    {
        Assert.Equal("null", BsonValue.Null.ToString());
    }

    [Fact]
    public void ToString_Double_ReturnsNonEmpty()
    {
        var s = BsonValue.FromDouble(1.5).ToString();
        Assert.NotEmpty(s);
    }

    [Fact]
    public void ToString_ObjectId_ReturnsNonEmpty()
    {
        var oid = ObjectId.NewObjectId();
        var s = BsonValue.FromObjectId(oid).ToString();
        Assert.NotEmpty(s);
    }

    #endregion

    #region IsXxx type predicates

    [Fact]
    public void IsTimestamp_False_ForOtherTypes()
    {
        Assert.False(BsonValue.FromInt32(1).IsTimestamp);
        Assert.False(BsonValue.FromString("x").IsTimestamp);
        Assert.False(BsonValue.Null.IsTimestamp);
    }

    [Fact]
    public void IsDocument_False_ForPrimitives()
    {
        Assert.False(BsonValue.FromInt32(1).IsDocument);
        Assert.False(BsonValue.FromString("x").IsDocument);
    }

    [Fact]
    public void IsBinary_False_ForStrings()
    {
        Assert.False(BsonValue.FromString("not binary").IsBinary);
    }

    [Fact]
    public void IsObjectId_False_ForStrings()
    {
        Assert.False(BsonValue.FromString("not oid").IsObjectId);
    }

    #endregion
}
