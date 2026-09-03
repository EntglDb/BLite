namespace BLite.Bson;

/// <summary>
/// BSON type codes as defined in BSON spec
/// </summary>
public enum BsonType : byte
{
    EndOfDocument = 0x00,
    Double = 0x01,
    String = 0x02,
    Document = 0x03,
    Array = 0x04,
    Binary = 0x05,
    Undefined = 0x06, // Deprecated
    ObjectId = 0x07,
    Boolean = 0x08,
    DateTime = 0x09,
    Null = 0x0A,
    Regex = 0x0B,
    DBPointer = 0x0C, // Deprecated
    JavaScript = 0x0D,
    Symbol = 0x0E, // Deprecated
    JavaScriptWithScope = 0x0F,
    Int32 = 0x10,
    Timestamp = 0x11,
    Int64 = 0x12,
    Decimal128 = 0x13,

    /// <summary>
    /// BLite extension, not part of the BSON spec (0x14 is unassigned there). Distinct from
    /// <see cref="DateTime"/> because it carries a UTC-offset alongside the instant - see
    /// <see cref="BsonValue.FromDateTimeOffset"/> / <c>BsonSpanWriter.WriteDateTimeOffset</c>.
    /// A <see cref="DateTimeOffset"/> value written before this type existed is still tagged
    /// <see cref="DateTime"/> on disk (offset already lost at write time, not recoverable) -
    /// readers must keep handling that tag for <see cref="DateTimeOffset"/> fields too.
    /// </summary>
    DateTimeOffset = 0x14,

    MinKey = 0xFF,
    MaxKey = 0x7F
}
