using System;
using BLite.Bson;

namespace BLite.Core.Query.Blql;

/// <summary>
/// Provides total ordering for <see cref="BsonValue"/> instances,
/// following BSON comparison semantics used by BLQL predicates.
/// 
/// Order across types (ascending): Null &lt; Boolean &lt; (Int32 | Int64 | Double | Decimal) &lt; String &lt; ObjectId &lt; DateTime
/// Within the same numerical family, values are compared numerically.
/// </summary>
internal static class BsonValueComparer
{
    /// <summary>
    /// Returns negative, zero or positive indicating the ordering of <paramref name="a"/> vs <paramref name="b"/>.
    /// </summary>
    public static int Compare(BsonValue a, BsonValue b)
    {
        // Handle null
        if (a.IsNull && b.IsNull) return 0;
        if (a.IsNull) return -1;
        if (b.IsNull) return 1;

        // Numeric family: Int32, Int64, Double, Decimal128 — promote to double for mixed comparisons
        if (IsNumeric(a.Type) && IsNumeric(b.Type))
            return ToDouble(a).CompareTo(ToDouble(b));

        // Same type comparisons
        if (a.Type == b.Type)
        {
            return a.Type switch
            {
                BsonType.String    => string.Compare(a.AsString, b.AsString, StringComparison.Ordinal),
                BsonType.Boolean   => a.AsBoolean.CompareTo(b.AsBoolean),
                BsonType.DateTime  => a.AsDateTimeOffset.CompareTo(b.AsDateTimeOffset),
                BsonType.ObjectId  => CompareObjectIds(a.AsObjectId, b.AsObjectId),
                BsonType.Binary    => CompareBytes(a.AsBinary, b.AsBinary),
                _                  => 0
            };
        }

        // Cross-type: fall back to type-order
        return TypeOrder(a.Type).CompareTo(TypeOrder(b.Type));
    }

    private static bool IsNumeric(BsonType t) =>
        t is BsonType.Int32 or BsonType.Int64 or BsonType.Double or BsonType.Decimal128;

    private static double ToDouble(BsonValue v) => v.Type switch
    {
        BsonType.Int32     => v.AsInt32,
        BsonType.Int64     => (double)v.AsInt64,
        BsonType.Double    => v.AsDouble,
        BsonType.Decimal128 => (double)v.AsDecimal,
        _ => 0
    };

    private static int TypeOrder(BsonType t) => t switch
    {
        BsonType.Null      => 0,
        BsonType.Boolean   => 1,
        BsonType.Int32     => 2,
        BsonType.Int64     => 2,
        BsonType.Double    => 2,
        BsonType.Decimal128 => 2,
        BsonType.String    => 3,
        BsonType.ObjectId  => 4,
        BsonType.DateTime  => 5,
        BsonType.Binary    => 6,
        BsonType.Document  => 7,
        BsonType.Array     => 8,
        _                  => 99
    };

    private static int CompareObjectIds(ObjectId a, ObjectId b)
    {
        Span<byte> ba = stackalloc byte[12];
        Span<byte> bb = stackalloc byte[12];
        a.WriteTo(ba);
        b.WriteTo(bb);
        return ba.SequenceCompareTo(bb);
    }

    private static int CompareBytes(byte[] a, byte[] b)
    {
        var len = Math.Min(a.Length, b.Length);
        for (int i = 0; i < len; i++)
        {
            int c = a[i].CompareTo(b[i]);
            if (c != 0) return c;
        }
        return a.Length.CompareTo(b.Length);
    }
}
