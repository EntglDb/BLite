using System;
using System.Collections.Generic;

namespace BLite.Bson;

public partial class BsonField
{
    public required string Name { get; init; }
    public BsonType Type { get; init; }
    public bool IsNullable { get; init; }
    public BsonSchema? NestedSchema { get; init; }
    public BsonType? ArrayItemType { get; init; }

    public void ToBson(ref BsonSpanWriter writer)
    {
        var size = writer.BeginDocument();
        writer.WriteString("n", Name);
        writer.WriteInt32("t", (int)Type);
        writer.WriteBoolean("b", IsNullable);
        
        if (NestedSchema != null)
        {
            writer.WriteElementHeader(BsonType.Document, "s");
            NestedSchema.ToBson(ref writer);
        }
        
        if (ArrayItemType != null)
        {
            writer.WriteInt32("a", (int)ArrayItemType.Value);
        }
        
        writer.EndDocument(size);
    }

    public static BsonField FromBson(ref BsonSpanReader reader)
    {
        reader.ReadInt32(); // Read doc size
        
        string name = "";
        BsonType type = BsonType.Null;
        bool isNullable = false;
        BsonSchema? nestedSchema = null;
        BsonType? arrayItemType = null;

        while (reader.Remaining > 1)
        {
            var btype = reader.ReadBsonType();
            if (btype == BsonType.EndOfDocument) break;
            
            var key = reader.ReadElementHeader();
            switch (key)
            {
                case "n": name = reader.ReadString(); break;
                case "t": type = (BsonType)reader.ReadInt32(); break;
                case "b": isNullable = reader.ReadBoolean(); break;
                case "s": nestedSchema = BsonSchema.FromBson(ref reader); break;
                case "a": arrayItemType = (BsonType)reader.ReadInt32(); break;
                default: reader.SkipValue(btype); break;
            }
        }
        
        return new BsonField
        {
            Name = name,
            Type = type,
            IsNullable = isNullable,
            NestedSchema = nestedSchema,
            ArrayItemType = arrayItemType
        };
    }

    public long GetHash()
    {
        var hash = new HashCode();
        hash.Add(Name);
        hash.Add((int)Type);
        hash.Add(IsNullable);
        hash.Add(ArrayItemType);
        if (NestedSchema != null) hash.Add(NestedSchema.GetHash());
        return hash.ToHashCode();
    }

    /// <summary>
    /// Calculates the exact number of bytes this field will occupy when serialized
    /// via <see cref="ToBson"/>. Used to pre-allocate the correct buffer size.
    /// </summary>
    public int CalculateSize()
    {
        int nameBytes = System.Text.Encoding.UTF8.GetByteCount(Name);
        int size = 4;                       // BeginDocument (size placeholder)
        size += 3 + 4 + nameBytes + 1;     // WriteString("n", Name): header(3) + length-prefix(4) + bytes + null(1)
        size += 7;                          // WriteInt32("t", type): header(3) + int32(4)
        size += 4;                          // WriteBoolean("b", nullable): header(3) + bool(1)
        if (NestedSchema != null)
            size += 3 + NestedSchema.CalculateSize(); // WriteElementHeader("s")(3) + nested schema
        if (ArrayItemType != null)
            size += 7;                      // WriteInt32("a", arrayItemType): header(3) + int32(4)
        size += 1;                          // EndDocument (end-of-document marker)
        return size;
    }

    public bool Equals(BsonField? other)
    {
        if (other == null) return false;
        return GetHash() == other.GetHash();
    }

    public override bool Equals(object? obj) => Equals(obj as BsonField);
    public override int GetHashCode() => (int)GetHash();
}
