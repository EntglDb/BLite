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

    public bool Equals(BsonField? other)
    {
        if (other == null) return false;
        return GetHash() == other.GetHash();
    }

    public override bool Equals(object? obj) => Equals(obj as BsonField);
    public override int GetHashCode() => (int)GetHash();
}
