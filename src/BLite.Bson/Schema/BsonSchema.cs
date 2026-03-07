using System;
using System.Collections.Generic;

namespace BLite.Bson;

public partial class BsonSchema
{
    public string? Title { get; set; }
    public int? Version { get; set; }
    public List<BsonField> Fields { get; } = new();

    public void ToBson(ref BsonSpanWriter writer)
    {
        var size = writer.BeginDocument();
        if (Title != null) writer.WriteString("t", Title);
        if (Version != null) writer.WriteInt32("_v", Version.Value);
        
        var fieldsSize = writer.BeginArray("f");
        for (int i = 0; i < Fields.Count; i++)
        {
            writer.WriteArrayElementHeader(BsonType.Document, i);
            Fields[i].ToBson(ref writer);
        }
        writer.EndArray(fieldsSize);
        
        writer.EndDocument(size);
    }

    public static BsonSchema FromBson(ref BsonSpanReader reader)
    {
        reader.ReadInt32(); // Read doc size
        
        var schema = new BsonSchema();

        while (reader.Remaining > 1)
        {
            var btype = reader.ReadBsonType();
            if (btype == BsonType.EndOfDocument) break;
            
            var key = reader.ReadElementHeader();
            switch (key)
            {
                case "t": schema.Title = reader.ReadString(); break;
                case "_v": schema.Version = reader.ReadInt32(); break;
                case "f":
                    reader.ReadInt32(); // array size
                    while (reader.Remaining > 1)
                    {
                        var itemType = reader.ReadBsonType();
                        if (itemType == BsonType.EndOfDocument) break;
                        reader.SkipArrayKey(); // raw positional index
                        schema.Fields.Add(BsonField.FromBson(ref reader));
                    }
                    break;
                default: reader.SkipValue(btype); break;
            }
        }
        
        return schema;
    }

    public long GetHash()
    {
        var hash = new HashCode();
        hash.Add(Title);
        foreach (var field in Fields)
        {
            hash.Add(field.GetHash());
        }
        return hash.ToHashCode();
    }

    public bool Equals(BsonSchema? other)
    {
        if (other == null) return false;
        return GetHash() == other.GetHash();
    }

    public override bool Equals(object? obj) => Equals(obj as BsonSchema);
    public override int GetHashCode() => (int)GetHash();

    public IEnumerable<string> GetAllKeys()
    {
        foreach (var field in Fields)
        {
            yield return field.Name;
            if (field.NestedSchema != null)
            {
                foreach (var nestedKey in field.NestedSchema.GetAllKeys())
                {
                    yield return nestedKey;
                }
            }
        }
    }

    /// <summary>
    /// Calculates the exact number of bytes this schema will occupy when serialized
    /// via <see cref="ToBson"/>. Used to pre-allocate the correct buffer size.
    /// </summary>
    public int CalculateSize()
    {
        int size = 4; // BeginDocument (size placeholder)
        if (Title != null)
            size += 3 + 4 + System.Text.Encoding.UTF8.GetByteCount(Title) + 1; // WriteString("t", Title)
        if (Version != null)
            size += 7; // WriteInt32("_v", version): header(3) + int32(4)
        // BeginArray("f"): WriteElementHeader(Array,"f")(3) + size-placeholder(4) = 7
        size += 7;
        foreach (var field in Fields)
        {
            size += 3;                    // WriteArrayElementHeader: type(1) + ushort-index(2)
            size += field.CalculateSize();
        }
        size += 1; // EndArray  (end-of-array marker)
        size += 1; // EndDocument (end-of-document marker)
        return size;
    }
}
