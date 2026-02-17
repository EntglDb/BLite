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
            writer.WriteElementHeader(BsonType.Document, i.ToString());
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
                        reader.ReadElementHeader(); // index
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
}
