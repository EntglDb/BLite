using DocumentDb.Bson;
using DocumentDb.Core.Collections;
using System.Buffers;
using System.Runtime.InteropServices;

namespace DocumentDb.Benchmark;

public class PersonMapper : IDocumentMapper<Person>
{
    public string CollectionName => "people";

    public ObjectId GetId(Person entity) => entity.Id;

    public void SetId(Person entity, ObjectId id) => entity.Id = id;

    public int Serialize(Person entity, Span<byte> buffer)
    {
        var writer = new BsonSpanWriter(buffer);
        var sizePos = writer.BeginDocument();
        
        writer.WriteObjectId("_id", entity.Id);
        writer.WriteString("FirstName", entity.FirstName);
        writer.WriteString("LastName", entity.LastName);
        writer.WriteInt32("Age", entity.Age);
        writer.WriteString("Bio", entity.Bio);
        writer.WriteInt64("CreatedAt", entity.CreatedAt.Ticks);
        
        writer.EndDocument(sizePos);
        
        return writer.Position;
    }

    public void Serialize(Person entity, IBufferWriter<byte> writer)
    {
        var bson = new BsonBufferWriter(writer);
        var sizePos = bson.BeginDocument();
        
        bson.WriteObjectId("_id", entity.Id);
        bson.WriteString("FirstName", entity.FirstName);
        bson.WriteString("LastName", entity.LastName);
        bson.WriteInt32("Age", entity.Age);
        bson.WriteString("Bio", entity.Bio);
        bson.WriteInt64("CreatedAt", entity.CreatedAt.Ticks);
        
        bson.EndDocument(sizePos);
    }

    public Person Deserialize(ReadOnlySpan<byte> data)
    {
        var reader = new BsonSpanReader(data);
        var person = new Person();
        
        reader.ReadDocumentSize();
        
        while (reader.Remaining > 0)
        {
            var type = reader.ReadBsonType();
            if (type == BsonType.EndOfDocument)
                break;
                
            var name = reader.ReadCString();
            
            if (name == "_id")
                person.Id = reader.ReadObjectId();
            else if (name == "FirstName")
                person.FirstName = reader.ReadString();
            else if (name == "LastName")
                person.LastName = reader.ReadString();
            else if (name == "Age")
                person.Age = reader.ReadInt32();
            else if (name == "Bio")
                person.Bio = reader.ReadString();
            else if (name == "CreatedAt")
                person.CreatedAt = new DateTime(reader.ReadInt64());
            else
                reader.SkipValue(type);
        }
        
        return person;
    }
}
