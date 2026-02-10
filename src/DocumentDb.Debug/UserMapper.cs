using System;
using System.Buffers;
using DocumentDb.Bson;
using DocumentDb.Core.Collections;

namespace DocumentDb.Tests;

/// <summary>
/// Simple user entity for testing
/// </summary>
public class User
{
    public ObjectId Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
}

/// <summary>
/// Zero-allocation mapper for User entity
/// </summary>
public class UserMapper : ObjectIdMapperBase<User>
{
    public override string CollectionName => "users";

    public override int Serialize(User entity, Span<byte> buffer)
    {
        var writer = new BsonSpanWriter(buffer);
        
        // Begin document
        var sizePos = writer.BeginDocument();
        
        // Write fields
        writer.WriteObjectId("_id", entity.Id);
        writer.WriteString("name", entity.Name);
        writer.WriteInt32("age", entity.Age);
        
        // End document (patches size)
        writer.EndDocument(sizePos);
        
        return writer.Position;
    }

    public override User Deserialize(ReadOnlySpan<byte> buffer)
    {
        var reader = new BsonSpanReader(buffer);
        var user = new User();
        
        // Read document size
        reader.ReadDocumentSize();
        
        // Read fields
        while (reader.Position < buffer.Length)
        {
            var type = reader.ReadBsonType();
            if (type == BsonType.EndOfDocument)
                break;
            
            var name = reader.ReadCString();
            
            switch (name)
            {
                case "_id":
                    user.Id = reader.ReadObjectId();
                    break;
                case "name":
                    user.Name = reader.ReadString();
                    break;
                case "age":
                    user.Age = reader.ReadInt32();
                    break;
                default:
                    // Skip unknown fields
                    reader.SkipValue(type);
                    break;
            }
        }
        
        return user;
    }

    public override ObjectId GetId(User entity) => entity.Id;

    public override void SetId(User entity, ObjectId id) => entity.Id = id;
}
