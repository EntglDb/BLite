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
        if (entity.Bio != null)
            writer.WriteString("Bio", entity.Bio);
        else
            writer.WriteNull("Bio");

        writer.WriteInt64("CreatedAt", entity.CreatedAt.Ticks);
        
        // Complex fields
        writer.WriteDouble("Balance", (double)entity.Balance);
        
        // Nested Object: Address
        var addrPos = writer.BeginDocument("HomeAddress");
        writer.WriteString("Street", entity.HomeAddress.Street);
        writer.WriteString("City", entity.HomeAddress.City);
        writer.WriteString("ZipCode", entity.HomeAddress.ZipCode);
        writer.EndDocument(addrPos);
        
        // Collection: EmploymentHistory
        var histPos = writer.BeginArray("EmploymentHistory");
        for (int i = 0; i < entity.EmploymentHistory.Count; i++)
        {
            var item = entity.EmploymentHistory[i];
            // Array elements are keys "0", "1", "2"...
            var itemPos = writer.BeginDocument(i.ToString());
            
            writer.WriteString("CompanyName", item.CompanyName);
            writer.WriteString("Title", item.Title);
            writer.WriteInt32("DurationYears", item.DurationYears);
            
            // Nested Collection: Tags
            var tagsPos = writer.BeginArray("Tags");
            for (int j = 0; j < item.Tags.Count; j++)
            {
                writer.WriteString(j.ToString(), item.Tags[j]);
            }
            writer.EndArray(tagsPos);
            
            writer.EndDocument(itemPos);
        }
        writer.EndArray(histPos);
        
        writer.EndDocument(sizePos);
        
        return writer.Position;
    }

    public void Serialize(Person entity, IBufferWriter<byte> writer)
    {
        // Re-using Span logic via ArrayBufferWriter would be cleaner but for benchmark strictness we duplicate logic
        // Or simplified: use BsonBufferWriter which mirrors BsonSpanWriter
        var bson = new BsonBufferWriter(writer);
        var sizePos = bson.BeginDocument();
        
        bson.WriteObjectId("_id", entity.Id);
        bson.WriteString("FirstName", entity.FirstName);
        bson.WriteString("LastName", entity.LastName);
        bson.WriteInt32("Age", entity.Age);
        
        if (entity.Bio != null)
            bson.WriteString("Bio", entity.Bio);
        else
            bson.WriteNull("Bio");
            
        bson.WriteInt64("CreatedAt", entity.CreatedAt.Ticks);
        
        bson.WriteDouble("Balance", (double)entity.Balance);
        
        var addrPos = bson.BeginDocument("HomeAddress");
        bson.WriteString("Street", entity.HomeAddress.Street);
        bson.WriteString("City", entity.HomeAddress.City);
        bson.WriteString("ZipCode", entity.HomeAddress.ZipCode);
        bson.EndDocument(addrPos);
        
        var histPos = bson.BeginArray("EmploymentHistory");
        for (int i = 0; i < entity.EmploymentHistory.Count; i++)
        {
            var item = entity.EmploymentHistory[i];
            var itemPos = bson.BeginDocument(i.ToString());
            
            bson.WriteString("CompanyName", item.CompanyName);
            bson.WriteString("Title", item.Title);
            bson.WriteInt32("DurationYears", item.DurationYears);
            
            var tagsPos = bson.BeginArray("Tags");
            for (int j = 0; j < item.Tags.Count; j++)
            {
                bson.WriteString(j.ToString(), item.Tags[j]);
            }
            bson.EndArray(tagsPos);
            
            bson.EndDocument(itemPos);
        }
        bson.EndArray(histPos);
        
        bson.EndDocument(sizePos);
        
        // Patch size if possible
        if (writer is ArrayBufferWriter<byte> arrayWriter)
        {
            var readOnlySpan = arrayWriter.WrittenSpan;
            var span = MemoryMarshal.CreateSpan(
                ref MemoryMarshal.GetReference(readOnlySpan), 
                readOnlySpan.Length
            );
            
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(
                span.Slice(sizePos, 4), 
                bson.Position
            );
        }
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
            
            switch (name)
            {
                case "_id": person.Id = reader.ReadObjectId(); break;
                case "FirstName": person.FirstName = reader.ReadString(); break;
                case "LastName": person.LastName = reader.ReadString(); break;
                case "Age": person.Age = reader.ReadInt32(); break;
                case "Bio": 
                    if (type == BsonType.Null) person.Bio = null;
                    else person.Bio = reader.ReadString(); 
                    break;
                case "CreatedAt": person.CreatedAt = new DateTime(reader.ReadInt64()); break;
                case "Balance": person.Balance = (decimal)reader.ReadDouble(); break;
                
                case "HomeAddress":
                    reader.ReadDocumentSize(); // Enter document
                    while (reader.Remaining > 0)
                    {
                        var addrType = reader.ReadBsonType();
                        if (addrType == BsonType.EndOfDocument) break;
                        var addrName = reader.ReadCString();
                        
                        // We assume strict schema for benchmark speed, but should handle skipping
                        if (addrName == "Street") person.HomeAddress.Street = reader.ReadString();
                        else if (addrName == "City") person.HomeAddress.City = reader.ReadString();
                        else if (addrName == "ZipCode") person.HomeAddress.ZipCode = reader.ReadString();
                        else reader.SkipValue(addrType);
                    }
                    break;
                    
                case "EmploymentHistory":
                    reader.ReadDocumentSize(); // Enter Array
                    while (reader.Remaining > 0)
                    {
                        var arrType = reader.ReadBsonType();
                        if (arrType == BsonType.EndOfDocument) break;
                        reader.ReadCString(); // Array index "0", "1"... ignore
                        
                        // Read WorkHistory item
                        var workItem = new WorkHistory();
                        reader.ReadDocumentSize(); // Enter Item Document
                        while (reader.Remaining > 0)
                        {
                            var itemType = reader.ReadBsonType();
                            if (itemType == BsonType.EndOfDocument) break;
                            var itemName = reader.ReadCString();
                            
                            if (itemName == "CompanyName") workItem.CompanyName = reader.ReadString();
                            else if (itemName == "Title") workItem.Title = reader.ReadString();
                            else if (itemName == "DurationYears") workItem.DurationYears = reader.ReadInt32();
                            else if (itemName == "Tags")
                            {
                                reader.ReadDocumentSize(); // Enter Tags Array
                                while (reader.Remaining > 0)
                                {
                                    var tagType = reader.ReadBsonType();
                                    if (tagType == BsonType.EndOfDocument) break;
                                    reader.ReadCString(); // Index
                                    if (tagType == BsonType.String)
                                        workItem.Tags.Add(reader.ReadString());
                                    else
                                        reader.SkipValue(tagType);
                                }
                            }
                            else reader.SkipValue(itemType);
                        }
                        person.EmploymentHistory.Add(workItem);
                    }
                    break;
                    
                default:
                    reader.SkipValue(type);
                    break;
            }
        }
        
        return person;
    }
}
