using BLite.Bson;
using BLite.Core.Collections;
using System.Buffers;
using System.Runtime.InteropServices;

namespace BLite.Benchmark;

public class PersonMapper : ObjectIdMapperBase<Person>
{
    public override string CollectionName => "people";

    public override ObjectId GetId(Person entity) => entity.Id;

    public override void SetId(Person entity, ObjectId id) => entity.Id = id;

    public override int Serialize(Person entity, BsonSpanWriter writer)
    {
        var sizePos = writer.BeginDocument();
        
        writer.WriteObjectId("_id", entity.Id);
        writer.WriteString("firstname", entity.FirstName);
        writer.WriteString("lastname", entity.LastName);
        writer.WriteInt32("age", entity.Age);
        if (entity.Bio != null)
            writer.WriteString("bio", entity.Bio);
        else
            writer.WriteNull("bio");

        writer.WriteInt64("createdat", entity.CreatedAt.Ticks);
        
        // Complex fields
        writer.WriteDouble("balance", (double)entity.Balance);
        
        // Nested Object: Address
        var addrPos = writer.BeginDocument("homeaddress");
        writer.WriteString("street", entity.HomeAddress.Street);
        writer.WriteString("city", entity.HomeAddress.City);
        writer.WriteString("zipcode", entity.HomeAddress.ZipCode);
        writer.EndDocument(addrPos);
        
        // Collection: EmploymentHistory
        var histPos = writer.BeginArray("employmenthistory");
        for (int i = 0; i < entity.EmploymentHistory.Count; i++)
        {
            var item = entity.EmploymentHistory[i];
            // Array elements are keys "0", "1", "2"...
            var itemPos = writer.BeginDocument(i.ToString());
            
            writer.WriteString("companyname", item.CompanyName);
            writer.WriteString("title", item.Title);
            writer.WriteInt32("durationyears", item.DurationYears);
            
            // Nested Collection: Tags
            var tagsPos = writer.BeginArray("tags");
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

    public override Person Deserialize(BsonSpanReader reader)
    {
        var person = new Person();
        
        reader.ReadDocumentSize();
        
        while (reader.Remaining > 0)
        {
            var type = reader.ReadBsonType();
            if (type == BsonType.EndOfDocument)
                break;
                
            var name = reader.ReadElementHeader();
            
            switch (name)
            {
                case "_id": person.Id = reader.ReadObjectId(); break;
                case "firstname": person.FirstName = reader.ReadString(); break;
                case "lastname": person.LastName = reader.ReadString(); break;
                case "age": person.Age = reader.ReadInt32(); break;
                case "bio": 
                    if (type == BsonType.Null) person.Bio = null;
                    else person.Bio = reader.ReadString(); 
                    break;
                case "createdat": person.CreatedAt = new DateTime(reader.ReadInt64()); break;
                case "balance": person.Balance = (decimal)reader.ReadDouble(); break;
                
                case "homeaddress":
                    reader.ReadDocumentSize(); // Enter document
                    while (reader.Remaining > 0)
                    {
                        var addrType = reader.ReadBsonType();
                        if (addrType == BsonType.EndOfDocument) break;
                        var addrName = reader.ReadElementHeader();
                        
                        // We assume strict schema for benchmark speed, but should handle skipping
                        if (addrName == "street") person.HomeAddress.Street = reader.ReadString();
                        else if (addrName == "city") person.HomeAddress.City = reader.ReadString();
                        else if (addrName == "zipcode") person.HomeAddress.ZipCode = reader.ReadString();
                        else reader.SkipValue(addrType);
                    }
                    break;
                    
                case "employmenthistory":
                    reader.ReadDocumentSize(); // Enter Array
                    while (reader.Remaining > 0)
                    {
                        var arrType = reader.ReadBsonType();
                        if (arrType == BsonType.EndOfDocument) break;
                        reader.ReadElementHeader(); // Array index "0", "1"... ignore
                        
                        // Read WorkHistory item
                        var workItem = new WorkHistory();
                        reader.ReadDocumentSize(); // Enter Item Document
                        while (reader.Remaining > 0)
                        {
                            var itemType = reader.ReadBsonType();
                            if (itemType == BsonType.EndOfDocument) break;
                            var itemName = reader.ReadElementHeader();
                            
                            if (itemName == "companyname") workItem.CompanyName = reader.ReadString();
                            else if (itemName == "title") workItem.Title = reader.ReadString();
                            else if (itemName == "durationyears") workItem.DurationYears = reader.ReadInt32();
                            else if (itemName == "tags")
                            {
                                reader.ReadDocumentSize(); // Enter Tags Array
                                while (reader.Remaining > 0)
                                {
                                    var tagType = reader.ReadBsonType();
                                    if (tagType == BsonType.EndOfDocument) break;
                                    reader.ReadElementHeader(); // Index
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
