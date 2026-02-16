using System.ComponentModel.DataAnnotations;
using BLite.Bson;
using BLite.Shared;
using BLite.Tests.TestDbContext_TestDbContext_Mappers;

namespace BLite.Tests
{
    public class AttributeTests
    {
        // Use full path for mapper until we are sure of the namespace
        private BLite_Shared_AnnotatedUserMapper CreateMapper() => new();
        
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, ushort> _keyMap = new(StringComparer.OrdinalIgnoreCase);
        private readonly System.Collections.Concurrent.ConcurrentDictionary<ushort, string> _keys = new();

        public AttributeTests()
        {
            ushort id = 1;
            string[] keys = ["_id", "display_name", "age", "location", "0", "1"];
            foreach (var key in keys)
            {
                _keyMap[key] = id;
                _keys[id] = key;
                id++;
            }
        }

        [Fact]
        public void Test_Table_Attribute_Mapping()
        {
            // Verify that the generated mapper has the correct collection name
            var mapper = CreateMapper();
            Assert.Equal("test.custom_users", mapper.CollectionName);
        }

        [Fact]
        public void Test_Required_Validation()
        {
            var mapper = CreateMapper();
            var user = new AnnotatedUser { Name = "" }; // Required name is empty
            var writer = new BsonSpanWriter(new byte[1024], _keyMap);

            bool thrown = false;
            try
            {
                mapper.Serialize(user, writer);
            }
            catch (ValidationException)
            {
                thrown = true;
            }
            Assert.True(thrown, "Should throw ValidationException for empty Name.");
        }

        [Fact]
        public void Test_StringLength_Validation()
        {
            var mapper = CreateMapper();
            var user = new AnnotatedUser { Name = "Jo" }; // Too short
            var writer = new BsonSpanWriter(new byte[1024], _keyMap);

            bool thrown = false;
            try { mapper.Serialize(user, writer); } catch (ValidationException) { thrown = true; }
            Assert.True(thrown, "Should throw ValidationException for Name too short.");

            user.Name = new string('A', 51); // Too long
            thrown = false;
            try { mapper.Serialize(user, writer); } catch (ValidationException) { thrown = true; }
            Assert.True(thrown, "Should throw ValidationException for Name too long.");
        }

        [Fact]
        public void Test_Range_Validation()
        {
            var mapper = CreateMapper();
            var user = new AnnotatedUser { Name = "John", Age = 200 }; // Out of range
            var writer = new BsonSpanWriter(new byte[1024], _keyMap);

            bool thrown = false;
            try { mapper.Serialize(user, writer); } catch (ValidationException) { thrown = true; }
            Assert.True(thrown, "Should throw ValidationException for Age out of range.");
        }

        [Fact]
        public void Test_Column_Name_Mapping()
        {
            var mapper = CreateMapper();
            var user = new AnnotatedUser { Name = "John", Age = 30 };
            var buffer = new byte[1024];
            var writer = new BsonSpanWriter(buffer, _keyMap);
            
            mapper.Serialize(user, writer);
            
            var reader = new BsonSpanReader(buffer, _keys);
            reader.ReadDocumentSize();
            
            bool foundDisplayName = false;
            while (reader.Remaining > 0)
            {
                var type = reader.ReadBsonType();
                if (type == BsonType.EndOfDocument) break;
                
                var name = reader.ReadElementHeader();
                if (name == "display_name") foundDisplayName = true;
                reader.SkipValue(type);
            }
            
            Assert.True(foundDisplayName, "BSON field name should be 'display_name' from [Column] attribute.");
        }

        [Fact]
        public void Test_NotMapped_Attribute()
        {
            var mapper = CreateMapper();
            var user = new AnnotatedUser { Name = "John", Age = 30 };
            var buffer = new byte[1024];
            var writer = new BsonSpanWriter(buffer, _keyMap);
            
            mapper.Serialize(user, writer);
            
            var reader = new BsonSpanReader(buffer, _keys);
            reader.ReadDocumentSize();
            
            bool foundComputed = false;
            while (reader.Remaining > 0)
            {
                var type = reader.ReadBsonType();
                if (type == BsonType.EndOfDocument) break;
                
                var name = reader.ReadElementHeader();
                if (name == "ComputedInfo") foundComputed = true;
                reader.SkipValue(type);
            }
            
            Assert.False(foundComputed, "ComputedInfo should not be mapped to BSON.");
        }
    }
}
