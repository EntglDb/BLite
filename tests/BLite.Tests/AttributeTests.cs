using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using BLite.Bson;
using BLite.Core.Collections;
using Xunit;

namespace BLite.Tests
{
    [Table("custom_users", Schema = "test")]
    public class AnnotatedUser
    {
        [Key]
        public ObjectId Id { get; set; }

        [Required]
        [Column("display_name")]
        [StringLength(50, MinimumLength = 3)]
        public string Name { get; set; } = "";

        [Range(0, 150)]
        public int Age { get; set; }

        [NotMapped]
        public string ComputedInfo => $"{Name} ({Age})";

        [Column(TypeName = "geopoint")]
        public (double Lat, double Lon) Location { get; set; }
    }

    public partial class TestAttributeContext : global::BLite.Core.DocumentDbContext
    {
        public TestAttributeContext() : base("test.db") { }

        public global::BLite.Core.Collections.DocumentCollection<ObjectId, AnnotatedUser> Users { get; set; } = null!;
        
        protected override void OnModelCreating(global::BLite.Core.Metadata.ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<AnnotatedUser>();
        }
    }

    public class AttributeTests
    {
        // Use full path for mapper until we are sure of the namespace
        private global::BLite.Tests.TestAttributeContext_AttributeTests_Mappers.BLite_Tests_AnnotatedUserMapper CreateMapper() => new();
        
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, ushort> _keyMap = new(StringComparer.OrdinalIgnoreCase);
        private readonly System.Collections.Concurrent.ConcurrentDictionary<ushort, string> _keys = new();

        public AttributeTests()
        {
            ushort id = 1;
            string[] keys = { "_id", "display_name", "age", "location", "0", "1" };
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
