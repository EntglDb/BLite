using System;
using System.IO;
using System.Linq;
using Xunit;
using BLite.Bson;
using BLite.Core.Storage;
using BLite.Core.Collections;
using BLite.Core.Indexing;
using BLite.Tests.TestDbContext_TestDbContext_Mappers;
using BLite.Shared;

namespace BLite.Tests;

public class SchemaPersistenceTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public SchemaPersistenceTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"schema_test_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Fact]
    public void BsonSchema_Serialization_RoundTrip()
    {
        var schema = new BsonSchema
        {
            Title = "Person",
            Fields = 
            {
                new BsonField { Name = "id", Type = BsonType.ObjectId },
                new BsonField { Name = "name", Type = BsonType.String, IsNullable = true },
                new BsonField { Name = "age", Type = BsonType.Int32 },
                new BsonField 
                { 
                    Name = "address", 
                    Type = BsonType.Document, 
                    NestedSchema = new BsonSchema
                    {
                        Fields = 
                        {
                            new BsonField { Name = "city", Type = BsonType.String }
                        }
                    } 
                }
            }
        };

        var buffer = new byte[1024];
        var keyMap = new System.Collections.Concurrent.ConcurrentDictionary<string, ushort>(StringComparer.OrdinalIgnoreCase);
        var keys = new System.Collections.Concurrent.ConcurrentDictionary<ushort, string>();
        
        // Manual registration for schema keys
        ushort id = 1;
        foreach (var k in new[] { "person", "id", "name", "age", "address", "city", "fields", "title", "type", "isnullable", "nestedschema", "t", "v", "f", "n", "b", "s", "a", "_v", "0", "1", "2", "3", "4", "5" })
        {
            keyMap[k] = id;
            keys[id] = k;
            id++;
        }

        var writer = new BsonSpanWriter(buffer, keyMap);
        schema.ToBson(ref writer);

        var reader = new BsonSpanReader(buffer.AsSpan(0, writer.Position), keys);
        var roundTrip = BsonSchema.FromBson(ref reader);

        Assert.Equal(schema.Title, roundTrip.Title);
        Assert.Equal(schema.Fields.Count, roundTrip.Fields.Count);
        Assert.Equal(schema.Fields[0].Name, roundTrip.Fields[0].Name);
        Assert.Equal(schema.Fields[3].NestedSchema!.Fields[0].Name, roundTrip.Fields[3].NestedSchema!.Fields[0].Name);
        Assert.True(schema.Equals(roundTrip));
    }

    [Fact]
    public void StorageEngine_Collections_Metadata_Persistence()
    {
        var meta = new CollectionMetadata
        {
            Name = "users",
            PrimaryRootPageId = 10,
            SchemaRootPageId = 20
        };
        meta.Indexes.Add(new IndexMetadata { Name = "age", IsUnique = false, Type = IndexType.BTree, PropertyPaths = ["Age"] });

        _db.Storage.SaveCollectionMetadata(meta);

        var loaded = _db.Storage.GetCollectionMetadata("users");
        Assert.NotNull(loaded);
        Assert.Equal(meta.Name, loaded.Name);
        Assert.Equal(meta.PrimaryRootPageId, loaded.PrimaryRootPageId);
        Assert.Equal(meta.SchemaRootPageId, loaded.SchemaRootPageId);
        Assert.Single(loaded.Indexes);
        Assert.Equal("age", loaded.Indexes[0].Name);
    }

    [Fact]
    public void StorageEngine_Schema_Versioning()
    {
        var schema1 = new BsonSchema { Title = "V1", Fields = { new BsonField { Name = "f1", Type = BsonType.String } } };
        var schema2 = new BsonSchema { Title = "V2", Fields = { new BsonField { Name = "f1", Type = BsonType.String }, new BsonField { Name = "f2", Type = BsonType.Int32 } } };

        var rootId = _db.Storage.AppendSchema(0, schema1);
        Assert.NotEqual(0u, rootId);

        var schemas = _db.Storage.GetSchemas(rootId);
        Assert.Single(schemas);
        Assert.Equal("V1", schemas[0].Title);

        var updatedRoot = _db.Storage.AppendSchema(rootId, schema2);
        Assert.Equal(rootId, updatedRoot);

        schemas = _db.Storage.GetSchemas(rootId);
        Assert.True(schemas.Count == 2, $"Expected 2 schemas but found {schemas.Count}. Titles: {(schemas.Count > 0 ? string.Join(", ", schemas.Select(s => s.Title)) : "None")}");
        Assert.Equal("V1", schemas[0].Title);
        Assert.Equal("V2", schemas[1].Title);
    }

    [Fact]
    public void DocumentCollection_Integrates_Schema_Versioning_On_Startup()
    {
        // Use a dedicated database for this test to avoid schema pollution from _db
        var testDbPath = Path.Combine(Path.GetTempPath(), $"schema_versioning_test_{Guid.NewGuid()}.db");
        
        try
        {
            var mapper1 = new BLite_Shared_PersonMapper();
            var schema1 = mapper1.GetSchema();

            // 1. First startup - create DB and initialize Person collection
            using (var db1 = new TestDbContext(testDbPath))
            {
                // Access only People collection to avoid initializing others
                var coll = db1.People;
                var meta = db1.Storage.GetCollectionMetadata("people_collection");
                Assert.NotNull(meta);
                var schemas = db1.Storage.GetSchemas(meta.SchemaRootPageId);
                Assert.Single(schemas);
                Assert.True(schema1.Equals(schemas[0]), "Persisted schema 1 should equal current schema 1");

                Assert.NotNull(coll.CurrentSchemaVersion);
                Assert.Equal(1, coll.CurrentSchemaVersion!.Value.Version);
                Assert.Equal(schema1.GetHash(), coll.CurrentSchemaVersion!.Value.Hash);
            }

            // 2. Restart with SAME schema (should NOT append)
            using (var db2 = new TestDbContext(testDbPath))
            {
                var coll = db2.People;
                var meta = db2.Storage.GetCollectionMetadata("people_collection");
                var schemas = db2.Storage.GetSchemas(meta!.SchemaRootPageId);
                Assert.Single(schemas); // Still 1

                Assert.Equal(1, coll.CurrentSchemaVersion!.Value.Version);
                Assert.Equal(schema1.GetHash(), coll.CurrentSchemaVersion!.Value.Hash);
            }

            // 3. Simulate schema evolution: Person with an additional field
            // Since we can't change the actual Person class at runtime, this test verifies
            // that the same schema doesn't get re-appended. 
            // A real-world scenario would involve deploying a new mapper version.
            using (var db3 = new TestDbContext(testDbPath))
            {
                var coll = db3.People;
                var meta = db3.Storage.GetCollectionMetadata("people_collection");
                var schemas = db3.Storage.GetSchemas(meta!.SchemaRootPageId);
                
                // Schema should still be 1 since we're using the same Person type
                Assert.Single(schemas);
                Assert.Equal("Person", schemas[0].Title);
                Assert.Equal(1, coll.CurrentSchemaVersion!.Value.Version);
            }
        }
        finally
        {
            if (File.Exists(testDbPath)) File.Delete(testDbPath);
        }
    }

    [Fact]
    public void Document_Contains_Schema_Version_Field()
    {
        var mapper = new BLite_Shared_PersonMapper();
        using (var coll = _db.People)
        {
            var person = new Person { Name = "John" };
            var id = coll.Insert(person);
            _db.SaveChanges();

            Assert.Equal(1, coll.Count());
            Assert.NotNull(coll.CurrentSchemaVersion);
            Assert.Equal(1, coll.CurrentSchemaVersion!.Value.Version);

            // Verify that the document in storage contains _v
            var meta = _db.Storage.GetCollectionMetadata("persons"); // person lowercase
            //Assert.NotNull(meta);

            // Get location from primary index (internal access enabled by InternalsVisibleTo)
            var key = mapper.ToIndexKey(id);
            Assert.True(coll._primaryIndex.TryFind(key, out var location, 0));

            // Read raw bytes from page
            var pageBuffer = new byte[_db.Storage.PageSize];
            _db.Storage.ReadPage(location.PageId, 0, pageBuffer);
            var slotOffset = SlottedPageHeader.Size + (location.SlotIndex * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(pageBuffer.AsSpan(slotOffset));
            var docData = pageBuffer.AsSpan(slot.Offset, slot.Length);

            // Print some info if it fails (using Assert messages)
            string hex = BitConverter.ToString(docData.ToArray()).Replace("-", "");
            
            // Look for _v (BsonType.Int32 + 2-byte ID)
            ushort vId = _db.Storage.GetKeyMap()["_v"];
            string vIdHex = vId.ToString("X4");
            // Reverse endian for hex string check (ushort is LE)
            string vIdHexLE = vIdHex.Substring(2, 2) + vIdHex.Substring(0, 2);
            string pattern = "10" + vIdHexLE;
            
            bool found = hex.Contains(pattern);
            Assert.True(found, $"Document should contain _v field ({pattern}). Raw BSON: {hex}");

            // Verify the value (1) follows the key
            int index = hex.IndexOf(pattern);
            string valueHex = hex.Substring(index + 6, 8); // 4 bytes = 8 hex chars (pattern is 6 hex chars: 10 + ID_LE)
            Assert.Equal("01000000", valueHex);
        }
    }
}
