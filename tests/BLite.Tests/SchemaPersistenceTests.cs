using System;
using System.IO;
using System.Linq;
using Xunit;
using BLite.Bson;
using BLite.Core.Storage;
using BLite.Core.Collections;
using BLite.Core.Indexing;

namespace BLite.Tests;

public class SchemaPersistenceTests : IDisposable
{
    private readonly string _dbPath;
    private readonly StorageEngine _storage;

    public SchemaPersistenceTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"schema_test_{Guid.NewGuid()}.db");
        _storage = new StorageEngine(_dbPath, PageFileConfig.Small);
    }

    public void Dispose()
    {
        _storage.Dispose();
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
        meta.Indexes.Add(new IndexMetadata { Name = "age", IsUnique = false, Type = IndexType.BTree, PropertyPaths = new[] { "Age" } });

        _storage.SaveCollectionMetadata(meta);

        var loaded = _storage.GetCollectionMetadata("users");
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

        var rootId = _storage.AppendSchema(0, schema1);
        Assert.NotEqual(0u, rootId);

        var schemas = _storage.GetSchemas(rootId);
        Assert.Single(schemas);
        Assert.Equal("V1", schemas[0].Title);

        var updatedRoot = _storage.AppendSchema(rootId, schema2);
        Assert.Equal(rootId, updatedRoot);

        schemas = _storage.GetSchemas(rootId);
        Assert.True(schemas.Count == 2, $"Expected 2 schemas but found {schemas.Count}. Titles: {(schemas.Count > 0 ? string.Join(", ", schemas.Select(s => s.Title)) : "None")}");
        Assert.Equal("V1", schemas[0].Title);
        Assert.Equal("V2", schemas[1].Title);
    }

    public class Person { public ObjectId Id { get; set; } public string Name { get; set; } = string.Empty; }
    public class PersonV2 { public ObjectId Id { get; set; } public string Name { get; set; } = string.Empty; public int Age { get; set; } }

    public class TestMapper<TId, T> : DocumentMapperBase<TId, T> where T : class
    {
        private readonly string _name;
        public TestMapper(string name) => _name = name;
        public override string CollectionName => _name;
        public override int Serialize(T entity, BsonSpanWriter writer)
        {
            var size = writer.BeginDocument();
            // Write a dummy field to make it non-empty if needed
            writer.WriteString("dummy", "value");
            writer.EndDocument(size);
            return writer.Position;
        }
        public override IEnumerable<string> UsedKeys => new[] { "dummy", "_v" };
        public override T Deserialize(BsonSpanReader reader) => null!;
        public override TId GetId(T entity) => default!;
        public override void SetId(T entity, TId id) { }
    }

    [Fact]
    public void DocumentCollection_Integrates_Schema_Versioning_On_Startup()
    {
        var mapper1 = new TestMapper<ObjectId, Person>("Person");
        var schema1 = mapper1.GetSchema();

        // 1. First startup
        using (var coll = new DocumentCollection<ObjectId, Person>(_storage, mapper1))
        {
            var meta = _storage.GetCollectionMetadata(mapper1.CollectionName);
            Assert.NotNull(meta);
            var schemas = _storage.GetSchemas(meta.SchemaRootPageId);
            Assert.Single(schemas);
            Assert.True(schema1.Equals(schemas[0]), "Persisted schema 1 should equal current schema 1");

            Assert.NotNull(coll.CurrentSchemaVersion);
            Assert.Equal(1, coll.CurrentSchemaVersion!.Value.Version);
            Assert.Equal(schema1.GetHash(), coll.CurrentSchemaVersion!.Value.Hash);
        }

        // 2. Restart with SAME schema (should NOT append)
        using (var coll = new DocumentCollection<ObjectId, Person>(_storage, mapper1))
        {
            var meta = _storage.GetCollectionMetadata(mapper1.CollectionName);
            var schemas = _storage.GetSchemas(meta!.SchemaRootPageId);
            Assert.Single(schemas); // Still 1

            Assert.Equal(1, coll.CurrentSchemaVersion!.Value.Version);
            Assert.Equal(schema1.GetHash(), coll.CurrentSchemaVersion!.Value.Hash);
        }

        // 3. Restart with updated schema (PersonV2)
        var mapper2 = new TestMapper<ObjectId, PersonV2>("Person");
        var schema2 = mapper2.GetSchema();
        Assert.False(schema1.Equals(schema2), "Person and PersonV2 schemas should differ");
        Assert.NotEqual(schema1.GetHash(), schema2.GetHash());

        using (var coll = new DocumentCollection<ObjectId, PersonV2>(_storage, mapper2))
        {
            var meta = _storage.GetCollectionMetadata(mapper1.CollectionName);
            var schemas = _storage.GetSchemas(meta!.SchemaRootPageId);
            Assert.True(schemas.Count == 2, $"Expected 2 schemas but found {schemas.Count}. Titles: {string.Join(", ", schemas.Select(s => s.Title))}");
            Assert.Equal("Person", schemas[0].Title);
            Assert.Equal("PersonV2", schemas[1].Title);

            Assert.Equal(2, coll.CurrentSchemaVersion!.Value.Version);
            Assert.Equal(schema2.GetHash(), coll.CurrentSchemaVersion!.Value.Hash);
        }
    }

    [Fact]
    public void Document_Contains_Schema_Version_Field()
    {
        var mapper = new TestMapper<ObjectId, Person>("Person");
        using (var coll = new DocumentCollection<ObjectId, Person>(_storage, mapper))
        {
            var person = new Person { Name = "John" };
            var id = coll.Insert(person);

            Assert.Equal(1, coll.Count());
            Assert.NotNull(coll.CurrentSchemaVersion);
            Assert.Equal(1, coll.CurrentSchemaVersion!.Value.Version);

            // Verify that the document in storage contains _v
            var meta = _storage.GetCollectionMetadata("person"); // person lowercase
            Assert.NotNull(meta);

            // Get location from primary index (internal access enabled by InternalsVisibleTo)
            var key = mapper.ToIndexKey(id);
            Assert.True(coll._primaryIndex.TryFind(key, out var location, 0));

            // Read raw bytes from page
            var pageBuffer = new byte[_storage.PageSize];
            _storage.ReadPage(location.PageId, 0, pageBuffer);
            var slotOffset = SlottedPageHeader.Size + (location.SlotIndex * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(pageBuffer.AsSpan(slotOffset));
            var docData = pageBuffer.AsSpan(slot.Offset, slot.Length);

            // Print some info if it fails (using Assert messages)
            string hex = BitConverter.ToString(docData.ToArray()).Replace("-", "");
            
            // Look for _v (BsonType.Int32 + 2-byte ID)
            ushort vId = _storage.GetKeyMap()["_v"];
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
