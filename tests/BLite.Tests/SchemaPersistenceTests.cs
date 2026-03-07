using BLite.Bson;
using BLite.Core.Storage;
using BLite.Core.Indexing;
using BLite.Shared;
using BLite.Tests.TestDbContext_TestDbContext_Mappers;

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

    // -----------------------------------------------------------------------
    // Large-schema tests: schemi che superano una singola PageSize.
    // Riproduce il crash reale:
    //   ArgumentOutOfRangeException in BsonSpanWriter.WriteString
    //   dovuto al buffer fisso di PageSize in AppendSchema.
    // -----------------------------------------------------------------------

    /// <summary>
    /// Verifica che <see cref="BsonSchema.CalculateSize"/> produca un valore
    /// uguale ai byte effettivamente scritti da <see cref="BsonSchema.ToBson"/>.
    /// </summary>
    [Fact]
    public void BsonSchema_CalculateSize_MatchesActualSerializedSize()
    {
        var schema = BuildWideSchema(fieldCount: 40);

        int calculated = schema.CalculateSize();

        // Serialize to a buffer sized by CalculateSize() itself – must not throw.
        var buffer = new byte[calculated];
        var keyMap = BuildKeyMapForSchema(schema);
        var writer = new BsonSpanWriter(buffer, keyMap);
        schema.ToBson(ref writer);

        Assert.Equal(calculated, writer.Position);
    }

    /// <summary>
    /// Uno schema con molti campi flat supera 8 KB (PageFileConfig.Small).
    /// <see cref="StorageEngine.AppendSchema"/> non deve più lanciare
    /// <see cref="ArgumentOutOfRangeException"/> e deve persistere lo schema
    /// su più pagine collegate.
    /// </summary>
    [Fact]
    public void AppendSchema_LargerThanOnePage_DoesNotThrow_And_RoundTrips()
    {
        // Each flat string field costs ≈ 36 bytes serialized.
        // Use 600 fields to guarantee > 16 KB (the default PageSize = 16384).
        var schema = BuildWideSchema(fieldCount: 600);

        int serializedSize = schema.CalculateSize();
        Assert.True(serializedSize > _db.Storage.PageSize,
            $"Pre-condition failed: schema is {serializedSize} bytes but PageSize is {_db.Storage.PageSize}. Increase fieldCount.");

        // Must not throw ArgumentOutOfRangeException.
        uint rootId = _db.Storage.AppendSchema(0, schema);
        Assert.NotEqual(0u, rootId);

        // Must round-trip correctly.
        var loaded = _db.Storage.GetSchemas(rootId);
        Assert.Single(loaded);
        Assert.Equal(schema.Title, loaded[0].Title);
        Assert.Equal(schema.Fields.Count, loaded[0].Fields.Count);
        Assert.True(schema.Equals(loaded[0]), "Round-tripped schema hash mismatch.");
    }

    /// <summary>
    /// Aggiunge due versioni di schema entrambe superiori a PageSize e verifica
    /// che entrambe vengano recuperate correttamente.
    /// </summary>
    [Fact]
    public void AppendSchema_MultiVersionLargeSchemas_RoundTrip()
    {
        var schema1 = BuildWideSchema(fieldCount: 600, titleSuffix: "V1");
        var schema2 = BuildWideSchema(fieldCount: 610, titleSuffix: "V2");

        var rootId = _db.Storage.AppendSchema(0, schema1);
        _db.Storage.AppendSchema(rootId, schema2);

        var loaded = _db.Storage.GetSchemas(rootId);
        Assert.Equal(2, loaded.Count);
        Assert.Contains("V1", loaded[0].Title);
        Assert.Contains("V2", loaded[1].Title);
        Assert.Equal(schema1.Fields.Count, loaded[0].Fields.Count);
        Assert.Equal(schema2.Fields.Count, loaded[1].Fields.Count);
    }

    /// <summary>
    /// Verifica che uno schema con oggetti annidati su 4 livelli (simulando
    /// strutture dominio reali come SalesRecord) si serializzi e si legga
    /// correttamente anche quando il totale supera PageSize.
    /// </summary>
    [Fact]
    public void AppendSchema_DeeplyNestedSchema_LargerThanOnePage_RoundTrips()
    {
        var schema = BuildDeeplyNestedSchema(topLevelFields: 15, nestingDepth: 4, leafFields: 5);

        int serializedSize = schema.CalculateSize();

        // Serialize and deserialize regardless of size.
        uint rootId = _db.Storage.AppendSchema(0, schema);
        var loaded = _db.Storage.GetSchemas(rootId);

        Assert.Single(loaded);
        Assert.Equal(schema.Title, loaded[0].Title);
        Assert.True(schema.Equals(loaded[0]),
            $"Hash mismatch for deeply nested schema " +
            $"(serializedSize={serializedSize}, pageSize={_db.Storage.PageSize}).");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// <summary>
    /// Constructs a flat schema with <paramref name="fieldCount"/> string fields.
    /// Long enough names (field_000 … field_NNN) ensure the serialized size
    /// grows proportionally and can exceed a single page.
    /// </summary>
    private static BsonSchema BuildWideSchema(int fieldCount, string titleSuffix = "")
    {
        var schema = new BsonSchema { Title = $"WideEntity{titleSuffix}" };
        for (int i = 0; i < fieldCount; i++)
        {
            schema.Fields.Add(new BsonField
            {
                Name = $"field_{i:D3}",   // 9-char name → UTF-8 9 bytes
                Type = BsonType.String,
                IsNullable = true
            });
        }
        return schema;
    }

    /// <summary>
    /// Constructs a schema with <paramref name="topLevelFields"/> top-level document
    /// fields, each containing a nested schema recursed to <paramref name="nestingDepth"/>
    /// levels, with <paramref name="leafFields"/> leaf string fields at the bottom.
    /// </summary>
    private static BsonSchema BuildDeeplyNestedSchema(int topLevelFields, int nestingDepth, int leafFields)
    {
        var root = new BsonSchema { Title = "DeepEntity" };
        for (int t = 0; t < topLevelFields; t++)
        {
            root.Fields.Add(new BsonField
            {
                Name = $"top_{t:D2}",
                Type = BsonType.Document,
                NestedSchema = BuildNestedLevel(nestingDepth, leafFields)
            });
        }
        return root;
    }

    private static BsonSchema BuildNestedLevel(int depth, int leafFields)
    {
        var schema = new BsonSchema();
        if (depth <= 0)
        {
            for (int i = 0; i < leafFields; i++)
                schema.Fields.Add(new BsonField { Name = $"leaf_{i}", Type = BsonType.String });
            return schema;
        }
        schema.Fields.Add(new BsonField
        {
            Name = $"nested_d{depth}",
            Type = BsonType.Document,
            NestedSchema = BuildNestedLevel(depth - 1, leafFields)
        });
        return schema;
    }

    /// <summary>
    /// Builds a minimal key map covering all field names that appear in
    /// <paramref name="schema"/> plus the fixed schema meta-keys used by ToBson.
    /// </summary>
    private static System.Collections.Concurrent.ConcurrentDictionary<string, ushort> BuildKeyMapForSchema(BsonSchema schema)
    {
        var map = new System.Collections.Concurrent.ConcurrentDictionary<string, ushort>(StringComparer.OrdinalIgnoreCase);
        ushort id = 1;

        // Fixed keys used by BsonSchema.ToBson / BsonField.ToBson
        foreach (var k in new[] { "t", "_v", "f", "n", "b", "s", "a" })
        {
            if (!map.ContainsKey(k)) map[k] = id++;
        }

        // Dynamic keys from the schema itself
        foreach (var key in schema.GetAllKeys())
        {
            if (!map.ContainsKey(key)) map[key] = id++;
        }

        return map;
    }
}
