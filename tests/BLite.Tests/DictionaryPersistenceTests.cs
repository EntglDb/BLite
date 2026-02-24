using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Storage;

namespace BLite.Tests;

public class DictionaryPersistenceTests : IDisposable
{
    private readonly string _dbPath;
    private readonly StorageEngine _storage;

    public DictionaryPersistenceTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_dict_{Guid.NewGuid():N}.db");
        _storage = new StorageEngine(_dbPath, PageFileConfig.Default);
    }

    public void Dispose()
    {
        _storage.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var walPath = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(walPath)) File.Delete(walPath);
    }

    private class MockMapper : DocumentMapperBase<ObjectId, Dictionary<string, object>>
    {
        private readonly string _collectionName;
        private readonly List<string> _keys;

        public MockMapper(string name, params string[] keys)
        {
            _collectionName = name;
            _keys = keys.ToList();
        }

        public override string CollectionName => _collectionName;
        public override IEnumerable<string> UsedKeys => _keys;

        public override BsonSchema GetSchema() => new BsonSchema { Title = _collectionName };
        public override ObjectId GetId(Dictionary<string, object> entity) => throw new NotImplementedException();
        public override void SetId(Dictionary<string, object> entity, ObjectId id) => throw new NotImplementedException();
        public override int Serialize(Dictionary<string, object> entity, BsonSpanWriter writer) => throw new NotImplementedException();
        public override Dictionary<string, object> Deserialize(BsonSpanReader reader) => throw new NotImplementedException();
    }

    [Fact]
    public void RegisterMappers_Registers_All_Unique_Keys()
    {
        var mapper1 = new MockMapper("Coll1", "Name", "Age");
        var mapper2 = new MockMapper("Coll2", "Name", "Address", "City");

        _storage.RegisterMappers(new IDocumentMapper[] { mapper1, mapper2 });

        // Verify keys in cache
        Assert.NotEqual(0, _storage.GetOrAddDictionaryEntry("Name"));
        Assert.NotEqual(0, _storage.GetOrAddDictionaryEntry("Age"));
        Assert.NotEqual(0, _storage.GetOrAddDictionaryEntry("Address"));
        Assert.NotEqual(0, _storage.GetOrAddDictionaryEntry("City"));

        // Verify they have unique IDs (at least 4 unique IDs for 4 unique keys + internal ones)
        var ids = new HashSet<ushort>
        {
            _storage.GetOrAddDictionaryEntry("Name"),
            _storage.GetOrAddDictionaryEntry("Age"),
            _storage.GetOrAddDictionaryEntry("Address"),
            _storage.GetOrAddDictionaryEntry("City")
        };
        Assert.Equal(4, ids.Count);
    }

    [Fact]
    public void Dictionary_Keys_Persist_Across_Restarts()
    {
        var mapper = new MockMapper("Coll1", "PersistedKey");
        _storage.RegisterMappers(new IDocumentMapper[] { mapper });
        
        var originalId = _storage.GetOrAddDictionaryEntry("PersistedKey");
        Assert.NotEqual(0, originalId);

        _storage.Dispose();

        // Re-open
        using var storage2 = new StorageEngine(_dbPath, PageFileConfig.Default);
        
        var recoveredId = storage2.GetOrAddDictionaryEntry("PersistedKey");
        Assert.Equal(originalId, recoveredId);
    }

    private class NestedMockMapper : DocumentMapperBase<ObjectId, object>
    {
        public override string CollectionName => "Nested";
        public override BsonSchema GetSchema()
        {
            var schema = new BsonSchema { Title = "Nested" };
            schema.Fields.Add(new BsonField 
            { 
                Name = "Top", 
                Type = BsonType.Document,
                NestedSchema = new BsonSchema 
                { 
                    Fields = { new BsonField { Name = "Child", Type = BsonType.String } } 
                }
            });
            return schema;
        }

        public override ObjectId GetId(object entity) => throw new NotImplementedException();
        public override void SetId(object entity, ObjectId id) => throw new NotImplementedException();
        public override int Serialize(object entity, BsonSpanWriter writer) => throw new NotImplementedException();
        public override object Deserialize(BsonSpanReader reader) => throw new NotImplementedException();
    }

    [Fact]
    public void RegisterMappers_Handles_Nested_Keys()
    {
        var mapper = new NestedMockMapper();
        _storage.RegisterMappers(new IDocumentMapper[] { mapper });

        Assert.NotEqual(0, _storage.GetOrAddDictionaryEntry("Top"));
        Assert.NotEqual(0, _storage.GetOrAddDictionaryEntry("Child"));
    }
}
