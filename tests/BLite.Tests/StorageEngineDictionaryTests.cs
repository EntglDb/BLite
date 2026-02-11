using BLite.Core.Storage;
using Xunit;

namespace BLite.Tests;

public class StorageEngineDictionaryTests
{
    private string GetTempDbPath() => Path.Combine(Path.GetTempPath(), $"test_storage_dict_{Guid.NewGuid()}.db");

    private void Cleanup(string path)
    {
        if (File.Exists(path)) File.Delete(path);
        if (File.Exists(Path.ChangeExtension(path, ".wal"))) File.Delete(Path.ChangeExtension(path, ".wal"));
    }

    [Fact]
    public void StorageEngine_ShouldInitializeDictionary()
    {
        var path = GetTempDbPath();
        try
        {
            using (var storage = new StorageEngine(path, PageFileConfig.Default))
            {
                // Should generate ID > 100
                var id = storage.GetOrAddDictionaryEntry("TestKey");
                Assert.True(id > DictionaryPage.ReservedValuesEnd);
                
                var key = storage.GetDictionaryKey(id);
                Assert.Equal("testkey", key);
            }
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public void StorageEngine_ShouldPersistDictionary()
    {
        var path = GetTempDbPath();
        try
        {
            ushort id1, id2;
            using (var storage = new StorageEngine(path, PageFileConfig.Default))
            {
                id1 = storage.GetOrAddDictionaryEntry("Key1");
                id2 = storage.GetOrAddDictionaryEntry("Key2");
            }

            // Reopen
            using (var storage = new StorageEngine(path, PageFileConfig.Default))
            {
                var val1 = storage.GetOrAddDictionaryEntry("Key1");
                var val2 = storage.GetOrAddDictionaryEntry("Key2");
                
                Assert.Equal(id1, val1);
                Assert.Equal(id2, val2);
                
                Assert.Equal("key1", storage.GetDictionaryKey(val1));
                Assert.Equal("key2", storage.GetDictionaryKey(val2));
            }
        }
        finally { Cleanup(path); }
    }
    
    [Fact]
    public void StorageEngine_ShouldHandleManyKeys()
    {
        var path = GetTempDbPath();
        try
        {
            const int keyCount = 3000;
            var expectedIds = new Dictionary<string, ushort>();

            using (var storage = new StorageEngine(path, PageFileConfig.Default))
            {
                for (int i = 0; i < keyCount; i++)
                {
                    var key = $"Key_{i}";
                    var id = storage.GetOrAddDictionaryEntry(key);
                    expectedIds[key] = id;
                }
            }

            // Reopen and Verify
            using (var storage = new StorageEngine(path, PageFileConfig.Default))
            {
                for (int i = 0; i < keyCount; i++)
                {
                    var key = $"Key_{i}";
                    var id = storage.GetOrAddDictionaryEntry(key); // Should get existing
                    Assert.Equal(expectedIds[key], id);
                    
                    var loadedKey = storage.GetDictionaryKey(id);
                    Assert.Equal(key.ToLowerInvariant(), loadedKey);
                }
                
                // Add new one
                var newId = storage.GetOrAddDictionaryEntry("NewKeyAfterReopen");
                Assert.True(newId > 0);
                Assert.False(expectedIds.ContainsValue(newId));
            }
        }
        finally { Cleanup(path); }
    }
}
