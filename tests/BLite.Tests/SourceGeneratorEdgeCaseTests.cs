using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Regression tests for source generator edge cases:
/// 1. Properties of type <see cref="Dictionary{TKey,TValue}"/> — previously misidentified as
///    IEnumerable&lt;KeyValuePair&gt; which produced invalid mapper class names containing '&lt;' and '&gt;'.
/// 2. Properties whose declared type is a closed generic class (e.g. <c>GenericWrapper&lt;string&gt;</c>) —
///    previously <c>GetTypeByMetadataName</c> failed for the display-format name, and the generated
///    mapper name contained illegal identifier characters.
/// </summary>
public class SourceGeneratorEdgeCaseTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public SourceGeneratorEdgeCaseTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_sg_edge_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var walPath = _dbPath.Replace(".db", ".wal");
        if (File.Exists(walPath)) File.Delete(walPath);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Dictionary properties
    // ─────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Dictionary_StringValues_RoundTrip()
    {
        var entity = new EntityWithDictionary
        {
            Name = "Dict-String",
            Labels = new Dictionary<string, string>
            {
                ["env"]     = "production",
                ["region"]  = "eu-west-1",
                ["version"] = "2.0"
            }
        };

        var id = await _db.DictionaryEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        var retrieved = await _db.DictionaryEntities.FindByIdAsync(id);

        Assert.NotNull(retrieved);
        Assert.Equal("Dict-String", retrieved.Name);
        Assert.Equal(3, retrieved.Labels.Count);
        Assert.Equal("production", retrieved.Labels["env"]);
        Assert.Equal("eu-west-1",  retrieved.Labels["region"]);
        Assert.Equal("2.0",        retrieved.Labels["version"]);
    }

    [Fact]
    public async Task Dictionary_IntValues_RoundTrip()
    {
        var entity = new EntityWithDictionary
        {
            Name = "Dict-Int",
            Counters = new Dictionary<string, int>
            {
                ["hits"]   = 100,
                ["misses"] = 5,
                ["errors"] = 0
            }
        };

        var id = await _db.DictionaryEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        var retrieved = await _db.DictionaryEntities.FindByIdAsync(id);

        Assert.NotNull(retrieved);
        Assert.Equal(3,   retrieved.Counters.Count);
        Assert.Equal(100, retrieved.Counters["hits"]);
        Assert.Equal(5,   retrieved.Counters["misses"]);
        Assert.Equal(0,   retrieved.Counters["errors"]);
    }

    [Fact]
    public async Task Dictionary_EmptyCollection_RoundTrip()
    {
        var entity = new EntityWithDictionary { Name = "Dict-Empty" };

        var id = await _db.DictionaryEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        var retrieved = await _db.DictionaryEntities.FindByIdAsync(id);

        Assert.NotNull(retrieved);
        Assert.Empty(retrieved.Labels);
        Assert.Empty(retrieved.Counters);
    }

    [Fact]
    public async Task Dictionary_NullableProperty_RoundTrip()
    {
        // Insert with null optional dictionary
        var entityNoMeta = new EntityWithDictionary { Name = "Dict-NullMeta" };
        var idNoMeta = await _db.DictionaryEntities.InsertAsync(entityNoMeta);

        // Insert with populated optional dictionary
        var entityWithMeta = new EntityWithDictionary
        {
            Name = "Dict-WithMeta",
            OptionalMeta = new Dictionary<string, string> { ["key"] = "value" }
        };
        var idWithMeta = await _db.DictionaryEntities.InsertAsync(entityWithMeta);
        await _db.SaveChangesAsync();

        var retrievedNoMeta   = await _db.DictionaryEntities.FindByIdAsync(idNoMeta);
        var retrievedWithMeta = await _db.DictionaryEntities.FindByIdAsync(idWithMeta);

        Assert.NotNull(retrievedNoMeta);
        Assert.NotNull(retrievedWithMeta);
        Assert.Null(retrievedNoMeta.OptionalMeta);
        Assert.NotNull(retrievedWithMeta.OptionalMeta);
        Assert.Equal("value", retrievedWithMeta.OptionalMeta["key"]);
    }

    [Fact]
    public async Task Dictionary_UpdateEntry_Persists()
    {
        var entity = new EntityWithDictionary
        {
            Name = "Dict-Update",
            Labels = new Dictionary<string, string> { ["status"] = "pending" }
        };

        var id = await _db.DictionaryEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        var retrieved = await _db.DictionaryEntities.FindByIdAsync(id);
        Assert.NotNull(retrieved);
        retrieved.Labels["status"] = "active";
        retrieved.Labels["owner"]  = "alice";

        await _db.DictionaryEntities.UpdateAsync(retrieved);
        await _db.SaveChangesAsync();

        var updated = await _db.DictionaryEntities.FindByIdAsync(id);
        Assert.NotNull(updated);
        Assert.Equal("active", updated.Labels["status"]);
        Assert.Equal("alice",  updated.Labels["owner"]);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Generic class property
    // ─────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GenericNestedProperty_StringWrapper_RoundTrip()
    {
        var entity = new EntityWithGenericProperty
        {
            Name = "Generic-String",
            StringWrapper = new GenericWrapper<string> { Value = "hello", Label = "greeting", Count = 1 }
        };

        var id = await _db.GenericPropertyEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        var retrieved = await _db.GenericPropertyEntities.FindByIdAsync(id);

        Assert.NotNull(retrieved);
        Assert.Equal("Generic-String", retrieved.Name);
        Assert.NotNull(retrieved.StringWrapper);
        Assert.Equal("hello",    retrieved.StringWrapper.Value);
        Assert.Equal("greeting", retrieved.StringWrapper.Label);
        Assert.Equal(1,          retrieved.StringWrapper.Count);
    }

    [Fact]
    public async Task GenericNestedProperty_IntWrapper_RoundTrip()
    {
        var entity = new EntityWithGenericProperty
        {
            Name = "Generic-Int",
            IntWrapper = new GenericWrapper<int> { Value = 42, Label = "answer", Count = 7 }
        };

        var id = await _db.GenericPropertyEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        var retrieved = await _db.GenericPropertyEntities.FindByIdAsync(id);

        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.IntWrapper);
        Assert.Equal(42,       retrieved.IntWrapper.Value);
        Assert.Equal("answer", retrieved.IntWrapper.Label);
        Assert.Equal(7,        retrieved.IntWrapper.Count);
    }

    [Fact]
    public async Task GenericNestedProperty_BothWrappers_RoundTrip()
    {
        var entity = new EntityWithGenericProperty
        {
            Name = "Generic-Both",
            StringWrapper = new GenericWrapper<string> { Value = "text", Label = "s-label", Count = 2 },
            IntWrapper    = new GenericWrapper<int>    { Value = 99,     Label = "i-label", Count = 3 }
        };

        var id = await _db.GenericPropertyEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        var retrieved = await _db.GenericPropertyEntities.FindByIdAsync(id);

        Assert.NotNull(retrieved);
        Assert.Equal("text",    retrieved.StringWrapper.Value);
        Assert.Equal("s-label", retrieved.StringWrapper.Label);
        Assert.Equal(99,        retrieved.IntWrapper.Value);
        Assert.Equal("i-label", retrieved.IntWrapper.Label);
    }

    [Fact]
    public async Task GenericNestedProperty_Update_Persists()
    {
        var entity = new EntityWithGenericProperty
        {
            Name = "Generic-Update",
            StringWrapper = new GenericWrapper<string> { Value = "original", Label = "lbl" }
        };

        var id = await _db.GenericPropertyEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        var retrieved = await _db.GenericPropertyEntities.FindByIdAsync(id);
        Assert.NotNull(retrieved);
        retrieved.StringWrapper.Value = "updated";

        await _db.GenericPropertyEntities.UpdateAsync(retrieved);
        await _db.SaveChangesAsync();

        var updated = await _db.GenericPropertyEntities.FindByIdAsync(id);
        Assert.NotNull(updated);
        Assert.Equal("updated", updated.StringWrapper.Value);
    }
}
