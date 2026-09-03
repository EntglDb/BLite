using BLite.Bson;
using BLite.Core;

namespace BLite.Tests;

public class UpsertDynamicCollectionTests : IDisposable
{
    private readonly string _dbPath;
    private BLiteEngine _engine;

    public UpsertDynamicCollectionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_upsert_dyncol_{Guid.NewGuid():N}.db");
        _engine = new BLiteEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    private BsonDocument MakeDoc(string name, int age)
    {
        var col = _engine.GetOrCreateCollection("tmp_schema");
        return col.CreateDocument(["name", "age"], b => b
            .AddString("name", name)
            .AddInt32("age", age));
    }

    private BsonDocument MakeDocWithId(BsonId id, string name, int age)
    {
        var col = _engine.GetOrCreateCollection("tmp_schema");
        return col.CreateDocument(["_id", "name", "age"], b => b
            .AddId(id)
            .AddString("name", name)
            .AddInt32("age", age));
    }

    [Fact]
    public async Task Upsert_Without_Id_Inserts_New_Document()
    {
        var col = _engine.GetOrCreateCollection("users");
        var doc = MakeDoc("Alice", 30);

        var result = await col.UpsertAsync(doc);
        await _engine.CommitAsync();

        Assert.True(result.Inserted);

        var found = await col.FindByIdAsync(result.Id);
        Assert.NotNull(found);
        found!.TryGetString("name", out var name);
        Assert.Equal("Alice", name);
    }

    [Fact]
    public async Task Upsert_With_Unused_Id_Inserts()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id = new BsonId(ObjectId.NewObjectId());
        var doc = MakeDocWithId(id, "Bob", 25);

        var result = await col.UpsertAsync(doc);
        await _engine.CommitAsync();

        Assert.True(result.Inserted);
        Assert.Equal(id, result.Id);
        Assert.Equal(1, await col.CountAsync());
    }

    [Fact]
    public async Task Upsert_With_Existing_Id_Replaces_Document()
    {
        var col = _engine.GetOrCreateCollection("users");
        var id = await col.InsertAsync(MakeDoc("Alice", 30));
        await _engine.CommitAsync();

        var result = await col.UpsertAsync(MakeDocWithId(id, "Alice", 31));
        await _engine.CommitAsync();

        Assert.False(result.Inserted);
        Assert.Equal(id, result.Id);
        Assert.Equal(1, await col.CountAsync());

        var found = await col.FindByIdAsync(id);
        Assert.NotNull(found);
        found!.TryGetInt32("age", out var age);
        Assert.Equal(31, age);
    }

    [Fact]
    public async Task UpsertBulk_Mixes_Inserts_And_Updates()
    {
        var col = _engine.GetOrCreateCollection("users");
        var existingId = await col.InsertAsync(MakeDoc("Alice", 30));
        await _engine.CommitAsync();

        var newId = new BsonId(ObjectId.NewObjectId());
        var results = await col.UpsertBulkAsync(new[]
        {
            MakeDocWithId(existingId, "Alice", 31),
            MakeDocWithId(newId, "Charlie", 40),
        });
        await _engine.CommitAsync();

        Assert.Equal(2, results.Count);
        Assert.False(results[0].Inserted);
        Assert.True(results[1].Inserted);
        Assert.Equal(2, await col.CountAsync());
    }
}
