using BLite.Bson;
using BLite.Core;

namespace BLite.Tests;

/// <summary>
/// Tests for DynamicCollection secondary indexes on nested (embedded) properties
/// using dot-notation paths (e.g., "address.city.name").
/// </summary>
public class DynamicCollectionNestedIndexTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;

    public DynamicCollectionNestedIndexTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_dynamic_nested_{Guid.NewGuid():N}.db");
        _engine = new BLiteEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        var walPath = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(walPath))
            File.Delete(walPath);
    }

    [Fact]
    public void CreateIndex_On_Nested_Path_Succeeds()
    {
        var col = _engine.GetOrCreateCollection("users");

        col.CreateIndex("address.city", name: "idx_city");

        var indexes = col.ListIndexes();
        Assert.Contains("idx_city", indexes);
    }

    [Fact]
    public void Insert_With_Nested_Property_Indexes_Value()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.CreateIndex("address.city", name: "idx_city");

        var id = ObjectId.NewObjectId();
        var doc = col.CreateDocument(["_id", "name", "address", "street", "city"], b => b
            .AddId((BsonId)id)
            .AddString("name", "Alice")
            .AddDocument("address", inner => inner
                .AddString("street", "123 Main St")
                .AddString("city", "Seattle")));

        col.Insert(doc);

        // Index lookup: exactly "Seattle"
        var results = col.QueryIndex("idx_city", "Seattle", "Seattle").ToList();
        Assert.Single(results);
        results[0].TryGetString("name", out var name);
        Assert.Equal("Alice", name);
    }

    [Fact]
    public void Insert_With_Missing_Intermediate_Skips_Index()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.CreateIndex("address.city", name: "idx_city");

        // Document with no address field at all
        var doc1 = col.CreateDocument(["_id", "name"], b => b
            .AddId((BsonId)ObjectId.NewObjectId())
            .AddString("name", "Bob"));

        // Document with address but no city
        var doc2 = col.CreateDocument(["_id", "name", "address", "street"], b => b
            .AddId((BsonId)ObjectId.NewObjectId())
            .AddString("name", "Charlie")
            .AddDocument("address", inner => inner
                .AddString("street", "456 Oak Ave")));

        // Both should insert without throwing
        col.Insert(doc1);
        col.Insert(doc2);

        Assert.Equal(2, col.Count());

        // Index lookup should return nothing
        var indexed = col.QueryIndex("idx_city", null, null).ToList();
        Assert.Empty(indexed);
    }

    [Fact]
    public void QueryIndex_On_Nested_Path_Returns_Correct_Docs()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.CreateIndex("address.city", name: "idx_city");

        col.Insert(col.CreateDocument(["_id", "name", "address", "city"], b => b
            .AddId((BsonId)ObjectId.NewObjectId())
            .AddString("name", "Alice")
            .AddDocument("address", inner => inner.AddString("city", "Seattle"))));

        col.Insert(col.CreateDocument(["_id", "name", "address", "city"], b => b
            .AddId((BsonId)ObjectId.NewObjectId())
            .AddString("name", "Bob")
            .AddDocument("address", inner => inner.AddString("city", "Portland"))));

        // No address â†’ not in index
        col.Insert(col.CreateDocument(["_id", "name"], b => b
            .AddId((BsonId)ObjectId.NewObjectId())
            .AddString("name", "Charlie")));

        var results = col.QueryIndex("idx_city", "Seattle", "Seattle").ToList();
        Assert.Single(results);
        results[0].TryGetString("name", out var name);
        Assert.Equal("Alice", name);
    }

    [Fact]
    public void Update_Nested_Property_Updates_Index()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.CreateIndex("address.city", name: "idx_city");

        var id = ObjectId.NewObjectId();
        col.Insert(col.CreateDocument(["_id", "name", "address", "city"], b => b
            .AddId((BsonId)id)
            .AddString("name", "Alice")
            .AddDocument("address", inner => inner.AddString("city", "Seattle"))));

        // Replace document with updated city
        var updated = col.CreateDocument(["_id", "name", "address", "city"], b => b
            .AddId((BsonId)id)
            .AddString("name", "Alice")
            .AddDocument("address", inner => inner.AddString("city", "Portland")));

        Assert.True(col.Update((BsonId)id, updated));

        // New value in index
        var newResults = col.QueryIndex("idx_city", "Portland", "Portland").ToList();
        Assert.Single(newResults);

        // Old value removed from index
        var oldResults = col.QueryIndex("idx_city", "Seattle", "Seattle").ToList();
        Assert.Empty(oldResults);
    }

    [Fact]
    public void Delete_With_Nested_Property_Removes_From_Index()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.CreateIndex("address.city", name: "idx_city");

        var id = ObjectId.NewObjectId();
        col.Insert(col.CreateDocument(["_id", "name", "address", "city"], b => b
            .AddId((BsonId)id)
            .AddString("name", "Alice")
            .AddDocument("address", inner => inner.AddString("city", "Seattle"))));

        // Verify indexed before delete
        var before = col.QueryIndex("idx_city", "Seattle", "Seattle").ToList();
        Assert.Single(before);

        col.Delete((BsonId)id);

        // Should be removed from index
        var after = col.QueryIndex("idx_city", "Seattle", "Seattle").ToList();
        Assert.Empty(after);
    }

    [Fact]
    public void Deeply_Nested_Three_Level_Index_Works()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.CreateIndex("address.location.zip", name: "idx_zip");

        var id = ObjectId.NewObjectId();
        col.Insert(col.CreateDocument(["_id", "name", "address", "location", "zip"], b => b
            .AddId((BsonId)id)
            .AddString("name", "Alice")
            .AddDocument("address", addr => addr
                .AddDocument("location", loc => loc
                    .AddString("zip", "98101")))));

        var results = col.QueryIndex("idx_zip", "98101", "98101").ToList();
        Assert.Single(results);
        results[0].TryGetString("name", out var name);
        Assert.Equal("Alice", name);
    }

    [Fact]
    public void Multiple_Nested_Indexes_Work_Independently()
    {
        var col = _engine.GetOrCreateCollection("users");
        col.CreateIndex("address.city", name: "idx_city");
        col.CreateIndex("billing.country", name: "idx_country");

        var id = ObjectId.NewObjectId();
        col.Insert(col.CreateDocument(["_id", "name", "address", "billing", "city", "country"], b => b
            .AddId((BsonId)id)
            .AddString("name", "Alice")
            .AddDocument("address", addr => addr.AddString("city", "Seattle"))
            .AddDocument("billing", bill => bill.AddString("country", "USA"))));

        var cityResults = col.QueryIndex("idx_city", "Seattle", "Seattle").ToList();
        Assert.Single(cityResults);

        var countryResults = col.QueryIndex("idx_country", "USA", "USA").ToList();
        Assert.Single(countryResults);
    }

    [Fact]
    public void Nested_Index_Rebuilt_After_Existing_Data()
    {
        var col = _engine.GetOrCreateCollection("users");

        // Insert data BEFORE creating the index
        for (int i = 0; i < 5; i++)
        {
            col.Insert(col.CreateDocument(["_id", "name", "address", "city"], b => b
                .AddId((BsonId)ObjectId.NewObjectId())
                .AddString("name", $"User{i}")
                .AddDocument("address", addr => addr.AddString("city", i % 2 == 0 ? "Seattle" : "Portland"))));
        }

        // Create index AFTER insertion â†’ must re-index existing data
        col.CreateIndex("address.city", name: "idx_city");

        var seattleResults = col.QueryIndex("idx_city", "Seattle", "Seattle").ToList();
        var portlandResults = col.QueryIndex("idx_city", "Portland", "Portland").ToList();

        Assert.Equal(3, seattleResults.Count); // User0, User2, User4
        Assert.Equal(2, portlandResults.Count); // User1, User3
    }
}
