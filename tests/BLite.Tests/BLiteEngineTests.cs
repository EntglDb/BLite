using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Standalone tests: creates databases with BLiteEngine and validates all DynamicCollection features.
/// </summary>
public class BLiteEngineTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;

    public BLiteEngineTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_engine_{Guid.NewGuid()}.db");
        _engine = new BLiteEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var walPath = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(walPath)) File.Delete(walPath);
    }

    #region Collection Management

    [Fact]
    public void GetOrCreateCollection_Returns_Same_Instance()
    {
        var col1 = _engine.GetOrCreateCollection("users");
        var col2 = _engine.GetOrCreateCollection("users");
        Assert.Same(col1, col2);
    }

    [Fact]
    public void GetCollection_Returns_Null_If_Not_Created()
    {
        var col = _engine.GetCollection("nonexistent");
        Assert.Null(col);
    }

    [Fact]
    public void ListCollections_Returns_All_Created()
    {
        _engine.GetOrCreateCollection("users");
        _engine.GetOrCreateCollection("orders");
        _engine.GetOrCreateCollection("products");

        var names = _engine.ListCollections();
        Assert.Equal(3, names.Count);
        Assert.Contains("users", names);
        Assert.Contains("orders", names);
        Assert.Contains("products", names);
    }

    [Fact]
    public void DropCollection_Removes_Collection()
    {
        _engine.GetOrCreateCollection("temp");
        Assert.True(_engine.DropCollection("temp"));
        Assert.Null(_engine.GetCollection("temp"));
        Assert.False(_engine.DropCollection("temp")); // already dropped
    }

    #endregion

    #region Insert & FindById (ObjectId)

    [Fact]
    public void Insert_And_FindById_ObjectId()
    {
        var col = _engine.GetOrCreateCollection("users");
        var doc = col.CreateDocument(["_id", "name", "age"], b => b
            .AddString("name", "Alice")
            .AddInt32("age", 30));

        var id = col.Insert(doc);
        _engine.Commit();

        Assert.False(id.IsEmpty);
        Assert.Equal(BsonIdType.ObjectId, id.Type);

        var found = col.FindById(id);
        Assert.NotNull(found);
        Assert.True(found.TryGetString("name", out var name));
        Assert.Equal("Alice", name);
        Assert.True(found.TryGetInt32("age", out var age));
        Assert.Equal(30, age);
    }

    #endregion

    #region Insert & FindById (Int32)

    [Fact]
    public void Insert_And_FindById_Int32()
    {
        var col = _engine.GetOrCreateCollection("products", BsonIdType.Int32);
        var doc = col.CreateDocument(["_id", "title", "price"], b => b
            .AddId((BsonId)42)
            .AddString("title", "Widget")
            .AddInt32("price", 999));

        var id = col.Insert(doc);
        _engine.Commit();

        Assert.Equal(42, id.AsInt32());

        var found = col.FindById(id);
        Assert.NotNull(found);
        Assert.True(found.TryGetString("title", out var title));
        Assert.Equal("Widget", title);
    }

    #endregion

    #region Insert & FindById (String)

    [Fact]
    public void Insert_And_FindById_String()
    {
        var col = _engine.GetOrCreateCollection("configs", BsonIdType.String);
        var doc = col.CreateDocument(["_id", "value"], b => b
            .AddId((BsonId)"app.setting.1")
            .AddString("value", "enabled"));

        var id = col.Insert(doc);
        _engine.Commit();

        Assert.Equal("app.setting.1", id.AsString());

        var found = col.FindById(id);
        Assert.NotNull(found);
        Assert.True(found.TryGetString("value", out var val));
        Assert.Equal("enabled", val);
    }

    #endregion

    #region Insert & FindById (Guid)

    [Fact]
    public void Insert_And_FindById_Guid()
    {
        var guid = Guid.NewGuid();
        var col = _engine.GetOrCreateCollection("sessions", BsonIdType.Guid);
        var doc = col.CreateDocument(["_id", "user"], b => b
            .AddId((BsonId)guid)
            .AddString("user", "bob"));

        var id = col.Insert(doc);
        _engine.Commit();

        Assert.Equal(guid, id.AsGuid());

        var found = col.FindById(id);
        Assert.NotNull(found);
        Assert.True(found.TryGetString("user", out var user));
        Assert.Equal("bob", user);
    }

    #endregion

    #region Auto-generated ID

    [Fact]
    public void Insert_Without_Id_AutoGenerates_ObjectId()
    {
        var col = _engine.GetOrCreateCollection("auto");
        var doc = col.CreateDocument(["name"], b => b
            .AddString("name", "NoIdDoc"));

        var id = col.Insert(doc);
        _engine.Commit();

        Assert.False(id.IsEmpty);
        Assert.Equal(BsonIdType.ObjectId, id.Type);

        var found = col.FindById(id);
        Assert.NotNull(found);
        Assert.True(found.TryGetString("name", out var n));
        Assert.Equal("NoIdDoc", n);
    }

    #endregion

    #region FindAll & Count

    [Fact]
    public void FindAll_Returns_All_Documents()
    {
        var col = _engine.GetOrCreateCollection("items");
        for (int i = 0; i < 5; i++)
        {
            var doc = col.CreateDocument(["_id", "idx"], b => b
                .AddId((BsonId)(i + 1))
                .AddInt32("idx", i));
            col.Insert(doc);
        }
        _engine.Commit();

        var all = col.FindAll().ToList();
        Assert.Equal(5, all.Count);
        Assert.Equal(5, col.Count());
    }

    #endregion

    #region Update

    [Fact]
    public void Update_Replaces_Document()
    {
        var col = _engine.GetOrCreateCollection("updatable");
        var doc = col.CreateDocument(["_id", "name", "version"], b => b
            .AddId((BsonId)1)
            .AddString("name", "v1")
            .AddInt32("version", 1));

        var id = col.Insert(doc);
        _engine.Commit();

        var updated = col.CreateDocument(["_id", "name", "version"], b => b
            .AddId((BsonId)1)
            .AddString("name", "v2")
            .AddInt32("version", 2));

        Assert.True(col.Update(id, updated));
        _engine.Commit();

        var found = col.FindById(id);
        Assert.NotNull(found);
        Assert.True(found.TryGetString("name", out var name));
        Assert.Equal("v2", name);
        Assert.True(found.TryGetInt32("version", out var ver));
        Assert.Equal(2, ver);
    }

    [Fact]
    public void Update_NonExistent_Returns_False()
    {
        var col = _engine.GetOrCreateCollection("ghost");
        var doc = col.CreateDocument(["name"], b => b.AddString("name", "x"));
        Assert.False(col.Update((BsonId)999, doc));
    }

    #endregion

    #region Delete

    [Fact]
    public void Delete_Removes_Document()
    {
        var col = _engine.GetOrCreateCollection("deletable");
        var doc = col.CreateDocument(["_id", "name"], b => b
            .AddId((BsonId)1)
            .AddString("name", "ToDelete"));

        var id = col.Insert(doc);
        _engine.Commit();

        Assert.True(col.Delete(id));
        _engine.Commit();

        Assert.Null(col.FindById(id));
        Assert.Equal(0, col.Count());
    }

    [Fact]
    public void Delete_NonExistent_Returns_False()
    {
        var col = _engine.GetOrCreateCollection("empty");
        Assert.False(col.Delete((BsonId)999));
    }

    #endregion

    #region Multiple Documents

    [Fact]
    public void Insert_Multiple_And_FindAll()
    {
        var col = _engine.GetOrCreateCollection("people");
        var names = new[] { "Alice", "Bob", "Charlie", "Diana", "Eve" };

        foreach (var name in names)
        {
            var doc = col.CreateDocument(["name", "active"], b => b
                .AddString("name", name)
                .AddBoolean("active", true));
            col.Insert(doc);
        }
        _engine.Commit();

        var all = col.FindAll().ToList();
        Assert.Equal(5, all.Count);
    }

    #endregion

    #region Transaction Management

    [Fact]
    public void Rollback_Discards_Changes()
    {
        var col = _engine.GetOrCreateCollection("transactional");
        var doc = col.CreateDocument(["_id", "name"], b => b
            .AddId((BsonId)1)
            .AddString("name", "WillRollback"));

        col.Insert(doc);
        _engine.Rollback();

        // After rollback, the document should not be findable
        // (Note: exact behavior depends on WAL/transaction isolation implementation)
        var count = col.Count();
        Assert.Equal(0, count);
    }

    #endregion

    #region Convenience CRUD (BLiteEngine shortcuts)

    [Fact]
    public void Engine_Insert_And_FindById_Convenience()
    {
        var doc = _engine.CreateDocument(["name", "role"], b => b
            .AddString("name", "Admin")
            .AddString("role", "superuser"));

        var id = _engine.Insert("staff", doc);

        var found = _engine.FindById("staff", id);
        Assert.NotNull(found);
        Assert.True(found.TryGetString("role", out var role));
        Assert.Equal("superuser", role);
    }

    [Fact]
    public void Engine_FindAll_Convenience()
    {
        for (int i = 0; i < 3; i++)
        {
            var doc = _engine.CreateDocument(["idx"], b => b.AddInt32("idx", i));
            _engine.Insert("batch", doc);
        }

        var all = _engine.FindAll("batch").ToList();
        Assert.Equal(3, all.Count);
    }

    [Fact]
    public void Engine_Update_Convenience()
    {
        var doc = _engine.CreateDocument(["name"], b => b.AddString("name", "Old"));
        var id = _engine.Insert("mutable", doc);

        var newDoc = _engine.CreateDocument(["name"], b => b.AddString("name", "New"));
        Assert.True(_engine.Update("mutable", id, newDoc));

        var found = _engine.FindById("mutable", id);
        Assert.NotNull(found);
        Assert.True(found.TryGetString("name", out var name));
        Assert.Equal("New", name);
    }

    [Fact]
    public void Engine_Delete_Convenience()
    {
        var doc = _engine.CreateDocument(["name"], b => b.AddString("name", "Bye"));
        var id = _engine.Insert("removable", doc);

        Assert.True(_engine.Delete("removable", id));
        Assert.Null(_engine.FindById("removable", id));
    }

    #endregion

    #region BSON Types

    [Fact]
    public void Supports_All_Primitive_Types()
    {
        var col = _engine.GetOrCreateCollection("types");
        var now = DateTime.UtcNow;
        var objId = ObjectId.NewObjectId();
        var guid = Guid.NewGuid();

        var doc = col.CreateDocument(
            ["_id", "int32", "int64", "dbl", "str", "boolT", "boolF", "dt", "oid", "guid"],
            b => b
                .AddId((BsonId)1)
                .AddInt32("int32", 42)
                .AddInt64("int64", long.MaxValue)
                .AddDouble("dbl", 3.14)
                .AddString("str", "hello")
                .AddBoolean("boolT", true)
                .AddBoolean("boolF", false)
                .AddDateTime("dt", now)
                .AddObjectId("oid", objId)
                .AddGuid("guid", guid));

        col.Insert(doc);
        _engine.Commit();

        var found = col.FindById((BsonId)1)!;
        var fields = found.EnumerateFields();

        // Verify via TryGet methods
        Assert.True(found.TryGetInt32("int32", out var i32));
        Assert.Equal(42, i32);

        Assert.True(found.TryGetString("str", out var str));
        Assert.Equal("hello", str);

        Assert.True(found.TryGetObjectId("oid", out var oid));
        Assert.Equal(objId, oid);

        // Verify via BsonValue
        var int64Val = found.GetValue("int64");
        Assert.Equal(BsonType.Int64, int64Val.Type);
        Assert.Equal(long.MaxValue, int64Val.AsInt64);

        var dblVal = found.GetValue("dbl");
        Assert.Equal(BsonType.Double, dblVal.Type);
        Assert.Equal(3.14, dblVal.AsDouble, precision: 10);

        var boolTVal = found.GetValue("boolt");
        Assert.Equal(BsonType.Boolean, boolTVal.Type);
        Assert.True(boolTVal.AsBoolean);

        var boolFVal = found.GetValue("boolf");
        Assert.Equal(BsonType.Boolean, boolFVal.Type);
        Assert.False(boolFVal.AsBoolean);
    }

    #endregion

    #region BsonId Types

    [Fact]
    public void BsonId_Equality_And_Comparison()
    {
        BsonId id1 = (BsonId)42;
        BsonId id2 = (BsonId)42;
        BsonId id3 = (BsonId)99;

        Assert.Equal(id1, id2);
        Assert.NotEqual(id1, id3);
        Assert.True(id1 == id2);
        Assert.True(id1 != id3);
        Assert.True(id1.CompareTo(id3) < 0);
    }

    [Fact]
    public void BsonId_ToBytes_Roundtrip()
    {
        var ids = new BsonId[]
        {
            (BsonId)ObjectId.NewObjectId(),
            (BsonId)42,
            (BsonId)123456789L,
            (BsonId)"key-string",
            (BsonId)Guid.NewGuid()
        };

        foreach (var id in ids)
        {
            var bytes = id.ToBytes();
            var restored = BsonId.FromBytes(bytes, id.Type);
            Assert.Equal(id, restored);
        }
    }

    #endregion

    #region BsonValue

    [Fact]
    public void BsonValue_Factory_And_Accessors()
    {
        Assert.Equal(42, BsonValue.FromInt32(42).AsInt32);
        Assert.Equal(123L, BsonValue.FromInt64(123).AsInt64);
        Assert.Equal(3.14, BsonValue.FromDouble(3.14).AsDouble);
        Assert.Equal("hello", BsonValue.FromString("hello").AsString);
        Assert.True(BsonValue.FromBoolean(true).AsBoolean);
        Assert.False(BsonValue.FromBoolean(false).AsBoolean);
        Assert.True(BsonValue.Null.IsNull);
    }

    [Fact]
    public void BsonValue_Implicit_Conversions()
    {
        BsonValue v1 = 42;
        BsonValue v2 = "text";
        BsonValue v3 = 3.14;
        BsonValue v4 = true;

        Assert.Equal(BsonType.Int32, v1.Type);
        Assert.Equal(BsonType.String, v2.Type);
        Assert.Equal(BsonType.Double, v3.Type);
        Assert.Equal(BsonType.Boolean, v4.Type);
    }

    #endregion

    #region EnumerateFields

    [Fact]
    public void EnumerateFields_Returns_All_Fields()
    {
        var col = _engine.GetOrCreateCollection("enum");
        var doc = col.CreateDocument(["_id", "a", "b", "c"], b => b
            .AddId((BsonId)1)
            .AddString("a", "alpha")
            .AddInt32("b", 2)
            .AddBoolean("c", true));

        col.Insert(doc);
        _engine.Commit();

        var found = col.FindById((BsonId)1)!;
        var fields = found.EnumerateFields();

        Assert.True(fields.Count >= 4); // _id, a, b, c
        Assert.Contains(fields, f => f.Name == "_id");
        Assert.Contains(fields, f => f.Name == "a" && f.Value.AsString == "alpha");
        Assert.Contains(fields, f => f.Name == "b" && f.Value.AsInt32 == 2);
        Assert.Contains(fields, f => f.Name == "c" && f.Value.AsBoolean == true);
    }

    #endregion

    #region Secondary Index

    [Fact]
    public void CreateIndex_And_QueryIndex()
    {
        var col = _engine.GetOrCreateCollection("indexed");

        // Insert documents with an "age" field
        for (int i = 0; i < 10; i++)
        {
            var doc = col.CreateDocument(["_id", "name", "age"], b => b
                .AddId((BsonId)(i + 1))
                .AddString("name", $"User{i}")
                .AddInt32("age", 20 + i));
            col.Insert(doc);
        }
        _engine.Commit();

        // Create secondary index on "age"
        col.CreateIndex("age", "idx_age");

        var indexes = col.ListIndexes();
        Assert.Contains("idx_age", indexes);

        // Query: age between 23 and 27 (inclusive)
        var results = col.QueryIndex("idx_age", 23, 27).ToList();
        Assert.True(results.Count >= 3); // ages 23, 24, 25, 26, 27

        foreach (var r in results)
        {
            Assert.True(r.TryGetInt32("age", out var age));
            Assert.InRange(age, 23, 27);
        }
    }

    [Fact]
    public void DropIndex_Removes_Index()
    {
        var col = _engine.GetOrCreateCollection("dropidx");
        col.CreateIndex("name", "idx_name");
        Assert.Contains("idx_name", col.ListIndexes());

        Assert.True(col.DropIndex("idx_name"));
        Assert.DoesNotContain("idx_name", col.ListIndexes());
    }

    #endregion

    #region Persistence Across Restart

    [Fact]
    public void Data_Persists_After_Engine_Restart()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_persist_{Guid.NewGuid()}.db");
        BsonId insertedId;

        try
        {
            // Phase 1: Insert data
            using (var engine1 = new BLiteEngine(dbPath))
            {
                var doc = engine1.CreateDocument(["name", "count"], b => b
                    .AddString("name", "Persistent")
                    .AddInt32("count", 42));
                insertedId = engine1.Insert("data", doc);
            }

            // Phase 2: Reopen and verify
            using (var engine2 = new BLiteEngine(dbPath))
            {
                var col = engine2.GetOrCreateCollection("data");
                var found = col.FindById(insertedId);
                Assert.NotNull(found);
                Assert.True(found.TryGetString("name", out var name));
                Assert.Equal("Persistent", name);
                Assert.True(found.TryGetInt32("count", out var count));
                Assert.Equal(42, count);
            }
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            var walPath = Path.ChangeExtension(dbPath, ".wal");
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    #endregion

    #region Edge Cases

    [Fact]
    public void FindById_NonExistent_Returns_Null()
    {
        var col = _engine.GetOrCreateCollection("sparse");
        Assert.Null(col.FindById((BsonId)ObjectId.NewObjectId()));
    }

    [Fact]
    public void Empty_Collection_FindAll_Returns_Empty()
    {
        var col = _engine.GetOrCreateCollection("empty_col");
        Assert.Empty(col.FindAll());
        Assert.Equal(0, col.Count());
    }

    [Fact]
    public void Multiple_Collections_Are_Independent()
    {
        var users = _engine.GetOrCreateCollection("users2");
        var orders = _engine.GetOrCreateCollection("orders2");

        var userDoc = users.CreateDocument(["_id", "name"], b => b
            .AddId((BsonId)1).AddString("name", "Alice"));
        users.Insert(userDoc);
        _engine.Commit();

        var orderDoc = orders.CreateDocument(["_id", "item"], b => b
            .AddId((BsonId)1).AddString("item", "Widget"));
        orders.Insert(orderDoc);
        _engine.Commit();

        Assert.Equal(1, users.Count());
        Assert.Equal(1, orders.Count());

        // Verify data isolation
        var user = users.FindById((BsonId)1)!;
        Assert.True(user.TryGetString("name", out var name));
        Assert.Equal("Alice", name);

        var order = orders.FindById((BsonId)1)!;
        Assert.True(order.TryGetString("item", out var item));
        Assert.Equal("Widget", item);
    }

    #endregion

    #region Diagnostic

    [Fact]
    public void Diagnostic_Builder_Bytes()
    {
        var col = _engine.GetOrCreateCollection("diag", BsonIdType.Int32);
        var doc = col.CreateDocument(["_id", "name"], b => b
            .AddId((BsonId)42)
            .AddString("name", "Test"));

        // Dump raw bytes for inspection
        var raw = doc.RawData.ToArray();
        var hex = BitConverter.ToString(raw);

        // Size field should be the total length
        var size = BitConverter.ToInt32(raw, 0);
        Assert.Equal(raw.Length, size);

        // End marker should be 0x00
        Assert.Equal(0x00, raw[^1]);

        // TryGetId should find the _id
        Assert.True(doc.TryGetId(out var id), $"TryGetId failed. Raw bytes: {hex}");
        Assert.Equal(42, id.AsInt32());

        // TryGetString should find name
        Assert.True(doc.TryGetString("name", out var name), $"TryGetString failed. Raw bytes: {hex}");
        Assert.Equal("Test", name);
    }

    #endregion
}
