using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;
using BLite.Core.Indexing;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Cross-path tests: data is written via TestDbContext (typed embedded path)
/// and read back via BLiteEngine (schema-less dynamic path).
/// Validates that both paths share the same kernel and produce compatible BSON.
/// </summary>
public class CrossPathTests : IDisposable
{
    private readonly string _dbPath;

    public CrossPathTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_crosspath_{Guid.NewGuid()}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var walPath = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(walPath)) File.Delete(walPath);
    }

    #region Write with DbContext, Read with BLiteEngine

    [Fact]
    public void Read_Users_Written_By_DbContext()
    {
        // Arrange: write data with typed path
        using (var db = new TestDbContext(_dbPath))
        {
            db.Users.Insert(new User { Name = "Alice", Age = 30 });
            db.Users.Insert(new User { Name = "Bob", Age = 25 });
            db.Users.Insert(new User { Name = "Charlie", Age = 35 });
            db.SaveChanges();
            db.ForceCheckpoint();
        }

        // Act: read data with dynamic path
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("users");
        var docs = col.FindAll().ToList();

        // Assert
        Assert.Equal(3, docs.Count);

        var names = docs
            .Select(d => { d.TryGetString("name", out var n); return n; })
            .OrderBy(n => n)
            .ToList();

        Assert.Equal(["Alice", "Bob", "Charlie"], names);
    }

    [Fact]
    public void Read_User_Fields_Match_Typed_Entity()
    {
        ObjectId aliceId;

        using (var db = new TestDbContext(_dbPath))
        {
            aliceId = db.Users.Insert(new User { Name = "Alice", Age = 30 });
            db.SaveChanges();
            db.ForceCheckpoint();
        }

        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("users");
        var doc = col.FindById((BsonId)aliceId);

        Assert.NotNull(doc);

        // Verify all fields via TryGet
        Assert.True(doc.TryGetObjectId("_id", out var id));
        Assert.Equal(aliceId, id);

        Assert.True(doc.TryGetString("name", out var name));
        Assert.Equal("Alice", name);

        Assert.True(doc.TryGetInt32("age", out var age));
        Assert.Equal(30, age);
    }

    [Fact]
    public void Read_User_Fields_Via_BsonValue()
    {
        ObjectId bobId;

        using (var db = new TestDbContext(_dbPath))
        {
            bobId = db.Users.Insert(new User { Name = "Bob", Age = 25 });
            db.SaveChanges();
            db.ForceCheckpoint();
        }

        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("users");
        var doc = col.FindById((BsonId)bobId)!;

        // Verify via EnumerateFields + BsonValue
        var fields = doc.EnumerateFields();
        Assert.True(fields.Count >= 3); // _id, name, age

        var nameField = fields.First(f => f.Name == "name");
        Assert.Equal(BsonType.String, nameField.Value.Type);
        Assert.Equal("Bob", nameField.Value.AsString);

        var ageField = fields.First(f => f.Name == "age");
        Assert.Equal(BsonType.Int32, ageField.Value.Type);
        Assert.Equal(25, ageField.Value.AsInt32);
    }

    [Fact]
    public void Read_User_TryGetId_Returns_BsonId()
    {
        ObjectId insertedId;

        using (var db = new TestDbContext(_dbPath))
        {
            insertedId = db.Users.Insert(new User { Name = "Eve", Age = 28 });
            db.SaveChanges();
            db.ForceCheckpoint();
        }

        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("users");
        var doc = col.FindById((BsonId)insertedId)!;

        Assert.True(doc.TryGetId(out var bsonId));
        Assert.Equal(BsonIdType.ObjectId, bsonId.Type);
        Assert.Equal(insertedId, bsonId.AsObjectId());
    }

    #endregion

    #region Int32 Key (Person)

    [Fact]
    public void Read_Persons_With_Int32_Key()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            db.People.Insert(new Person { Id = 1, Name = "Mario", Age = 40 });
            db.People.Insert(new Person { Id = 2, Name = "Luigi", Age = 38 });
            db.People.Insert(new Person { Id = 3, Name = "Peach", Age = 35 });
            db.SaveChanges();
            db.ForceCheckpoint();
        }

        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("people_collection", BsonIdType.Int32);

        // Find by int id
        var mario = col.FindById((BsonId)1);
        Assert.NotNull(mario);
        Assert.True(mario.TryGetString("name", out var name));
        Assert.Equal("Mario", name);
        Assert.True(mario.TryGetInt32("age", out var age));
        Assert.Equal(40, age);

        // Count
        Assert.Equal(3, col.Count());
    }

    [Fact]
    public void FindAll_Persons_Returns_All_Typed_Entities()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            for (int i = 1; i <= 10; i++)
                db.People.Insert(new Person { Id = i, Name = $"Person{i}", Age = 20 + i });
            db.SaveChanges();
            db.ForceCheckpoint();
        }

        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("people_collection", BsonIdType.Int32);
        var all = col.FindAll().ToList();

        Assert.Equal(10, all.Count);

        // Verify field consistency
        foreach (var doc in all)
        {
            Assert.True(doc.TryGetInt32("_id", out var id));
            Assert.InRange(id, 1, 10);
            Assert.True(doc.TryGetString("name", out var n));
            Assert.StartsWith("Person", n);
            Assert.True(doc.TryGetInt32("age", out var a));
            Assert.InRange(a, 21, 30);
        }
    }

    #endregion

    #region Products (Int32 + Decimal)

    [Fact]
    public void Read_Products_With_Decimal_Price()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            db.Products.Insert(new Product { Id = 1, Title = "Widget", Price = 19.99m });
            db.Products.Insert(new Product { Id = 2, Title = "Gadget", Price = 49.95m });
            db.SaveChanges();
            db.ForceCheckpoint();
        }

        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("products_collection", BsonIdType.Int32);

        var widget = col.FindById((BsonId)1);
        Assert.NotNull(widget);
        Assert.True(widget.TryGetString("title", out var title));
        Assert.Equal("Widget", title);

        // Price is stored as Decimal128 in C-BSON
        var priceValue = widget.GetValue("price");
        Assert.Equal(BsonType.Decimal128, priceValue.Type);
        Assert.Equal(19.99m, priceValue.AsDecimal);
    }

    #endregion

    #region String Key

    [Fact]
    public void Read_StringEntity_With_String_Key()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            db.StringEntities.Insert(new StringEntity { Id = "key-1", Value = "hello" });
            db.StringEntities.Insert(new StringEntity { Id = "key-2", Value = "world" });
            db.SaveChanges();
            db.ForceCheckpoint();
        }

        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("stringentitys", BsonIdType.String);

        var doc1 = col.FindById((BsonId)"key-1");
        Assert.NotNull(doc1);
        Assert.True(doc1.TryGetString("value", out var val));
        Assert.Equal("hello", val);
    }

    #endregion

    #region Guid Key

    [Fact]
    public void Read_GuidEntity_With_Guid_Key()
    {
        var guid1 = Guid.NewGuid();
        var guid2 = Guid.NewGuid();

        using (var db = new TestDbContext(_dbPath))
        {
            db.GuidEntities.Insert(new GuidEntity { Id = guid1, Name = "First" });
            db.GuidEntities.Insert(new GuidEntity { Id = guid2, Name = "Second" });
            db.SaveChanges();
            db.ForceCheckpoint();
        }

        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("guidentitys", BsonIdType.Guid);

        var doc = col.FindById((BsonId)guid1);
        Assert.NotNull(doc);
        Assert.True(doc.TryGetString("name", out var name));
        Assert.Equal("First", name);
    }

    #endregion

    #region Complex Nested Entity

    [Fact]
    public void Read_ComplexUser_Nested_Fields()
    {
        ObjectId complexUserId;

        using (var db = new TestDbContext(_dbPath))
        {
            complexUserId = db.ComplexUsers.Insert(new ComplexUser
            {
                Name = "ComplexAlice",
                MainAddress = new Address
                {
                    Street = "Via Roma 42",
                    City = new City { Name = "Milano", ZipCode = "20100" }
                },
                Tags = ["dotnet", "csharp", "blite"]
            });
            db.SaveChanges();
            db.ForceCheckpoint();
        }

        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("complex_users");

        var doc = col.FindById((BsonId)complexUserId);
        Assert.NotNull(doc);

        Assert.True(doc.TryGetString("name", out var name));
        Assert.Equal("ComplexAlice", name);

        // Verify nested data is readable via EnumerateFields
        var fields = doc.EnumerateFields();
        Assert.True(fields.Count >= 3); // _id, name, mainaddress, tags, ...

        // mainaddress should be a nested document
        var addressField = fields.FirstOrDefault(f => f.Name == "mainaddress");
        Assert.Equal(BsonType.Document, addressField.Value.Type);
    }

    #endregion

    #region Bidirectional: Write with Engine, Read with DbContext

    [Fact]
    public void Write_With_Engine_Read_With_DbContext()
    {
        // Phase 1: Write with BLiteEngine using Person's schema (Int32 _id, name, age)
        using (var engine = new BLiteEngine(_dbPath))
        {
            var col = engine.GetOrCreateCollection("people_collection", BsonIdType.Int32);
            var doc = col.CreateDocument(["_id", "name", "age"], b => b
                .AddId((BsonId)100)
                .AddString("name", "DynamicPerson")
                .AddInt32("age", 50));
            col.Insert(doc);
            engine.Commit();
        }

        // Phase 2: Read with TestDbContext
        using var db = new TestDbContext(_dbPath);
        var person = db.People.FindById(100);
        Assert.NotNull(person);
        Assert.Equal("DynamicPerson", person.Name);
        Assert.Equal(50, person.Age);
    }

    [Fact]
    public void Write_Users_With_Engine_Read_With_DbContext()
    {
        var objectId = ObjectId.NewObjectId();

        // Phase 1: Write with BLiteEngine
        using (var engine = new BLiteEngine(_dbPath))
        {
            var col = engine.GetOrCreateCollection("users");
            var doc = col.CreateDocument(["_id", "name", "age"], b => b
                .AddId((BsonId)objectId)
                .AddString("name", "EngineUser")
                .AddInt32("age", 33));
            col.Insert(doc);
            engine.Commit();
        }

        // Phase 2: Read with TestDbContext
        using var db = new TestDbContext(_dbPath);
        var user = db.Users.FindById(objectId);
        Assert.NotNull(user);
        Assert.Equal("EngineUser", user.Name);
        Assert.Equal(33, user.Age);
    }

    #endregion

    #region Mixed Operations: Both Paths on Same DB

    [Fact]
    public void DbContext_And_Engine_Share_Storage()
    {
        // Phase 1: Write persons 1 and 2 with the typed path, then close
        using (var db = new TestDbContext(_dbPath))
        {
            db.People.Insert(new Person { Id = 1, Name = "Typed1", Age = 20 });
            db.People.Insert(new Person { Id = 2, Name = "Typed2", Age = 30 });
            db.SaveChanges();
            db.ForceCheckpoint();
        }

        // Phase 2: Open a fresh BLiteEngine on the same file, add person 3, close
        using (var engine = new BLiteEngine(_dbPath))
        {
            var col = engine.GetOrCreateCollection("people_collection", BsonIdType.Int32);
            var doc = col.CreateDocument(["_id", "name", "age"], b => b
                .AddId((BsonId)3)
                .AddString("name", "Dynamic3")
                .AddInt32("age", 40));
            col.Insert(doc);
            engine.Commit();
        }

        // Phase 3: Re-open with BLiteEngine and verify all 3 are there
        using (var engine = new BLiteEngine(_dbPath))
        {
            var col = engine.GetOrCreateCollection("people_collection", BsonIdType.Int32);
            var allDynamic = col.FindAll().ToList();
            Assert.Equal(3, allDynamic.Count);
        }

        // Phase 4: Re-open with typed path and verify person 3 (written by engine) is readable
        using (var db = new TestDbContext(_dbPath))
        {
            var allTyped = db.People.FindAll().ToList();
            Assert.Equal(3, allTyped.Count);

            var person3 = db.People.FindById(3);
            Assert.NotNull(person3);
            Assert.Equal("Dynamic3", person3.Name);
            Assert.Equal(40, person3.Age);
        }
    }

    #endregion

    #region Count Consistency

    [Fact]
    public void Count_Matches_Between_Paths()
    {
        // Phase 1: Write 20 persons with typed path, then close
        using (var db = new TestDbContext(_dbPath))
        {
            for (int i = 1; i <= 20; i++)
                db.People.Insert(new Person { Id = i, Name = $"P{i}", Age = i + 20 });
            db.SaveChanges();
            db.ForceCheckpoint();
        }

        // Phase 2: Read count with dynamic path
        int dynamicCount;
        using (var engine = new BLiteEngine(_dbPath))
        {
            var col = engine.GetOrCreateCollection("people_collection", BsonIdType.Int32);
            dynamicCount = col.Count();
        }

        // Phase 3: Read count with typed path
        int typedCount;
        using (var db = new TestDbContext(_dbPath))
        {
            typedCount = db.People.Count();
        }

        Assert.Equal(20, dynamicCount);
        Assert.Equal(typedCount, dynamicCount);
    }

    #endregion

    #region Diagnostics

    [Fact]
    public void Diagnostic_RawStorage_AfterTypedWrite()
    {
        // Write via typed path
        int personId;
        using (var db = new TestDbContext(_dbPath))
        {
            personId = db.People.Insert(new Person { Id = 42, Name = "Diag", Age = 99 });
            db.SaveChanges();
            db.ForceCheckpoint();
        }

        // Open raw storage and inspect directly
        var config = PageFileConfig.Default;
        using var storage = new StorageEngine(_dbPath, config);

        // 1. Check collection metadata
        var metadata = storage.GetCollectionMetadata("people_collection");
        Assert.NotNull(metadata); // metadata must exist
        Assert.NotEqual(0u, metadata!.PrimaryRootPageId); // must have root page

        // 2. Manually create BTreeIndex and scan
        var opts = IndexOptions.CreateBTree("_id");
        var btree = new BTreeIndex(storage, opts, metadata.PrimaryRootPageId);

        var txn = storage.BeginTransaction();
        var entries = btree.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txn.TransactionId).ToList();

        // Must have exactly 1 entry
        Assert.Equal(1, entries.Count);

        // 3. Verify the key matches
        var expected = new IndexKey(BitConverter.GetBytes(personId));
        Assert.Equal(expected, entries[0].Key);
    }

    [Fact]
    public void GetAllCollectionsMetadata_ReturnsAllWrittenCollections()
    {
        // Arrange: scrive dati su più collezioni via typed path
        using (var db = new TestDbContext(_dbPath))
        {
            db.People.Insert(new Person { Id = 1, Name = "Alice", Age = 30 });
            db.Products.Insert(new Product { Id = 1, Title = "Widget", Price = 9.99m });
            db.Users.Insert(new User { Name = "Bob", Age = 25 });
            db.SaveChanges();
            db.ForceCheckpoint();
        }

        // Act: apre il raw StorageEngine e recupera tutte le collezioni
        var config = PageFileConfig.Default;
        using var storage = new StorageEngine(_dbPath, config);

        var all = storage.GetAllCollectionsMetadata();

        // Assert: le tre collezioni devono essere presenti
        Assert.NotNull(all);
        Assert.True(all.Count >= 3, $"Expected at least 3 collections, got {all.Count}: [{string.Join(", ", all.Select(m => m.Name))}]");

        var names = all.Select(m => m.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        Assert.Contains("people_collection", names);
        Assert.Contains("products_collection", names);
        Assert.Contains("users", names);

        // Ogni collezione deve avere un PrimaryRootPageId valido (!=0)
        foreach (var meta in all)
        {
            Assert.True(meta.PrimaryRootPageId != 0,
                $"Collection '{meta.Name}' has PrimaryRootPageId=0");
        }
    }

    #endregion
}