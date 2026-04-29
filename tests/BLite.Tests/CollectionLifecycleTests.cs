using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using BLite.Core.Storage;
using BLite.Shared;
using BLite.Tests.TestDbContext_TestDbContext_Mappers;

namespace BLite.Tests;

public class CollectionLifecycleTests : IDisposable
{
    private readonly List<string> _paths = [];

    [Fact]
    public async Task TruncateCollectionAsync_ClearsDocuments_And_PreservesIndexes()
    {
        var dbPath = TrackTempFile(".db");
        using var db = new TestDbContext(dbPath);

        await db.People.InsertAsync(new Person { Id = 1, Name = "Alice", Age = 30 });
        await db.People.InsertAsync(new Person { Id = 2, Name = "Bob", Age = 40 });

        Assert.Single(db.People.GetIndexes());

        var deleted = await db.TruncateCollectionAsync<Person>();

        Assert.Equal(2, deleted);
        Assert.Empty(await db.People.FindAllAsync().ToListAsync());
        Assert.Single(db.People.GetIndexes());

        await db.People.InsertAsync(new Person { Id = 3, Name = "Carol", Age = 25 });
        Assert.Single(await db.People.FindAllAsync().ToListAsync());
    }

    [Fact]
    public async Task DropCollectionAsync_InvalidatesConcreteCollectionReference()
    {
        var dbPath = TrackTempFile(".db");
        using var db = new TestDbContext(dbPath);

        var original = db.Users;
        await db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });

        await db.DropCollectionAsync<User>();

        Assert.Same(original, db.Users);
        Assert.Null(db.Storage.GetCollectionMetadata("users"));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await db.Users.FindAllAsync().ToListAsync());
        Assert.Equal("Collection 'users' has been dropped.", ex.Message);

        ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await db.Set<User>().FindAllAsync().ToListAsync());
        Assert.Equal("Collection 'users' has been dropped.", ex.Message);
    }

    [Fact]
    public async Task DropCollectionAsync_ReplacesInterfaceCollectionWithProxy()
    {
        var dbPath = TrackTempFile(".db");
        using var db = new InterfaceCollectionDbContext(dbPath);

        var original = db.Users;
        await db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });

        await db.DropCollectionAsync<User>();

        Assert.NotSame(original, db.Users);
        Assert.IsNotType<DocumentCollection<ObjectId, User>>(db.Users);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await db.Users.FindAllAsync().ToListAsync());
        Assert.Equal("Collection 'users' has been dropped.", ex.Message);
    }

    [Fact]
    public async Task DropCollection_SingleFile_ReleasesPagesToFreeList()
    {
        var dbPath = TrackTempFile(".db");
        using (var engine = new BLiteEngine(dbPath))
        {
            var collection = engine.GetOrCreateCollection("temp");
            await collection.InsertAsync(collection.CreateDocument(["_id", "name"], b => b.AddString("name", "value")));
            await engine.CommitAsync();

            Assert.True(engine.DropCollection("temp"));
        }

        using var stream = File.OpenRead(dbPath);
        var headerBytes = new byte[32];
        _ = stream.Read(headerBytes, 0, headerBytes.Length);
        var header = PageHeader.ReadFrom(headerBytes);
        Assert.NotEqual(0u, header.NextPageId);
    }

    public void Dispose()
    {
        foreach (var path in _paths)
        {
            try
            {
                if (File.Exists(path))
                    File.Delete(path);

                var walPath = Path.ChangeExtension(path, ".wal");
                if (File.Exists(walPath))
                    File.Delete(walPath);
            }
            catch
            {
            }
        }
    }

    private string TrackTempFile(string extension)
    {
        var path = Path.Combine(Path.GetTempPath(), $"blite_collection_lifecycle_{Guid.NewGuid()}{extension}");
        _paths.Add(path);
        return path;
    }

    private sealed class InterfaceCollectionDbContext : DocumentDbContext
    {
        public IDocumentCollection<ObjectId, User> Users { get; set; } = null!;

        public InterfaceCollectionDbContext(string databasePath) : base(databasePath)
        {
            InitializeCollections();
        }

        protected override void InitializeCollections()
        {
            Users = CreateCollection(new BLite_Shared_UserMapper());
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<User>().ToCollection("users");
        }

        public override IDocumentCollection<TId, T> Set<TId, T>()
        {
            if (typeof(TId) == typeof(ObjectId) && typeof(T) == typeof(User))
                return (IDocumentCollection<TId, T>)(object)Users;

            return base.Set<TId, T>();
        }
    }
}
