using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Tests.TestDbContext_TestDbContext_Mappers;
using Xunit;

namespace BLite.Tests;

public class InsertBulkTests : IDisposable
{
    private readonly string _testFile;
    private readonly StorageEngine _storage;
    private readonly DocumentCollection<User> _collection;

    public InsertBulkTests()
    {
        _testFile = Path.GetTempFileName();
        _storage = new StorageEngine(_testFile, PageFileConfig.Default);
        var mapper = new BLite_Tests_UserMapper();
        _collection = new DocumentCollection<User>(_storage, mapper);
    }

    public void Dispose()
    {
        _storage.Dispose();
    }

    [Fact]
    public void InsertBulk_PersistsData_ImmediatelyVisible()
    {
        var users = new List<User>();
        for (int i = 0; i < 50; i++)
        {
            users.Add(new User { Id = BLite.Bson.ObjectId.NewObjectId(), Name = $"User {i}", Age = 20 });
        }

        _collection.InsertBulk(users);

        var insertedUsers = _collection.FindAll().ToList();

        Assert.Equal(50, insertedUsers.Count);
    }
    
    [Fact]
    public void InsertBulk_SpanningMultiplePages_PersistsCorrectly()
    {
        // 16KB page. User ~50 bytes. 400 users -> ~20KB -> 2 pages.
        var users = new List<User>();
        for (int i = 0; i < 400; i++)
        {
            users.Add(new User { Id = BLite.Bson.ObjectId.NewObjectId(), Name = $"User {i} with some long padding text to ensure we fill space {new string('x', 50)}", Age = 20 });
        }

        _collection.InsertBulk(users);

        Assert.Equal(400, _collection.Count());
    }
}
