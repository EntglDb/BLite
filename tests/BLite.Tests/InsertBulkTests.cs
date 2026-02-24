using BLite.Shared;

namespace BLite.Tests;

public class InsertBulkTests : IDisposable
{
    private readonly string _testFile;
    private readonly TestDbContext _db;

    public InsertBulkTests()
    {
        _testFile = Path.GetTempFileName();
        _db = new TestDbContext(_testFile);
    }

    public void Dispose()
    {
        _db.Dispose();
    }

    [Fact]
    public void InsertBulk_PersistsData_ImmediatelyVisible()
    {
        var users = new List<User>();
        for (int i = 0; i < 50; i++)
        {
            users.Add(new User { Id = BLite.Bson.ObjectId.NewObjectId(), Name = $"User {i}", Age = 20 });
        }

        _db.Users.InsertBulk(users);
        _db.SaveChanges();

        var insertedUsers = _db.Users.FindAll().ToList();

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

        _db.Users.InsertBulk(users);
        _db.SaveChanges();

        Assert.Equal(400, _db.Users.Count());
    }
}
