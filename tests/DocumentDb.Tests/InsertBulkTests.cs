using DocumentDb.Core;
using DocumentDb.Core.Collections;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Transactions;
using Xunit;

namespace DocumentDb.Tests;

public class InsertBulkTests : IDisposable
{
    private readonly string _testFile;
    private readonly PageFile _pageFile;
    private readonly TransactionManager _txnManager;
    private readonly DocumentCollection<User> _collection;

    public InsertBulkTests()
    {
        _testFile = Path.GetTempFileName();
        _pageFile = new PageFile(_testFile, PageFileConfig.Default); // Use Default config (16KB)
        _pageFile.Open();
        _txnManager = new TransactionManager(_testFile + ".wal", _pageFile); // Correct arg order
        var mapper = new UserMapper();
        _collection = new DocumentCollection<User>(mapper, _pageFile, _txnManager);
    }

    public void Dispose()
    {
        _txnManager.Dispose();
        _pageFile.Dispose();
        if (File.Exists(_testFile)) File.Delete(_testFile);
        if (File.Exists(_testFile + ".wal")) File.Delete(_testFile + ".wal");
    }

    [Fact]
    public void InsertBulk_PersistsData_ImmediatelyVisible()
    {
        var users = new List<User>();
        for (int i = 0; i < 50; i++)
        {
            users.Add(new User { Id = DocumentDb.Bson.ObjectId.NewObjectId(), Name = $"User {i}", Age = 20 });
        }

        _collection.InsertBulk(users);

        // Verify all exist
        foreach (var u in users)
        {
            var stored = _collection.FindById(u.Id);
            Assert.NotNull(stored);
            Assert.Equal(u.Name, stored.Name);
        }
    }
    
    [Fact]
    public void InsertBulk_SpanningMultiplePages_PersistsCorrectly()
    {
        // 16KB page. User ~50 bytes. 400 users -> ~20KB -> 2 pages.
        var users = new List<User>();
        for (int i = 0; i < 400; i++)
        {
            users.Add(new User { Id = DocumentDb.Bson.ObjectId.NewObjectId(), Name = $"User {i} with some long padding text to ensure we fill space {new string('x', 50)}", Age = 20 });
        }

        _collection.InsertBulk(users);

        Assert.Equal(400, _collection.Count());
        
        foreach (var u in users)
        {
            var stored = _collection.FindById(u.Id);
            Assert.NotNull(stored);
        }
    }
}
