using DocumentDb.Core.Storage;
using DocumentDb.Core.Collections;
using DocumentDb.Bson;
using DocumentDb.Core.Transactions;
using Xunit;

namespace DocumentDb.Tests;

public class InsertBulkDebugTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;
    private readonly PageFile _pageFile;
    private readonly TransactionManager _txnManager;
    private readonly DocumentCollection<User> _collection;

    public InsertBulkDebugTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_debug_{Guid.NewGuid()}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"test_debug_{Guid.NewGuid()}.wal");
        
        _pageFile = new PageFile(_dbPath, PageFileConfig.Default);
        _pageFile.Open();
        
        _txnManager = new TransactionManager(_walPath, _pageFile);
        
        var mapper = new UserMapper();
        _collection = new DocumentCollection<User>(mapper, _pageFile, _txnManager);
    }

    public void Dispose()
    {
        _pageFile.Dispose();
        _txnManager.Dispose();
        
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);
    }

    [Fact]
    public void InsertBulk_RespectsIds()
    {
        var id = ObjectId.NewObjectId();
        var user = new User { Id = id, Name = "Debug", Age = 99 };
        var users = new List<User> { user };

        _collection.InsertBulk(users);

        var stored = _collection.FindById(id);
        Assert.NotNull(stored);
        Assert.Equal(id, stored.Id);
    }
}
