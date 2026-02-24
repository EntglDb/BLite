using BLite.Shared;

namespace BLite.Tests;

public class AsyncTests : IDisposable
{
    private readonly string _dbPath;

    public AsyncTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_async_{Guid.NewGuid()}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(Path.ChangeExtension(_dbPath, ".wal"))) File.Delete(Path.ChangeExtension(_dbPath, ".wal"));
    }

    [Fact]
    public async Task Async_Transaction_Commit_Should_Persist_Data()
    {
        using (var db = new TestDbContext(_dbPath))
        {
            using (var txn = await db.BeginTransactionAsync())
            {
                db.AsyncDocs.Insert(new AsyncDoc { Id = 1, Name = "Async1" });
                db.AsyncDocs.Insert(new AsyncDoc { Id = 2, Name = "Async2" });
                await db.SaveChangesAsync();
            }
        }

        // Verify with new storage engine instance
        using var db2 = new TestDbContext(_dbPath);
        var doc1 = db2.AsyncDocs.FindById(1);
        Assert.NotNull(doc1);
        Assert.Equal("Async1", doc1.Name);

        var doc2 = db2.AsyncDocs.FindById(2);
        Assert.NotNull(doc2);
        Assert.Equal("Async2", doc2.Name);
    }

    [Fact]
    public async Task Async_Transaction_Rollback_Should_Discard_Data()
    {
        using var db = new TestDbContext(_dbPath);
        using (var txn = await db.BeginTransactionAsync())
        {
            db.AsyncDocs.Insert(new AsyncDoc { Id = 3, Name = "RollbackMe" });
        }

        var doc = db.AsyncDocs.FindById(3);
        Assert.Null(doc);
    }
    
    [Fact]
    public async Task Bulk_Async_Insert_Should_Persist_Data()
    {
        using var db = new TestDbContext(_dbPath);
        var docs = Enumerable.Range(1, 100).Select(i => new AsyncDoc { Id = i + 5000, Name = $"Bulk{i}" });
        
        var ids = await db.AsyncDocs.InsertBulkAsync(docs);
        
        Assert.Equal(100, ids.Count);
        
        var doc50 = db.AsyncDocs.FindById(5050);
        Assert.NotNull(doc50);
        Assert.Equal("Bulk50", doc50.Name);
    }

    [Fact]
    public async Task Bulk_Async_Update_Should_Persist_Changes()
    {
        using var db = new TestDbContext(_dbPath);
        // 1. Insert 100 docs
        var docs = Enumerable.Range(1, 100).Select(i => new AsyncDoc { Id = i + 6000, Name = $"Original{i}" }).ToList();
        await db.AsyncDocs.InsertBulkAsync(docs);
        
        // 2. Update all docs
        foreach (var doc in docs)
        {
            doc.Name = $"Updated{doc.Id - 6000}";
        }
        
        var count = await db.AsyncDocs.UpdateBulkAsync(docs);
        
        Assert.Equal(100, count);
        
        // 3. Verify updates
        var doc50 = db.AsyncDocs.FindById(6050);
        Assert.NotNull(doc50);
        Assert.Equal("Updated50", doc50.Name);
    }

    [Fact]
    public async Task High_Concurrency_Async_Commits()
    {
        using var db = new TestDbContext(Path.Combine(Path.GetTempPath(), $"blite_async_concurrency_{Guid.NewGuid()}.db"));
        int threadCount = 2;
        int docsPerThread = 50;
        
        var tasks = Enumerable.Range(0, threadCount).Select(async i => 
        {
            // Test mix of implicit and explicit transactions
            for (int j = 0; j < docsPerThread; j++)
            {
                int id = (i * docsPerThread) + j + 8000;
                await db.AsyncDocs.InsertAsync(new AsyncDoc { Id = id, Name = $"Thread{i}_Doc{j}" });
            }
        });

        await Task.WhenAll(tasks);
        await db.SaveChangesAsync();

        // Verify count
        var count = db.AsyncDocs.Scan(_ => true).Count();
        Assert.Equal(threadCount * docsPerThread, count);
    }
}
