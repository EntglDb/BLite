using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using Xunit;

namespace BLite.Tests;

public class AsyncTests : IDisposable
{
    private readonly string _dbPath;
    private readonly StorageEngine _storage;
    private readonly DocumentCollection<int, AsyncDoc> _collection;

    public AsyncTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_async_{Guid.NewGuid()}.db");
        _storage = new StorageEngine(_dbPath, PageFileConfig.Small); // Small pages to trigger overflow/splits easily
        _collection = new DocumentCollection<int, AsyncDoc>(_storage, new AsyncDocMapper());
    }

    public void Dispose()
    {
        _storage.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(Path.ChangeExtension(_dbPath, ".wal"))) File.Delete(Path.ChangeExtension(_dbPath, ".wal"));
    }

    [Fact]
    public async Task Async_Transaction_Commit_Should_Persist_Data()
    {
        using (var txn = await _storage.BeginTransactionAsync())
        {
            _collection.Insert(new AsyncDoc { Id = 1, Name = "Async1" }, txn);
            _collection.Insert(new AsyncDoc { Id = 2, Name = "Async2" }, txn);
            
            await txn.CommitAsync();
        }

        // Verify with new storage engine instance
        _storage.Dispose();
        using var storage2 = new StorageEngine(_dbPath, PageFileConfig.Small);
        var col2 = new DocumentCollection<int, AsyncDoc>(storage2, new AsyncDocMapper());

        var doc1 = col2.FindById(1);
        Assert.NotNull(doc1);
        Assert.Equal("Async1", doc1.Name);

        var doc2 = col2.FindById(2);
        Assert.NotNull(doc2);
        Assert.Equal("Async2", doc2.Name);
    }

    [Fact]
    public async Task Async_Transaction_Rollback_Should_Discard_Data()
    {
        using (var txn = await _storage.BeginTransactionAsync())
        {
            _collection.Insert(new AsyncDoc { Id = 3, Name = "RollbackMe" }, txn);
            // No CommitAsync call -> Auto Rollback on Dispose
        }

        var doc = _collection.FindById(3);
        Assert.Null(doc);
    }
    
    [Fact]
    public async Task Bulk_Async_Insert_Should_Persist_Data()
    {
        var docs = Enumerable.Range(1, 100).Select(i => new AsyncDoc { Id = i + 5000, Name = $"Bulk{i}" });
        
        var ids = await _collection.InsertBulkAsync(docs);
        
        Assert.Equal(100, ids.Count);
        
        var doc50 = _collection.FindById(5050);
        Assert.NotNull(doc50);
        Assert.Equal("Bulk50", doc50.Name);
    }

    [Fact]
    public async Task Bulk_Async_Update_Should_Persist_Changes()
    {
        // 1. Insert 100 docs
        var docs = Enumerable.Range(1, 100).Select(i => new AsyncDoc { Id = i + 6000, Name = $"Original{i}" }).ToList();
        await _collection.InsertBulkAsync(docs);
        
        // 2. Update all docs
        foreach (var doc in docs)
        {
            doc.Name = $"Updated{doc.Id - 6000}";
        }
        
        var count = await _collection.UpdateBulkAsync(docs);
        
        Assert.Equal(100, count);
        
        // 3. Verify updates
        var doc50 = _collection.FindById(6050);
        Assert.NotNull(doc50);
        Assert.Equal("Updated50", doc50.Name);
    }

    [Fact]
    public async Task High_Concurrency_Async_Commits()
    {
        int threadCount = 2;
        int docsPerThread = 50;
        
        var tasks = Enumerable.Range(0, threadCount).Select(async i => 
        {
            // Test mix of implicit and explicit transactions
            if (i % 2 == 0)
            {
                using var txn = await _storage.BeginTransactionAsync();
                for (int j = 0; j < docsPerThread; j++)
                {
                    int id = (i * docsPerThread) + j + 8000;
                    await _collection.InsertAsync(new AsyncDoc { Id = id, Name = $"Thread{i}_Doc{j}" }, txn);
                }
                await txn.CommitAsync();
            }
            else
            {
                // Implicit transactions with await
                for (int j = 0; j < docsPerThread; j++)
                {
                    int id = (i * docsPerThread) + j + 8000;
                    await _collection.InsertAsync(new AsyncDoc { Id = id, Name = $"Thread{i}_Doc{j}" });
                }
            }
        });
        
        await Task.WhenAll(tasks);
        
        // Verify count
        var count = _collection.Scan(_ => true).Count();
        Assert.Equal(threadCount * docsPerThread, count);
    }
}

public class AsyncDoc
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
}

public class AsyncDocMapper : IDocumentMapper<int, AsyncDoc>
{
    public string CollectionName => "AsyncDocs";
    public IEnumerable<string> UsedKeys => new[] { "_id", "Name" };

    public int GetId(AsyncDoc document) => document.Id;
    public void SetId(AsyncDoc document, int id) => document.Id = id;

    public int Serialize(AsyncDoc document, BLite.Bson.BsonSpanWriter writer)
    {
        var sizePos = writer.BeginDocument();
        writer.WriteInt32("_id", document.Id);
        writer.WriteString("name", document.Name);
        writer.EndDocument(sizePos);
        return writer.Position;
    }

    public AsyncDoc Deserialize(BLite.Bson.BsonSpanReader reader)
    {
        var doc = new AsyncDoc();
        reader.ReadDocumentSize(); // Read doc size header

        while (reader.Remaining > 0)
        {
            var type = reader.ReadBsonType();
            if (type == BLite.Bson.BsonType.EndOfDocument) break;
            
            var key = reader.ReadElementHeader();
            switch (key)
            {
                case "_id": doc.Id = reader.ReadInt32(); break;
                case "name": doc.Name = reader.ReadString(); break;
                default: reader.SkipValue(type); break;
            }
        }
        return doc;
    }
    
    public BLite.Bson.BsonSchema GetSchema() => new BLite.Bson.BsonSchema { Version = 1 };

    public BLite.Core.Indexing.IndexKey ToIndexKey(int id)
    {
        return new BLite.Core.Indexing.IndexKey(id);
    }

    public int FromIndexKey(BLite.Core.Indexing.IndexKey key)
    {
        return key.As<int>();
    }
}
