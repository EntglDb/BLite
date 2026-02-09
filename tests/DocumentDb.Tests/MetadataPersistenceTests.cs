using DocumentDb.Core.Collections;
using DocumentDb.Core.Indexing;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Transactions;
using Xunit;

namespace DocumentDb.Tests;

public class MetadataPersistenceTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;

    public MetadataPersistenceTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"docdb_meta_{Guid.NewGuid()}.db");
        _walPath = Path.ChangeExtension(_dbPath, ".wal");
    }

    [Fact]
    public void IndexDefinitions_ArePersisted_AndReloaded()
    {
        // 1. Create index in first session
        using (var storage = new StorageEngine(_dbPath, PageFileConfig.Default))
        {
            // Disable auto-checkpoint to ensure cleaner test tracing, though not strictly required
            // storage.CheckpointManager.StopAutoCheckpoint();

            var mapper = new UserMapper();
            var indexManager = new CollectionIndexManager<User>(storage, mapper);
            
            // Create 2 indexes
            indexManager.CreateIndex(u => u.Age, "idx_age");
            indexManager.CreateIndex(u => u.Name, unique: true); // name auto-generated
        }

        // 2. Re-open storage and verify indexes exist
        using (var storage = new StorageEngine(_dbPath, PageFileConfig.Default))
        {
            var mapper = new UserMapper();
            var indexManager = new CollectionIndexManager<User>(storage, mapper);
            
            var indexes = indexManager.GetAllIndexes().ToList();
            
            Assert.Equal(2, indexes.Count);
            
            var ageIdx = indexManager.GetIndex("idx_age");
            Assert.NotNull(ageIdx);
            Assert.False(ageIdx.Definition.IsUnique);
            Assert.Single(ageIdx.Definition.PropertyPaths);
            Assert.Equal("Age", ageIdx.Definition.PropertyPaths[0]);
            
            // Check auto-generated name index
            var nameIdx = indexes.FirstOrDefault(i => i.Definition.PropertyPaths[0] == "Name");
            Assert.NotNull(nameIdx);
            Assert.True(nameIdx.Definition.IsUnique);
        }
    }
    
    [Fact]
    public void EnsureIndex_DoesNotRecreate_IfIndexExists()
    {
        // 1. Create index
        using (var storage = new StorageEngine(_dbPath, PageFileConfig.Default))
        {
            var txnMgr = new TransactionManager(storage);
            var mapper = new UserMapper();
            var collection = new DocumentCollection<User>(mapper, storage, txnMgr);
            
            collection.EnsureIndex(u => u.Age);
        }
        
        // 2. Re-open and EnsureIndex again - should be fast/no-op
        using (var storage = new StorageEngine(_dbPath, PageFileConfig.Default))
        {
            var txnMgr = new TransactionManager(storage);
            var mapper = new UserMapper();
            var collection = new DocumentCollection<User>(mapper, storage, txnMgr);
            
            // Use reflection or diagnostic to check if it triggered rebuild?
            // Currently hard to verify "no rebuild" without logs or mocking.
            // But we can verify it doesn't throw and index is still valid.
            
            var idx = collection.EnsureIndex(u => u.Age);
            Assert.NotNull(idx);
            
            // Verify functioning
            using var txn = txnMgr.BeginTransaction();
            collection.Insert(new User { Name = "Bob", Age = 50 }, txn);
            txn.Commit();
            
            // Should find it via index
            var results = collection.Find(u => u.Age == 50).ToList();
            Assert.Single(results);
        }
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_walPath)) File.Delete(_walPath);
    }
}
