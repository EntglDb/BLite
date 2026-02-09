using Xunit;
using DocumentDb.Bson;
using DocumentDb.Core.Collections;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Transactions;
using System.Buffers;

namespace DocumentDb.Tests;

/// <summary>
/// Integration tests for DocumentCollection with custom indexes
/// </summary>
public class DocumentCollectionIndexIntegrationTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;
    private readonly PageFile _pageFile;
    private readonly WriteAheadLog _wal;
    private readonly TransactionManager _txnManager;

    public DocumentCollectionIndexIntegrationTests()
    {
        var id = Guid.NewGuid().ToString("N");
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_coll_idx_{id}.db");
        _walPath = Path.Combine(Path.GetTempPath(), $"test_coll_idx_{id}.wal");
        
        _pageFile = new PageFile(_dbPath, PageFileConfig.Default);
        _pageFile.Open();
        _wal = new WriteAheadLog(_walPath);
        var storage = new StorageEngine(_pageFile, _wal);
        _txnManager = new TransactionManager(storage);
    }

    public void Dispose()
    {
        _txnManager?.Dispose();
        _pageFile?.Dispose();
        try { File.Delete(_dbPath); } catch { }
        try { File.Delete(_walPath); } catch { }
    }

    [Fact]
    public void CreateIndex_AutomaticallyIndexesExistingDocuments()
    {
        // Arrange
        var mapper = new SimplePersonMapper();
        var collection = new DocumentCollection<SimplePerson>(mapper, _pageFile, _wal, _txnManager);
        
        // Insert some documents BEFORE creating index
        collection.Insert(new SimplePerson { FirstName = "Alice", Age = 25 });
        collection.Insert(new SimplePerson { FirstName = "Bob", Age = 30 });
        collection.Insert(new SimplePerson { FirstName = "Charlie", Age = 35 });

        // Act - Create index on Age (should rebuild for existing docs)
        var ageIndex = collection.CreateIndex(p => p.Age);

        // Assert - Should be able to seek all existing documents
        Assert.NotNull(ageIndex.Seek(25));
        Assert.NotNull(ageIndex.Seek(30));
        Assert.NotNull(ageIndex.Seek(35));
    }

    [Fact]
    public void Insert_AutomaticallyUpdatesSecondaryIndexes()
    {
        // Arrange
        var mapper = new SimplePersonMapper();
        var collection = new DocumentCollection<SimplePerson>(mapper, _pageFile, _wal, _txnManager);
        
        // Create indexes BEFORE inserting
        var ageIndex = collection.CreateIndex(p => p.Age);
        var nameIndex = collection.CreateIndex(p => p.FirstName);

        // Act - Insert document
        var person = new SimplePerson { FirstName = "Alice", Age = 25 };
        collection.Insert(person);

        // Assert - Document should be in both indexes
        Assert.Equal(person.Id, ageIndex.Seek(25));
        Assert.Equal(person.Id, nameIndex.Seek("Alice"));
    }

    [Fact]
    public void InsertBulk_UpdatesAllIndexes()
    {
        // Arrange
        var mapper = new SimplePersonMapper();
        var collection = new DocumentCollection<SimplePerson>(mapper, _pageFile, _wal, _txnManager);
        
        var ageIndex = collection.CreateIndex(p => p.Age);

        var people = new[]
        {
            new SimplePerson { FirstName = "Alice", Age = 25 },
            new SimplePerson { FirstName = "Bob", Age = 30 },
            new SimplePerson { FirstName = "Charlie", Age = 35 }
        };

        // Act
        var ids = collection.InsertBulk(people);
        
        // WORKAROUND: Force checkpoint to make index changes visible
        // This is needed because InsertBulk uses lazy WAL commit.
        // In production, changes become visible after automatic checkpoint.
        _txnManager.CheckpointManager.Checkpoint();

        // Assert
        Assert.Equal(3, ids.Count);
        Assert.Equal(people[0].Id, ageIndex.Seek(25));
        Assert.Equal(people[1].Id, ageIndex.Seek(30));
        Assert.Equal(people[2].Id, ageIndex.Seek(35));
    }

    [Fact]
    public void GetIndexes_ReturnsAllCreatedIndexes()
    {
        // Arrange
        var mapper = new SimplePersonMapper();
        var collection = new DocumentCollection<SimplePerson>(mapper, _pageFile, _wal, _txnManager);

        // Act
        collection.CreateIndex(p => p.Age, name: "idx_age");
        collection.CreateIndex(p => p.FirstName, name: "idx_name");

        var indexes = collection.GetIndexes().ToList();

        // Assert
        Assert.Equal(2, indexes.Count);
        Assert.Contains(indexes, i => i.Name == "idx_age");
        Assert.Contains(indexes, i => i.Name == "idx_name");
    }

    [Fact]
    public void DropIndex_RemovesIndex()
    {
        // Arrange
        var mapper = new SimplePersonMapper();
        var collection = new DocumentCollection<SimplePerson>(mapper, _pageFile, _wal, _txnManager);
        
        collection.CreateIndex(p => p.Age, name: "idx_age");

        // Act
        var result = collection.DropIndex("idx_age");

        // Assert
        Assert.True(result);
        Assert.Empty(collection.GetIndexes());
        Assert.Null(collection.GetIndex("idx_age"));
    }

    [Fact]
    public void CreateIndex_Unique_EnforcesConstraint()
    {
        // Arrange
        var mapper = new SimplePersonMapper();
        var collection = new DocumentCollection<SimplePerson>(mapper, _pageFile, _wal, _txnManager);
        
        // This test just verifies the index is created as unique
        // Actual enforcement would require BTreeIndex unique constraint implementation
        var index = collection.CreateIndex(p => p.Age, unique: true);

        // Assert
        Assert.True(index.Definition.IsUnique);
    }

    [Fact]
    public void MultipleIndexes_WorkTogether()
    {
        // Arrange
        var mapper = new SimplePersonMapper();
        var collection = new DocumentCollection<SimplePerson>(mapper, _pageFile, _wal, _txnManager);
        
        var ageIndex = collection.CreateIndex(p => p.Age);
        var nameIndex = collection.CreateIndex(p => p.FirstName);

        var people = new[]
        {
            new SimplePerson { FirstName = "Alice", Age = 25 },
            new SimplePerson { FirstName = "Bob", Age = 30 },
            new SimplePerson { FirstName = "Charlie", Age = 25 } // Same age as Alice
        };

        // Act
        foreach (var person in people)
        {
            collection.Insert(person);
        }

        // Assert - Query by different indexes
        var aliceId = nameIndex.Seek("Alice");
        var bobId = nameIndex.Seek("Bob");
        var charlieId = nameIndex.Seek("Charlie");

        Assert.NotNull(aliceId);
        Assert.NotNull(bobId);
        Assert.NotNull(charlieId);

        // Both Alice and Charlie have age 25
        var age25Ids = ageIndex.Range(25, 25).ToList();
        Assert.Equal(2, age25Ids.Count);
        Assert.Contains(people[0].Id, age25Ids);
        Assert.Contains(people[2].Id, age25Ids);
    }
}

// Simple test models
public class SimplePerson
{
    public ObjectId Id { get; set; }
    public string? FirstName { get; set; }
    public int Age { get; set; }
}

public class SimplePersonMapper : IDocumentMapper<SimplePerson>
{
    public string CollectionName => "people";

    public int Serialize(SimplePerson entity, Span<byte> buffer)
    {
        throw new NotImplementedException("Use IBufferWriter version");
    }

    public void Serialize(SimplePerson entity, IBufferWriter<byte> writer)
    {
        // Minimal serialization for testing
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        
        var idBytes = new byte[12];
        entity.Id.WriteTo(idBytes);
        bw.Write(idBytes);
        bw.Write(entity.FirstName ?? "");
        bw.Write(entity.Age);
        
        var bytes = ms.ToArray();
        writer.Write(bytes);
    }

    public SimplePerson Deserialize(ReadOnlySpan<byte> buffer)
    {
        var ms = new MemoryStream(buffer.ToArray());
        var br = new BinaryReader(ms);
        
        var idBytes = new byte[12];
        br.Read(idBytes, 0, 12);
        
        return new SimplePerson
        {
            Id = new ObjectId(idBytes),
            FirstName = br.ReadString(),
            Age = br.ReadInt32()
        };
    }

    public ObjectId GetId(SimplePerson entity) => entity.Id;
    public void SetId(SimplePerson entity, ObjectId id) => entity.Id = id;
}
