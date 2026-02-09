using Xunit;
using DocumentDb.Bson;
using DocumentDb.Core.Indexing;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Collections;
using System.Buffers;
using DocumentDb.Core.Transactions;

namespace DocumentDb.Tests;

// Test model classes
public class Person
{
    public ObjectId Id { get; set; }
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public int Age { get; set; }
    public Address HomeAddress { get; set; } = new();
}

public class Address
{
    public string? Street { get; set; }
    public string? City { get; set; }
    public string? ZipCode { get; set; }
}

// PersonMapper for tests (simplified)
public class PersonMapper : IDocumentMapper<Person>
{
    public string CollectionName => "people";

    public int Serialize(Person entity, Span<byte> buffer)
    {
        throw new NotImplementedException("Use IBufferWriter version");
    }

    public void Serialize(Person entity, IBufferWriter<byte> writer)
    {
        // Minimal BSON-like serialization for testing
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);

        // Write ID
        var idBytes = new byte[12];
        entity.Id.WriteTo(idBytes);
        bw.Write(idBytes);

        // Write other fields
        bw.Write(entity.FirstName ?? "");
        bw.Write(entity.LastName ?? "");
        bw.Write(entity.Age);

        var bytes = ms.ToArray();
        writer.Write(bytes);
    }

    public Person Deserialize(ReadOnlySpan<byte> buffer)
    {
        // Minimal deserialization for testing
        var ms = new MemoryStream(buffer.ToArray());
        var br = new BinaryReader(ms);

        var idBytes = new byte[12];
        br.Read(idBytes, 0, 12);

        return new Person
        {
            Id = new ObjectId(idBytes),
            FirstName = br.ReadString(),
            LastName = br.ReadString(),
            Age = br.ReadInt32()
        };
    }

    public ObjectId GetId(Person entity) => entity.Id;
    public void SetId(Person entity, ObjectId id) => entity.Id = id;
}

/// <summary>
/// Tests for the custom index system (CollectionIndexManager, CollectionSecondaryIndex, etc.)
/// Tests in isolation before DocumentCollection integration.
/// </summary>
public class CustomIndexTests : IDisposable
{
    private readonly string _dbPath;
    private readonly PersonMapper _mapper;
    private readonly StorageEngine _storageEngine;
    private readonly TransactionManager _transactionManager;

    public CustomIndexTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_custom_index_{Guid.NewGuid():N}.db");
        var _pageFile = new PageFile(_dbPath, PageFileConfig.Default);
        _pageFile.Open();
        _mapper = new PersonMapper();
        var _wal = new WriteAheadLog(Path.ChangeExtension(_dbPath, ".wal"));
        _storageEngine = new StorageEngine(_pageFile, _wal);
        _transactionManager = new TransactionManager(_storageEngine);   
    }

    public void Dispose()
    {
        _storageEngine?.Dispose();
        try { File.Delete(_dbPath); } catch { }
    }

    #region CollectionIndexDefinition Tests

    [Fact]
    public void IndexDefinition_SimpleProperty_ExtractsCorrectly()
    {
        // Arrange
        var definition = new CollectionIndexDefinition<Person>(
            name: "idx_age",
            propertyPaths: new[] { "Age" },
            keySelectorExpression: p => p.Age,
            isUnique: false);

        // Assert
        Assert.Equal("idx_age", definition.Name);
        Assert.Single(definition.PropertyPaths);
        Assert.Equal("Age", definition.PropertyPaths[0]);
        Assert.False(definition.IsUnique);
        Assert.False(definition.IsPrimary);
    }

    [Fact]
    public void IndexDefinition_CompoundIndex_ExtractsCorrectly()
    {
        // Arrange
        var definition = new CollectionIndexDefinition<Person>(
            name: "idx_city_age",
            propertyPaths: new[] { "City", "Age" },
            keySelectorExpression: p => new { p.HomeAddress.City, p.Age },
            isUnique: false);

        // Assert
        Assert.Equal("idx_city_age", definition.Name);
        Assert.Equal(2, definition.PropertyPaths.Length);
        Assert.Equal("City", definition.PropertyPaths[0]);
        Assert.Equal("Age", definition.PropertyPaths[1]);
    }

    [Fact]
    public void IndexDefinition_CanSupportQuery_SimpleIndex()
    {
        // Arrange
        var definition = new CollectionIndexDefinition<Person>(
            name: "idx_age",
            propertyPaths: new[] { "Age" },
            keySelectorExpression: p => p.Age);

        // Act & Assert
        Assert.True(definition.CanSupportQuery("Age"));
        Assert.True(definition.CanSupportQuery("age")); // Case insensitive
        Assert.False(definition.CanSupportQuery("City"));
    }

    [Fact]
    public void IndexDefinition_CanSupportQuery_CompoundIndex()
    {
        // Arrange
        var definition = new CollectionIndexDefinition<Person>(
            name: "idx_city_age",
            propertyPaths: new[] { "City", "Age" },
            keySelectorExpression: p => new { p.HomeAddress.City, p.Age });

        // Act & Assert
        Assert.True(definition.CanSupportQuery("City")); // First component
        Assert.False(definition.CanSupportQuery("Age")); // Not first component
        Assert.True(definition.CanSupportCompoundQuery(new[] { "City" }));
        Assert.True(definition.CanSupportCompoundQuery(new[] { "City", "Age" }));
        Assert.False(definition.CanSupportCompoundQuery(new[] { "Age", "City" })); // Wrong order
    }

    [Fact]
    public void IndexDefinition_KeySelector_CompiledCorrectly()
    {
        // Arrange
        var definition = new CollectionIndexDefinition<Person>(
            name: "idx_age",
            propertyPaths: new[] { "Age" },
            keySelectorExpression: p => p.Age);

        var person = new Person { Age = 30 };

        // Act
        var result = definition.KeySelector(person);

        // Assert
        Assert.Equal(30, result);
    }

    [Fact]
    public void IndexDefinition_ToIndexOptions_ConvertsCorrectly()
    {
        // Arrange
        var definition = new CollectionIndexDefinition<Person>(
            name: "idx_age",
            propertyPaths: new[] { "Age" },
            keySelectorExpression: p => p.Age,
            isUnique: true,
            type: IndexType.BTree);

        // Act
        var options = definition.ToIndexOptions();

        // Assert
        Assert.Equal(IndexType.BTree, options.Type);
        Assert.True(options.Unique);
        Assert.Single(options.Fields);
        Assert.Equal("Age", options.Fields[0]);
    }

    #endregion

    #region CollectionSecondaryIndex Tests

    [Fact]
    public void SecondaryIndex_Insert_AddsToIndex()
    {
        // Arrange
        var definition = new CollectionIndexDefinition<Person>(
            "idx_age",
            new[] { "Age" },
            p => p.Age);

        var index = new CollectionSecondaryIndex<Person>(definition, _storageEngine, _mapper);

        var person = new Person
        {
            Id = ObjectId.NewObjectId(),
            FirstName = "John",
            Age = 30
        };

        using (var transaction = _transactionManager.BeginTransaction())
        {
            // Act
            index.Insert(person, transaction);

            // Assert - Seek should find the document
            var foundId = index.Seek(30, transaction);
            Assert.NotNull(foundId);
            Assert.Equal(person.Id, foundId.Value);
        }
    }

    [Fact]
    public void SecondaryIndex_Insert_MultipleDocuments()
    {
        // Arrange
        var definition = new CollectionIndexDefinition<Person>(
            "idx_age",
            new[] { "Age" },
            p => p.Age);

        var index = new CollectionSecondaryIndex<Person>(definition, _storageEngine, _mapper);

        var person1 = new Person { Id = ObjectId.NewObjectId(), Age = 25 };
        var person2 = new Person { Id = ObjectId.NewObjectId(), Age = 30 };
        var person3 = new Person { Id = ObjectId.NewObjectId(), Age = 35 };

        using (var transaction = _transactionManager.BeginTransaction())
        {
            // Act
            index.Insert(person1, transaction);
            index.Insert(person2, transaction);
            index.Insert(person3, transaction);

            // Assert
            Assert.Equal(person1.Id, index.Seek(25, transaction));
            Assert.Equal(person2.Id, index.Seek(30, transaction));
            Assert.Equal(person3.Id, index.Seek(35, transaction));
        }
    }

    [Fact]
    public void SecondaryIndex_Range_ReturnsDocumentsInOrder()
    {
        // Arrange
        var definition = new CollectionIndexDefinition<Person>(
            "idx_age",
            new[] { "Age" },
            p => p.Age);

        var index = new CollectionSecondaryIndex<Person>(definition, _storageEngine, _mapper);

        var person1 = new Person { Id = ObjectId.NewObjectId(), Age = 25 };
        var person2 = new Person { Id = ObjectId.NewObjectId(), Age = 30 };
        var person3 = new Person { Id = ObjectId.NewObjectId(), Age = 35 };
        var person4 = new Person { Id = ObjectId.NewObjectId(), Age = 40 };

        using (var transaction = _transactionManager.BeginTransaction())
        {
            index.Insert(person1, transaction);
            index.Insert(person2, transaction);
            index.Insert(person3, transaction);
            index.Insert(person4, transaction);

            // Act - Range query: 28 <= age <= 36
            var results = index.Range(28, 36, transaction).ToList();

            // Assert
            Assert.Equal(2, results.Count);
            Assert.Contains(person2.Id, results);
            Assert.Contains(person3.Id, results);
        }
    }

    [Fact]
    public void SecondaryIndex_Delete_RemovesFromIndex()
    {
        // Arrange
        var definition = new CollectionIndexDefinition<Person>(
            "idx_age",
            new[] { "Age" },
            p => p.Age);

        var index = new CollectionSecondaryIndex<Person>(definition, _storageEngine, _mapper);

        var person = new Person { Id = ObjectId.NewObjectId(), Age = 30 };

        using (var transaction = _transactionManager.BeginTransaction())
        {
            index.Insert(person, transaction);

            // Act
            index.Delete(person, transaction);

            // Assert
            var foundId = index.Seek(30, transaction);
            Assert.Null(foundId);
        }
    }

    [Fact]
    public void SecondaryIndex_Update_UpdatesIndex()
    {
        // Arrange
        var definition = new CollectionIndexDefinition<Person>(
            "idx_age",
            new[] { "Age" },
            p => p.Age);

        var index = new CollectionSecondaryIndex<Person>(definition, _storageEngine, _mapper);

        var oldPerson = new Person { Id = ObjectId.NewObjectId(), Age = 30 };
        var newPerson = new Person { Id = oldPerson.Id, Age = 35 };

        using (var transaction = _transactionManager.BeginTransaction())
        {
            index.Insert(oldPerson, transaction);

            // Act
            index.Update(oldPerson, newPerson, transaction);

            // Assert
            Assert.Null(index.Seek(30, transaction)); // Old value removed
            Assert.Equal(newPerson.Id, index.Seek(35, transaction)); // New value added
        }
    }

    [Fact]
    public void SecondaryIndex_Update_SameKey_NoChange()
    {
        // Arrange
        var definition = new CollectionIndexDefinition<Person>(
            "idx_age",
            new[] { "Age" },
            p => p.Age);

        var index = new CollectionSecondaryIndex<Person>(definition, _storageEngine, _mapper);

        var oldPerson = new Person { Id = ObjectId.NewObjectId(), FirstName = "John", Age = 30 };
        var newPerson = new Person { Id = oldPerson.Id, FirstName = "Jane", Age = 30 }; // Same age

        using (var transaction = _transactionManager.BeginTransaction())
        {
            index.Insert(oldPerson, transaction);

            // Act - Should optimize and not touch index
            index.Update(oldPerson, newPerson, transaction);

            // Assert
            Assert.Equal(newPerson.Id, index.Seek(30, transaction)); // Still works
        }
    }

    #endregion

    #region CollectionIndexManager Tests

    [Fact]
    public void IndexManager_CreateIndex_Success()
    {
        // Arrange
        var manager = new CollectionIndexManager<Person>(_storageEngine, _mapper);

        // Act
        var index = manager.CreateIndex(p => p.Age);

        // Assert
        Assert.NotNull(index);
        Assert.Equal("idx_Age", index.Definition.Name);
    }

    [Fact]
    public void IndexManager_CreateIndex_CustomName()
    {
        // Arrange
        var manager = new CollectionIndexManager<Person>(_storageEngine, _mapper);

        // Act
        var index = manager.CreateIndex(p => p.Age, name: "my_age_index");

        // Assert
        Assert.Equal("my_age_index", index.Definition.Name);
    }

    [Fact]
    public void IndexManager_CreateIndex_Unique()
    {
        // Arrange
        var manager = new CollectionIndexManager<Person>(_storageEngine, _mapper);

        // Act
        var index = manager.CreateIndex(p => p.Age, unique: true);

        // Assert
        Assert.True(index.Definition.IsUnique);
    }

    [Fact]
    public void IndexManager_CreateIndex_DuplicateName_Throws()
    {
        // Arrange
        var manager = new CollectionIndexManager<Person>(_storageEngine, _mapper);
        manager.CreateIndex(p => p.Age, name: "idx_age");

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() =>
            manager.CreateIndex(p => p.FirstName, name: "idx_age"));
    }

    [Fact]
    public void IndexManager_DropIndex_Success()
    {
        // Arrange
        var manager = new CollectionIndexManager<Person>(_storageEngine, _mapper);
        manager.CreateIndex(p => p.Age, name: "idx_age");

        // Act
        var result = manager.DropIndex("idx_age");

        // Assert
        Assert.True(result);
        Assert.Null(manager.GetIndex("idx_age"));
    }

    [Fact]
    public void IndexManager_DropIndex_NotFound_ReturnsFalse()
    {
        // Arrange
        var manager = new CollectionIndexManager<Person>(_storageEngine, _mapper);

        // Act
        var result = manager.DropIndex("nonexistent");

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void IndexManager_GetAllIndexes_ReturnsAll()
    {
        // Arrange
        var manager = new CollectionIndexManager<Person>(_storageEngine, _mapper);
        manager.CreateIndex(p => p.Age);
        manager.CreateIndex(p => p.FirstName);
        manager.CreateIndex(p => p.LastName);

        // Act
        var indexes = manager.GetAllIndexes().ToList();

        // Assert
        Assert.Equal(3, indexes.Count);
    }

    [Fact]
    public void IndexManager_FindBestIndex_SimpleQuery()
    {
        // Arrange
        var manager = new CollectionIndexManager<Person>(_storageEngine, _mapper);
        manager.CreateIndex(p => p.Age);
        manager.CreateIndex(p => p.FirstName);

        // Act
        var index = manager.FindBestIndex("Age");

        // Assert
        Assert.NotNull(index);
        Assert.Equal("idx_Age", index.Definition.Name);
    }

    [Fact]
    public void IndexManager_FindBestIndex_PreferUnique()
    {
        // Arrange
        var manager = new CollectionIndexManager<Person>(_storageEngine, _mapper);
        manager.CreateIndex(p => p.Age, name: "idx_age_non_unique", unique: false);
        manager.CreateIndex(p => p.Age, name: "idx_age_unique", unique: true);

        // Act
        var index = manager.FindBestIndex("Age");

        // Assert
        Assert.NotNull(index);
        Assert.Equal("idx_age_unique", index.Definition.Name);
    }

    [Fact]
    public void IndexManager_FindBestIndex_NoMatch_ReturnsNull()
    {
        // Arrange
        var manager = new CollectionIndexManager<Person>(_storageEngine, _mapper);
        manager.CreateIndex(p => p.Age);

        // Act
        var index = manager.FindBestIndex("NonExistentProperty");

        // Assert
        Assert.Null(index);
    }

    [Fact]
    public void IndexManager_InsertIntoAll_UpdatesAllIndexes()
    {
        // Arrange
        var manager = new CollectionIndexManager<Person>(_storageEngine, _mapper);
        var ageIndex = manager.CreateIndex(p => p.Age);
        var nameIndex = manager.CreateIndex(p => p.FirstName);

        var person = new Person
        {
            Id = ObjectId.NewObjectId(),
            FirstName = "John",
            Age = 30
        };

        using (var transaction = _transactionManager.BeginTransaction())
        {
            // Act
            manager.InsertIntoAll(person, transaction);

            // Assert
            Assert.Equal(person.Id, ageIndex.Seek(30, transaction));
            Assert.Equal(person.Id, nameIndex.Seek("John", transaction));
        }
    }

    [Fact]
    public void IndexManager_DeleteFromAll_RemovesFromAllIndexes()
    {
        // Arrange
        var manager = new CollectionIndexManager<Person>(_storageEngine, _mapper);
        var ageIndex = manager.CreateIndex(p => p.Age);
        var nameIndex = manager.CreateIndex(p => p.FirstName);

        var person = new Person
        {
            Id = ObjectId.NewObjectId(),
            FirstName = "John",
            Age = 30
        };

        using (var transaction = _transactionManager.BeginTransaction())
        {
            manager.InsertIntoAll(person, transaction);

            // Act
            manager.DeleteFromAll(person, transaction);

            // Assert
            Assert.Null(ageIndex.Seek(30, transaction));
            Assert.Null(nameIndex.Seek("John", transaction));
        }
    }

    #endregion

    #region ExpressionAnalyzer Tests

    [Fact]
    public void ExpressionAnalyzer_SimpleProperty_ExtractsCorrectly()
    {
        // Act
        var paths = ExpressionAnalyzer.ExtractPropertyPaths<Person, int>(p => p.Age);

        // Assert
        Assert.Single(paths);
        Assert.Equal("Age", paths[0]);
    }

    [Fact]
    public void ExpressionAnalyzer_CompoundKey_ExtractsCorrectly()
    {
        // Act
        var paths = ExpressionAnalyzer.ExtractPropertyPaths<Person, object>(
            p => new { p.FirstName, p.Age });

        // Assert
        Assert.Equal(2, paths.Length);
        Assert.Equal("FirstName", paths[0]);
        Assert.Equal("Age", paths[1]);
    }

    [Fact]
    public void ExpressionAnalyzer_InvalidExpression_Throws()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
            ExpressionAnalyzer.ExtractPropertyPaths<Person, int>(p => p.Age + 10));
    }

    #endregion

    #region Integration Tests

    [Fact]
    public void Integration_MultipleIndexes_WorkTogether()
    {
        // Arrange
        var manager = new CollectionIndexManager<Person>(_storageEngine, _mapper);
        manager.CreateIndex(p => p.Age);
        manager.CreateIndex(p => p.FirstName);
        manager.CreateIndex(p => p.LastName);

        var people = new[]
        {
            new Person { Id = ObjectId.NewObjectId(), FirstName = "Alice", LastName = "Smith", Age = 25 },
            new Person { Id = ObjectId.NewObjectId(), FirstName = "Bob", LastName = "Jones", Age = 30 },
            new Person { Id = ObjectId.NewObjectId(), FirstName = "Charlie", LastName = "Brown", Age = 35 }
        };

        using (var transaction = _transactionManager.BeginTransaction())
        {
            // Act - Insert all
            foreach (var person in people)
            {
                manager.InsertIntoAll(person, transaction);
            }

            // Assert - Query by different indexes
            var ageIndex = manager.GetIndex("idx_Age")!;
            var nameIndex = manager.GetIndex("idx_FirstName")!;
            var lastNameIndex = manager.GetIndex("idx_LastName")!;

            Assert.Equal(people[0].Id, ageIndex.Seek(25, transaction));
            Assert.Equal(people[1].Id, nameIndex.Seek("Bob", transaction));
            Assert.Equal(people[2].Id, lastNameIndex.Seek("Brown", transaction));
        }
    }

    [Fact]
    public void Integration_RangeQuery_MultipleDocs()
    {
        // Arrange
        var manager = new CollectionIndexManager<Person>(_storageEngine, _mapper);
        var ageIndex = manager.CreateIndex(p => p.Age);

        var people = Enumerable.Range(1, 100)
            .Select(i => new Person { Id = ObjectId.NewObjectId(), Age = i })
            .ToArray();

        using (var transaction = _transactionManager.BeginTransaction())
        {
            foreach (var person in people)
            {
                manager.InsertIntoAll(person, transaction);
            }

            // Act - Query ages 40-50
            var results = ageIndex.Range(40, 50, transaction).ToList();

            // Assert
            Assert.Equal(11, results.Count); // 40, 41, ..., 50

            // Verify all IDs in range
            var expectedIds = people.Skip(39).Take(11).Select(p => p.Id).ToHashSet();
            Assert.True(results.All(id => expectedIds.Contains(id)));
        }
    }

    #endregion
}
