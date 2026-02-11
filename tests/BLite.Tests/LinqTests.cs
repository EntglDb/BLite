using BLite.Core;
using BLite.Core.Storage;
using BLite.Core.Collections;
using BLite.Core.Indexing;
using BLite.Bson;
using Xunit;
using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;

namespace BLite.Tests
{
    public class LinqTests : IDisposable
    {
        private readonly string _testFile;
        private readonly StorageEngine _storage;
        private readonly DocumentCollection<TestDocument> _collection;

        public class TestDocument
        {
            public ObjectId Id { get; set; }
            public string? Name { get; set; }
            public int Age { get; set; }
        }

        public class TestDocumentMapper : ObjectIdMapperBase<TestDocument>
        {
            public override string CollectionName => "users";
            public override ObjectId GetId(TestDocument entity) => entity.Id;
            public override void SetId(TestDocument entity, ObjectId id) => entity.Id = id;

            public override int Serialize(TestDocument entity, Span<byte> destination)
            {
                var writer = new BsonSpanWriter(destination);
                var sizePos = writer.BeginDocument();
                writer.WriteObjectId("_id", entity.Id);
                if (entity.Name != null)
                    writer.WriteString("Name", entity.Name);
                else
                    writer.WriteNull("Name");
                writer.WriteInt32("Age", entity.Age);
                writer.EndDocument(sizePos);
                return writer.Position;
            }

            public override TestDocument Deserialize(ReadOnlySpan<byte> source)
            {
                var reader = new BsonSpanReader(source);
                reader.ReadDocumentSize();
                var doc = new TestDocument();

                while (reader.Remaining > 0)
                {
                    var type = reader.ReadBsonType();
                    if (type == 0) break;

                    var name = reader.ReadCString();

                    if (name == "_id") doc.Id = reader.ReadObjectId();
                    else if (name == "Name") doc.Name = reader.ReadString();
                    else if (name == "Age") doc.Age = reader.ReadInt32();
                    else reader.SkipValue(type);
                }
                return doc;
            }

            public override IndexKey ToIndexKey(ObjectId id) => new IndexKey(id);
            public override ObjectId FromIndexKey(IndexKey key) => new ObjectId(key.Data);
        }

        public LinqTests()
        {
            _testFile = Path.Combine(Path.GetTempPath(), $"linq_tests_{Guid.NewGuid()}.db");
            if (File.Exists(_testFile)) File.Delete(_testFile);
            var wal = Path.ChangeExtension(_testFile, ".wal");
            if (File.Exists(wal)) File.Delete(wal);

            var config = new PageFileConfig { PageSize = 4096, InitialFileSize = 4096 * 100 };
            _storage = new StorageEngine(_testFile, config);
            
            var mapper = new TestDocumentMapper();
            _collection = new DocumentCollection<TestDocument>(_storage, mapper, "users");

            // Seed Data
            _collection.Insert(new TestDocument { Name = "Alice", Age = 30 });
            _collection.Insert(new TestDocument { Name = "Bob", Age = 25 });
            _collection.Insert(new TestDocument { Name = "Charlie", Age = 35 });
            _collection.Insert(new TestDocument { Name = "Dave", Age = 20 });
            _collection.Insert(new TestDocument { Name = "Eve", Age = 40 });
        }

        public void Dispose()
        {
            _storage.Dispose();
            if (File.Exists(_testFile)) File.Delete(_testFile);
            var wal = Path.ChangeExtension(_testFile, ".wal");
            if (File.Exists(wal)) File.Delete(wal);
        }

        [Fact]
        public void Where_FiltersDocuments()
        {
            var query = _collection.AsQueryable().Where(x => x.Age > 28);
            var results = query.ToList();

            Assert.Equal(3, results.Count); // Alice(30), Charlie(35), Eve(40)
            Assert.DoesNotContain(results, d => d.Name == "Bob");
        }

        [Fact]
        public void OrderBy_SortsDocuments()
        {
            var results = _collection.AsQueryable().OrderBy(x => x.Age).ToList();

            Assert.Equal(5, results.Count);
            Assert.Equal("Dave", results[0].Name); // 20
            Assert.Equal("Bob", results[1].Name);  // 25
            Assert.Equal("Eve", results.Last().Name); // 40
        }

        [Fact]
        public void SkipTake_Pagination()
        {
            var results = _collection.AsQueryable()
                .OrderBy(x => x.Age)
                .Skip(1)
                .Take(2)
                .ToList();

            Assert.Equal(2, results.Count);
            Assert.Equal("Bob", results[0].Name); // 25 (Skipped Dave)
            Assert.Equal("Alice", results[1].Name); // 30
        }

        [Fact]
        public void Select_Projections()
        {
            var names = _collection.AsQueryable()
                .Where(x => x.Age < 30)
                .OrderBy(x => x.Age)
                .Select(x => x.Name)
                .ToList();

            Assert.Equal(2, names.Count);
            Assert.Equal("Dave", names[0]);
            Assert.Equal("Bob", names[1]);
        }
        [Fact]
        public void IndexedWhere_UsedIndex()
        {
            // Create index on Age
            _collection.EnsureIndex(x => x.Age, "idx_age", false);

            var query = _collection.AsQueryable().Where(x => x.Age > 25);
            var results = query.ToList();

            Assert.Equal(3, results.Count); // Alice(30), Charlie(35), Eve(40)
            Assert.DoesNotContain(results, d => d.Name == "Bob"); // Age 25 (filtered out by strict >)
            Assert.DoesNotContain(results, d => d.Name == "Dave"); // Age 20
        }
        [Fact]
        public void StartsWith_UsedIndex()
        {
             // Create index on Name
             _collection.EnsureIndex(x => x.Name!, "idx_name", false);
             
             // StartsWith "Cha" -> Should find "Charlie"
             var query = _collection.AsQueryable().Where(x => x.Name!.StartsWith("Cha"));
             var results = query.ToList();
             
             Assert.Single(results);
             Assert.Equal("Charlie", results[0].Name);
        }

        [Fact]
        public void Between_UsedIndex()
        {
             // Create index on Age
             _collection.EnsureIndex(x => x.Age, "idx_age_between", false);
             
             // Age >= 22 && Age <= 32
             // Alice(30), Bob(25) -> Should be found.
             // Dave(20), Charlie(35), Eve(40) -> excluded.
             
             var query = _collection.AsQueryable().Where(x => x.Age >= 22 && x.Age <= 32);
             var results = query.ToList();
             
             Assert.Equal(2, results.Count);
             Assert.Contains(results, x => x.Name == "Alice");
             Assert.Contains(results, x => x.Name == "Bob");
        }
    }
}
