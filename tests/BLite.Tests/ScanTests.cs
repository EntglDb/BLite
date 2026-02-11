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
    public class ScanTests : IDisposable
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

        public ScanTests()
        {
            _testFile = Path.Combine(Path.GetTempPath(), $"scan_tests_{Guid.NewGuid()}.db");
            if (File.Exists(_testFile)) File.Delete(_testFile);
            var wal = Path.ChangeExtension(_testFile, ".wal");
            if (File.Exists(wal)) File.Delete(wal);

            var config = new PageFileConfig { PageSize = 4096, InitialFileSize = 4096 * 100 }; // ample space
            _storage = new StorageEngine(_testFile, config);
            
            var mapper = new TestDocumentMapper();
            _collection = new DocumentCollection<TestDocument>(_storage, mapper, "users");
        }

        public void Dispose()
        {
            _storage.Dispose();
            if (File.Exists(_testFile)) File.Delete(_testFile);
            var wal = Path.ChangeExtension(_testFile, ".wal");
            if (File.Exists(wal)) File.Delete(wal);
        }

        [Fact]
        public void Scan_FindsMatchingDocuments()
        {
            // Arrange
            _collection.Insert(new TestDocument { Name = "Alice", Age = 30 });
            _collection.Insert(new TestDocument { Name = "Bob", Age = 25 });
            _collection.Insert(new TestDocument { Name = "Charlie", Age = 35 });

            // Act: Find users older than 28
            var results = _collection.Scan(reader => ParseAge(reader) > 28).ToList();

            // Assert
            Assert.Equal(2, results.Count);
            Assert.Contains(results, d => d.Name == "Alice");
            Assert.Contains(results, d => d.Name == "Charlie");
        }

        [Fact]
        public void ParallelScan_FindsMatchingDocuments()
        {
            // Arrange
            int count = 1000;
            for (int i = 0; i < count; i++)
            {
                _collection.Insert(new TestDocument { Name = $"User_{i}", Age = i });
            }

            // Act: Find users with Age >= 500
            // Parallelism 2 to force partitioning
            var results = _collection.ParallelScan(reader => ParseAge(reader) >= 500, degreeOfParallelism: 2).ToList();

            // Assert
            Assert.Equal(500, results.Count);
        }

        private int ParseAge(BsonSpanReader reader)
        {
            try 
            {
                reader.ReadDocumentSize();
                while (reader.Remaining > 0)
                {
                    var type = reader.ReadBsonType();
                    if (type == 0) break; // End of doc
                    
                    var name = reader.ReadCString();
                    
                    if (name == "Age")
                    {
                        return reader.ReadInt32();
                    }
                    else
                    {
                        reader.SkipValue(type);
                    }
                }
            }
            catch { return -1; }
            return -1;
        }
    }
}
