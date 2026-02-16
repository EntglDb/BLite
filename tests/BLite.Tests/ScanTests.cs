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
using BLite.Shared;

namespace BLite.Tests
{
    public class ScanTests : IDisposable
    {
        private readonly string _testFile;
        private readonly TestDbContext _db;

        public ScanTests()
        {
            _testFile = Path.Combine(Path.GetTempPath(), $"scan_tests_{Guid.NewGuid()}.db");
            if (File.Exists(_testFile)) File.Delete(_testFile);
            var wal = Path.ChangeExtension(_testFile, ".wal");
            if (File.Exists(wal)) File.Delete(wal);

            _db = new TestDbContext(_testFile);
        }

        public void Dispose()
        {
            _db.Dispose();
            if (File.Exists(_testFile)) File.Delete(_testFile);
            var wal = Path.ChangeExtension(_testFile, ".wal");
            if (File.Exists(wal)) File.Delete(wal);
        }

        [Fact]
        public void Scan_FindsMatchingDocuments()
        {
            // Arrange
            _db.Users.Insert(new User { Name = "Alice", Age = 30 });
            _db.Users.Insert(new User { Name = "Bob", Age = 25 });
            _db.Users.Insert(new User { Name = "Charlie", Age = 35 });
            _db.SaveChanges();

            // Act: Find users older than 28
            var results = _db.Users.Scan(reader => ParseAge(reader) > 28).ToList();

            // Assert
            Assert.Equal(2, results.Count);
            Assert.Contains(results, d => d.Name == "Alice");
            Assert.Contains(results, d => d.Name == "Charlie");
        }

        [Fact]
        public void Repro_Insert_Loop_Hang()
        {
            // Reproduce hang reported by user at 501 documents
            int count = 600;
            for (int i = 0; i < count; i++)
            {
                _db.Users.Insert(new User { Name = $"User_{i}", Age = i });
            }
            _db.SaveChanges();
        }

        [Fact]
        public void ParallelScan_FindsMatchingDocuments()
        {
            // Arrange
            int count = 1000;
            for (int i = 0; i < count; i++)
            {
                _db.Users.Insert(new User { Name = $"User_{i}", Age = i });
            }
            _db.SaveChanges();

            // Act: Find users with Age >= 500
            // Parallelism 2 to force partitioning
            var results = _db.Users.ParallelScan(reader => ParseAge(reader) >= 500, degreeOfParallelism: 2).ToList();

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
                    
                    var name = reader.ReadElementHeader();
                    
                    if (name == "age")
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
