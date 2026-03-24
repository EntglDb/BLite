using BLite.Bson;
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
        public async Task Scan_FindsMatchingDocuments()
        {
            // Arrange
            await _db.Users.InsertAsync(new User { Name = "Alice", Age = 30 });
            await _db.Users.InsertAsync(new User { Name = "Bob", Age = 25 });
            await _db.Users.InsertAsync(new User { Name = "Charlie", Age = 35 });
            await _db.SaveChangesAsync();

            // Act: Find users older than 28
            var results = await _db.Users.ScanAsync((BsonReaderPredicate)(reader => ParseAge(reader) > 28)).ToListAsync();

            // Assert
            Assert.Equal(2, results.Count);
            Assert.Contains(results, d => d.Name == "Alice");
            Assert.Contains(results, d => d.Name == "Charlie");
        }

        [Fact]
        public async Task Repro_Insert_Loop_Hang()
        {
            // Reproduce hang reported by user at 501 documents
            int count = 600;
            for (int i = 0; i < count; i++)
            {
                await _db.Users.InsertAsync(new User { Name = $"User_{i}", Age = i });
            }
            await _db.SaveChangesAsync();
        }

        [Fact]
        public async Task ParallelScan_FindsMatchingDocuments()
        {
            // Arrange
            int count = 1000;
            for (int i = 0; i < count; i++)
            {
                await _db.Users.InsertAsync(new User { Name = $"User_{i}", Age = i });
            }
            await _db.SaveChangesAsync();

            // Act: Find users with Age >= 500
            // Parallelism 2 to force partitioning
            var results = await _db.Users.ParallelScanAsync(reader => ParseAge(reader) >= 500, degreeOfParallelism: 2).ToListAsync();

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
