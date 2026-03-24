using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests
{
    public class LinqTests : IDisposable
    {
        private readonly string _testFile;
        private readonly TestDbContext _db;

        public LinqTests()
        {
            _testFile = Path.Combine(Path.GetTempPath(), $"linq_tests_{Guid.NewGuid()}.db");
            if (File.Exists(_testFile)) File.Delete(_testFile);
            var wal = Path.ChangeExtension(_testFile, ".wal");
            if (File.Exists(wal)) File.Delete(wal);

            _db = new TestDbContext(_testFile);

            // Seed Data
            _db.Users.InsertAsync(new User { Name = "Alice", Age = 30 }).GetAwaiter().GetResult();
            _db.Users.InsertAsync(new User { Name = "Bob", Age = 25 }).GetAwaiter().GetResult();
            _db.Users.InsertAsync(new User { Name = "Charlie", Age = 35 }).GetAwaiter().GetResult();
            _db.Users.InsertAsync(new User { Name = "Dave", Age = 20 }).GetAwaiter().GetResult();
            _db.Users.InsertAsync(new User { Name = "Eve", Age = 40 }).GetAwaiter().GetResult();
            _db.SaveChangesAsync().GetAwaiter().GetResult();
        }

        public void Dispose()
        {
            _db.Dispose();
            if (File.Exists(_testFile)) File.Delete(_testFile);
            var wal = Path.ChangeExtension(_testFile, ".wal");
            if (File.Exists(wal)) File.Delete(wal);
        }

        [Fact]
        public async Task Where_FiltersDocuments()
        {
            var query = _db.Users.AsQueryable().Where(x => x.Age > 28);
            var results = await query.ToListAsync();

            Assert.Equal(3, results.Count); // Alice(30), Charlie(35), Eve(40)
            Assert.DoesNotContain(results, d => d.Name == "Bob");
        }

        [Fact]
        public async Task OrderBy_SortsDocuments()
        {
            var results = await _db.Users.AsQueryable().OrderBy(x => x.Age).ToListAsync();

            Assert.Equal(5, results.Count);
            Assert.Equal("Dave", results[0].Name); // 20
            Assert.Equal("Bob", results[1].Name);  // 25
            Assert.Equal("Eve", results.Last().Name); // 40
        }

        [Fact]
        public async Task SkipTake_Pagination()
        {
            var results = await _db.Users.AsQueryable()
                .OrderBy(x => x.Age)
                .Skip(1)
                .Take(2)
                .ToListAsync();

            Assert.Equal(2, results.Count);
            Assert.Equal("Bob", results[0].Name); // 25 (Skipped Dave)
            Assert.Equal("Alice", results[1].Name); // 30
        }

        [Fact]
        public async Task Select_Projections()
        {
            var names = await _db.Users.AsQueryable()
                .Where(x => x.Age < 30)
                .OrderBy(x => x.Age)
                .Select(x => x.Name)
                .ToListAsync();

            Assert.Equal(2, names.Count);
            Assert.Equal("Dave", names[0]);
            Assert.Equal("Bob", names[1]);
        }
        [Fact]
        public async Task IndexedWhere_UsedIndex()
        {
            // Create index on Age
            await _db.Users.EnsureIndexAsync(x => x.Age, "idx_age", false);

            var query = _db.Users.AsQueryable().Where(x => x.Age > 25);
            var results = await query.ToListAsync();

            Assert.Equal(3, results.Count); // Alice(30), Charlie(35), Eve(40)
            Assert.DoesNotContain(results, d => d.Name == "Bob"); // Age 25 (filtered out by strict >)
            Assert.DoesNotContain(results, d => d.Name == "Dave"); // Age 20
        }
        [Fact]
        public async Task StartsWith_UsedIndex()
        {
            // Create index on Name
            await _db.Users.EnsureIndexAsync(x => x.Name!, "idx_name", false);
             
             // StartsWith "Cha" -> Should find "Charlie"
             var query = _db.Users.AsQueryable().Where(x => x.Name!.StartsWith("Cha"));
             var results = await query.ToListAsync();
             
             Assert.Single(results);
             Assert.Equal("Charlie", results[0].Name);
        }

        [Fact]
        public async Task Between_UsedIndex()
        {
            // Create index on Age
            await _db.Users.EnsureIndexAsync(x => x.Age, "idx_age_between", false);
             
             // Age >= 22 && Age <= 32
             // Alice(30), Bob(25) -> Should be found.
             // Dave(20), Charlie(35), Eve(40) -> excluded.
             
             var query = _db.Users.AsQueryable().Where(x => x.Age >= 22 && x.Age <= 32);
             var results = await query.ToListAsync();
             
             Assert.Equal(2, results.Count);
             Assert.Contains(results, x => x.Name == "Alice");
             Assert.Contains(results, x => x.Name == "Bob");
        }
    }
}
