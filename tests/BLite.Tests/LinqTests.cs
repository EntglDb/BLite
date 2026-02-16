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
            _db.Users.Insert(new User { Name = "Alice", Age = 30 });
            _db.Users.Insert(new User { Name = "Bob", Age = 25 });
            _db.Users.Insert(new User { Name = "Charlie", Age = 35 });
            _db.Users.Insert(new User { Name = "Dave", Age = 20 });
            _db.Users.Insert(new User { Name = "Eve", Age = 40 });
            _db.SaveChanges();
        }

        public void Dispose()
        {
            _db.Dispose();
            if (File.Exists(_testFile)) File.Delete(_testFile);
            var wal = Path.ChangeExtension(_testFile, ".wal");
            if (File.Exists(wal)) File.Delete(wal);
        }

        [Fact]
        public void Where_FiltersDocuments()
        {
            var query = _db.Users.AsQueryable().Where(x => x.Age > 28);
            var results = query.ToList();

            Assert.Equal(3, results.Count); // Alice(30), Charlie(35), Eve(40)
            Assert.DoesNotContain(results, d => d.Name == "Bob");
        }

        [Fact]
        public void OrderBy_SortsDocuments()
        {
            var results = _db.Users.AsQueryable().OrderBy(x => x.Age).ToList();

            Assert.Equal(5, results.Count);
            Assert.Equal("Dave", results[0].Name); // 20
            Assert.Equal("Bob", results[1].Name);  // 25
            Assert.Equal("Eve", results.Last().Name); // 40
        }

        [Fact]
        public void SkipTake_Pagination()
        {
            var results = _db.Users.AsQueryable()
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
            var names = _db.Users.AsQueryable()
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
            _db.Users.EnsureIndex(x => x.Age, "idx_age", false);

            var query = _db.Users.AsQueryable().Where(x => x.Age > 25);
            var results = query.ToList();

            Assert.Equal(3, results.Count); // Alice(30), Charlie(35), Eve(40)
            Assert.DoesNotContain(results, d => d.Name == "Bob"); // Age 25 (filtered out by strict >)
            Assert.DoesNotContain(results, d => d.Name == "Dave"); // Age 20
        }
        [Fact]
        public void StartsWith_UsedIndex()
        {
            // Create index on Name
            _db.Users.EnsureIndex(x => x.Name!, "idx_name", false);
             
             // StartsWith "Cha" -> Should find "Charlie"
             var query = _db.Users.AsQueryable().Where(x => x.Name!.StartsWith("Cha"));
             var results = query.ToList();
             
             Assert.Single(results);
             Assert.Equal("Charlie", results[0].Name);
        }

        [Fact]
        public void Between_UsedIndex()
        {
            // Create index on Age
            _db.Users.EnsureIndex(x => x.Age, "idx_age_between", false);
             
             // Age >= 22 && Age <= 32
             // Alice(30), Bob(25) -> Should be found.
             // Dave(20), Charlie(35), Eve(40) -> excluded.
             
             var query = _db.Users.AsQueryable().Where(x => x.Age >= 22 && x.Age <= 32);
             var results = query.ToList();
             
             Assert.Equal(2, results.Count);
             Assert.Contains(results, x => x.Name == "Alice");
             Assert.Contains(results, x => x.Name == "Bob");
        }
    }
}
