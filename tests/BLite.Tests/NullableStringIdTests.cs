using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests
{
    /// <summary>
    /// Tests for entities with nullable string Id (like UuidEntity scenario from CleanCore)
    /// This reproduces the bug where the generator incorrectly chose ObjectIdMapperBase 
    /// instead of StringMapperBase for inherited nullable string Id properties
    /// </summary>
    public class NullableStringIdTests : System.IDisposable
    {
        private const string DbPath = "nullable_string_id.db";

        public NullableStringIdTests()
        {
            if (File.Exists(DbPath)) File.Delete(DbPath);
        }

        public void Dispose()
        {
            if (File.Exists(DbPath)) File.Delete(DbPath);
        }

        [Fact]
        public void MockCounter_Collection_IsInitialized()
        {
            using var db = new TestDbContext(DbPath);
            
            // Verify Collection is not null (initialized by generated method)
            Assert.NotNull(db.MockCounters);
        }

        [Fact]
        public async Task MockCounter_Insert_And_FindById_Works()
        {
            using var db = new TestDbContext(DbPath);
            
            var counter = new MockCounter("test-id-123")
            {
                Name = "TestCounter",
                Value = 42
            };
            
            // Insert should work with string Id
            await db.MockCounters.InsertAsync(counter);
            
            // FindById should retrieve the entity
            var stored = await db.MockCounters.FindByIdAsync("test-id-123");
            Assert.NotNull(stored);
            Assert.Equal("test-id-123", stored.Id);
            Assert.Equal("TestCounter", stored.Name);
            Assert.Equal(42, stored.Value);
        }

        [Fact]
        public async Task MockCounter_Update_Works()
        {
            using var db = new TestDbContext(DbPath);
            
            var counter = new MockCounter("update-test")
            {
                Name = "Original",
                Value = 10
            };
            
            await db.MockCounters.InsertAsync(counter);
            
            // UpdateAsync the entity
            counter.Name = "Updated";
            counter.Value = 20;
            await db.MockCounters.UpdateAsync(counter);
            
            // Verify update
            var updated = await db.MockCounters.FindByIdAsync("update-test");
            Assert.NotNull(updated);
            Assert.Equal("Updated", updated.Name);
            Assert.Equal(20, updated.Value);
        }

        [Fact]
        public async Task MockCounter_Delete_Works()
        {
            using var db = new TestDbContext(DbPath);
            
            var counter = new MockCounter("delete-test")
            {
                Name = "ToDelete",
                Value = 99
            };
            
            await db.MockCounters.InsertAsync(counter);
            Assert.NotNull(await db.MockCounters.FindByIdAsync("delete-test"));
            
            // Delete the entity
            await db.MockCounters.DeleteAsync("delete-test");
            
            // Verify deletion
            var deleted = await db.MockCounters.FindByIdAsync("delete-test");
            Assert.Null(deleted);
        }

        [Fact]
        public async Task MockCounter_Query_Works()
        {
            using var db = new TestDbContext(DbPath);
            
            await db.MockCounters.InsertAsync(new MockCounter("q1") { Name = "First", Value = 100 });
            await db.MockCounters.InsertAsync(new MockCounter("q2") { Name = "Second", Value = 200 });
            await db.MockCounters.InsertAsync(new MockCounter("q3") { Name = "Third", Value = 150 });
            
            // Query all
            var all = await db.MockCounters.AsQueryable().ToListAsync();
            Assert.Equal(3, all.Count);
            
            // Query with condition
            var highValues = await db.MockCounters.AsQueryable()
                .Where(c => c.Value > 150)
                .ToListAsync();
            
            Assert.Single(highValues);
            Assert.Equal("Second", highValues[0].Name);
        }

        [Fact]
        public async Task MockCounter_InheritedId_IsStoredCorrectly()
        {
            using var db = new TestDbContext(DbPath);
            
            // Test that the inherited nullable string Id from MockBaseEntity works correctly
            var counter = new MockCounter("inherited-id-test")
            {
                Name = "Inherited",
                Value = 777
            };
            
            await db.MockCounters.InsertAsync(counter);
            
            var stored = await db.MockCounters.FindByIdAsync("inherited-id-test");
            Assert.NotNull(stored);
            
            // Verify the Id is correctly stored and retrieved through inheritance
            Assert.Equal("inherited-id-test", stored.Id);
            Assert.IsType<string>(stored.Id);
        }
    }
}
