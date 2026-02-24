using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests
{
    public class TemporalTypesTests : IDisposable
    {
        private readonly TestDbContext _db;
        private readonly string _dbPath;

        public TemporalTypesTests()
        {
            _dbPath = $"temporal_test_{Guid.NewGuid()}.db";
            _db = new TestDbContext(_dbPath);
        }

        public void Dispose()
        {
            _db?.Dispose();
            if (File.Exists(_dbPath))
                File.Delete(_dbPath);
        }

        [Fact]
        public void TemporalEntity_Collection_IsInitialized()
        {
            Assert.NotNull(_db.TemporalEntities);
        }

        [Fact]
        public void TemporalEntity_Insert_And_FindById_Works()
        {
            // Arrange
            var now = DateTime.UtcNow;
            var offset = DateTimeOffset.UtcNow;
            var duration = TimeSpan.FromHours(5.5);
            var birthDate = new DateOnly(1990, 5, 15);
            var openingTime = new TimeOnly(9, 30, 0);

            var entity = new TemporalEntity
            {
                Id = ObjectId.NewObjectId(),
                Name = "Test Entity",
                CreatedAt = now,
                UpdatedAt = offset,
                LastAccessedAt = offset.AddDays(1),
                Duration = duration,
                OptionalDuration = TimeSpan.FromMinutes(30),
                BirthDate = birthDate,
                Anniversary = new DateOnly(2020, 6, 10),
                OpeningTime = openingTime,
                ClosingTime = new TimeOnly(18, 0, 0)
            };

            // Act
            _db.TemporalEntities.Insert(entity);
            var retrieved = _db.TemporalEntities.FindById(entity.Id);

            // Assert
            Assert.NotNull(retrieved);
            Assert.Equal(entity.Name, retrieved.Name);
            
            // DateTime comparison (allowing some millisecond precision loss)
            Assert.Equal(entity.CreatedAt.Ticks / 10000, retrieved.CreatedAt.Ticks / 10000); // millisecond precision
            
            // DateTimeOffset comparison
            Assert.Equal(entity.UpdatedAt.UtcDateTime.Ticks / 10000, retrieved.UpdatedAt.UtcDateTime.Ticks / 10000);
            Assert.NotNull(retrieved.LastAccessedAt);
            Assert.Equal(entity.LastAccessedAt!.Value.UtcDateTime.Ticks / 10000, retrieved.LastAccessedAt!.Value.UtcDateTime.Ticks / 10000);
            
            // TimeSpan comparison
            Assert.Equal(entity.Duration, retrieved.Duration);
            Assert.NotNull(retrieved.OptionalDuration);
            Assert.Equal(entity.OptionalDuration!.Value, retrieved.OptionalDuration!.Value);
            
            // DateOnly comparison
            Assert.Equal(entity.BirthDate, retrieved.BirthDate);
            Assert.NotNull(retrieved.Anniversary);
            Assert.Equal(entity.Anniversary!.Value, retrieved.Anniversary!.Value);
            
            // TimeOnly comparison
            Assert.Equal(entity.OpeningTime, retrieved.OpeningTime);
            Assert.NotNull(retrieved.ClosingTime);
            Assert.Equal(entity.ClosingTime!.Value, retrieved.ClosingTime!.Value);
        }

        [Fact]
        public void TemporalEntity_Insert_WithNullOptionalFields_Works()
        {
            // Arrange
            var entity = new TemporalEntity
            {
                Id = ObjectId.NewObjectId(),
                Name = "Minimal Entity",
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTimeOffset.UtcNow,
                Duration = TimeSpan.FromHours(1),
                BirthDate = new DateOnly(1985, 3, 20),
                OpeningTime = new TimeOnly(8, 0, 0),
                // Optional fields left null
                LastAccessedAt = null,
                OptionalDuration = null,
                Anniversary = null,
                ClosingTime = null
            };

            // Act
            _db.TemporalEntities.Insert(entity);
            var retrieved = _db.TemporalEntities.FindById(entity.Id);

            // Assert
            Assert.NotNull(retrieved);
            Assert.Equal(entity.Name, retrieved.Name);
            Assert.Null(retrieved.LastAccessedAt);
            Assert.Null(retrieved.OptionalDuration);
            Assert.Null(retrieved.Anniversary);
            Assert.Null(retrieved.ClosingTime);
        }

        [Fact]
        public void TemporalEntity_Update_Works()
        {
            // Arrange
            var entity = new TemporalEntity
            {
                Id = ObjectId.NewObjectId(),
                Name = "Original",
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTimeOffset.UtcNow,
                Duration = TimeSpan.FromHours(1),
                BirthDate = new DateOnly(1990, 1, 1),
                OpeningTime = new TimeOnly(9, 0, 0)
            };

            _db.TemporalEntities.Insert(entity);

            // Act - Update temporal fields
            entity.Name = "Updated";
            entity.UpdatedAt = DateTimeOffset.UtcNow.AddDays(1);
            entity.Duration = TimeSpan.FromHours(2);
            entity.BirthDate = new DateOnly(1991, 2, 2);
            entity.OpeningTime = new TimeOnly(10, 0, 0);
            
            _db.TemporalEntities.Update(entity);
            var retrieved = _db.TemporalEntities.FindById(entity.Id);

            // Assert
            Assert.NotNull(retrieved);
            Assert.Equal("Updated", retrieved.Name);
            Assert.Equal(entity.Duration, retrieved.Duration);
            Assert.Equal(entity.BirthDate, retrieved.BirthDate);
            Assert.Equal(entity.OpeningTime, retrieved.OpeningTime);
        }

        [Fact]
        public void TemporalEntity_Query_Works()
        {
            // Arrange
            var birthDate1 = new DateOnly(1990, 1, 1);
            var birthDate2 = new DateOnly(1995, 6, 15);
            
            var entity1 = new TemporalEntity
            {
                Id = ObjectId.NewObjectId(),
                Name = "Person 1",
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTimeOffset.UtcNow,
                Duration = TimeSpan.FromHours(1),
                BirthDate = birthDate1,
                OpeningTime = new TimeOnly(9, 0, 0)
            };

            var entity2 = new TemporalEntity
            {
                Id = ObjectId.NewObjectId(),
                Name = "Person 2",
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTimeOffset.UtcNow,
                Duration = TimeSpan.FromHours(2),
                BirthDate = birthDate2,
                OpeningTime = new TimeOnly(10, 0, 0)
            };

            _db.TemporalEntities.Insert(entity1);
            _db.TemporalEntities.Insert(entity2);

            // Act
            var results = _db.TemporalEntities.AsQueryable()
                .Where(e => e.BirthDate == birthDate1)
                .ToList();

            // Assert
            Assert.Single(results);
            Assert.Equal("Person 1", results[0].Name);
        }

        [Fact]
        public void TimeSpan_EdgeCases_Work()
        {
            // Arrange - Test various TimeSpan values
            var entity = new TemporalEntity
            {
                Id = ObjectId.NewObjectId(),
                Name = "TimeSpan Test",
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTimeOffset.UtcNow,
                Duration = TimeSpan.Zero,
                OptionalDuration = TimeSpan.MaxValue,
                BirthDate = DateOnly.MinValue,
                OpeningTime = TimeOnly.MinValue
            };

            // Act
            _db.TemporalEntities.Insert(entity);
            var retrieved = _db.TemporalEntities.FindById(entity.Id);

            // Assert
            Assert.NotNull(retrieved);
            Assert.Equal(TimeSpan.Zero, retrieved.Duration);
            Assert.NotNull(retrieved.OptionalDuration);
            Assert.Equal(TimeSpan.MaxValue, retrieved.OptionalDuration!.Value);
            Assert.Equal(DateOnly.MinValue, retrieved.BirthDate);
            Assert.Equal(TimeOnly.MinValue, retrieved.OpeningTime);
        }
    }
}
