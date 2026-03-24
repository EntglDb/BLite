using BLite.Bson;
using BLite.Core.Query;
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
        public async Task TemporalEntity_Insert_And_FindById_Works()
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
            await _db.TemporalEntities.InsertAsync(entity);
            var retrieved = await _db.TemporalEntities.FindByIdAsync(entity.Id);

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
        public async Task TemporalEntity_Insert_WithNullOptionalFields_Works()
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
            await _db.TemporalEntities.InsertAsync(entity);
            var retrieved = await _db.TemporalEntities.FindByIdAsync(entity.Id);

            // Assert
            Assert.NotNull(retrieved);
            Assert.Equal(entity.Name, retrieved.Name);
            Assert.Null(retrieved.LastAccessedAt);
            Assert.Null(retrieved.OptionalDuration);
            Assert.Null(retrieved.Anniversary);
            Assert.Null(retrieved.ClosingTime);
        }

        [Fact]
        public async Task TemporalEntity_Update_Works()
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

            await _db.TemporalEntities.InsertAsync(entity);

            // Act - UpdateAsync temporal fields
            entity.Name = "Updated";
            entity.UpdatedAt = DateTimeOffset.UtcNow.AddDays(1);
            entity.Duration = TimeSpan.FromHours(2);
            entity.BirthDate = new DateOnly(1991, 2, 2);
            entity.OpeningTime = new TimeOnly(10, 0, 0);
            
            await _db.TemporalEntities.UpdateAsync(entity);
            var retrieved = await _db.TemporalEntities.FindByIdAsync(entity.Id);

            // Assert
            Assert.NotNull(retrieved);
            Assert.Equal("Updated", retrieved.Name);
            Assert.Equal(entity.Duration, retrieved.Duration);
            Assert.Equal(entity.BirthDate, retrieved.BirthDate);
            Assert.Equal(entity.OpeningTime, retrieved.OpeningTime);
        }

        [Fact]
        public async Task TemporalEntity_Query_Works()
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

            await _db.TemporalEntities.InsertAsync(entity1);
            await _db.TemporalEntities.InsertAsync(entity2);

            // Act
            var results = await _db.TemporalEntities.AsQueryable()
                .Where(e => e.BirthDate == birthDate1)
                .ToListAsync();

            // Assert
            Assert.Single(results);
            Assert.Equal("Person 1", results[0].Name);
        }

        [Fact]
        public async Task TimeSpan_EdgeCases_Work()
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
            await _db.TemporalEntities.InsertAsync(entity);
            var retrieved = await _db.TemporalEntities.FindByIdAsync(entity.Id);

            // Assert
            Assert.NotNull(retrieved);
            Assert.Equal(TimeSpan.Zero, retrieved.Duration);
            Assert.NotNull(retrieved.OptionalDuration);
            Assert.Equal(TimeSpan.MaxValue, retrieved.OptionalDuration!.Value);
            Assert.Equal(DateOnly.MinValue, retrieved.BirthDate);
            Assert.Equal(TimeOnly.MinValue, retrieved.OpeningTime);
        }

        /// <summary>
        /// Regression test: Select projecting to DateTimeOffset followed by OrderBy used to throw
        /// "ParameterExpression of type 'DateTimeOffset' cannot be used for delegate parameter of type 'T'"
        /// because the OrderByClause key-selector had a DateTimeOffset parameter instead of T.
        /// </summary>
        [Fact]
        public async Task Select_Then_OrderBy_DateTimeOffset_DoesNotThrow()
        {
            // Arrange
            var base_ = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

            await _db.TemporalEntities.InsertAsync(new TemporalEntity
            {
                Id = ObjectId.NewObjectId(),
                Name = "C",
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = base_.AddDays(2),
                Duration = TimeSpan.Zero,
                BirthDate = DateOnly.MinValue,
                OpeningTime = TimeOnly.MinValue
            });
            await _db.TemporalEntities.InsertAsync(new TemporalEntity
            {
                Id = ObjectId.NewObjectId(),
                Name = "A",
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = base_,
                Duration = TimeSpan.Zero,
                BirthDate = DateOnly.MinValue,
                OpeningTime = TimeOnly.MinValue
            });
            await _db.TemporalEntities.InsertAsync(new TemporalEntity
            {
                Id = ObjectId.NewObjectId(),
                Name = "B",
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = base_.AddDays(1),
                Duration = TimeSpan.Zero,
                BirthDate = DateOnly.MinValue,
                OpeningTime = TimeOnly.MinValue
            });

            // Act — this chain triggered the ArgumentException before the fix
            var dates = await _db.TemporalEntities.AsQueryable()
                .Select(e => e.UpdatedAt)
                .OrderBy(d => d)
                .ToListAsync();

            // Assert
            Assert.Equal(3, dates.Count);
            Assert.True(dates[0] < dates[1] && dates[1] < dates[2]);
            Assert.Equal(base_, dates[0]);
            Assert.Equal(base_.AddDays(1), dates[1]);
            Assert.Equal(base_.AddDays(2), dates[2]);
        }

        [Fact]
        public async Task Where_Then_Select_Then_OrderByDescending_DateTimeOffset_DoesNotThrow()
        {
            // Arrange
            var base_ = new DateTimeOffset(2024, 6, 1, 0, 0, 0, TimeSpan.Zero);

            for (int i = 0; i < 4; i++)
            {
                await _db.TemporalEntities.InsertAsync(new TemporalEntity
                {
                    Id = ObjectId.NewObjectId(),
                    Name = i % 2 == 0 ? "even" : "odd",
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = base_.AddDays(i),
                    Duration = TimeSpan.Zero,
                    BirthDate = DateOnly.MinValue,
                    OpeningTime = TimeOnly.MinValue
                });
            }

            // Act
            var dates = await _db.TemporalEntities.AsQueryable()
                .Where(e => e.Name == "even")
                .Select(e => e.UpdatedAt)
                .OrderByDescending(d => d)
                .ToListAsync();
            // Assert — only even-indexed items (day 0 and day 2), descending
            Assert.Equal(2, dates.Count);
            Assert.True(dates[0] > dates[1]);
            Assert.Equal(base_.AddDays(2), dates[0]);
            Assert.Equal(base_, dates[1]);
        }
    }
}
