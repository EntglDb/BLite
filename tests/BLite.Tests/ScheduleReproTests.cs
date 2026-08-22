using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests
{
    /// <summary>
    /// Isolates a NotSupportedException ("Skipping type N not supported") seen when reading back
    /// an entity with a List&lt;T&gt; of nested objects carrying nullable DateTime?/TimeSpan?
    /// fields, on a ~18-field root using the C-BSON v2 offset table.
    /// </summary>
    public class ScheduleReproTests : IDisposable
    {
        private readonly string _dbPath;
        private readonly TestDbContext _db;

        public ScheduleReproTests()
        {
            _dbPath = Path.Combine(Path.GetTempPath(), $"blite_schedule_{Guid.NewGuid()}.db");
            _db = new TestDbContext(_dbPath);
        }

        public void Dispose()
        {
            _db.Dispose();
            if (File.Exists(_dbPath)) File.Delete(_dbPath);
        }

        [Fact]
        public async Task Schedule_With_One_NonNull_Window_RoundTrips()
        {
            var schedule = new Schedule
            {
                Id = ObjectId.NewObjectId(),
                Name = "Test",
                CountryCode = "IT",
                Currency = 1,
                IsGrossValue = true,
                Windows =
                [
                    new ScheduleWindow
                    {
                        StartDate = new DateTime(2026, 1, 1),
                        EndDate = new DateTime(2026, 12, 31),
                        StartTime = TimeSpan.FromHours(9),
                        EndTime = TimeSpan.FromHours(18),
                        DayFlags = 127,
                    },
                ],
                Stores = ["store-1"],
                Groups = ["group-1"],
                TenantId = "tenant-1",
                RowVersion = 1,
            };

            await _db.Schedules.InsertAsync(schedule);
            await _db.SaveChangesAsync();

            var reloaded = await _db.Schedules.FindByIdAsync(schedule.Id);

            Assert.NotNull(reloaded);
            Assert.Single(reloaded!.Windows);
            Assert.Equal(TimeSpan.FromHours(9), reloaded.Windows[0].StartTime);
            Assert.Equal(TimeSpan.FromHours(18), reloaded.Windows[0].EndTime);
        }

        [Fact]
        public async Task Schedule_With_Empty_Windows_RoundTrips()
        {
            var schedule = new Schedule
            {
                Id = ObjectId.NewObjectId(),
                Name = "Empty",
                Currency = 1,
                IsGrossValue = true,
                Windows = [],
                Stores = [],
                Groups = [],
                RowVersion = 1,
            };

            await _db.Schedules.InsertAsync(schedule);
            await _db.SaveChangesAsync();

            var reloaded = await _db.Schedules.FindByIdAsync(schedule.Id);

            Assert.NotNull(reloaded);
            Assert.Empty(reloaded!.Windows);
        }

        [Fact]
        public async Task Schedule_Updated_From_Empty_To_NonEmpty_Windows_RoundTrips()
        {
            // Mirrors an insert-then-later-update flow: a record is created empty, then a later
            // pass finds it and updates it in place - this is the one code path the two
            // round-trip-only tests above never exercise.
            var id = ObjectId.NewObjectId();
            var schedule = new Schedule
            {
                Id = id,
                Name = "Grows",
                Currency = 1,
                IsGrossValue = true,
                Windows = [],
                Stores = [],
                Groups = [],
                RowVersion = 1,
            };

            await _db.Schedules.InsertAsync(schedule);
            await _db.SaveChangesAsync();

            var existing = await _db.Schedules.FindByIdAsync(id);
            Assert.NotNull(existing);

            existing!.Windows.Add(new ScheduleWindow
            {
                StartDate = new DateTime(2026, 1, 1),
                StartTime = TimeSpan.FromHours(9),
                EndTime = TimeSpan.FromHours(18),
                DayFlags = 127,
            });
            existing.RowVersion = 2;

            await _db.Schedules.UpdateAsync(existing);
            await _db.SaveChangesAsync();

            var reloaded = await _db.Schedules.FindByIdAsync(id);

            Assert.NotNull(reloaded);
            Assert.Single(reloaded!.Windows);
            Assert.Equal(TimeSpan.FromHours(9), reloaded.Windows[0].StartTime);
        }

        [Fact]
        public async Task Multiple_Schedules_In_Sequence_All_RoundTrip()
        {
            // A bulk-sync flow inserts a BATCH of records in one pass. If one record's
            // declared byte length is even slightly wrong, sequential scanning would misread
            // whichever record comes AFTER it - the record that throws might be an innocent victim
            // of an earlier one's corrupted size, not the actual culprit. Single-record round-trips
            // above can't catch that; this inserts several with varying window shapes and reads all
            // of them back.
            var ids = new List<ObjectId>();
            for (var i = 0; i < 5; i++)
            {
                var id = ObjectId.NewObjectId();
                ids.Add(id);
                var schedule = new Schedule
                {
                    Id = id,
                    Name = $"Schedule{i}",
                    Currency = 1,
                    IsGrossValue = true,
                    Windows = i % 2 == 0
                        ? []
                        :
                        [
                            new ScheduleWindow { DayFlags = i },
                            new ScheduleWindow
                            {
                                StartDate = new DateTime(2026, 1, 1),
                                StartTime = TimeSpan.FromHours(i),
                                EndTime = TimeSpan.FromHours(i + 1),
                                DayFlags = i * 2,
                            },
                        ],
                    Stores = i % 2 == 0 ? [] : [$"store-{i}"],
                    Groups = [],
                    RowVersion = 1,
                };
                await _db.Schedules.InsertAsync(schedule);
            }
            await _db.SaveChangesAsync();

            foreach (var id in ids)
            {
                var reloaded = await _db.Schedules.FindByIdAsync(id);
                Assert.NotNull(reloaded);
            }
        }

        [Fact]
        public async Task Schedule_Batch_Grow_Past_Slot_Then_FindAll_AllRecordsIntact()
        {
            // Mirrors a bulk-sync flow more closely than the single-record tests above: pass 1
            // inserts a BATCH of small records (empty Windows) that end up sharing pages, pass
            // 2 updates ONE of them (in the middle of the batch) to add Windows entries -
            // growing it past its original slot, forcing DocumentCollection.UpdateDataCore's
            // delete+reinsert-elsewhere path (bytesWritten > oldSlot.Length) instead of the
            // in-place path. Then re-reads the WHOLE collection via FindAllAsync, which uses a
            // per-query page-cache keyed by PageId (see QueryIndexAsync/FindAllAsync) - if the
            // relocation leaves a stale index entry or a freed slot gets misread, this is the
            // shape that would surface it, unlike the single-record round-trips above.
            var ids = new List<ObjectId>();
            for (var i = 0; i < 40; i++)
            {
                var id = ObjectId.NewObjectId();
                ids.Add(id);
                await _db.Schedules.InsertAsync(new Schedule
                {
                    Id = id,
                    Name = $"Batch{i}",
                    Currency = 1,
                    IsGrossValue = true,
                    Windows = [],
                    Stores = [],
                    Groups = [],
                    RowVersion = 1,
                });
            }
            await _db.SaveChangesAsync();

            var growId = ids[20];
            var existing = await _db.Schedules.FindByIdAsync(growId);
            Assert.NotNull(existing);
            existing!.Windows.AddRange(
            [
                new ScheduleWindow
                {
                    StartDate = new DateTime(2026, 1, 1),
                    EndDate = new DateTime(2026, 12, 31),
                    StartTime = TimeSpan.FromHours(9),
                    EndTime = TimeSpan.FromHours(18),
                    DayFlags = 127,
                },
                new ScheduleWindow
                {
                    StartDate = new DateTime(2026, 2, 1),
                    StartTime = TimeSpan.FromHours(10),
                    EndTime = TimeSpan.FromHours(14),
                    DayFlags = 62,
                },
            ]);
            existing.RowVersion = 2;
            await _db.Schedules.UpdateAsync(existing);
            await _db.SaveChangesAsync();

            var reloadedAll = new Dictionary<ObjectId, Schedule>();
            await foreach (var schedule in _db.Schedules.FindAllAsync())
            {
                reloadedAll[schedule.Id] = schedule;
            }

            Assert.Equal(40, reloadedAll.Count);
            foreach (var id in ids)
            {
                Assert.True(reloadedAll.ContainsKey(id), $"Missing record {id}");
            }

            var grown = reloadedAll[growId];
            Assert.Equal(2, grown.Windows.Count);
            Assert.Equal(TimeSpan.FromHours(9), grown.Windows[0].StartTime);
        }

        [Fact]
        public async Task Schedule_With_Null_Window_Fields_RoundTrips()
        {
            var schedule = new Schedule
            {
                Id = ObjectId.NewObjectId(),
                Name = "NullFields",
                Currency = 1,
                IsGrossValue = true,
                Windows =
                [
                    new ScheduleWindow
                    {
                        StartDate = null,
                        EndDate = null,
                        StartTime = null,
                        EndTime = null,
                        DayFlags = 0,
                    },
                ],
                Stores = [],
                Groups = [],
                RowVersion = 1,
            };

            await _db.Schedules.InsertAsync(schedule);
            await _db.SaveChangesAsync();

            var reloaded = await _db.Schedules.FindByIdAsync(schedule.Id);

            Assert.NotNull(reloaded);
            Assert.Single(reloaded!.Windows);
            Assert.Null(reloaded.Windows[0].StartTime);
        }
    }
}
