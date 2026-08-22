using BLite.Shared;

namespace BLite.Tests
{
    /// <summary>
    /// Regression coverage for concurrent writes to different collections of the same single-file
    /// database. Each collection locks its own writes, but page placement depends on file-wide
    /// state — the shared FreeSpaceIndex and the page allocator — and FindPageWithSpace and
    /// InsertIntoPage are two separate calls. Without a file-wide write lock two collections are
    /// handed the same page: the loser either fails the space check
    /// ("Not enough space: need N, have M | PageId=...") or, when the check passes, writes over
    /// slots another collection's primary index still points at.
    /// </summary>
    public class CrossCollectionWriteRaceTests : IDisposable
    {
        private static readonly TimeSpan Duration = TimeSpan.FromSeconds(5);

        private readonly string _path;

        public CrossCollectionWriteRaceTests()
        {
            _path = Path.Combine(Path.GetTempPath(), $"blite_xcoll_{Guid.NewGuid()}.db");
        }

        public void Dispose()
        {
            foreach (var suffix in new[] { "", ".wal" })
            {
                var file = _path + suffix;
                if (File.Exists(file)) File.Delete(file);
            }
        }

        [Fact]
        public async Task Concurrent_Inserts_Into_Different_Collections_LoseNothing()
        {
            using var db = new TestDbContext(_path);
            using var cts = new CancellationTokenSource(Duration);

            var failures = new System.Collections.Concurrent.ConcurrentBag<Exception>();
            var writtenIds = new System.Collections.Concurrent.ConcurrentBag<string>();

            var stringWriter = Run(async () =>
            {
                var i = 0;
                while (!cts.IsCancellationRequested)
                {
                    var id = Guid.NewGuid().ToString();
                    await db.StringEntities.InsertAsync(new StringEntity
                    {
                        Id = id,
                        Value = new string('s', 200 + (i % 900)),
                    });
                    writtenIds.Add(id);
                    i++;
                }
            });

            var intWriter = Run(async () =>
            {
                // Ids start at 1: 0 is default(int) and is treated as "no id assigned".
                var i = 1;
                while (!cts.IsCancellationRequested)
                {
                    await db.IntEntities.InsertAsync(new IntEntity
                    {
                        Id = i,
                        Name = new string('i', 200 + (i % 900)),
                    });
                    i++;
                }
            });

            var userWriter = Run(async () =>
            {
                var i = 0;
                while (!cts.IsCancellationRequested)
                {
                    await db.Users.InsertAsync(new User
                    {
                        Name = new string('u', 200 + (i % 900)),
                        Age = i,
                    });
                    i++;
                }
            });

            await Task.WhenAll(stringWriter, intWriter, userWriter);

            Assert.True(
                failures.IsEmpty,
                $"{failures.Count} writer failure(s); first: {failures.FirstOrDefault()}");

            // A page handed to two collections drops slots without reporting anything at write
            // time, so every id written has to come back and every document has to be intact.
            var seen = new HashSet<string>();
            await foreach (var e in db.StringEntities.FindAllAsync())
            {
                Assert.StartsWith("s", e.Value);
                seen.Add(e.Id);
            }

            var missing = writtenIds.Where(id => !seen.Contains(id)).ToList();
            Assert.True(
                missing.Count == 0,
                $"{missing.Count} of {writtenIds.Count} inserted documents are not enumerable "
                + $"(FindAllAsync returned {seen.Count}); first missing id: {missing.FirstOrDefault()}");

            await foreach (var e in db.IntEntities.FindAllAsync()) Assert.StartsWith("i", e.Name);
            await foreach (var e in db.Users.FindAllAsync()) Assert.StartsWith("u", e.Name);

            Task Run(Func<Task> body) => Task.Run(async () =>
            {
                try { await body(); }
                catch (OperationCanceledException) { }
                catch (Exception ex) { failures.Add(ex); }
            });
        }
    }
}
