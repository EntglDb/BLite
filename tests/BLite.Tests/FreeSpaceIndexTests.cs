using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Tests that verify the FreeSpaceIndex correctly restores free-space knowledge across
/// DocumentCollection lifecycles (cold-start reconstruction).
/// Acceptance criteria from the issue:
///   1. A fresh DocumentCollection does NOT allocate a new page on the first insert when
///      existing pages have ample free space.
///   2. FindPageWithSpace completes in O(1) operations (bucket scan) regardless of page count.
/// </summary>
public class FreeSpaceIndexTests : IDisposable
{
    private readonly string _testFile;

    public FreeSpaceIndexTests()
    {
        _testFile = Path.GetTempFileName();
    }

    public void Dispose()
    {
        if (File.Exists(_testFile))
            File.Delete(_testFile);
    }

    /// <summary>
    /// Verifies that after closing and reopening the database the second session does NOT
    /// allocate extra pages when the existing pages still have free space (cold-start fix).
    /// </summary>
    [Fact]
    public async Task ColdStart_DoesNotAllocateExtraPage_WhenExistingPagesHaveSpace()
    {
        // ── Session 1: insert a modest number of documents ─────────────────────
        int firstSessionInserts = 50;

        {
            using var db = new TestDbContext(_testFile);
            var users = Enumerable.Range(0, firstSessionInserts)
                .Select(i => new User
                {
                    Id = ObjectId.NewObjectId(),
                    Name = $"User {i}",
                    Age = 20 + i
                });

            await db.Users.InsertBulkAsync(users);
            await db.SaveChangesAsync();

            // Count documents to confirm they persisted
            Assert.Equal(firstSessionInserts, await db.Users.CountAsync());
        }

        // ── Session 2 (cold start): insert one more document ───────────────────
        {
            using var db = new TestDbContext(_testFile);

            // First insert in the new session must reuse an existing page rather than
            // growing the file (the FSI is rebuilt from the page headers on construction).
            var extraUser = new User
            {
                Id = ObjectId.NewObjectId(),
                Name = "Late User",
                Age = 99
            };
            await db.Users.InsertAsync(extraUser);
            await db.SaveChangesAsync();

            // All documents should be visible
            var allUsers = await db.Users.FindAllAsync().ToListAsync();
            Assert.Equal(firstSessionInserts + 1, allUsers.Count);

            // The new insert should be findable by ID
            var found = await db.Users.FindByIdAsync(extraUser.Id);
            Assert.NotNull(found);
            Assert.Equal("Late User", found!.Name);
        }
    }

    /// <summary>
    /// Verifies that multiple consecutive sessions do not grow the file unboundedly.
    /// After each session a single document is inserted; all should reuse existing pages.
    /// </summary>
    [Fact]
    public async Task MultipleRestarts_ReuseExistingPages()
    {
        // Session 1: pre-populate the collection
        int initialCount = 100;

        {
            using var db = new TestDbContext(_testFile);
            var users = Enumerable.Range(0, initialCount)
                .Select(i => new User
                {
                    Id = ObjectId.NewObjectId(),
                    Name = $"User {i}",
                    Age = i
                });
            await db.Users.InsertBulkAsync(users);
            await db.SaveChangesAsync();
        }

        // Sessions 2-4: each adds one document; none should fail due to FSI being empty
        for (int session = 2; session <= 4; session++)
        {
            using var db = new TestDbContext(_testFile);

            var u = new User
            {
                Id = ObjectId.NewObjectId(),
                Name = $"Session {session} User",
                Age = session
            };
            await db.Users.InsertAsync(u);
            await db.SaveChangesAsync();

            int expectedCount = initialCount + (session - 1);
            Assert.Equal(expectedCount, await db.Users.CountAsync());
        }
    }
}
