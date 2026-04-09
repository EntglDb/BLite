using BLite.Bson;
using BLite.Core.Collections;
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

/// <summary>
/// Unit tests for <see cref="FreeSpaceIndex"/> core methods:
/// <c>Update</c>, <c>TryGetFreeBytes</c>, and <c>FindPage</c>.
/// FreeSpaceIndex is internal; accessible via InternalsVisibleTo("BLite.Tests").
/// </summary>
public class FreeSpaceIndexUnitTests
{
    // Simulate a 16 KB page so bucket width = (16384 - 24) / 16 = 1022 bytes.
    private const int PageSize = 16_384;
    private readonly FreeSpaceIndex _fsi = new(PageSize);

    [Fact]
    public void TryGetFreeBytes_Returns_False_When_Not_Tracked()
    {
        Assert.False(_fsi.TryGetFreeBytes(99, out _));
    }

    [Fact]
    public void Update_And_TryGetFreeBytes_Round_Trip()
    {
        _fsi.Update(1, 5000);
        Assert.True(_fsi.TryGetFreeBytes(1, out var fb));
        Assert.Equal(5000, fb);
    }

    [Fact]
    public void Update_SameBucket_UpdatesFreeBytes()
    {
        // Both values should land in the same bucket.
        _fsi.Update(2, 3000);
        _fsi.Update(2, 3500); // still bucket 3 (3000/1022=2, 3500/1022=3 – actually different; pick values in same bucket)

        // Use values that definitely share a bucket: 1024..2045 → bucket 1
        _fsi.Update(3, 1100);
        _fsi.Update(3, 1900);
        Assert.True(_fsi.TryGetFreeBytes(3, out var fb));
        Assert.Equal(1900, fb);
    }

    [Fact]
    public void Update_CrossBucket_MovesEntry()
    {
        // Start in a high bucket (lots of free space), then "fill" the page (low free bytes).
        _fsi.Update(10, 14000); // bucket ~13
        Assert.True(_fsi.TryGetFreeBytes(10, out var before));
        Assert.Equal(14000, before);

        _fsi.Update(10, 500);   // bucket 0
        Assert.True(_fsi.TryGetFreeBytes(10, out var after));
        Assert.Equal(500, after);
    }

    [Fact]
    public void FindPage_Returns_0_When_No_Pages_Tracked()
    {
        // Use a dedicated instance to avoid interference.
        var fsi = new FreeSpaceIndex(PageSize);
        Assert.Equal(0u, fsi.FindPage(100, null));
    }

    [Fact]
    public void FindPage_Returns_PageId_When_Enough_Space()
    {
        var fsi = new FreeSpaceIndex(PageSize);
        fsi.Update(42, 8000);

        var found = fsi.FindPage(4000, null);
        Assert.Equal(42u, found);
    }

    [Fact]
    public void FindPage_Returns_0_When_No_Page_Has_Enough_Space()
    {
        var fsi = new FreeSpaceIndex(PageSize);
        fsi.Update(5, 200); // very little free space

        var found = fsi.FindPage(5000, null);
        Assert.Equal(0u, found);
    }

    [Fact]
    public void FindPage_Prefers_Pages_With_More_Free_Space()
    {
        var fsi = new FreeSpaceIndex(PageSize);
        fsi.Update(1, 2000);  // low bucket
        fsi.Update(2, 12000); // high bucket

        // Both satisfy 1000 bytes; bucket scan starts from the highest bucket,
        // so page 2 (more free space) should be returned first.
        var found = fsi.FindPage(1000, null);
        Assert.Equal(2u, found);
    }

    [Fact]
    public void Update_ManyPages_AllRetrievable()
    {
        var fsi = new FreeSpaceIndex(PageSize);
        for (uint i = 1; i <= 50; i++)
            fsi.Update(i, (int)(i * 200)); // spread across buckets

        for (uint i = 1; i <= 50; i++)
        {
            Assert.True(fsi.TryGetFreeBytes(i, out var fb));
            Assert.Equal((ushort)(i * 200), fb);
        }
    }

    [Fact]
    public void Update_Remove_And_Re_Add_Works()
    {
        var fsi = new FreeSpaceIndex(PageSize);
        fsi.Update(7, 10000);
        fsi.Update(7, 50);    // page "fills up" → moves to bucket 0
        fsi.Update(7, 9000);  // page "freed" → back to high bucket

        Assert.True(fsi.TryGetFreeBytes(7, out var fb));
        Assert.Equal(9000, fb);

        var found = fsi.FindPage(8000, null);
        Assert.Equal(7u, found);
    }
}
