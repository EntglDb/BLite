using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

public class InsertBulkTests : IDisposable
{
    private readonly string _testFile;
    private readonly TestDbContext _db;

    public InsertBulkTests()
    {
        _testFile = Path.GetTempFileName();
        _db = new TestDbContext(_testFile);
    }

    public void Dispose()
    {
        _db.Dispose();
    }

    [Fact]
    public async Task InsertBulk_PersistsData_ImmediatelyVisible()
    {
        var users = new List<User>();
        for (int i = 0; i < 50; i++)
        {
            users.Add(new User { Id = BLite.Bson.ObjectId.NewObjectId(), Name = $"User {i}", Age = 20 });
        }

        await _db.Users.InsertBulkAsync(users);
        await _db.SaveChangesAsync();

        var insertedUsers = await _db.Users.FindAllAsync().ToListAsync();

        Assert.Equal(50, insertedUsers.Count);
    }
    
    [Fact]
    public async Task InsertBulk_SpanningMultiplePages_PersistsCorrectly()
    {
        // 16KB page. User ~50 bytes. 400 users -> ~20KB -> 2 pages.
        var users = new List<User>();
        for (int i = 0; i < 400; i++)
        {
            users.Add(new User { Id = BLite.Bson.ObjectId.NewObjectId(), Name = $"User {i} with some long padding text to ensure we fill space {new string('x', 50)}", Age = 20 });
        }

        await _db.Users.InsertBulkAsync(users);
        await _db.SaveChangesAsync();

        Assert.Equal(400, await _db.Users.CountAsync());
    }

    /// <summary>
    /// Regression test for the FSI "poisoned-cache loop":
    /// when a transaction that compacted a page is rolled back, the in-memory FSI retains
    /// an inflated free-space value for that page.  Without the fix every subsequent insert
    /// would be routed to the same (now-full) page and throw "Not enough space" indefinitely.
    /// With the fix <c>InsertIntoPage</c> corrects the FSI before throwing so that the next
    /// insert attempt can allocate a new page and succeed.
    /// </summary>
    [Fact]
    public async Task InsertAsync_SucceedsAfterRollbackInducedFsiDesync()
    {
        // Each User with a 2000-character name serializes to ~2 050 bytes of BSON + 8 bytes
        // of SlotEntry = ~2 058 bytes per slot.  A 16 KB data page (16 360 bytes of usable
        // space) therefore holds at most 7 such documents, leaving ~2 004 bytes free.
        // 2 004 < 2 058, so no existing page can absorb another large user after the page
        // is full.  Inserting 300 large documents fills roughly 43 pages this way.
        const int docCount = 300;
        string largeName = new string('A', 2000);

        var users = Enumerable.Range(0, docCount)
            .Select(i => new User
            {
                Id = ObjectId.NewObjectId(),
                Name = largeName,
                Age = i % 100
            })
            .ToList();

        await _db.Users.InsertBulkAsync(users);
        Assert.Equal(docCount, await _db.Users.CountAsync());

        // Phase 2 – delete inside a transaction, then roll back.
        // DeleteCore compacts the page and calls _fsi.Update immediately so the FSI
        // believes there is ample free space on that page.  The rollback discards the
        // WAL-cache entry so the physical page reverts to the pre-delete state, but the
        // FSI retains the inflated (stale) value – this is exactly the desync from the bug.
        var toDelete = users.Take(5).Select(u => u.Id).ToList();
        using var txn = _db.BeginTransaction();
        await _db.Users.DeleteBulkAsync(toDelete, txn);
        await txn.RollbackAsync();

        // All 300 original documents must still be present after the rollback.
        Assert.Equal(docCount, await _db.Users.CountAsync());

        // Phase 3 – insert after the desync.
        // FindPageWithSpace will choose the stale page (FSI says ~10 KB free, physical
        // actually has ~2 004 bytes).  InsertIntoPage reads the real header, detects
        // the mismatch, and – with the fix – corrects the FSI before throwing.
        // On the next attempt all pages have an accurate FSI value (<= 2 004 bytes), so
        // FindPageWithSpace returns 0, AllocateNewDataPage is called, and the insert succeeds.
        //
        // Without the fix: FSI is never corrected → every subsequent insert is routed to
        // the same full page and raises InvalidOperationException indefinitely.
        const int maxAttempts = 2; // 1 correction attempt + 1 successful attempt
        bool inserted = false;
        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await _db.Users.InsertAsync(new User
                {
                    Id = ObjectId.NewObjectId(),
                    Name = largeName,
                    Age = 0
                });
                inserted = true;
                break;
            }
            catch (InvalidOperationException ex) when (ex.Message.StartsWith("Not enough space"))
            {
                // Expected on the first attempt when the stale FSI entry is encountered.
                // The fix must have corrected the FSI; the next attempt will succeed.
                if (attempt == maxAttempts)
                    throw; // Still failing after the fix – something is wrong.
            }
        }

        Assert.True(inserted, "Insert after rollback-induced FSI desync must succeed within 2 attempts.");
        Assert.Equal(docCount + 1, await _db.Users.CountAsync());
    }
}
