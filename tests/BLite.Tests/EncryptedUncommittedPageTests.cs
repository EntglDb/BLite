using BLite.Bson;
using BLite.Core.Encryption;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// An encrypted database must survive a data page that a transaction allocated and never committed
/// (#274). The page id is taken from the file at once, the page image is a write of the transaction,
/// so after a rollback, or while the transaction is still open, the page holds nothing the
/// transaction wrote. Reading its header (the next insert's fast path, the free-space rebuild on
/// open, a backup's copy being opened) must then see an empty page, as it does without encryption,
/// not fail AES-GCM authentication on bytes that were never encrypted.
/// </summary>
public sealed class EncryptedUncommittedPageTests : IDisposable
{
    private static readonly CryptoOptions Crypto = new("uncommitted-page-passphrase", iterations: 1);

    private readonly string _dir;

    public EncryptedUncommittedPageTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"blite_enc_uncommitted_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    private string DbPath(string name = "db.blite") => Path.Combine(_dir, name);

    private static User NewUser(string name) => new() { Id = ObjectId.NewObjectId(), Name = name, Age = 1 };

    private static async Task<List<string>> NamesAsync(TestDbContext db) =>
        (await db.Users.FindAllAsync().ToListAsync()).Select(u => u.Name).OrderBy(n => n, StringComparer.Ordinal).ToList();

    [Fact]
    public async Task Insert_After_Rollback_Of_The_Transaction_That_Allocated_The_First_Page()
    {
        var path = DbPath();
        using (var db = new TestDbContext(path, Crypto))
        {
            using (var tx = await db.BeginTransactionAsync())
            {
                await db.Users.InsertAsync(NewUser("aaaaaaaaaaaaaaaaaaaa"), tx);
                await tx.RollbackAsync();
            }

            // Threw AuthenticationTagMismatchException from FindPageWithSpace: the fast path read
            // the rolled-back page's header from the file.
            await db.Users.InsertAsync(NewUser("second"));
            Assert.Equal(new[] { "second" }, await NamesAsync(db));
        }

        using (var reopened = new TestDbContext(path, Crypto))
            Assert.Equal(new[] { "second" }, await NamesAsync(reopened));
    }

    [Fact]
    public async Task Backup_Taken_While_The_Allocating_Transaction_Is_Open_Can_Be_Opened()
    {
        var path = DbPath();
        var backupPath = DbPath("backup.blite");
        using (var db = new TestDbContext(path, Crypto))
        {
            using var tx = await db.BeginTransactionAsync();
            await db.Users.InsertAsync(NewUser("aaaaaaaaaaaaaaaaaaaa"), tx);

            await db.Storage.BackupAsync(backupPath);

            await tx.RollbackAsync();
        }

        // Threw AuthenticationTagMismatchException from RebuildFreeSpaceIndex on open.
        using (var backup = new TestDbContext(backupPath, Crypto))
        {
            Assert.Empty(await NamesAsync(backup));
            await backup.Users.InsertAsync(NewUser("into the backup"));
        }

        using (var backup = new TestDbContext(backupPath, Crypto))
            Assert.Equal(new[] { "into the backup" }, await NamesAsync(backup));

        using (var reopened = new TestDbContext(path, Crypto))
            Assert.Empty(await NamesAsync(reopened));
    }

    [Fact]
    public async Task Database_Closed_With_The_Allocating_Transaction_Open_Can_Be_Reopened()
    {
        var path = DbPath();
        using (var db = new TestDbContext(path, Crypto))
        {
            var tx = await db.BeginTransactionAsync();
            await db.Users.InsertAsync(NewUser("aaaaaaaaaaaaaaaaaaaa"), tx);
            // Closed without commit or rollback, as a process that dies mid-transaction leaves it.
        }

        // Threw AuthenticationTagMismatchException from RebuildFreeSpaceIndex on open.
        using (var reopened = new TestDbContext(path, Crypto))
        {
            Assert.Empty(await NamesAsync(reopened));
            await reopened.Users.InsertAsync(NewUser("after reopen"));
        }

        using (var again = new TestDbContext(path, Crypto))
            Assert.Equal(new[] { "after reopen" }, await NamesAsync(again));
    }
}
