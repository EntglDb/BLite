using System.Reflection;
using BLite.Bson;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Shared;
using Microsoft.Win32.SafeHandles;

namespace BLite.Tests;

/// <summary>
/// A commit is acknowledged only once its WAL records are synced to disk (#260).
/// <see cref="FileStream.FlushAsync(CancellationToken)"/>, which the WAL used to call, hands the
/// buffer to the OS and does not sync, so a power loss dropped acknowledged commits while the pages
/// they freed stayed freed. A power loss cannot be simulated here; these tests record the calls the
/// WAL makes on its stream and check the order: every byte written for a commit is followed by a
/// sync (<c>Flush(flushToDisk: true)</c>) before the commit returns, and a truncation is synced
/// before the checkpoint returns. The WAL crash test (<c>Oracles/WalCrashTests</c>) checks what
/// survives each crash point.
/// </summary>
public sealed class WalCommitSyncTests : IDisposable
{
    private const BindingFlags Private = BindingFlags.NonPublic | BindingFlags.Instance;
    private static readonly FieldInfo s_engineWal = typeof(StorageEngine).GetField("_wal", Private)!;
    private static readonly FieldInfo s_walStream = typeof(WriteAheadLog).GetField("_walStream", Private)!;

    private readonly string _dir;

    public WalCommitSyncTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"blite_walsync_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    /// <summary>Opens a context whose WAL writes through a <see cref="RecordingWalStream"/>.</summary>
    private (TestDbContext Db, RecordingWalStream Wal) OpenRecorded(WalSyncMode mode = WalSyncMode.Full)
    {
        var db = new TestDbContext(Path.Combine(_dir, "db.blite"), PageFileConfig.Default with { WalSync = mode });
        var wal = (WriteAheadLog)s_engineWal.GetValue(db.Storage)!;
        var original = (FileStream)s_walStream.GetValue(wal)!;
        original.Flush();
        var recording = new RecordingWalStream(original.SafeFileHandle, original.Position);
        // Both streams share the handle: the WAL disposes the new one, the old one must not close it.
        GC.SuppressFinalize(original);
        s_walStream.SetValue(wal, recording);
        return (db, recording);
    }

    private static void AssertSyncedAfterLastWrite(RecordingWalStream wal, string what)
    {
        var events = wal.Events;
        var lastWrite = events.FindLastIndex(e => e is "write" or "set-length");
        Assert.True(lastWrite >= 0, $"{what}: nothing was written to the WAL");
        var lastSync = events.FindLastIndex(e => e == "sync");
        Assert.True(lastSync > lastWrite,
            $"{what} returned before the WAL was synced to disk; WAL calls since the open: {string.Join(", ", events)}");
    }

    [Fact]
    public async Task AutoCommit_Insert_Returns_Only_After_The_Wal_Is_Synced()
    {
        var (db, wal) = OpenRecorded();
        using (db)
        {
            await db.Users.InsertAsync(new User { Id = ObjectId.NewObjectId(), Name = "a", Age = 1 });
            AssertSyncedAfterLastWrite(wal, "InsertAsync");
        }
    }

    [Fact]
    public async Task Transaction_CommitAsync_Returns_Only_After_The_Wal_Is_Synced()
    {
        var (db, wal) = OpenRecorded();
        using (db)
        {
            using var tx = await db.BeginTransactionAsync();
            await db.Users.InsertAsync(new User { Id = ObjectId.NewObjectId(), Name = "a", Age = 1 }, tx);
            await db.Users.InsertAsync(new User { Id = ObjectId.NewObjectId(), Name = "b", Age = 2 }, tx);
            wal.Events.Clear();

            await tx.CommitAsync();

            AssertSyncedAfterLastWrite(wal, "CommitAsync");
        }
    }

    [Fact]
    public async Task Checkpoint_Syncs_The_Wal_Truncation_Before_Returning()
    {
        var (db, wal) = OpenRecorded();
        using (db)
        {
            await db.Users.InsertAsync(new User { Id = ObjectId.NewObjectId(), Name = "a", Age = 1 });
            wal.Events.Clear();

            await db.CheckpointAsync();

            Assert.Contains("set-length", wal.Events);
            AssertSyncedAfterLastWrite(wal, "CheckpointAsync");
        }
    }

    [Fact]
    public async Task Normal_Mode_Acknowledges_A_Commit_Without_Syncing()
    {
        var (db, wal) = OpenRecorded(WalSyncMode.Normal);
        using (db)
        {
            await db.Users.InsertAsync(new User { Id = ObjectId.NewObjectId(), Name = "a", Age = 1 });

            Assert.Contains("write", wal.Events);
            Assert.DoesNotContain("sync", wal.Events);
        }
    }

    [Fact]
    public async Task Normal_Mode_Syncs_Before_Freeing_The_Pages_A_Commit_Released()
    {
        var (db, wal) = OpenRecorded(WalSyncMode.Normal);
        using (db)
        {
            // An overflow chain: deleting the document frees its pages when the delete commits.
            var big = new User { Id = ObjectId.NewObjectId(), Name = new string('x', 60_000), Age = 1 };
            await db.Users.InsertAsync(big);
            Assert.DoesNotContain("sync", wal.Events);

            await db.Users.DeleteAsync(big.Id);

            AssertSyncedAfterLastWrite(wal, "a delete that frees pages, in Normal mode,");
        }
    }

    [Fact]
    public async Task Normal_Mode_Syncs_Before_A_Checkpoint_Copies_Pages()
    {
        var (db, wal) = OpenRecorded(WalSyncMode.Normal);
        using (db)
        {
            await db.Users.InsertAsync(new User { Id = ObjectId.NewObjectId(), Name = "a", Age = 1 });
            Assert.DoesNotContain("sync", wal.Events);

            await db.CheckpointAsync();

            var firstSync = wal.Events.IndexOf("sync");
            var truncation = wal.Events.IndexOf("set-length");
            Assert.True(firstSync >= 0 && truncation > firstSync,
                $"the WAL must be synced before the checkpoint writes its pages; WAL calls: {string.Join(", ", wal.Events)}");
            AssertSyncedAfterLastWrite(wal, "CheckpointAsync in Normal mode");
        }
    }

    /// <summary>
    /// An unbuffered stream over the WAL's handle that records, in order, each write it passes to
    /// the OS, each truncation, each flush that only hands bytes to the OS (<c>flush</c>) and each
    /// flush to disk (<c>sync</c>).
    /// </summary>
    private sealed class RecordingWalStream : FileStream
    {
        private readonly object _gate = new();
        private readonly List<string> _events = new();

        public RecordingWalStream(SafeFileHandle handle, long position) : base(handle, FileAccess.ReadWrite, bufferSize: 0)
        {
            Position = position;
        }

        public List<string> Events
        {
            get { lock (_gate) return _events; }
        }

        private void Add(string e)
        {
            lock (_gate) _events.Add(e);
        }

        // FileStream routes a derived stream's span writes through the array overload, so the
        // array overload is the one that reaches the OS.
        public override void Write(ReadOnlySpan<byte> buffer)
        {
            var copy = buffer.ToArray();
            Write(copy, 0, copy.Length);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            base.Write(buffer, offset, count);
            Add("write");
        }

        public override void WriteByte(byte value) => Write(new[] { value });

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Write(buffer.Span);
            return ValueTask.CompletedTask;
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            Write(buffer, offset, count);
            return Task.CompletedTask;
        }

        public override void SetLength(long value)
        {
            base.SetLength(value);
            Add("set-length");
        }

        public override void Flush() => Flush(false);

        public override void Flush(bool flushToDisk)
        {
            base.Flush(flushToDisk);
            Add(flushToDisk ? "sync" : "flush");
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            Add("flush");
            return base.FlushAsync(cancellationToken);
        }
    }
}
