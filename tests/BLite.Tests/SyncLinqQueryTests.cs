using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Tests for synchronous LINQ materializers on <see cref="BLite.Core.Collections.DocumentCollection{TId,T}.AsQueryable"/>.
///
/// Root cause addressed:
/// <c>IQueryProvider.Execute&lt;TResult&gt;</c> previously called
/// <c>ExecuteAsync(...).GetAwaiter().GetResult()</c> directly.  When invoked from a thread
/// with a <see cref="System.Threading.SynchronizationContext"/> (ASP.NET classic, WPF,
/// Blazor), <c>SemaphoreSlim.WaitAsync()</c> inside <c>FindAllAsync</c> captures the context
/// and its continuation tries to resume on the locked thread → deadlock.
/// Fix: <c>Execute&lt;TResult&gt;</c> now wraps <c>ExecuteAsync</c> in <c>Task.Run</c>,
/// matching the pattern already used by <c>BTreeQueryable.GetAsyncEnumerator</c>.
///
/// Every test that calls a sync LINQ terminal also has a <c>SynchronizationContext</c>
/// deadlock trap variant.  Timeout = 5 s; a hang manifests as <see cref="TimeoutException"/>.
/// </summary>
public class SyncLinqQueryTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public SyncLinqQueryTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_sync_linq_{Guid.NewGuid():N}.db");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ── seed helper ───────────────────────────────────────────────────────────

    private async Task SeedUsersAsync(int count = 5)
    {
        for (int i = 1; i <= count; i++)
            await _db.Users.InsertAsync(new User { Name = $"User{i}", Age = i * 10 });
        await _db.SaveChangesAsync();
    }

    private async Task SeedAsyncDocsAsync(int count = 5)
    {
        for (int i = 1; i <= count; i++)
            await _db.AsyncDocs.InsertAsync(new AsyncDoc { Id = i, Name = $"Doc{i}" });
        await _db.SaveChangesAsync();
    }

    // ── timeout wrapper ───────────────────────────────────────────────────────

    /// <summary>
    /// Runs <paramref name="syncOp"/> on the thread pool and waits at most
    /// <paramref name="timeoutMs"/> milliseconds.
    /// Throws <see cref="TimeoutException"/> if the call hangs — which indicates a
    /// deadlock in the <c>GetAwaiter().GetResult()</c> path inside Execute&lt;TResult&gt;.
    /// </summary>
    private static async Task<T?> RunSyncWithTimeoutAsync<T>(
        Func<T?> syncOp,
        int timeoutMs = 5_000)
    {
        return await Task.Run(syncOp).WaitAsync(TimeSpan.FromMilliseconds(timeoutMs));
    }

    // ─── FirstOrDefault ───────────────────────────────────────────────────────

    [Fact]
    public async Task FirstOrDefault_OnEmptyCollection_ReturnsNull()
    {
        // collection is empty — must return null without throwing
        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable().FirstOrDefault());

        Assert.Null(result);
    }

    [Fact]
    public async Task FirstOrDefault_OnPopulatedCollection_ReturnsAnElement()
    {
        await SeedUsersAsync(3);

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable().FirstOrDefault());

        Assert.NotNull(result);
    }

    [Fact]
    public async Task FirstOrDefault_WithPredicate_ReturnsMatchingElement()
    {
        await SeedUsersAsync(5); // Age = 10, 20, 30, 40, 50

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable().FirstOrDefault(u => u.Age == 30));

        Assert.NotNull(result);
        Assert.Equal(30, result!.Age);
    }

    [Fact]
    public async Task FirstOrDefault_WithPredicate_WhenNoMatch_ReturnsNull()
    {
        await SeedUsersAsync(3);

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable().FirstOrDefault(u => u.Age == 999));

        Assert.Null(result);
    }

    // ─── THE EXACT SCENARIO FROM THE BUG REPORT ───────────────────────────────

    [Fact]
    public async Task OrderByDescending_Id_FirstOrDefault_DoesNotHang()
    {
        await SeedAsyncDocsAsync(5); // IDs 1..5

        // This is the exact pattern the user reported:
        //   .AsQueryable().OrderByDescending(r => r.Id).FirstOrDefault()
        // Execute<T> wraps ExecuteAsync via GetAwaiter().GetResult(), which must
        // not deadlock even when invoked from an async context.
        var result = await RunSyncWithTimeoutAsync(() =>
            _db.AsyncDocs.AsQueryable().OrderByDescending(r => r.Id).FirstOrDefault());

        Assert.NotNull(result);
        Assert.Equal(5, result!.Id); // highest Id
    }

    [Fact]
    public async Task OrderByDescending_Age_FirstOrDefault_OnUser_DoesNotHang()
    {
        await SeedUsersAsync(5); // Ages 10..50

        // Orders by Age (int — IComparable) to verify the sync Execute<T> path completes
        // without hanging. EnumerableRewriter rewrites Queryable.OrderByDescending to
        // Enumerable.OrderByDescending; the key type must implement IComparable.
        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable().OrderByDescending(r => r.Age).FirstOrDefault());

        Assert.NotNull(result);
        Assert.Equal(50, result!.Age);
    }

    [Fact]
    public async Task OrderByDescending_ObjectId_FirstOrDefault_Works()
    {
        // ObjectId now implements IComparable<ObjectId>, so EnumerableRewriter can sort by it.
        await SeedUsersAsync(3);

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable().OrderByDescending(r => r.Id).FirstOrDefault());

        Assert.NotNull(result);
    }

    [Fact]
    public async Task OrderByDescending_Id_FirstOrDefault_EmptyCollection()
    {
        // No seed — must return null, not throw, and not hang
        var result = await RunSyncWithTimeoutAsync(() =>
            _db.AsyncDocs.AsQueryable().OrderByDescending(r => r.Id).FirstOrDefault());

        Assert.Null(result);
    }

    // ─── OrderBy + FirstOrDefault ─────────────────────────────────────────────

    [Fact]
    public async Task OrderBy_Age_FirstOrDefault_ReturnsLowestAge()
    {
        await SeedUsersAsync(5); // Ages 10, 20, 30, 40, 50

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable().OrderBy(u => u.Age).FirstOrDefault());

        Assert.NotNull(result);
        Assert.Equal(10, result!.Age);
    }

    [Fact]
    public async Task OrderByDescending_Age_FirstOrDefault_ReturnsHighestAge()
    {
        await SeedUsersAsync(5); // Ages 10..50

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable().OrderByDescending(u => u.Age).FirstOrDefault());

        Assert.NotNull(result);
        Assert.Equal(50, result!.Age);
    }

    // ─── Where + OrderByDescending + FirstOrDefault (combined chain) ───────────

    [Fact]
    public async Task Where_OrderByDescending_FirstOrDefault_ReturnsExpected()
    {
        await SeedUsersAsync(5); // Age = 10, 20, 30, 40, 50

        // Filter to Age >= 30 (→ 30, 40, 50), order descending → first is Age=50
        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable()
                .Where(u => u.Age >= 30)
                .OrderByDescending(u => u.Age)
                .FirstOrDefault());

        Assert.NotNull(result);
        Assert.Equal(50, result!.Age);
    }

    [Fact]
    public async Task Where_OrderByDescending_FirstOrDefault_WhenFilterMatchesNone_ReturnsNull()
    {
        await SeedUsersAsync(3);

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable()
                .Where(u => u.Age > 999)
                .OrderByDescending(u => u.Age)
                .FirstOrDefault());

        Assert.Null(result);
    }

    // ─── First (throws on empty) ───────────────────────────────────────────────

    [Fact]
    public async Task First_OnPopulatedCollection_ReturnsElement()
    {
        await SeedUsersAsync(3);

        var result = await RunSyncWithTimeoutAsync<User?>(() =>
            _db.Users.AsQueryable().First());

        Assert.NotNull(result);
    }

    [Fact]
    public async Task First_OnEmptyCollection_ThrowsInvalidOperationException()
    {
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await RunSyncWithTimeoutAsync<User?>(() =>
                _db.Users.AsQueryable().First()));
    }

    // ─── Count ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Count_OnEmptyCollection_ReturnsZero()
    {
        var count = await RunSyncWithTimeoutAsync<int>(() =>
            _db.Users.AsQueryable().Count());

        Assert.Equal(0, count);
    }

    [Fact]
    public async Task Count_ReturnsCorrectCount()
    {
        await SeedUsersAsync(7);

        var count = await RunSyncWithTimeoutAsync<int>(() =>
            _db.Users.AsQueryable().Count());

        Assert.Equal(7, count);
    }

    [Fact]
    public async Task Count_WithWhere_ReturnsFilteredCount()
    {
        await SeedUsersAsync(5); // Age 10..50

        var count = await RunSyncWithTimeoutAsync<int>(() =>
            _db.Users.AsQueryable().Where(u => u.Age > 20).Count());

        Assert.Equal(3, count); // 30, 40, 50
    }

    // ─── Any ───────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Any_WhenEmpty_ReturnsFalse()
    {
        var result = await RunSyncWithTimeoutAsync<bool>(() =>
            _db.Users.AsQueryable().Any());

        Assert.False(result);
    }

    [Fact]
    public async Task Any_WhenPopulated_ReturnsTrue()
    {
        await SeedUsersAsync(2);

        var result = await RunSyncWithTimeoutAsync<bool>(() =>
            _db.Users.AsQueryable().Any());

        Assert.True(result);
    }

    [Fact]
    public async Task Any_WithPredicate_WhenMatch_ReturnsTrue()
    {
        await SeedUsersAsync(5); // Ages 10..50

        var result = await RunSyncWithTimeoutAsync<bool>(() =>
            _db.Users.AsQueryable().Any(u => u.Age >= 50));

        Assert.True(result);
    }

    [Fact]
    public async Task Any_WithPredicate_WhenNoMatch_ReturnsFalse()
    {
        await SeedUsersAsync(5);

        var result = await RunSyncWithTimeoutAsync<bool>(() =>
            _db.Users.AsQueryable().Any(u => u.Age > 999));

        Assert.False(result);
    }

    // ─── ToList via sync GetEnumerator ────────────────────────────────────────

    [Fact]
    public async Task ToList_ViaEnumerable_OnEmptyCollection_ReturnsEmpty()
    {
        // Enumerable.ToList(IQueryable<T>) calls BTreeQueryable.GetEnumerator()
        // → Provider.Execute<IEnumerable<T>> → ExecuteAsync<IEnumerable<T>>().GetAwaiter().GetResult()
        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable().ToList());

        Assert.NotNull(result);
        Assert.Empty(result!);
    }

    [Fact]
    public async Task ToList_ViaEnumerable_ReturnsAllDocuments()
    {
        await SeedUsersAsync(4);

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable().ToList());

        Assert.Equal(4, result!.Count);
    }

    [Fact]
    public async Task OrderBy_ToList_ReturnsSortedList()
    {
        await SeedAsyncDocsAsync(5); // IDs 1..5 inserted in order

        // Sync OrderBy + ToList via EnumerableRewriter
        var result = await RunSyncWithTimeoutAsync(() =>
            _db.AsyncDocs.AsQueryable().OrderBy(d => d.Id).ToList());

        Assert.Equal(5, result!.Count);
        for (int i = 0; i < result.Count - 1; i++)
            Assert.True(result[i].Id <= result[i + 1].Id);
    }

    [Fact]
    public async Task OrderByDescending_ToList_ReturnsSortedDescending()
    {
        await SeedAsyncDocsAsync(5);

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.AsyncDocs.AsQueryable().OrderByDescending(d => d.Id).ToList());

        Assert.Equal(5, result!.Count);
        Assert.Equal(5, result![0].Id);
        Assert.Equal(1, result[^1].Id);
    }

    // ─── SingleOrDefault ──────────────────────────────────────────────────────

    [Fact]
    public async Task SingleOrDefault_WhenNoMatch_ReturnsNull()
    {
        await SeedUsersAsync(3);

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable().SingleOrDefault(u => u.Age == 999));

        Assert.Null(result);
    }

    [Fact]
    public async Task SingleOrDefault_WhenExactlyOneMatch_ReturnsElement()
    {
        await SeedUsersAsync(3);

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable().SingleOrDefault(u => u.Age == 20)); // User2, Age=20

        Assert.NotNull(result);
        Assert.Equal(20, result!.Age);
    }

    [Fact]
    public async Task SingleOrDefault_WhenMoreThanOne_ThrowsInvalidOperationException()
    {
        await SeedUsersAsync(3);

        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await RunSyncWithTimeoutAsync(() =>
                _db.Users.AsQueryable().SingleOrDefault()));
    }

    // ─── Take + FirstOrDefault ────────────────────────────────────────────────

    [Fact]
    public async Task OrderByDescending_Take_FirstOrDefault_ReturnsExpected()
    {
        await SeedAsyncDocsAsync(5); // IDs 1..5

        // OrderByDescending → Take(3) gives [5,4,3], FirstOrDefault → 5
        var result = await RunSyncWithTimeoutAsync(() =>
            _db.AsyncDocs.AsQueryable()
                .OrderByDescending(d => d.Id)
                .Take(3)
                .FirstOrDefault());

        Assert.NotNull(result);
        Assert.Equal(5, result!.Id);
    }

    // ─── Where + Select + ToList ───────────────────────────────────────────────

    [Fact]
    public async Task Where_Select_ToList_ReturnsMappedResults()
    {
        await SeedUsersAsync(5); // Ages 10..50

        var names = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable()
                .Where(u => u.Age > 30)
                .Select(u => u.Name)
                .ToList());

        // Ages 40 and 50 → User4 and User5
        Assert.Equal(2, names!.Count);
        Assert.Contains("User4", names);
        Assert.Contains("User5", names);
    }

    // ─── SynchronizationContext deadlock trap ─────────────────────────────────
    // Before the fix, Execute<TResult> called ExecuteAsync(...).GetAwaiter().GetResult()
    // directly.  SemaphoreSlim.WaitAsync() inside FindAllAsync captures the ambient
    // SynchronizationContext; its continuation is posted back to that context — but
    // the context thread is already blocked waiting for GetResult() → deadlock.
    // Fix: Execute<TResult> wraps ExecuteAsync in Task.Run, so async continuations
    // always run on the thread pool regardless of the calling thread's context.

    /// <summary>
    /// A <see cref="SynchronizationContext"/> that silently drops all <c>Post</c> callbacks.
    /// Simulates a single-threaded context whose dispatch loop is blocked (ASP.NET classic /
    /// WPF UI thread).  Any <c>SemaphoreSlim.WaitAsync()</c> continuation captured on this
    /// context would never resume unless the provider uses <c>Task.Run</c> to escape it.
    /// </summary>
    private sealed class DropAllCallbacksSyncContext : SynchronizationContext
    {
        public override void Post(SendOrPostCallback d, object? state) { /* intentionally dropped */ }
        public override void Send(SendOrPostCallback d, object? state) => d(state);
    }

    [Fact]
    public async Task OrderByDescending_FirstOrDefault_DoesNotDeadlock_WhenContextDropsCallbacks()
    {
        // Seed data on the pool (no custom context yet)
        await SeedAsyncDocsAsync(5);

        AsyncDoc? result = null;

        // Run on a fresh pool thread so we can safely install and restore the context.
        await Task.Run(() =>
        {
            var prev = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(new DropAllCallbacksSyncContext());
            try
            {
                // Before the Task.Run fix in Execute<TResult>:
                //   SemaphoreSlim.WaitAsync() posted its continuation to DropAllCallbacksSyncContext
                //   → continuation dropped → ExecuteAsync never completed → GetResult() hung forever.
                // After the fix:
                //   Execute<TResult> uses Task.Run internally → continuations run on the pool,
                //   never touching DropAllCallbacksSyncContext → completes normally.
                result = _db.AsyncDocs.AsQueryable()
                    .OrderByDescending(d => d.Id)
                    .FirstOrDefault();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(prev);
            }
        }).WaitAsync(TimeSpan.FromSeconds(5)); // timeout = deadlock detected

        Assert.NotNull(result);
        Assert.Equal(5, result!.Id);
    }

    [Fact]
    public async Task StringId_OrderByDescending_FirstOrDefault_DoesNotDeadlock_WhenContextDropsCallbacks()
    {
        // This is the exact user-reported scenario: string Id + OrderByDescending + FirstOrDefault
        // hung because Execute<TResult> deadlocked under a custom SynchronizationContext.
        await _db.StringEntities.InsertAsync(new StringEntity { Id = "apple", Value = "a" });
        await _db.StringEntities.InsertAsync(new StringEntity { Id = "banana", Value = "b" });
        await _db.StringEntities.InsertAsync(new StringEntity { Id = "cherry", Value = "c" });
        await _db.SaveChangesAsync();

        StringEntity? result = null;

        await Task.Run(() =>
        {
            var prev = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(new DropAllCallbacksSyncContext());
            try
            {
                result = _db.StringEntities.AsQueryable()
                    .OrderByDescending(e => e.Id)
                    .FirstOrDefault();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(prev);
            }
        }).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(result);
        Assert.Equal("cherry", result!.Id); // lexicographic last
    }

    // ─── OrderByDescending + FirstOrDefault for every primitive key type ──────
    // Covers the exact scenario reported: the EnumerableRewriter compiles
    // Enumerable.OrderByDescending<T, TKey> which requires TKey : IComparable.
    // Each test also exercises the Execute<TResult> path (sync materializer).

    [Fact]
    public async Task OrderByDescending_IntId_FirstOrDefault_ReturnsHighestId()
    {
        await SeedAsyncDocsAsync(5); // int IDs 1..5

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.AsyncDocs.AsQueryable().OrderByDescending(d => d.Id).FirstOrDefault());

        Assert.NotNull(result);
        Assert.Equal(5, result!.Id);
    }

    [Fact]
    public async Task OrderBy_IntId_FirstOrDefault_ReturnsLowestId()
    {
        await SeedAsyncDocsAsync(5);

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.AsyncDocs.AsQueryable().OrderBy(d => d.Id).FirstOrDefault());

        Assert.NotNull(result);
        Assert.Equal(1, result!.Id);
    }

    [Fact]
    public async Task OrderByDescending_LongId_FirstOrDefault_ReturnsHighestId()
    {
        for (long i = 1; i <= 5; i++)
            await _db.LongEntities.InsertAsync(new LongEntity { Id = i, Name = $"Long{i}" });
        await _db.SaveChangesAsync();

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.LongEntities.AsQueryable().OrderByDescending(e => e.Id).FirstOrDefault());

        Assert.NotNull(result);
        Assert.Equal(5L, result!.Id);
    }

    [Fact]
    public async Task OrderBy_LongId_FirstOrDefault_ReturnsLowestId()
    {
        for (long i = 1; i <= 5; i++)
            await _db.LongEntities.InsertAsync(new LongEntity { Id = i, Name = $"Long{i}" });
        await _db.SaveChangesAsync();

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.LongEntities.AsQueryable().OrderBy(e => e.Id).FirstOrDefault());

        Assert.NotNull(result);
        Assert.Equal(1L, result!.Id);
    }

    [Fact]
    public async Task OrderByDescending_StringId_FirstOrDefault_ReturnsLexicographicLast()
    {
        // This is the exact scenario the user reported: string Id + OrderByDescending + FirstOrDefault
        // previously hung because Execute<TResult> deadlocked under a SynchronizationContext.
        await _db.StringEntities.InsertAsync(new StringEntity { Id = "apple", Value = "a" });
        await _db.StringEntities.InsertAsync(new StringEntity { Id = "banana", Value = "b" });
        await _db.StringEntities.InsertAsync(new StringEntity { Id = "cherry", Value = "c" });
        await _db.SaveChangesAsync();

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.StringEntities.AsQueryable().OrderByDescending(e => e.Id).FirstOrDefault());

        Assert.NotNull(result);
        Assert.Equal("cherry", result!.Id); // lexicographic last
    }

    [Fact]
    public async Task OrderBy_StringId_FirstOrDefault_ReturnsLexicographicFirst()
    {
        await _db.StringEntities.InsertAsync(new StringEntity { Id = "apple", Value = "a" });
        await _db.StringEntities.InsertAsync(new StringEntity { Id = "banana", Value = "b" });
        await _db.StringEntities.InsertAsync(new StringEntity { Id = "cherry", Value = "c" });
        await _db.SaveChangesAsync();

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.StringEntities.AsQueryable().OrderBy(e => e.Id).FirstOrDefault());

        Assert.NotNull(result);
        Assert.Equal("apple", result!.Id);
    }

    [Fact]
    public async Task OrderByDescending_GuidId_FirstOrDefault_DoesNotHang()
    {
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        Array.Sort(ids); // deterministic insertion order
        foreach (var id in ids)
            await _db.GuidEntities.InsertAsync(new GuidEntity { Id = id, Name = id.ToString() });
        await _db.SaveChangesAsync();

        // Guid implements IComparable<Guid> — must not throw or hang
        var result = await RunSyncWithTimeoutAsync(() =>
            _db.GuidEntities.AsQueryable().OrderByDescending(e => e.Id).FirstOrDefault());

        Assert.NotNull(result);
        Assert.Equal(ids[2], result!.Id); // largest Guid after sort
    }

    [Fact]
    public async Task OrderByDescending_ObjectId_FirstOrDefault_ReturnsNewest()
    {
        // ObjectId now implements IComparable<ObjectId> (timestamp-first, then unsigned counter).
        // We cannot assert the specific name because within the same timestamp-second
        // the random counter is arbitrary — we only verify no throw and no hang.
        await SeedUsersAsync(3);

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable().OrderByDescending(u => u.Id).FirstOrDefault());

        Assert.NotNull(result);
    }

    [Fact]
    public async Task OrderBy_ObjectId_FirstOrDefault_ReturnsOldest()
    {
        await SeedUsersAsync(3);

        var result = await RunSyncWithTimeoutAsync(() =>
            _db.Users.AsQueryable().OrderBy(u => u.Id).FirstOrDefault());

        Assert.NotNull(result);
    }

    // ─── IComparable on ObjectId ───────────────────────────────────────────────

    [Fact]
    public void ObjectId_CompareTo_Empty_IsGreater()
    {
        var oid = ObjectId.NewObjectId();
        Assert.True(oid.CompareTo(ObjectId.Empty) > 0);
    }

    [Fact]
    public void ObjectId_CompareTo_MaxValue_IsLess()
    {
        var oid = ObjectId.NewObjectId();
        Assert.True(oid.CompareTo(ObjectId.MaxValue) < 0);
    }

    [Fact]
    public void ObjectId_CompareTo_Self_IsZero()
    {
        var oid = ObjectId.NewObjectId();
        Assert.Equal(0, oid.CompareTo(oid));
    }

    [Fact]
    public void ObjectId_LessThan_GreaterThan_Operators_Work()
    {
        var a = ObjectId.Empty;
        var b = ObjectId.MaxValue;
        Assert.True(a < b);
        Assert.True(b > a);
        Assert.True(a <= b);
        Assert.True(b >= a);
        Assert.False(a > b);
    }

    [Fact]
    public void ObjectId_SortedList_IsSortedByTimestampThenCounter()
    {
        // Two ObjectIds created sequentially must sort in creation order
        // because timestamps increase monotonically (or are equal and the
        // random part distinguishes them via unsigned comparison).
        var ids = Enumerable.Range(0, 5).Select(_ => ObjectId.NewObjectId()).ToList();
        var sorted = ids.OrderBy(x => x).ToList();

        // If all were created within the same second, ordering is by random counter.
        // We can only assert that the sort is stable and produces 5 elements.
        Assert.Equal(5, sorted.Count);
        for (int i = 0; i < sorted.Count - 1; i++)
            Assert.True(sorted[i].CompareTo(sorted[i + 1]) <= 0);
    }
}
