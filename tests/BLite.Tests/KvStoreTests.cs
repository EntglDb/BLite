using BLite.Core;
using BLite.Core.KeyValue;

namespace BLite.Tests;

public class KvStoreTests
{
    private static string TempDb() =>
        Path.Combine(Path.GetTempPath(), $"kv_{Guid.NewGuid():N}.db");

    private static void Cleanup(string path)
    {
        if (File.Exists(path)) File.Delete(path);
        var wal = Path.ChangeExtension(path, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ── Basic CRUD ────────────────────────────────────────────────────────────

    [Fact]
    public void Set_Get_RoundTrip()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            engine.KvStore.Set("hello", "world"u8.ToArray());
            Assert.Equal("world"u8.ToArray(), engine.KvStore.Get("hello"));
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public void Get_MissingKey_ReturnsNull()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            Assert.Null(engine.KvStore.Get("no-such-key"));
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public void Set_OverwritesExistingKey()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            engine.KvStore.Set("k", "v1"u8.ToArray());
            engine.KvStore.Set("k", "v2"u8.ToArray());
            Assert.Equal("v2"u8.ToArray(), engine.KvStore.Get("k"));
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public void Delete_RemovesKey()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            engine.KvStore.Set("k", "v"u8.ToArray());
            Assert.True(engine.KvStore.Delete("k"));
            Assert.Null(engine.KvStore.Get("k"));
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public void Delete_MissingKey_ReturnsFalse()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            Assert.False(engine.KvStore.Delete("ghost"));
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public void Exists_LiveKey_ReturnsTrue_MissingKey_ReturnsFalse()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            engine.KvStore.Set("alive", "1"u8.ToArray());
            Assert.True(engine.KvStore.Exists("alive"));
            Assert.False(engine.KvStore.Exists("dead"));
        }
        finally { Cleanup(path); }
    }

    // ── TTL / Expiry ──────────────────────────────────────────────────────────

    [Fact]
    public void TTL_WithinWindow_Returns()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            engine.KvStore.Set("session", "data"u8.ToArray(), TimeSpan.FromMinutes(10));
            Assert.NotNull(engine.KvStore.Get("session"));
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public void TTL_ExpiredKey_ReturnsNull()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            engine.KvStore.Set("token", "abc"u8.ToArray(), TimeSpan.FromMilliseconds(50));
            Thread.Sleep(200);
            Assert.Null(engine.KvStore.Get("token"));
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public void Refresh_ExtendsExpiry()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            engine.KvStore.Set("s", "data"u8.ToArray(), TimeSpan.FromMilliseconds(300));
            Thread.Sleep(150);
            Assert.True(engine.KvStore.Refresh("s", TimeSpan.FromMinutes(1)));
            Thread.Sleep(250); // would expire without the refresh
            Assert.NotNull(engine.KvStore.Get("s"));
        }
        finally { Cleanup(path); }
    }

    // ── ScanKeys ──────────────────────────────────────────────────────────────

    [Fact]
    public void ScanKeys_WithPrefix_FiltersCorrectly()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            engine.KvStore.Set("user:1", "a"u8.ToArray());
            engine.KvStore.Set("user:2", "b"u8.ToArray());
            engine.KvStore.Set("product:1", "c"u8.ToArray());
            var keys = engine.KvStore.ScanKeys("user:").OrderBy(k => k).ToList();
            Assert.Equal(["user:1", "user:2"], keys);
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public void ScanKeys_NoPrefix_ReturnsAll()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            engine.KvStore.Set("a", "1"u8.ToArray());
            engine.KvStore.Set("b", "2"u8.ToArray());
            engine.KvStore.Set("c", "3"u8.ToArray());
            Assert.Equal(3, engine.KvStore.ScanKeys().Count());
        }
        finally { Cleanup(path); }
    }

    // ── PurgeExpired ──────────────────────────────────────────────────────────

    [Fact]
    public void PurgeExpired_RemovesOnlyExpiredEntries()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            engine.KvStore.Set("exp1", "v"u8.ToArray(), TimeSpan.FromMilliseconds(50));
            engine.KvStore.Set("exp2", "v"u8.ToArray(), TimeSpan.FromMilliseconds(50));
            engine.KvStore.Set("perm", "v"u8.ToArray());
            Thread.Sleep(200);
            Assert.Equal(2, engine.KvStore.PurgeExpired());
            Assert.NotNull(engine.KvStore.Get("perm"));
        }
        finally { Cleanup(path); }
    }

    // ── Persistence ───────────────────────────────────────────────────────────

    [Fact]
    public void Persistence_SurvivesEngineReopen()
    {
        var path = TempDb();
        try
        {
            using (var e = new BLiteEngine(path))
                e.KvStore.Set("persisted", "hello"u8.ToArray());

            using (var e = new BLiteEngine(path))
                Assert.Equal("hello"u8.ToArray(), e.KvStore.Get("persisted"));
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public void Persistence_DeletedKey_Gone_AfterReopen()
    {
        var path = TempDb();
        try
        {
            using (var e = new BLiteEngine(path))
            {
                e.KvStore.Set("temp", "v"u8.ToArray());
                e.KvStore.Delete("temp");
            }
            using (var e = new BLiteEngine(path))
                Assert.Null(e.KvStore.Get("temp"));
        }
        finally { Cleanup(path); }
    }

    // ── Batch ─────────────────────────────────────────────────────────────────

    [Fact]
    public void Batch_SetsAndDeletes_Atomically()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            engine.KvStore.Set("existing", "old"u8.ToArray());

            int count = engine.KvStore.Batch()
                .Set("k1", "v1"u8.ToArray())
                .Set("k2", "v2"u8.ToArray())
                .Delete("existing")
                .Execute();

            Assert.Equal(3, count);
            Assert.Equal("v1"u8.ToArray(), engine.KvStore.Get("k1"));
            Assert.Equal("v2"u8.ToArray(), engine.KvStore.Get("k2"));
            Assert.Null(engine.KvStore.Get("existing"));
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public void Batch_EmptyBatch_ReturnsZero()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            Assert.Equal(0, engine.KvStore.Batch().Execute());
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public void Batch_ClearsAfterExecute()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            var batch = engine.KvStore.Batch().Set("k", "v"u8.ToArray());
            Assert.Equal(1, batch.Count);
            batch.Execute();
            Assert.Equal(0, batch.Count);
        }
        finally { Cleanup(path); }
    }

    // ── BLiteKvOptions ────────────────────────────────────────────────────────

    [Fact]
    public void DefaultTtl_AppliedWhenNoneSpecified()
    {
        var path = TempDb();
        try
        {
            var opts = new BLiteKvOptions { DefaultTtl = TimeSpan.FromMilliseconds(100) };
            using var engine = new BLiteEngine(path, opts);
            engine.KvStore.Set("k", "v"u8.ToArray()); // no explicit TTL
            Assert.NotNull(engine.KvStore.Get("k"));
            Thread.Sleep(300);
            Assert.Null(engine.KvStore.Get("k"));
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public void PurgeExpiredOnOpen_RemovesExpiredEntries()
    {
        var path = TempDb();
        try
        {
            using (var e = new BLiteEngine(path))
            {
                e.KvStore.Set("expires", "v"u8.ToArray(), TimeSpan.FromMilliseconds(50));
                e.KvStore.Set("perm", "v"u8.ToArray());
            }
            Thread.Sleep(200);

            using var e2 = new BLiteEngine(path, new BLiteKvOptions { PurgeExpiredOnOpen = true });
            Assert.Null(e2.KvStore.Get("expires"));
            Assert.NotNull(e2.KvStore.Get("perm"));
        }
        finally { Cleanup(path); }
    }

    // ── Large value ───────────────────────────────────────────────────────────

    [Fact]
    public void LargeValue_RoundTripSucceeds()
    {
        var path = TempDb();
        try
        {
            using var engine = new BLiteEngine(path);
            var big = new byte[4096];
            Random.Shared.NextBytes(big);
            engine.KvStore.Set("big", big);
            Assert.Equal(big, engine.KvStore.Get("big"));
        }
        finally { Cleanup(path); }
    }
}
