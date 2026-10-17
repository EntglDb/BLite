// BLite Native AOT smoke test: the Consumer. Usage:
//
//   BLite.AotSmoke <dump file> [seed=1] [steps=300]
//
// Runs one deterministic sequence per feature against fresh databases under a temp directory and
// writes a canonical dump. run.sh runs it under JIT and as a Native AOT binary and diffs the two.
// Every section catches its own exception, writes it to the dump as a line starting with "!!" and
// makes the exit code 1, so a feature that fails under AOT does not hide the others.

using System.Globalization;
using System.Text;
using System.Threading.Channels;
using BLite.Bson;
using BLite.Caching;
using BLite.Core;
using BLite.Core.CDC;
using BLite.Core.Encryption;
using BLite.Core.Query.Blql;
using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace AotSmoke;

internal static partial class Program
{
    // The default names of the indexes OnModelCreating declares (idx_{property}); the typed section
    // dumps GetIndexes(), so a change of the naming shows there first.
    public const string CountIndex = "idx_Count";
    public const string CodeIndex = "idx_Code";

    private const string Passphrase = "aot-smoke passphrase";
    private const string PlaintextMarker = "PLAINTEXT-MARKER-7f3a";

    private static async Task<int> Main(string[] args)
    {
        if (args.Length < 1)
        {
            Console.Error.WriteLine("usage: BLite.AotSmoke <dump file> [seed] [steps]");
            return 2;
        }
        CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
        CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
        var dumpPath = Path.GetFullPath(args[0]);
        var seed = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 1;
        var steps = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 300;

        var root = Path.Combine(Path.GetTempPath(), $"blite-aot-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        var dump = new Dump(root);
        dump.Line($"# BLite AOT smoke dump, seed={seed} steps={steps}");

        var sections = new List<(string Name, Func<Dump, string, Task> Run)>
        {
            ("typed collection: keys from the generated mapper", MapperKeysAsync),
            ("typed collection: store sequence", (d, r) => TypedAsync(d, r, seed, steps)),
            ("encryption at rest: store sequence", (d, r) => EncryptedSequenceAsync(d, r, seed, steps)),
            ("encryption at rest: marker, reopen, wrong passphrase", (d, r) => EncryptedAsync(d, r, seed, steps)),
            ("dynamic collection: CRUD, index, CDC, BLQL", DynamicAsync),
            ("key-value store", KeyValueAsync),
            ("BLite.Caching", CachingAsync),
        };
        AddSections(sections);

        int failures = 0;
        foreach (var (name, run) in sections)
        {
            dump.Section(name);
            var dir = Path.Combine(root, $"s{sections.FindIndex(s => s.Name == name)}");
            Directory.CreateDirectory(dir);
            try
            {
                await run(dump, dir);
            }
            catch (Exception e)
            {
                failures++;
                dump.Line($"!! {name}: {e.GetType().FullName}: {e.Message.Split('\n')[0].Trim()}");
                Console.Error.WriteLine($"section '{name}' failed: {e}");
            }
        }
        dump.Section("end");
        dump.Line($"sections={sections.Count} failed={failures}");

        File.WriteAllText(dumpPath, dump.ToString(), new UTF8Encoding(false));
        try { Directory.Delete(root, recursive: true); } catch { /* best-effort */ }
        Console.WriteLine($"dump written to {dumpPath}: {sections.Count} sections, {failures} failed");
        return failures == 0 ? 0 : 1;
    }

    /// <summary>Consumer.Linq adds its section here; the Consumer has none.</summary>
    static partial void AddSections(List<(string Name, Func<Dump, string, Task> Run)> sections);

    // ── Typed collection ──────────────────────────────────────────────────────

    /// <summary>The BSON keys the generated mapper of <see cref="StoreDoc"/> writes.</summary>
    private static readonly string[] s_storeDocKeys = ["_id", "name", "count", "code", "payload"];

    /// <summary>
    /// Opens the typed context over an engine with the mapper's keys registered first. Under Native
    /// AOT the generated mapper reports no keys (candidate A1), and every write would then fail;
    /// registering them is the workaround a consumer can apply, so the rest of the sequence is
    /// compared. <see cref="MapperKeysAsync"/> keeps A1 itself in view. With <paramref name="crypto"/>
    /// the engine is configured as <c>DocumentDbContext(path, CryptoOptions)</c> configures it.
    /// </summary>
    private static AotDbContext OpenTyped(string path, CryptoOptions? crypto)
    {
        var config = crypto == null ? PageFileConfig.Default : PageFileConfig.Default with { CryptoProvider = new AesGcmCryptoProvider(crypto) };
        var storage = new StorageEngine(path, config);
        try
        {
            storage.RegisterKeys(s_storeDocKeys);
            return new AotDbContext(storage);
        }
        catch
        {
            storage.Dispose();
            throw;
        }
    }

    /// <summary>A fresh typed context that relies on the generated mapper for its keys, as a consumer would.</summary>
    private static async Task MapperKeysAsync(Dump dump, string dir)
    {
        using var db = new AotDbContext(new StorageEngine(Path.Combine(dir, "keys.db"), PageFileConfig.Default));
        await db.Docs.InsertAsync(new StoreDoc { Id = 1, Name = "first", Count = 2, Code = "c01", Payload = "p" });
        await db.Docs.InsertBulkAsync([new StoreDoc { Id = 2, Name = "second", Count = 3 }, new StoreDoc { Id = 3, Name = "third", Count = 3, Code = "c03" }]);
        await db.Docs.UpdateAsync(new StoreDoc { Id = 2, Name = "second'", Count = 4, Code = "c02" });
        foreach (var d in (await StoreRun.ReadAll(db.Docs.FindAllAsync())).Values.OrderBy(d => d.Id))
            dump.Line($"doc {Dump.Describe(d)}");
        dump.Line($"FindByIdAsync(3): {(await db.Docs.FindByIdAsync(3) is { } three ? Dump.Describe(three) : "null")}");
    }

    // ── Typed collection: the store model's sequence ──────────────────────────

    private static async Task TypedAsync(Dump dump, string dir, int seed, int steps)
    {
        var path = Path.Combine(dir, "typed.db");
        AotDbContext Open(string p) => OpenTyped(p, crypto: null);

        using var run = new StoreRun(() => Open(path), Open, Path.Combine(dir, "backup.db"), dump);
        var indexes = run.Db.Docs.GetIndexes().OrderBy(i => i.Name, StringComparer.Ordinal).ToList();
        foreach (var i in indexes)
            dump.Line($"index {i.Name}: paths=[{string.Join(",", i.PropertyPaths)}] unique={i.IsUnique} type={i.Type} primary={i.IsPrimary}");

        await run.RunAsync(StoreModel.Generate(seed, steps));
        await DumpTypedAsync(dump, run.Db, "final");

        await run.Db.CheckpointAsync();
        dump.Line("checkpoint: done");
        var backup = Path.Combine(dir, "final-backup.db");
        await run.Db.Engine.BackupAsync(backup);
        using (var copy = Open(backup))
        {
            var contents = await StoreRun.ReadAll(copy.Docs.FindAllAsync());
            StoreRun.CheckSame(run.Committed, contents, "the final backup");
            dump.Line($"backup: {contents.Count} documents, same as the committed view");
        }
    }

    private static async Task DumpTypedAsync(Dump dump, AotDbContext db, string label)
    {
        var all = await StoreRun.ReadAll(db.Docs.FindAllAsync());
        dump.Line($"{label}: count={await db.Docs.CountAsync()} scanned={all.Count}");
        foreach (var d in all.Values.OrderBy(d => d.Id))
            dump.Line($"  doc {Dump.Describe(d)}");
        foreach (var name in new[] { CountIndex, CodeIndex })
        {
            var forward = new List<string>();
            await foreach (var d in db.Docs.QueryIndexAsync(name, null, null))
                forward.Add($"{(name == CountIndex ? d.Count.ToString(CultureInfo.InvariantCulture) : d.Code ?? "null")}:{d.Id}");
            var backward = new List<string>();
            await foreach (var d in db.Docs.QueryIndexAsync(name, null, null, ascending: false))
                backward.Add($"{(name == CountIndex ? d.Count.ToString(CultureInfo.InvariantCulture) : d.Code ?? "null")}:{d.Id}");
            dump.Line($"  index {name} ({forward.Count} entries): {string.Join(" ", forward)}");
            dump.Line($"  index {name} backward: {string.Join(" ", backward)}");
        }
        var paged = new List<int>();
        await foreach (var d in db.Docs.QueryIndexAsync(CountIndex, 2, 7, ascending: true, skip: 3, take: 5))
            paged.Add(d.Id);
        dump.Line($"  index {CountIndex} [2,7] skip 3 take 5: [{string.Join(",", paged)}]");
        var scanned = new List<int>();
        await foreach (var d in db.Docs.ScanAsync((BsonReaderPredicate)CountAtLeastFive))
            scanned.Add(d.Id);
        scanned.Sort();
        dump.Line($"  ScanAsync(count >= 5): [{string.Join(",", scanned)}]");
        var found = new List<int>();
        for (int id = 1; id <= StoreModel.IdPool; id++)
            if (await db.Docs.FindByIdAsync(id) != null) found.Add(id);
        dump.Line($"  FindByIdAsync 1..{StoreModel.IdPool}: [{string.Join(",", found)}]");
    }

    /// <summary>A static <see cref="BsonReaderPredicate"/>: the document's "count" field is at least 5.</summary>
    public static bool CountAtLeastFive(BsonSpanReader reader)
    {
        reader.ReadDocumentSize();
        while (reader.Remaining > 0)
        {
            var type = reader.ReadBsonType();
            if (type == BsonType.EndOfDocument) break;
            var name = reader.ReadElementHeader();
            if (name == "count") return type == BsonType.Int32 && reader.ReadInt32() >= 5;
            reader.SkipValue(type);
        }
        return false;
    }

    // ── Encryption at rest ────────────────────────────────────────────────────

    /// <summary>The store sequence on an encrypted context; a backup is opened with the same passphrase.</summary>
    private static async Task EncryptedSequenceAsync(Dump dump, string dir, int seed, int steps)
    {
        var path = Path.Combine(dir, "encrypted.db");
        var crypto = new CryptoOptions(Passphrase, iterations: 1000);
        var ops = StoreModel.Generate(seed + 1, Math.Min(steps, 150));
        using var run = new StoreRun(() => OpenTyped(path, crypto), p => OpenTyped(p, crypto), Path.Combine(dir, "backup.db"), dump);
        await run.RunAsync(ops);
        await DumpTypedAsync(dump, run.Db, "encrypted final");
    }

    /// <summary>
    /// The same sequence with every transaction committed, no reopen and no backup in between (a page
    /// allocated by a transaction that never commits trips candidate A2), then a marker document, the
    /// files searched for it, a backup, and reopens with the passphrase and with a wrong one.
    /// </summary>
    private static async Task EncryptedAsync(Dump dump, string dir, int seed, int steps)
    {
        var path = Path.Combine(dir, "encrypted.db");
        var crypto = new CryptoOptions(Passphrase, iterations: 1000);
        var ops = StoreModel.Generate(seed + 1, Math.Min(steps, 150))
            .Select(op => op.Kind switch
            {
                StoreOpKind.Rollback => op with { Kind = StoreOpKind.Commit },
                StoreOpKind.Reopen or StoreOpKind.Backup => op with { Kind = StoreOpKind.Checkpoint },
                _ => op,
            }).ToList();
        using (var run = new StoreRun(() => OpenTyped(path, crypto), p => OpenTyped(p, crypto), Path.Combine(dir, "backup.db"), dump))
        {
            await run.RunAsync(ops);
            await run.Db.Docs.InsertAsync(new StoreDoc { Id = 1000, Name = PlaintextMarker, Count = 1, Payload = PlaintextMarker });
            await DumpTypedAsync(dump, run.Db, "encrypted final");
            await run.Db.CheckpointAsync();
            var backup = Path.Combine(dir, "encrypted-backup.db");
            await run.Db.Engine.BackupAsync(backup);
            using var copy = OpenTyped(backup, crypto);
            var contents = await StoreRun.ReadAll(copy.Docs.FindAllAsync());
            dump.Line($"backup: {contents.Count} documents, marker {(contents.ContainsKey(1000) ? "present" : "missing")}");
        }

        var marker = Encoding.UTF8.GetBytes(PlaintextMarker);
        var files = Directory.GetFiles(dir, "encrypted*", SearchOption.AllDirectories).Order(StringComparer.Ordinal).ToList();
        var leaking = files.Where(f => File.ReadAllBytes(f).AsSpan().IndexOf(marker) >= 0).Select(Path.GetFileName).ToList();
        dump.Line($"files: {files.Count}; plaintext marker found in: [{string.Join(",", leaking)}]");

        using (var reopened = OpenTyped(path, crypto))
        {
            var doc = await reopened.Docs.FindByIdAsync(1000);
            dump.Line($"reopened with the passphrase: marker document {(doc == null ? "missing" : Dump.Describe(doc))}");
        }
        try
        {
            using var wrong = OpenTyped(path, new CryptoOptions("wrong passphrase", iterations: 1000));
            var doc = await wrong.Docs.FindByIdAsync(1000);
            dump.Line($"reopened with a wrong passphrase: opened, marker document {(doc == null ? "missing" : "readable")}");
        }
        catch (Exception e)
        {
            dump.Line($"reopened with a wrong passphrase: {e.GetType().Name}");
        }
    }

    // ── DynamicCollection, CDC and BLQL ───────────────────────────────────────

    private static async Task DynamicAsync(Dump dump, string dir)
    {
        var path = Path.Combine(dir, "dynamic.db");
        var events = new List<string>();
        var subscribed = Channel.CreateUnbounded<(OperationType Op, BsonId Id)>();
        var rnd = new Random(7);

        using (var engine = new BLiteEngine(path))
        {
            var col = engine.GetOrCreateCollection("people", BsonIdType.Int32);
            await col.CreateIndexAsync("score");
            await col.CreateIndexAsync("email", unique: true);
            var observer = new Observer(events);
            using var watch = col.Watch(capturePayload: true).Subscribe(observer);
            using var sub = engine.SubscribeToChanges("people", subscribed.Writer);

            BsonDocument Person(int id, int score) => col.CreateDocument(["_id", "name", "score", "email", "active"], b => b
                .AddId(new BsonId(id)).AddString("name", $"p{id}").AddInt32("score", score)
                .AddString("email", $"p{id}@example.org").AddBoolean("active", id % 2 == 0));

            for (int id = 1; id <= 40; id++)
                await col.InsertAsync(Person(id, rnd.Next(30)));
            var bulk = await col.InsertBulkAsync(Enumerable.Range(41, 10).Select(id => Person(id, rnd.Next(30))));
            dump.Line($"inserted 40 single, bulk ids [{string.Join(",", bulk)}]");
            for (int id = 1; id <= 10; id++)
                dump.Line($"update {id}: {await col.UpdateAsync(new BsonId(id), Person(id, 100 + id))}");
            var upsert = await col.UpsertAsync(Person(60, 15));
            dump.Line($"upsert 60: inserted={upsert.Inserted} id={upsert.Id}");
            foreach (var id in new[] { 3, 13, 23, 99 })
                dump.Line($"delete {id}: {await col.DeleteAsync(new BsonId(id))}");
            try
            {
                await col.InsertAsync(col.CreateDocument(["_id", "name", "email"], b => b.AddId(new BsonId(70)).AddString("name", "dup").AddString("email", "p5@example.org")));
                dump.Line("duplicate email: accepted");
            }
            catch (BLiteValidationException) { dump.Line("duplicate email: rejected"); }

            using (var tx = await engine.BeginTransactionAsync())
            {
                await col.InsertAsync(Person(80, 5), tx);
                await col.DeleteAsync(new BsonId(81), tx);
                await tx.RollbackAsync();
            }
            dump.Line($"after rollback: 80 is {(await col.FindByIdAsync(new BsonId(80)) == null ? "absent" : "present")}");
            using (var tx = await engine.BeginTransactionAsync())
            {
                await col.InsertAsync(Person(81, 6), tx);
                await col.InsertAsync(Person(82, 7), tx);
                await tx.CommitAsync();
            }

            // 50 inserts, 10 updates, 1 upsert, 3 deletes and 2 committed inserts; the channel of
            // SubscribeToChanges gets the inserts and updates only.
            await WaitFor(() => { lock (events) return events.Count >= 66; });
            var forwarded = new List<string>();
            while (forwarded.Count < 63 && await WaitRead(subscribed.Reader) is { } e)
                forwarded.Add($"{e.Op}:{e.Id}");

            await DumpDynamicAsync(dump, col);
            await engine.CheckpointAsync();
            var backup = await engine.BackupAsync(new BackupOptions { DestinationPath = Path.Combine(dir, "dynamic-backup.db") });
            dump.Line($"backup: files={backup.FileCount}");

            lock (events) dump.Line($"Watch ({events.Count} events): {string.Join(" ", events)}");
            dump.Line($"SubscribeToChanges ({forwarded.Count} events): {string.Join(" ", forwarded)}");
            lock (events)
                if (events.Count != 66 || forwarded.Count != 63)
                    throw new SmokeMismatchException($"CDC: Watch delivered {events.Count} events (expected 66), SubscribeToChanges {forwarded.Count} (expected 63)");
        }

        using (var engine = new BLiteEngine(Path.Combine(dir, "dynamic-backup.db")))
        {
            dump.Line($"backup collections: [{string.Join(",", engine.ListCollections().Order(StringComparer.Ordinal))}]");
            var col = engine.GetOrCreateCollection("people", BsonIdType.Int32);
            dump.Line($"backup reopened: {await col.CountAsync()} documents, indexes [{string.Join(",", col.ListIndexes().Order(StringComparer.Ordinal))}]");
        }
        using (var engine = new BLiteEngine(path))
        {
            var col = engine.GetOrCreateCollection("people", BsonIdType.Int32);
            dump.Line("reopened:");
            await DumpDynamicAsync(dump, col);
        }
    }

    private static async Task DumpDynamicAsync(Dump dump, DynamicCollection col)
    {
        dump.Line($"  count={await col.CountAsync()} indexes=[{string.Join(",", col.ListIndexes().Order(StringComparer.Ordinal))}]");
        var all = new List<BsonDocument>();
        await foreach (var d in col.FindAllAsync()) all.Add(d);
        foreach (var d in all.OrderBy(d => d.TryGetId(out var id) ? id.AsInt32() : int.MinValue))
            dump.Line($"  doc {Dump.Describe(d)}");
        dump.Line($"  FindByIdAsync(7): {(await col.FindByIdAsync(new BsonId(7)) is { } seven ? Dump.Describe(seven) : "null")}");
        var range = new List<string>();
        await foreach (var d in col.QueryIndexAsync("idx_score", 10, 20)) range.Add(Dump.Describe(d));
        dump.Line($"  QueryIndexAsync(idx_score, 10, 20): {string.Join(" ", range)}");
        var scan = new List<string>();
        await foreach (var d in col.ScanAsync(ScoreAbove100)) scan.Add(d.TryGetId(out var id) ? id.ToString() : "?");
        dump.Line($"  ScanAsync(score > 100): [{string.Join(",", scan.Order(StringComparer.Ordinal))}]");

        var blql = col.Query("{ \"score\": { \"$gte\": 10, \"$lt\": 25 }, \"active\": true }").OrderBy("score").Skip(1).Take(5).ToList();
        dump.Line($"  BLQL json filter, OrderBy score, Skip 1, Take 5: {string.Join(" ", blql.Select(Dump.Describe))}");
        var typed = col.Query(BlqlFilter.Gt("score", 100)).OrderByDescending("score").Project(BlqlProjection.Include("name", "score")).ToList();
        dump.Line($"  BLQL Gt(score, 100) desc, projected: {string.Join(" ", typed.Select(Dump.Describe))}");
        dump.Line($"  BLQL Count(active) = {await col.Query(BlqlFilter.Eq("active", true)).CountAsync()}, Any(score > 1000) = {col.Query(BlqlFilter.Gt("score", 1000)).Any()}");
    }

    public static bool ScoreAbove100(BsonSpanReader reader)
    {
        reader.ReadDocumentSize();
        while (reader.Remaining > 0)
        {
            var type = reader.ReadBsonType();
            if (type == BsonType.EndOfDocument) break;
            var name = reader.ReadElementHeader();
            if (name == "score") return type == BsonType.Int32 && reader.ReadInt32() > 100;
            reader.SkipValue(type);
        }
        return false;
    }

    private sealed class Observer(List<string> events) : IObserver<BsonChangeEvent>
    {
        public void OnNext(BsonChangeEvent e)
        {
            var id = BsonId.FromBytes(e.IdBytes.Span, e.IdType);
            lock (events) events.Add($"{e.Type}:{id}{(e.Payload != null ? "+payload" : "")}");
        }
        public void OnError(Exception error) { lock (events) events.Add($"error:{error.GetType().Name}"); }
        public void OnCompleted() { }
    }

    // ── Key-value store and BLite.Caching ─────────────────────────────────────

    private static Task KeyValueAsync(Dump dump, string dir)
    {
        var path = Path.Combine(dir, "kv.db");
        using (var engine = new BLiteEngine(path))
        {
            var kv = engine.KvStore;
            for (int i = 0; i < 30; i++)
                kv.Set($"k{i:00}", Encoding.UTF8.GetBytes($"value-{i}"));
            kv.Set("ttl", [1, 2, 3], TimeSpan.FromDays(1));
            dump.Line($"delete k05: {kv.Delete("k05")}, delete missing: {kv.Delete("nope")}");
            dump.Line($"exists k06: {kv.Exists("k06")}, exists k05: {kv.Exists("k05")}, refresh ttl: {kv.Refresh("ttl", TimeSpan.FromDays(2))}");
            var executed = kv.Batch().Set("b1", [9]).Set("b2", [8, 8]).Delete("k10").Delete("k11").Execute();
            dump.Line($"batch executed {executed}; purge expired: {kv.PurgeExpired()}");
        }
        using (var engine = new BLiteEngine(path))
        {
            var kv = engine.KvStore;
            var keys = kv.ScanKeys().Order(StringComparer.Ordinal).ToList();
            dump.Line($"reopened: {keys.Count} keys");
            foreach (var k in keys)
                dump.Line($"  {k} = {Dump.Hex(kv.Get(k))}");
            dump.Line($"  ScanKeys(k1): [{string.Join(",", kv.ScanKeys("k1").Order(StringComparer.Ordinal))}]");
        }
        return Task.CompletedTask;
    }

    private static async Task CachingAsync(Dump dump, string dir)
    {
        using var engine = new BLiteEngine(Path.Combine(dir, "cache.db"));
        var cache = new BLiteDistributedCache(engine.KvStore);
        var options = new Microsoft.Extensions.Caching.Distributed.DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromHours(1) };
        cache.Set("raw", [1, 2, 3], options);
        await cache.SetAsync("raw2", [4, 5], options);
        dump.Line($"raw: {Dump.Hex(cache.Get("raw"))}, raw2: {Dump.Hex(await cache.GetAsync("raw2"))}");
        cache.Remove("raw");
        dump.Line($"raw after Remove: {Dump.Hex(cache.Get("raw"))}");

        // The typed helpers serialize with System.Text.Json; AOT_LIMITATIONS.md does not mention them.
        cache.Set("typed", new CacheEntry { Name = "entry", Hits = 3 });
        var typed = cache.Get<CacheEntry>("typed");
        dump.Line($"typed: {(typed == null ? "null" : $"{typed.Name}/{typed.Hits}")}");
        var made = cache.GetOrSet("made", () => new CacheEntry { Name = "made", Hits = 1 });
        dump.Line($"GetOrSet: {made.Name}/{made.Hits}");
    }

    public sealed class CacheEntry
    {
        public string Name { get; set; } = "";
        public int Hits { get; set; }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static async Task WaitFor(Func<bool> condition)
    {
        for (int i = 0; i < 200 && !condition(); i++) await Task.Delay(25);
    }

    private static async Task<(OperationType Op, BsonId Id)?> WaitRead(ChannelReader<(OperationType Op, BsonId Id)> reader)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        try { return await reader.ReadAsync(cts.Token); }
        catch (OperationCanceledException) { return null; }
    }

    public static void DeleteDatabase(string path)
    {
        foreach (var f in new[] { path, Path.ChangeExtension(path, ".wal"), path + "-shm", Path.ChangeExtension(path, ".wal-shm"), Path.ChangeExtension(path, ".manifest.json") })
            try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
        var collDir = path + "_coll";
        if (Directory.Exists(collDir))
            try { Directory.Delete(collDir, recursive: true); } catch { /* best-effort */ }
    }
}
