using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Query;
using BLite.Core.Query.Blql;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Wasm.Storage;
using BLite.Wasm.Transactions;
using Microsoft.JSInterop;

namespace BLite.Wasm.BrowserHost;

/// <summary>Outcome of one scenario, returned to the test as JSON.</summary>
public sealed class ScenarioResult
{
    public string Scenario { get; set; } = string.Empty;
    public bool Ok { get; set; }
    public double ElapsedMs { get; set; }
    public string? ErrorType { get; set; }
    public string? Error { get; set; }
    public string? Stack { get; set; }
    public Dictionary<string, string> Data { get; set; } = new();
}

[JsonSerializable(typeof(ScenarioResult))]
internal partial class HarnessJson : JsonSerializerContext;

/// <summary>
/// Scenarios the Playwright tests call through <c>DotNet.invokeMethodAsync</c>.
/// Each one catches its own exceptions and reports them, so a failing engine call
/// becomes data rather than a broken page. A call that blocks the browser's only
/// thread never returns; the test detects that with its own timeout.
/// </summary>
public static class Scenarios
{
    private const string Collection = "docs";

    // Engine kept open across calls, for scenarios that span several calls or tabs.
    private static BLiteEngine? _held;
    private static readonly List<string> Unobserved = new();

    static Scenarios()
    {
        TaskScheduler.UnobservedTaskException += (_, e) =>
        {
            lock (Unobserved) Unobserved.Add($"{e.Exception.InnerException?.GetType().Name}: {e.Exception.InnerException?.Message}");
            e.SetObserved();
        };
    }

    // ── Environment ────────────────────────────────────────────────────────────

    [JSInvokable]
    public static string Ping() => "pong";

    [JSInvokable]
    public static Task<string> Probe() => Run(nameof(Probe), async data =>
    {
        data["isBrowser"] = OperatingSystem.IsBrowser().ToString();
        data["isDynamicCodeSupported"] = RuntimeFeature.IsDynamicCodeSupported.ToString();
        data["isDynamicCodeCompiled"] = RuntimeFeature.IsDynamicCodeCompiled.ToString();
        data["processorCount"] = Environment.ProcessorCount.ToString();
        data["framework"] = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription;
        data["detectedBackend"] = (await BLiteWasm.DetectBestBackendAsync()).ToString();
    });

    /// <summary>Calls a backend's public IsAvailable() as the first BLite call in the tab.</summary>
    [JSInvokable]
    public static Task<string> IsAvailableFirst(string backend) => Run($"{nameof(IsAvailableFirst)}:{backend}", data =>
    {
        data["available"] = ParseBackend(backend) switch
        {
            WasmStorageBackend.IndexedDb => IndexedDbPageStorage.IsAvailable().ToString(),
            WasmStorageBackend.Opfs => OpfsPageStorage.IsAvailable().ToString(),
            _ => throw new ArgumentOutOfRangeException(nameof(backend), backend, null)
        };
        return Task.CompletedTask;
    });

    [JSInvokable]
    public static string DrainUnobserved()
    {
        lock (Unobserved)
        {
            var text = string.Join("\n", Unobserved);
            Unobserved.Clear();
            return text;
        }
    }

    // ── Dynamic engine, async API ────────────────────────────────────────────

    [JSInvokable]
    public static Task<string> Crud(string backend, string db, int count) => Run(nameof(Crud), async data =>
    {
        using var engine = await BLiteWasm.CreateAsync(db, ParseBackend(backend));
        var sw = Stopwatch.StartNew();
        var ids = new List<BsonId>();
        for (var i = 0; i < count; i++)
            ids.Add(await engine.InsertAsync(Collection, Doc(engine, $"item-{i}", i)));
        data["insertMs"] = sw.Elapsed.TotalMilliseconds.ToString("F1");

        var updated = await engine.UpdateAsync(Collection, ids[0], Doc(engine, "item-0", -1));
        var deleted = await engine.DeleteAsync(Collection, ids[1]);
        var first = await engine.FindByIdAsync(Collection, ids[0]);
        first!.TryGetInt32("v", out var firstValue);

        data["updated"] = updated.ToString();
        data["deleted"] = deleted.ToString();
        data["firstValue"] = firstValue.ToString();
        data["count"] = (await CountAsync(engine)).ToString();
        Expect(firstValue == -1, "updated value not read back");
        Expect(await CountAsync(engine) == count - 1, "count after delete is wrong");
    });

    [JSInvokable]
    public static Task<string> ConcurrentInserts(string backend, string db, int count) => Run(nameof(ConcurrentInserts), async data =>
    {
        using var engine = await BLiteWasm.CreateAsync(db, ParseBackend(backend));
        var tasks = Enumerable.Range(0, count)
            .Select(i => engine.InsertAsync(Collection, Doc(engine, $"c-{i}", i)).AsTask());
        await Task.WhenAll(tasks);
        var n = await CountAsync(engine);
        data["count"] = n.ToString();
        Expect(n == count, $"expected {count} documents, found {n}");
    });

    [JSInvokable]
    public static Task<string> BlqlAsync(string backend, string db) => Run(nameof(BlqlAsync), async data =>
    {
        using var engine = await BLiteWasm.CreateAsync(db, ParseBackend(backend));
        await SeedAsync(engine, 20);
        var query = engine.GetOrCreateCollection(Collection).Query();
        data["count"] = (await query.CountAsync()).ToString();
        data["first"] = ((await query.FirstOrDefaultAsync()) != null).ToString();
    });

    // ── Sync-over-async entry points ──────────────────────────────────────────

    /// <summary>The synchronous BLQL terminal operators (ToList, Count, FirstOrDefault, Any).</summary>
    [JSInvokable]
    public static Task<string> BlqlSync(string backend, string db, string op) => Run($"{nameof(BlqlSync)}:{op}", async data =>
    {
        using var engine = await BLiteWasm.CreateAsync(db, ParseBackend(backend));
        await SeedAsync(engine, 20);
        var query = engine.GetOrCreateCollection(Collection).Query();
        data["result"] = op switch
        {
            "tolist" => query.ToList().Count.ToString(),
            "count" => query.Count().ToString(),
            "first" => (query.FirstOrDefault() != null).ToString(),
            "any" => query.Any().ToString(),
            _ => throw new ArgumentOutOfRangeException(nameof(op), op, null)
        };
    });

    /// <summary>Typed context with the async LINQ operators: the control for <see cref="TypedSyncLinq"/>.</summary>
    [JSInvokable]
    public static Task<string> TypedAsyncLinq(string backend, string db, bool indexed) => Run(nameof(TypedAsyncLinq), async data =>
    {
        var (ctx, storage) = await OpenTypedAsync(backend, db, indexed);
        using (storage)
        using (ctx)
        {
            await SeedTypedAsync(ctx, 20);
            var q = ctx.Items.AsQueryable().Where(x => x.Value >= 10);
            data["toList"] = (await q.ToListAsync()).Count.ToString();
            data["count"] = (await q.CountAsync()).ToString();
            data["any"] = (await q.AnyAsync()).ToString();
            Expect((await q.CountAsync()) == 10, "async LINQ count is wrong");
        }
    });

    /// <summary>Typed context with synchronous System.Linq operators, which go through BTreeQueryProvider.Execute.</summary>
    [JSInvokable]
    public static Task<string> TypedSyncLinq(string backend, string db, string op, bool indexed) => Run($"{nameof(TypedSyncLinq)}:{op}", async data =>
    {
        var (ctx, storage) = await OpenTypedAsync(backend, db, indexed);
        using (storage)
        using (ctx)
        {
            await SeedTypedAsync(ctx, 20);
            IQueryable<HarnessItem> q = ctx.Items.AsQueryable().Where(x => x.Value >= 10);
            data["result"] = op switch
            {
                "tolist" => q.ToList().Count.ToString(),
                "count" => q.Count().ToString(),
                "first" => (q.FirstOrDefault() != null).ToString(),
                "any" => q.Any().ToString(),
                "foreach" => q.AsEnumerable().Count().ToString(),
                _ => throw new ArgumentOutOfRangeException(nameof(op), op, null)
            };
        }
    });

    /// <summary>Explicit transactions: async commit, sync BeginTransaction, and Dispose of an uncommitted transaction.</summary>
    [JSInvokable]
    public static Task<string> Transactions(string backend, string db, string mode) => Run($"{nameof(Transactions)}:{mode}", async data =>
    {
        using var engine = await BLiteWasm.CreateAsync(db, ParseBackend(backend));
        var col = engine.GetOrCreateCollection(Collection);
        switch (mode)
        {
            case "async-commit":
            {
                var txn = await engine.BeginTransactionAsync();
                await col.InsertAsync(Doc(engine, "t1", 1), txn);
                await txn.CommitAsync();
                txn.Dispose();
                break;
            }
            case "sync-begin":
            {
                using var txn = engine.BeginTransaction();
                await col.InsertAsync(Doc(engine, "t1", 1), txn);
                await txn.CommitAsync();
                break;
            }
            case "dispose-uncommitted":
            {
                var txn = await engine.BeginTransactionAsync();
                await col.InsertAsync(Doc(engine, "t1", 1), txn);
                txn.Dispose(); // Transaction.Dispose → RollbackAsync().GetAwaiter().GetResult()
                break;
            }
            case "sync-rollback":
            {
                var txn = engine.BeginTransaction();
                await col.InsertAsync(Doc(engine, "t1", 1), txn);
                engine.Rollback();
                break;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
        }
        data["count"] = (await CountAsync(engine)).ToString();
    });

    /// <summary>Times Dispose of an engine on its own, apart from the work before it.</summary>
    [JSInvokable]
    public static Task<string> DisposeTiming(string backend, string db) => Run(nameof(DisposeTiming), async data =>
    {
        var sw = Stopwatch.StartNew();
        var engine = await BLiteWasm.CreateAsync(db, ParseBackend(backend));
        await engine.InsertAsync(Collection, Doc(engine, "d1", 1));
        data["workMs"] = sw.Elapsed.TotalMilliseconds.ToString("F0");
        sw.Restart();
        engine.Dispose();
        data["disposeMs"] = sw.Elapsed.TotalMilliseconds.ToString("F0");
    });

    /// <summary>
    /// A typed context over a persistent backend assembled from the public storage
    /// types alone (page storage, WAL, StorageEngine, RecoverAsync), with no help from
    /// BLiteWasm's internal interop: insert, SaveChanges, count.
    /// </summary>
    [JSInvokable]
    public static Task<string> TypedOpenPublic(string backend, string db) => Run(nameof(TypedOpenPublic), async data =>
    {
        var (ctx, storage) = await OpenTypedAsync(backend, db, indexed: false, publicTypesOnly: true);
        using (storage)
        using (ctx)
        {
            await ctx.Items.InsertAsync(new HarnessItem { Name = "p1", Value = 1 });
            await ctx.SaveChangesAsync();
            data["count"] = (await ctx.Items.CountAsync()).ToString();
        }
    });

    // ── Durability across reloads ─────────────────────────────────────────────

    /// <summary>
    /// Inserts a marker document and then either checkpoints, disposes, or leaves the
    /// engine open with the change only committed to the WAL ("commit-only").
    /// The test then reloads the page and calls <see cref="ReadMarkers"/>.
    /// </summary>
    [JSInvokable]
    public static Task<string> WriteMarker(string backend, string db, string marker, string mode) => Run($"{nameof(WriteMarker)}:{mode}", async data =>
    {
        var engine = await BLiteWasm.CreateAsync(db, ParseBackend(backend));
        await engine.InsertAsync(Collection, Doc(engine, marker, 1));
        switch (mode)
        {
            case "checkpoint":
                await engine.CheckpointAsync();
                _held = engine;
                break;
            case "dispose":
                engine.Dispose();
                break;
            case "commit-only":
                _held = engine;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
        }
    });

    [JSInvokable]
    public static Task<string> ReadMarkers(string backend, string db) => Run(nameof(ReadMarkers), async data =>
    {
        using var engine = await BLiteWasm.CreateAsync(db, ParseBackend(backend));
        var names = await NamesAsync(engine);
        data["markers"] = string.Join(",", names.OrderBy(n => n, StringComparer.Ordinal));
        data["count"] = names.Count.ToString();
        // A document whose field names are missing from the key dictionary is found but unreadable.
        data["documents"] = (await CountAsync(engine)).ToString();
        data["keyMapHasName"] = engine.GetKeyMap().ContainsKey("name").ToString();
    });

    /// <summary>Typed context: insert, then reload and read back through a new context.</summary>
    [JSInvokable]
    public static Task<string> TypedWrite(string backend, string db, string name, bool checkpoint, bool indexed) => Run(nameof(TypedWrite), async data =>
    {
        var (ctx, storage) = await OpenTypedAsync(backend, db, indexed);
        await ctx.Items.InsertAsync(new HarnessItem { Name = name, Value = 42 });
        await ctx.SaveChangesAsync();
        if (checkpoint)
            await storage.CheckpointAsync();
        // Left open on purpose, like a tab that is closed without cleanup.
        data["count"] = (await ctx.Items.CountAsync()).ToString();
    });

    [JSInvokable]
    public static Task<string> TypedRead(string backend, string db, bool indexed) => Run(nameof(TypedRead), async data =>
    {
        var (ctx, storage) = await OpenTypedAsync(backend, db, indexed);
        using (storage)
        using (ctx)
        {
            var names = new List<string>();
            await foreach (var item in ctx.Items.FindAllAsync())
                names.Add(item.Name);
            data["names"] = string.Join(",", names);
            data["count"] = names.Count.ToString();
            data["indexQuery"] = (await ctx.Items.AsQueryable().Where(x => x.Value == 42).CountAsync()).ToString();
        }
    });

    // ── Several tabs on one database ──────────────────────────────────────────

    [JSInvokable]
    public static Task<string> Hold(string backend, string db) => Run(nameof(Hold), async data =>
    {
        _held?.Dispose();
        _held = await BLiteWasm.CreateAsync(db, ParseBackend(backend));
        data["count"] = (await CountAsync(_held)).ToString();
    });

    [JSInvokable]
    public static Task<string> HeldInsert(string marker, bool checkpoint) => Run(nameof(HeldInsert), async data =>
    {
        var engine = _held ?? throw new InvalidOperationException("No engine held; call Hold first.");
        await engine.InsertAsync(Collection, Doc(engine, marker, 1));
        if (checkpoint)
            await engine.CheckpointAsync();
        data["count"] = (await CountAsync(engine)).ToString();
    });

    [JSInvokable]
    public static Task<string> HeldRead() => Run(nameof(HeldRead), async data =>
    {
        var engine = _held ?? throw new InvalidOperationException("No engine held; call Hold first.");
        var names = await NamesAsync(engine);
        data["markers"] = string.Join(",", names.OrderBy(n => n, StringComparer.Ordinal));
    });

    // ── Size ──────────────────────────────────────────────────────────────────

    /// <summary>Fills the database with roughly <paramref name="megabytes"/> MB of documents, then checkpoints.</summary>
    [JSInvokable]
    public static Task<string> Fill(string backend, string db, int megabytes, int docBytes) => Run(nameof(Fill), async data =>
    {
        using var engine = await BLiteWasm.CreateAsync(db, ParseBackend(backend));
        var payload = new string('x', docBytes);
        var total = (long)megabytes * 1024 * 1024 / docBytes;
        var sw = Stopwatch.StartNew();
        const int batch = 500;
        for (long done = 0; done < total; done += batch)
        {
            var docs = new List<BsonDocument>(batch);
            for (var i = 0; i < batch && done + i < total; i++)
                docs.Add(engine.CreateDocument(["name", "v", "p"], b => b.AddString("name", "f").AddInt32("v", i).AddString("p", payload)));
            await engine.InsertBulkAsync(Collection, docs);
        }
        data["insertMs"] = sw.Elapsed.TotalMilliseconds.ToString("F0");
        sw.Restart();
        await engine.CheckpointAsync();
        data["checkpointMs"] = sw.Elapsed.TotalMilliseconds.ToString("F0");
        data["documents"] = total.ToString();
        data["managedHeapMB"] = (GC.GetTotalMemory(false) / 1048576.0).ToString("F1");
    });

    /// <summary>Opens an existing database and reports open time and managed memory.</summary>
    [JSInvokable]
    public static Task<string> OpenTiming(string backend, string db) => Run(nameof(OpenTiming), async data =>
    {
        var before = GC.GetTotalMemory(true);
        var sw = Stopwatch.StartNew();
        using var engine = await BLiteWasm.CreateAsync(db, ParseBackend(backend));
        data["openMs"] = sw.Elapsed.TotalMilliseconds.ToString("F0");
        data["managedHeapDeltaMB"] = ((GC.GetTotalMemory(true) - before) / 1048576.0).ToString("F1");
        sw.Restart();
        data["documents"] = (await CountAsync(engine)).ToString();
        data["scanMs"] = sw.Elapsed.TotalMilliseconds.ToString("F0");
    });

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static async Task<string> Run(string scenario, Func<Dictionary<string, string>, Task> body)
    {
        var result = new ScenarioResult { Scenario = scenario };
        var sw = Stopwatch.StartNew();
        try
        {
            await body(result.Data);
            result.Ok = true;
        }
        catch (Exception ex)
        {
            var root = ex is AggregateException { InnerException: { } inner } ? inner : ex;
            result.ErrorType = root.GetType().FullName;
            result.Error = root.Message;
            result.Stack = root.StackTrace;
        }
        result.ElapsedMs = sw.Elapsed.TotalMilliseconds;
        return JsonSerializer.Serialize(result, HarnessJson.Default.ScenarioResult);
    }

    private static void Expect(bool condition, string message)
    {
        if (!condition)
            throw new InvalidOperationException("Expectation failed: " + message);
    }

    private static WasmStorageBackend ParseBackend(string backend) => Enum.Parse<WasmStorageBackend>(backend, ignoreCase: true);

    private static BsonDocument Doc(BLiteEngine engine, string name, int value) =>
        engine.CreateDocument(["name", "v"], b => b.AddString("name", name).AddInt32("v", value));

    private static async Task SeedAsync(BLiteEngine engine, int count)
    {
        for (var i = 0; i < count; i++)
            await engine.InsertAsync(Collection, Doc(engine, $"s-{i}", i));
    }

    private static async Task SeedTypedAsync(ITypedHarness ctx, int count)
    {
        for (var i = 0; i < count; i++)
            await ctx.Items.InsertAsync(new HarnessItem { Name = $"s-{i}", Value = i });
        await ctx.SaveChangesAsync();
    }

    private static async Task<int> CountAsync(BLiteEngine engine)
    {
        var n = 0;
        await foreach (var _ in engine.FindAllAsync(Collection))
            n++;
        return n;
    }

    private static async Task<List<string>> NamesAsync(BLiteEngine engine)
    {
        var names = new List<string>();
        await foreach (var doc in engine.FindAllAsync(Collection))
            if (doc.TryGetString("name", out var name) && name != null)
                names.Add(name);
        return names;
    }

    /// <summary>
    /// BLiteWasm only returns a dynamic BLiteEngine, so a typed context has to assemble
    /// the same backend by hand through the public storage types. For OPFS that needs a
    /// workaround (see below); <paramref name="publicTypesOnly"/> leaves it out.
    /// </summary>
    private static async Task<(ITypedHarness Context, StorageEngine Storage)> OpenTypedAsync(string backend, string db, bool indexed, bool publicTypesOnly = false)
    {
        IPageStorage pages;
        IWriteAheadLog wal;
        switch (ParseBackend(backend))
        {
            case WasmStorageBackend.IndexedDb:
                var idbPages = new IndexedDbPageStorage(db, 16384);
                await idbPages.OpenAsync();
                var idbWal = new IndexedDbWriteAheadLog(db);
                await idbWal.OpenAsync();
                (pages, wal) = (idbPages, idbWal);
                break;
            case WasmStorageBackend.Opfs:
                var opfsPages = new OpfsPageStorage(db, 16384);
                await opfsPages.OpenAsync();
                // IndexedDbWriteAheadLog.OpenAsync needs its IndexedDB database to be open
                // already; BLiteWasm does that through the internal IndexedDbInterop.OpenAsync.
                // The only public way is to open an IndexedDbPageStorage of the same name.
                if (!publicTypesOnly)
                    await new IndexedDbPageStorage(db + "-wal", 16384).OpenAsync();
                var opfsWal = new IndexedDbWriteAheadLog(db + "-wal");
                await opfsWal.OpenAsync();
                (pages, wal) = (opfsPages, opfsWal);
                break;
            case WasmStorageBackend.InMemory:
                var memPages = new MemoryPageStorage(16384);
                memPages.Open();
                (pages, wal) = (memPages, new MemoryWriteAheadLog());
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(backend), backend, null);
        }

        var storage = new StorageEngine(pages, wal);
        await storage.RecoverAsync();
        return (indexed ? new IndexedHarness(new HarnessDbContext(storage)) : new PlainHarness(new HarnessPlainDbContext(storage)), storage);
    }
}

/// <summary>Lets a scenario run against either typed context.</summary>
internal interface ITypedHarness : IDisposable
{
    DocumentCollection<ObjectId, HarnessItem> Items { get; }
    ValueTask SaveChangesAsync();
}

internal sealed class IndexedHarness(HarnessDbContext ctx) : ITypedHarness
{
    public DocumentCollection<ObjectId, HarnessItem> Items => ctx.Items;
    public ValueTask SaveChangesAsync() => ctx.SaveChangesAsync();
    public void Dispose() => ctx.Dispose();
}

internal sealed class PlainHarness(HarnessPlainDbContext ctx) : ITypedHarness
{
    public DocumentCollection<ObjectId, HarnessItem> Items => ctx.Items;
    public ValueTask SaveChangesAsync() => ctx.SaveChangesAsync();
    public void Dispose() => ctx.Dispose();
}
