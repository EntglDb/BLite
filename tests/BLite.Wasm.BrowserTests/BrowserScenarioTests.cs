using System.IO.Compression;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.Playwright;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace BLite.Wasm.BrowserTests;

/// <summary>
/// BLite.Wasm in a real browser. Every test runs in a fresh browser context, so
/// each starts with empty OPFS and IndexedDB storage. Each test is one row, named
/// by its method and label; a failing or hanging scenario fails its row unless
/// <c>candidates.md</c> registers the row with that failure, in which case the row is
/// reported as KNOWN. A registered row that passes fails with "candidate fixed".
/// Every row's outcome is also appended to the fixture's report.
/// </summary>
[Collection(BrowserCollection.Name)]
public sealed class BrowserScenarioTests : IAsyncLifetime
{
    private readonly BrowserFixture _fixture;
    private readonly ITestOutputHelper _output;
    private IBrowserContext _context = null!;
    private readonly List<HarnessPage> _pages = new();

    public BrowserScenarioTests(BrowserFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    public static TheoryData<string> AllBackends => new() { "IndexedDb", "Opfs", "InMemory" };
    public static TheoryData<string> PersistentBackends => new() { "IndexedDb", "Opfs" };

    public async Task InitializeAsync() => _context = await _fixture.Browser.NewContextAsync();

    public async Task DisposeAsync()
    {
        foreach (var page in _pages)
            await page.DisposeAsync();
        await Task.WhenAny(_context.CloseAsync(), Task.Delay(10_000));
    }

    // ── Environment ────────────────────────────────────────────────────────────

    [Fact]
    public Task Probe_reports_runtime_and_backend_detection() => RowAsync("", async () =>
    {
        var page = await OpenAsync();
        var probe = await CallAsync(page, "Probe", "-");
        AssertOk(probe);
    });

    /// <summary>
    /// The public static IsAvailable() probes call [JSImport] functions whose module is
    /// only imported by BLiteWasm.CreateAsync / DetectBestBackendAsync.
    /// </summary>
    [Theory]
    [InlineData("IndexedDb")]
    [InlineData("Opfs")]
    public Task IsAvailable_before_any_other_call(string backend) => RowAsync(backend, async () =>
    {
        var page = await OpenAsync();
        AssertOk(await CallAsync(page, "IsAvailableFirst", backend, backend));
    });

    // ── Async API, every backend ─────────────────────────────────────────────

    [Theory, MemberData(nameof(AllBackends))]
    public Task Crud_through_the_async_api(string backend) => RowAsync(backend, async () =>
    {
        var page = await OpenAsync();
        AssertOk(await CallAsync(page, "Crud", backend, backend, "crud", 50));
    });

    [Theory, MemberData(nameof(AllBackends))]
    public Task Concurrent_async_inserts(string backend) => RowAsync(backend, async () =>
    {
        var page = await OpenAsync();
        AssertOk(await CallAsync(page, "ConcurrentInserts", backend, backend, "concurrent", 50));
    });

    [Theory, MemberData(nameof(AllBackends))]
    public Task Blql_async_operators(string backend) => RowAsync(backend, async () =>
    {
        var page = await OpenAsync();
        AssertOk(await CallAsync(page, "BlqlAsync", backend, backend, "blql-async"));
    });

    [Theory]
    [InlineData("IndexedDb", false)]
    [InlineData("Opfs", false)]
    [InlineData("InMemory", false)]
    [InlineData("InMemory", true)]
    public Task Typed_context_with_async_linq(string backend, bool indexed) => RowAsync($"{backend}/indexed={indexed}", async () =>
    {
        var page = await OpenAsync();
        AssertOk(await CallAsync(page, "TypedAsyncLinq", $"{backend}/indexed={indexed}", backend, "typed-async", indexed));
    });

    [Theory]
    [InlineData("IndexedDb", "async-commit")]
    [InlineData("IndexedDb", "sync-begin")]
    [InlineData("IndexedDb", "dispose-uncommitted")]
    [InlineData("IndexedDb", "sync-rollback")]
    [InlineData("InMemory", "dispose-uncommitted")]
    public Task Explicit_transactions(string backend, string mode) => RowAsync($"{backend}/{mode}", async () =>
    {
        var page = await OpenAsync();
        AssertOk(await CallAsync(page, "Transactions", backend, backend, "txn-" + mode, mode));
    });

    /// <summary>
    /// A typed context over a browser backend assembled from the public storage types
    /// only, the way a consumer without BLiteWasm's internal interop has to do it.
    /// </summary>
    [Theory, MemberData(nameof(PersistentBackends))]
    public Task Typed_context_opens_through_the_public_storage_types(string backend) => RowAsync(backend, async () =>
    {
        var page = await OpenAsync();
        AssertOk(await CallAsync(page, "TypedOpenPublic", backend, backend, "typed-public"));
    });

    // ── Synchronous entry points (sync-over-async in Core) ────────────────────

    [Theory]
    [InlineData("tolist")]
    [InlineData("count")]
    [InlineData("first")]
    [InlineData("any")]
    [InlineData("foreach")]
    public Task Typed_sync_linq_operator(string op) => RowAsync("InMemory/" + op, async () =>
    {
        var page = await OpenAsync();
        AssertOk(await CallAsync(page, "TypedSyncLinq", "InMemory/" + op, "InMemory", "typed-sync", op, false));
    });

    [Theory]
    [InlineData("tolist")]
    [InlineData("count")]
    [InlineData("first")]
    [InlineData("any")]
    public Task Blql_sync_operator(string op) => RowAsync("InMemory/" + op, async () =>
    {
        var page = await OpenAsync();
        AssertOk(await CallAsync(page, "BlqlSync", "InMemory/" + op, "InMemory", "blql-sync", op));
    });

    /// <summary>
    /// Dispose of an engine on the browser's only thread. The work before it takes
    /// tens of milliseconds; the dispose itself must not block the tab for seconds.
    /// </summary>
    [Fact]
    public Task Dispose_returns_promptly() => RowAsync("", async () =>
    {
        var page = await OpenAsync();
        var outcome = await CallAsync(page, "DisposeTiming", "InMemory", "InMemory", "dispose-timing");
        AssertOk(outcome);
        var disposeMs = double.Parse(outcome.Data["disposeMs"], System.Globalization.CultureInfo.InvariantCulture);
        Assert.True(disposeMs < 1000, $"Dispose blocked the browser thread for {disposeMs:F0} ms (the work before it took {outcome.Data["workMs"]} ms).");
    });

    // ── Durability across a page reload ───────────────────────────────────────

    [Theory]
    [InlineData("IndexedDb", "checkpoint")]
    [InlineData("IndexedDb", "dispose")]
    [InlineData("IndexedDb", "commit-only")]
    [InlineData("Opfs", "checkpoint")]
    [InlineData("Opfs", "dispose")]
    [InlineData("Opfs", "commit-only")]
    public Task Committed_write_survives_reload(string backend, string mode) => RowAsync($"{backend}/{mode}", async () =>
    {
        var page = await OpenAsync();
        var walDb = backend == "Opfs" ? "durable-wal" : "durable";
        AssertOk(await CallAsync(page, "WriteMarker", $"{backend}/{mode}", backend, "durable", "m1", mode));
        _output.WriteLine($"IndexedDB before reload: {await page.IndexedDbCountsAsync(walDb)}");
        await page.ReloadAsync();
        _output.WriteLine($"IndexedDB after reload: {await page.IndexedDbCountsAsync(walDb)}");
        var read = await CallAsync(page, "ReadMarkers", $"{backend}/{mode}", backend, "durable");
        _output.WriteLine($"IndexedDB after reopen: {await page.IndexedDbCountsAsync(walDb)}");
        if (read.Ok && read.Data["markers"] != "m1")
        {
            // Diagnostic only: does a second open, after recovery wrote the pages, see the marker?
            await page.ReloadAsync();
            await CallAsync(page, "ReadMarkers", $"{backend}/{mode}/second-open", backend, "durable");
        }
        AssertOk(read);
        Assert.True(read.Data["markers"] == "m1", $"Expected marker m1 after reload, found '{read.Data["markers"]}'.");
    });

    [Theory]
    [InlineData("IndexedDb", true, false)]
    [InlineData("IndexedDb", false, false)]
    [InlineData("Opfs", true, false)]
    [InlineData("Opfs", false, false)]
    [InlineData("IndexedDb", true, true)]
    public Task Typed_write_survives_reload(string backend, bool checkpoint, bool indexed) => RowAsync($"{backend}/checkpoint={checkpoint}/indexed={indexed}", async () =>
    {
        var page = await OpenAsync();
        var label = $"{backend}/checkpoint={checkpoint}/indexed={indexed}";
        AssertOk(await CallAsync(page, "TypedWrite", label, backend, "typed-durable", "t1", checkpoint, indexed));
        await page.ReloadAsync();
        var read = await CallAsync(page, "TypedRead", label, backend, "typed-durable", indexed);
        AssertOk(read);
        Assert.True(read.Data["names"] == "t1", $"Expected t1 after reload, found '{read.Data["names"]}'.");
        Assert.True(read.Data["indexQuery"] == "1", $"Index query after reload returned {read.Data["indexQuery"]}.");
    });

    // ── Two tabs on one database ──────────────────────────────────────────────

    [Theory, MemberData(nameof(PersistentBackends))]
    public Task Two_tabs_writing_the_same_database_keep_both_writes(string backend) => RowAsync(backend, async () =>
    {
        var a = await OpenAsync();
        var b = await OpenAsync();
        AssertOk(await CallAsync(a, "Hold", backend + "/tab-a", backend, "shared"));
        AssertOk(await CallAsync(b, "Hold", backend + "/tab-b", backend, "shared"));
        AssertOk(await CallAsync(a, "HeldInsert", backend + "/tab-a", "a", true));
        AssertOk(await CallAsync(b, "HeldInsert", backend + "/tab-b", "b", true));
        var seenByB = await CallAsync(b, "HeldRead", backend + "/tab-b");
        _output.WriteLine($"tab b sees: {seenByB.Data.GetValueOrDefault("markers")}");

        var c = await OpenAsync();
        var read = await CallAsync(c, "ReadMarkers", backend + "/tab-c", backend, "shared");
        AssertOk(read);
        Assert.True(read.Data["markers"] == "a,b", $"A fresh tab should see both writes, found '{read.Data["markers"]}'.");
    });

    // ── Size ──────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("IndexedDb", 5)]
    [InlineData("IndexedDb", 20)]
    [InlineData("IndexedDb", 50)]
    [InlineData("Opfs", 5)]
    [InlineData("Opfs", 20)]
    [InlineData("Opfs", 50)]
    public Task Large_database_open_time_and_memory(string backend, int megabytes) => RowAsync($"{backend}/{megabytes}MB", async () =>
    {
        var page = await OpenAsync();
        var walDb = backend == "Opfs" ? "large-wal" : "large";
        var fill = await CallAsync(page, "Fill", $"{backend}/{megabytes}MB", TimeSpan.FromMinutes(15), backend, "large", megabytes, 1000);
        AssertOk(fill);
        var counts = await page.IndexedDbCountsAsync(walDb);
        _output.WriteLine($"IndexedDB after fill: {counts}");
        _fixture.Record(new { scenario = "IndexedDbAfterFill", label = $"{backend}/{megabytes}MB", counts });
        await page.ReloadAsync();
        var baseline = await page.WasmMemoryBytesAsync();
        var open = await CallAsync(page, "OpenTiming", $"{backend}/{megabytes}MB", TimeSpan.FromMinutes(10), backend, "large");
        var after = await page.WasmMemoryBytesAsync();
        _output.WriteLine($"wasm memory: {baseline / 1048576:F0} MB before open, {after / 1048576:F0} MB after");
        _fixture.Record(new { scenario = "WasmMemory", label = $"{backend}/{megabytes}MB", beforeMB = baseline / 1048576, afterMB = after / 1048576 });
        AssertOk(open);
        Assert.True(open.Data["documents"] == fill.Data["documents"],
            $"Filled {fill.Data["documents"]} documents and checkpointed, but the reopened database holds {open.Data["documents"]}.");
    });

    // ── The package ───────────────────────────────────────────────────────────

    /// <summary>
    /// The interop imports four ES modules from the site root. A consumer of the NuGet
    /// package can only have them if the package ships them as static web assets.
    /// </summary>
    [Fact]
    public Task Package_ships_the_js_modules() => RowAsync("", async () =>
    {
        var project = typeof(BrowserScenarioTests).Assembly.GetCustomAttributes<AssemblyMetadataAttribute>()
            .Single(a => a.Key == "WasmProjectPath").Value!;
        var output = Path.Combine(AppContext.BaseDirectory, "wasm-nupkg");
        if (Directory.Exists(output))
            Directory.Delete(output, recursive: true);
        var psi = new ProcessStartInfo("dotnet", $"pack \"{project}\" -c Release -o \"{output}\" -nologo")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        using var process = Process.Start(psi)!;
        var log = await process.StandardOutput.ReadToEndAsync() + await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        Assert.True(process.ExitCode == 0, $"dotnet pack failed:\n{log}");

        var nupkg = Directory.GetFiles(output, "BLite.Wasm.*.nupkg").Single();
        using var zip = ZipFile.OpenRead(nupkg);
        var entries = zip.Entries.Select(e => e.FullName).ToList();
        var modules = Directory.GetFiles(Path.Combine(Path.GetDirectoryName(project)!, "wwwroot"), "*.mjs")
            .Select(Path.GetFileName).OrderBy(n => n, StringComparer.Ordinal).ToList();
        var missing = modules.Where(m => !entries.Any(e => e.EndsWith("/" + m, StringComparison.Ordinal))).ToList();
        _output.WriteLine($"{Path.GetFileName(nupkg)}: {string.Join(", ", entries)}");
        Assert.True(missing.Count == 0,
            $"{Path.GetFileName(nupkg)} ships none of the JS modules the interop imports ({string.Join(", ", missing)}); entries: {string.Join(", ", entries.Where(e => !e.StartsWith("_rels") && !e.StartsWith("package/")))}.");
    });

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Runs one row and applies the candidate registry to its outcome: a failure that a
    /// registered candidate matches is reported as KNOWN (or keeps failing with the
    /// candidate's id while its issue is empty); a pass of a row registered as failing
    /// every run fails with "candidate fixed", so the fix removes the entry.
    /// </summary>
    private async Task RowAsync(string label, Func<Task> body, [CallerMemberName] string test = "")
    {
        var row = string.IsNullOrEmpty(label) ? test : $"{test} {label}";
        var candidates = Candidates.Covering(row);
        try
        {
            await body();
        }
        catch (Exception ex)
        {
            var hit = candidates.FirstOrDefault(c => ex.Message.Contains(c.Match, StringComparison.Ordinal));
            if (hit == null)
            {
                _fixture.RecordRow(row, "fail", null, FirstLine(ex.Message));
                throw;
            }
            if (string.IsNullOrEmpty(hit.Issue))
            {
                _fixture.RecordRow(row, "fail", hit, FirstLine(ex.Message));
                throw new XunitException($"[known candidate: {hit.Id}, no issue filed yet] {ex.Message}");
            }
            _fixture.RecordRow(row, "known", hit, FirstLine(ex.Message));
            _output.WriteLine($"KNOWN   {row} [known candidate: {hit.Id}, {hit.Issue}]: {FirstLine(ex.Message)}");
            return;
        }

        var fixedOne = candidates.FirstOrDefault(c => c.Kind == "always");
        if (fixedOne != null)
        {
            _fixture.RecordRow(row, "fixed", fixedOne, null);
            throw new XunitException($"candidate fixed: remove the entry for '{row}' ({fixedOne.Id}, {fixedOne.Issue}) from candidates.md");
        }
        _fixture.RecordRow(row, "pass", null, null);
    }

    private static string FirstLine(string message)
    {
        var i = message.IndexOf('\n');
        return (i < 0 ? message : message[..i]).TrimEnd('\r');
    }

    private async Task<HarnessPage> OpenAsync()
    {
        var page = await HarnessPage.OpenAsync(_context, _fixture.BaseUrl);
        _pages.Add(page);
        return page;
    }

    private Task<ScenarioOutcome> CallAsync(HarnessPage page, string method, string label, params object[] args) =>
        CallAsync(page, method, label, null, args);

    private async Task<ScenarioOutcome> CallAsync(HarnessPage page, string method, string label, TimeSpan? timeout, params object[] args)
    {
        var outcome = await page.CallAsync(method, args, timeout);
        var unobserved = outcome.Status is "hang" or "crash" ? "" : await page.Page.EvaluateAsync<string>("() => window.harness.run('DrainUnobserved')");
        _output.WriteLine($"[{label}] {outcome.Describe()}");
        if (!string.IsNullOrEmpty(unobserved))
            _output.WriteLine($"[{label}] unobserved task exceptions:\n{unobserved}");
        _fixture.Record(new
        {
            browser = _fixture.BrowserName,
            variant = _fixture.Variant,
            scenario = outcome.Scenario,
            label,
            status = outcome.Status,
            elapsedMs = Math.Round(outcome.ElapsedMs, 1),
            errorType = outcome.ErrorType,
            error = outcome.Error,
            stack = outcome.Stack,
            data = outcome.Data,
            unobserved
        });
        return outcome;
    }

    private static void AssertOk(ScenarioOutcome outcome) => Assert.True(outcome.Ok, outcome.Describe());
}
