using System.Text.Json;
using Microsoft.Playwright;

namespace BLite.Wasm.BrowserTests;

/// <summary>
/// Outcome of one scenario call. <see cref="Status"/> is <c>pass</c>, <c>fail</c> (the
/// scenario threw), <c>hang</c> (no answer: the browser thread is blocked) or
/// <c>crash</c> (the .NET runtime in the tab exited).
/// </summary>
public sealed record ScenarioOutcome(
    string Scenario,
    string Status,
    double ElapsedMs,
    string? ErrorType,
    string? Error,
    string? Stack,
    IReadOnlyDictionary<string, string> Data)
{
    public bool Ok => Status == "pass";

    public string Describe() => Status switch
    {
        "pass" => $"{Scenario}: ok {string.Join(", ", Data.Select(kv => $"{kv.Key}={kv.Value}"))}",
        "hang" => $"{Scenario}: no answer within the timeout (the browser thread is blocked)\n{Stack}",
        "crash" => $"{Scenario}: the .NET runtime in the tab exited: {Error}\n{Stack}",
        _ => $"{Scenario}: {ErrorType}: {Error}\n{Stack}"
    };
}

/// <summary>One browser tab running the harness host.</summary>
public sealed class HarnessPage : IAsyncDisposable
{
    private readonly List<string> _console = new();
    private readonly TaskCompletionSource<string> _runtimeExit = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public IPage Page { get; }

    private HarnessPage(IPage page)
    {
        Page = page;
        page.Console += (_, msg) =>
        {
            lock (_console) _console.Add($"[{msg.Type}] {msg.Text}");
            // A failed runtime assertion terminates the .NET runtime; pending calls never settle.
            if (msg.Type == "error" && msg.Text.Contains("Assert failed", StringComparison.Ordinal))
                _runtimeExit.TrySetResult(msg.Text);
        };
        page.PageError += (_, error) =>
        {
            lock (_console) _console.Add($"[pageerror] {error}");
            if (error.Contains("runtime already exited", StringComparison.Ordinal))
                _runtimeExit.TrySetResult(error);
        };
    }

    public string ConsoleText
    {
        get { lock (_console) return string.Join("\n", _console); }
    }

    public static async Task<HarnessPage> OpenAsync(IBrowserContext context, string baseUrl)
    {
        var page = new HarnessPage(await context.NewPageAsync());
        await page.Page.GotoAsync(baseUrl);
        await page.WaitReadyAsync();
        return page;
    }

    public async Task ReloadAsync()
    {
        await Page.ReloadAsync();
        await WaitReadyAsync();
    }

    private Task WaitReadyAsync() =>
        Page.WaitForFunctionAsync("() => window.harness && window.harness.ready === true", null,
            new PageWaitForFunctionOptions { Timeout = 60_000 });

    /// <summary>
    /// Calls a [JSInvokable] scenario. A call that has not answered after
    /// <paramref name="timeout"/> is reported as a hang: on the single-threaded
    /// browser runtime a blocking wait freezes the tab, so the promise never settles.
    /// </summary>
    public async Task<ScenarioOutcome> CallAsync(string method, object[] args, TimeSpan? timeout = null)
    {
        var limit = timeout ?? TimeSpan.FromSeconds(60);
        var call = Page.EvaluateAsync<string>("([m, a]) => window.harness.run(m, ...a)", new object[] { method, args });
        var finished = await Task.WhenAny(call, _runtimeExit.Task, Task.Delay(limit));
        if (finished != call)
        {
            _ = call.ContinueWith(t => _ = t.Exception, TaskScheduler.Default);
            return finished == _runtimeExit.Task
                ? new ScenarioOutcome(method, "crash", 0, null, _runtimeExit.Task.Result, ConsoleText, new Dictionary<string, string>())
                : new ScenarioOutcome(method, "hang", limit.TotalMilliseconds, null, null, ConsoleText, new Dictionary<string, string>());
        }

        string json;
        try
        {
            json = await call;
        }
        catch (PlaywrightException ex)
        {
            // The runtime aborted or the JS call itself rejected.
            var status = _runtimeExit.Task.IsCompleted ? "crash" : "fail";
            return new ScenarioOutcome(method, status, 0, "PlaywrightException", ex.Message, ConsoleText,
                new Dictionary<string, string>());
        }

        var result = JsonSerializer.Deserialize<HostResult>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!;
        return new ScenarioOutcome(result.Scenario, result.Ok ? "pass" : "fail", result.ElapsedMs, result.ErrorType, result.Error,
            result.Stack, result.Data ?? new Dictionary<string, string>());
    }

    /// <summary>Entry count of every object store in the IndexedDB database BLite created for <paramref name="dbName"/>.</summary>
    public Task<string> IndexedDbCountsAsync(string dbName) => Page.EvaluateAsync<string>("""
        async (name) => {
            const dbs = (await indexedDB.databases()).map(d => d.name);
            if (!dbs.includes(name)) return "(no database " + name + ")";
            const db = await new Promise((resolve, reject) => {
                const r = indexedDB.open(name);
                r.onsuccess = () => resolve(r.result);
                r.onerror = () => reject(r.error);
            });
            const counts = [];
            for (const store of db.objectStoreNames) {
                const n = await new Promise(resolve => {
                    const q = db.transaction(store).objectStore(store).count();
                    q.onsuccess = () => resolve(q.result);
                });
                counts.push(store + "=" + n);
            }
            db.close();
            return counts.join(" ");
        }
        """, "blite_" + dbName);

    /// <summary>Bytes of the WebAssembly linear memory, or -1 if the runtime does not expose it.</summary>
    public Task<double> WasmMemoryBytesAsync() =>
        Page.EvaluateAsync<double>("() => { try { return globalThis.getDotnetRuntime(0).Module.HEAPU8.length; } catch { return -1; } }");

    public async ValueTask DisposeAsync()
    {
        // A blocked tab may not close cleanly; never let that stall the run.
        await Task.WhenAny(Page.CloseAsync(new PageCloseOptions { RunBeforeUnload = false }), Task.Delay(5_000));
    }

    private sealed class HostResult
    {
        public string Scenario { get; set; } = string.Empty;
        public bool Ok { get; set; }
        public double ElapsedMs { get; set; }
        public string? ErrorType { get; set; }
        public string? Error { get; set; }
        public string? Stack { get; set; }
        public Dictionary<string, string>? Data { get; set; }
    }
}
