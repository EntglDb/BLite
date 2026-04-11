using System.Collections.Concurrent;
using System.Diagnostics;
using BLite.Core.Metrics;
using BLite.Core.Query;
using BLite.StressMonitor;
using Spectre.Console;
using Spectre.Console.Rendering;

// ── Console encoding (required for Unicode block characters on Windows) ──────
Console.OutputEncoding = System.Text.Encoding.UTF8;

// ── Configuration ────────────────────────────────────────────────────────────
const int TestDurationSeconds  = 60;   // 1 minute
const int InsertWorkers        = 8;
const int UpdateWorkers        = 6;
const int DeleteWorkers        = 2;
const int FindWorkers          = 10;
const int WarmupDocCount       = 2_000; // inserted before the clock starts
const int MinPoolSizeForDelete = 500;   // don't delete if pool drops below this
const int DashboardIntervalMs  = 1_000;
const int MaxJitterUs          = 10000;   // max random inter-op jitter (microseconds)
string[]  Categories           = ["alpha", "beta", "gamma", "delta", "epsilon"];

// ── Database setup ───────────────────────────────────────────────────────────
var dbDir  = Path.Combine(Path.GetTempPath(), "blite_stress_monitor");
var dbPath = Path.Combine(dbDir, $"stress-{Guid.NewGuid()}.db");

if (Directory.Exists(dbDir)) Directory.Delete(dbDir, recursive: true);
Directory.CreateDirectory(dbDir);

using var db = new StressDbContext(dbPath);
db.EnableMetrics(MetricsOptions.Default);

// ── Shared state ─────────────────────────────────────────────────────────────
var idCounter    = new AtomicCounter();   // monotonically increasing doc ID
var availableIds = new ConcurrentQueue<int>();
var rng          = new Random(42);

// ── Rate history (circular queue, one entry per second, up to 60s) ───────────
const int HistoryLength = 60;
var insertHistory = new FixedQueue(HistoryLength);
var updateHistory = new FixedQueue(HistoryLength);
var deleteHistory = new FixedQueue(HistoryLength);
var findHistory   = new FixedQueue(HistoryLength);
var commitHistory = new FixedQueue(HistoryLength);
var queryHistory  = new FixedQueue(HistoryLength);
MetricsSnapshot? prevSnap = null;

// ── Warmup: populate initial documents ───────────────────────────────────────
AnsiConsole.MarkupLine("[grey]Seeding warmup documents...[/]");
var docsToAdd = new List<StressDoc>();
for (int i = 0; i < WarmupDocCount; i++)
{
    int id = idCounter.Next();
    var doc = new StressDoc
    {
        Id = id,
        Name = $"doc-{id}",
        Category = Categories[id % Categories.Length],
        Value = id * 1.5,
        Version = 0,
    };
    docsToAdd.Add(doc);
    availableIds.Enqueue(id);
}
await db.Docs.InsertBulkAsync(docsToAdd);

Console.WriteLine("Seeded warmup documents.");
Console.Clear();

// ── Load workers ─────────────────────────────────────────────────────────────
var cts   = new CancellationTokenSource(TimeSpan.FromSeconds(TestDurationSeconds));
var clock = Stopwatch.StartNew();
var tasks = new List<Task>();

// Insert workers
for (int w = 0; w < InsertWorkers; w++)
{
    tasks.Add(Task.Run(async () =>
    {
        var localRng = new Random();
        while (!cts.Token.IsCancellationRequested)
        {
            await Jitter(localRng, cts.Token);
            int id = idCounter.Next();
            var doc = new StressDoc
            {
                Id       = id,
                Name     = $"doc-{id}",
                Category = Categories[localRng.Next(Categories.Length)],
                Value    = localRng.NextDouble() * 10_000,
                Version  = 0,
            };
            try
            {
                await db.Docs.InsertAsync(doc, cts.Token);
                availableIds.Enqueue(id);
            }
            catch (OperationCanceledException) { break; }
            catch { /* absorb transient errors */ }
        }
    }, cts.Token));
}

// Update workers
for (int w = 0; w < UpdateWorkers; w++)
{
    tasks.Add(Task.Run(async () =>
    {
        var localRng = new Random();
        while (!cts.Token.IsCancellationRequested)
        {
            await Jitter(localRng, cts.Token);
            if (!availableIds.TryDequeue(out int id))
            {
                await Task.Delay(5, cts.Token).ConfigureAwait(false);
                continue;
            }

            try
            {
                var doc = await db.Docs.FindByIdAsync(id, cts.Token);
                if (doc != null)
                {
                    doc.Value   = localRng.NextDouble() * 10_000;
                    doc.Version++;
                    await db.Docs.UpdateAsync(doc, cts.Token);
                }
                availableIds.Enqueue(id);
            }
            catch (OperationCanceledException) { break; }
            catch { availableIds.Enqueue(id); }
        }
    }, cts.Token));
}

// Delete workers
for (int w = 0; w < DeleteWorkers; w++)
{
    tasks.Add(Task.Run(async () =>
    {
        var localRng = new Random();
        while (!cts.Token.IsCancellationRequested)
        {
            await Jitter(localRng, cts.Token);
            if (availableIds.Count < MinPoolSizeForDelete)
            {
                await Task.Delay(5, cts.Token).ConfigureAwait(false);
                continue;
            }

            if (!availableIds.TryDequeue(out int id))
            {
                await Task.Delay(5, cts.Token).ConfigureAwait(false);
                continue;
            }

            try
            {
                await db.Docs.DeleteAsync(id, cts.Token);
            }
            catch (OperationCanceledException) { break; }
            catch { /* doc may not exist, skip */ }
        }
    }, cts.Token));
}

// Find workers
for (int w = 0; w < FindWorkers; w++)
{
    tasks.Add(Task.Run(async () =>
    {
        var localRng = new Random();
        while (!cts.Token.IsCancellationRequested)
        {
            await Jitter(localRng, cts.Token);
            string cat = Categories[localRng.Next(Categories.Length)];
            try
            {
                var results = await db.Docs.AsQueryable()
                    .Where(d => d.Category == cat)
                    .ToListAsync<StressDoc>(cts.Token);

                _ = results;
            }
            catch (OperationCanceledException) { break; }
            catch { /* absorb */ }
        }
    }, cts.Token));
}

// ── Spectre.Console Live Dashboard ───────────────────────────────────────────
await AnsiConsole.Live(BuildDashboard(null, 0, TestDurationSeconds))
    .AutoClear(false)
    .Overflow(VerticalOverflow.Ellipsis)
    .StartAsync(async ctx =>
    {
        while (!cts.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(DashboardIntervalMs, cts.Token);
            }
            catch (OperationCanceledException) { }

            var snap = db.GetMetrics();
            if (snap != null)
            {
                // Compute per-second deltas
                insertHistory.Push(snap.InsertsTotal          - (prevSnap?.InsertsTotal          ?? 0));
                updateHistory.Push(snap.UpdatesTotal          - (prevSnap?.UpdatesTotal          ?? 0));
                deleteHistory.Push(snap.DeletesTotal          - (prevSnap?.DeletesTotal          ?? 0));
                findHistory  .Push(snap.FindsTotal            - (prevSnap?.FindsTotal            ?? 0));
                commitHistory.Push(snap.TransactionCommitsTotal - (prevSnap?.TransactionCommitsTotal ?? 0));
                queryHistory .Push(snap.QueriesTotal            - (prevSnap?.QueriesTotal            ?? 0));
                prevSnap = snap;
            }

            int elapsed = (int)clock.Elapsed.TotalSeconds;
            ctx.UpdateTarget(BuildDashboard(prevSnap, elapsed, TestDurationSeconds));
            ctx.Refresh();
        }

        // Final snapshot
        var finalSnap = db.GetMetrics();
        int totalElapsed = (int)clock.Elapsed.TotalSeconds;
        ctx.UpdateTarget(BuildDashboard(finalSnap, totalElapsed, TestDurationSeconds));
        ctx.Refresh();
    });

// Wait for all workers to finish gracefully
try { await Task.WhenAll(tasks); } catch { /* workers are cancelled */ }

db.GetMetrics()?.Let(s =>
{
    AnsiConsole.MarkupLine($"\n[bold green]Run complete.[/]  " +
        $"Inserts: [cyan]{s.InsertsTotal:N0}[/]  " +
        $"Updates: [yellow]{s.UpdatesTotal:N0}[/]  " +
        $"Deletes: [red]{s.DeletesTotal:N0}[/]  " +
        $"Finds: [blue]{s.FindsTotal:N0}[/]  " +
        $"Queries: [green]{s.QueriesTotal:N0}[/]  " +
        $"Commits: [grey]{s.TransactionCommitsTotal:N0}[/]");
});

db.Dispose();

// Cleanup temp files
Directory.Delete(dbDir, recursive: true);

// ── Dashboard renderer ────────────────────────────────────────────────────────
IRenderable BuildDashboard(MetricsSnapshot? snap, int elapsedSec, int totalSec)
{
    var grid = new Grid();
    grid.AddColumn();

    // ── Header ───────────────────────────────────────────────────────────────
    var elapsed = TimeSpan.FromSeconds(elapsedSec);
    var total   = TimeSpan.FromSeconds(totalSec);
    double pct  = Math.Clamp((double)elapsedSec / totalSec, 0.0, 1.0);

    var progressBar = ProgressBar(pct, 60);
    var header = new Panel(
            new Markup($"[bold white]BLite Stress Monitor[/]  " +
                       $"[grey]{elapsed:mm\\:ss}[/] [grey]/[/] [grey]{total:mm\\:ss}[/]  " +
                       $"{progressBar}  [yellow]{pct * 100:F1}%[/]"))
        .NoBorder()
        .Expand();

    grid.AddRow(header);

    // ── Throughput table ─────────────────────────────────────────────────────
    double intervalSec = DashboardIntervalMs / 1000.0;
    double eSec = Math.Max(1.0, elapsedSec);

    var thruTable = new Table()
        .BorderColor(Color.Grey)
        .AddColumn(new TableColumn("[grey]Metric[/]").Width(10))
        .AddColumn(new TableColumn("[grey]Last 60s history[/]").Width(62))
        .AddColumn(new TableColumn("[grey]  /sec[/]").RightAligned().Width(8))
        .AddColumn(new TableColumn("[grey]Avg 60s[/]").RightAligned().Width(9))
        .AddColumn(new TableColumn("[grey]Avg all[/]").RightAligned().Width(9));

    thruTable.Title = new TableTitle("[bold]Throughput[/]");

    void ThruRow(string label, string color, FixedQueue history, Color sparkColor, long total)
    {
        double rate   = history.Last() / intervalSec;
        double avg60  = history.Count > 0 ? history.Sum() / (history.Count * intervalSec) : 0;
        double avgAll = total / eSec;
        thruTable.AddRow(
            new Markup($"[{color}]{label}[/]"),
            Sparkline(history.ToArray(), sparkColor),
            new Markup($"[{color}]{rate,6:N0}[/]"),
            new Markup($"[{color}]{avg60,7:N0}[/]"),
            new Markup($"[{color}]{avgAll,7:N0}[/]"));
    }

    ThruRow("Inserts", "cyan",   insertHistory, Color.Cyan1,  snap?.InsertsTotal ?? 0);
    ThruRow("Updates", "yellow", updateHistory, Color.Yellow, snap?.UpdatesTotal ?? 0);
    ThruRow("Deletes", "red",    deleteHistory, Color.Red,    snap?.DeletesTotal ?? 0);
    ThruRow("Finds",   "blue",   findHistory,   Color.Blue,   snap?.FindsTotal ?? 0);
    ThruRow("Queries", "green",  queryHistory,  Color.Green,  snap?.QueriesTotal ?? 0);
    ThruRow("Commits", "grey",   commitHistory, Color.Grey,   snap?.TransactionCommitsTotal ?? 0);

    grid.AddRow(thruTable);

    // ── Latency table + transaction stats side by side ────────────────────────
    double maxLat = snap == null ? 1.0 : Math.Max(1.0, new[]
    {
        snap.AvgInsertLatencyUs,
        snap.AvgUpdateLatencyUs,
        snap.AvgDeleteLatencyUs,
        snap.AvgQueryLatencyUs,
    }.Max());

    var latTable = new Table()
        .BorderColor(Color.Grey)
        .AddColumn(new TableColumn("[grey]Op[/]").Width(8))
        .AddColumn(new TableColumn("[grey]Avg latency[/]").Width(25))
        .AddColumn(new TableColumn("[grey]μs[/]").RightAligned().Width(8));

    latTable.Title = new TableTitle("[bold]Latency[/]");

    latTable.AddRow(new Markup("[cyan]Insert[/]"),
        LatencyBar(snap?.AvgInsertLatencyUs ?? 0, maxLat, 20),
        new Markup($"[cyan]{snap?.AvgInsertLatencyUs ?? 0,6:F1}[/]"));
    latTable.AddRow(new Markup("[yellow]Update[/]"),
        LatencyBar(snap?.AvgUpdateLatencyUs ?? 0, maxLat, 20),
        new Markup($"[yellow]{snap?.AvgUpdateLatencyUs ?? 0,6:F1}[/]"));
    latTable.AddRow(new Markup("[red]Delete[/]"),
        LatencyBar(snap?.AvgDeleteLatencyUs ?? 0, maxLat, 20),
        new Markup($"[red]{snap?.AvgDeleteLatencyUs ?? 0,6:F1}[/]"));
    latTable.AddRow(new Markup("[green]Query[/]"),
        LatencyBar(snap?.AvgQueryLatencyUs ?? 0, maxLat, 20),
        new Markup($"[green]{snap?.AvgQueryLatencyUs ?? 0,6:F1}[/]"));

    var txTable = new Table()
        .BorderColor(Color.Grey)
        .AddColumn(new TableColumn("[grey]Counter[/]").Width(22))
        .AddColumn(new TableColumn("[grey]Value[/]").RightAligned().Width(14));

    txTable.Title = new TableTitle("[bold]Transactions[/]");

    txTable.AddRow(new Markup("[grey]Commits total[/]"),
        new Markup($"[white]{snap?.TransactionCommitsTotal ?? 0,12:N0}[/]"));
    txTable.AddRow(new Markup("[grey]Rollbacks[/]"),
        new Markup($"[white]{snap?.TransactionRollbacksTotal ?? 0,12:N0}[/]"));
    txTable.AddRow(new Markup("[grey]Group batches[/]"),
        new Markup($"[white]{snap?.GroupCommitBatchesTotal ?? 0,12:N0}[/]"));
    txTable.AddRow(new Markup("[grey]Avg batch size[/]"),
        new Markup($"[white]{snap?.GroupCommitAvgBatchSize ?? 0,12:F1}[/]"));
    txTable.AddRow(new Markup("[grey]Avg commit (μs)[/]"),
        new Markup($"[white]{snap?.AvgCommitLatencyUs ?? 0,12:F1}[/]"));
    txTable.AddRow(new Markup("[grey]Checkpoints[/]"),
        new Markup($"[white]{snap?.CheckpointsTotal ?? 0,12:N0}[/]"));

    var sideRow = new Columns(latTable, txTable);
    grid.AddRow(sideRow);

    // ── Totals ───────────────────────────────────────────────────────────────
    var totalsTable = new Table()
        .BorderColor(Color.Grey)
        .AddColumn("[grey]Inserts[/]")
        .AddColumn("[grey]Updates[/]")
        .AddColumn("[grey]Deletes[/]")
        .AddColumn("[grey]Finds[/]")
        .AddColumn("[grey]Queries[/]");

    totalsTable.Title = new TableTitle("[bold]Cumulative totals[/]");

    totalsTable.AddRow(
        new Markup($"[cyan]{snap?.InsertsTotal ?? 0:N0}[/]"),
        new Markup($"[yellow]{snap?.UpdatesTotal ?? 0:N0}[/]"),
        new Markup($"[red]{snap?.DeletesTotal ?? 0:N0}[/]"),
        new Markup($"[blue]{snap?.FindsTotal ?? 0:N0}[/]"),
        new Markup($"[green]{snap?.QueriesTotal ?? 0:N0}[/]"));

    grid.AddRow(totalsTable);

    return grid;
}

// ── Rendering helpers ─────────────────────────────────────────────────────────

/// <summary>Random inter-op jitter: breaks tight-loop synchronisation between workers.</summary>
static async Task Jitter(Random rng, CancellationToken ct)
{
    int us = rng.Next(MaxJitterUs);
    if (us < 50) return; // below scheduler resolution — skip entirely
    await Task.Delay(TimeSpan.FromMicroseconds(us), ct).ConfigureAwait(false);
}

static IRenderable Sparkline(long[] values, Color color)
{
    // Unicode block characters: index 0→space, 1→▁ … 8→█
    const string Blocks = " ▁▂▃▄▅▆▇█";
    if (values.Length == 0)
        return new Markup("[grey]" + new string('▁', 60) + "[/]");

    long max = values.Max();
    var chars = values.Select(v =>
        max == 0 ? '▁' : Blocks[(int)Math.Clamp(v * 8 / max, 0, 8)]).ToArray();

    // Pad left with spaces if history is shorter than 60
    string padded = new string(' ', Math.Max(0, 60 - chars.Length)) + new string(chars);

    return new Markup($"[{color.ToMarkup()}]{Markup.Escape(padded)}[/]");
}

static IRenderable LatencyBar(double latency, double maxLatency, int width)
{
    if (maxLatency <= 0) return new Markup("[grey]" + new string('░', width) + "[/]");
    int filled = (int)Math.Round(latency / maxLatency * width);
    filled = Math.Clamp(filled, 0, width);
    return new Markup("[white]" + new string('█', filled) + "[/][grey]" + new string('░', width - filled) + "[/]");
}

static string ProgressBar(double pct, int width)
{
    int filled = (int)Math.Round(pct * width);
    filled = Math.Clamp(filled, 0, width);
    return "[green]" + new string('█', filled) + "[/][grey]" + new string('░', width - filled) + "[/]";
}

// ── Supporting types ──────────────────────────────────────────────────────────

/// <summary>Thread-safe monotonically increasing integer counter.</summary>
sealed class AtomicCounter
{
    private int _value = 0;
    public int Next() => Interlocked.Increment(ref _value);
    public int Current => Volatile.Read(ref _value);
}

/// <summary>Fixed-capacity circular queue of <see cref="long"/> values.</summary>
sealed class FixedQueue(int capacity)
{
    private readonly long[] _buf = new long[capacity];
    private int _head = 0;
    private int _count = 0;

    public void Push(long value)
    {
        _buf[_head] = value;
        _head = (_head + 1) % capacity;
        if (_count < capacity) _count++;
    }

    public int Count => _count;

    public long Sum()
    {
        long s = 0;
        int start = (_head - _count + capacity) % capacity;
        for (int i = 0; i < _count; i++)
            s += _buf[(start + i) % capacity];
        return s;
    }

    public long Last() => _count == 0 ? 0 : _buf[(_head - 1 + capacity) % capacity];

    public long[] ToArray()
    {
        if (_count == 0) return [];
        var result = new long[_count];
        int start = (_head - _count + capacity) % capacity;
        for (int i = 0; i < _count; i++)
            result[i] = _buf[(start + i) % capacity];
        return result;
    }
}

// ── Color extension ───────────────────────────────────────────────────────────
static class ColorExtensions
{
    public static string ToMarkup(this Color c) =>
        c == Color.Cyan1   ? "cyan"   :
        c == Color.Yellow  ? "yellow" :
        c == Color.Red     ? "red"    :
        c == Color.Blue    ? "blue"   :
        c == Color.Green   ? "green"  :
        c == Color.Grey    ? "grey"   :
                             "white";
}

// ── Null-safe Let extension ───────────────────────────────────────────────────
static class ObjectExtensions
{
    public static void Let<T>(this T? value, Action<T> action) where T : class
    {
        if (value != null) action(value);
    }
}
