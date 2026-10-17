using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using BLite.Core;
using BLite.Core.Indexing;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Shared;
using Microsoft.Win32.SafeHandles;
using Xunit.Abstractions;

namespace BLite.Tests.Oracles;

/// <summary>
/// Fault injection at the storage boundary of a single process, checked against the store model of
/// <see cref="StoreModelTests"/>. A seeded random sequence of the model's operations runs once per
/// seed while every write the engine makes is recorded: each page written, allocated or freed in the
/// page file, each write of the WAL that leaves its buffer for the OS, each WAL truncation and
/// flush. Any prefix of that record is a state the files can be in when the process dies, so the
/// test rebuilds the files at many such points (a crash), reopens them and requires that
/// <list type="bullet">
/// <item>the reopen succeeds (<c>Recovery</c>);</item>
/// <item>the collection, through a scan, the count, point lookups and both secondary indexes, equals
/// the model's committed view at some commit boundary, never a state between two commits
/// (<c>Atomicity</c>);</item>
/// <item>that boundary is not older than the last commit the engine acknowledged before the crash
/// (<c>Durability</c>);</item>
/// <item>the pages add up as in <see cref="BTreeModelTests"/>: every page reached from an index root,
/// a document or an overflow chain is reached once and is not free (<c>Pages.Reachable</c>), the free
/// list is a sound chain of exactly the pages marked free (<c>Pages.FreeList</c>), and no written
/// index or overflow page is neither reachable nor free (<c>Pages.Leaked</c>);</item>
/// <item>a second reopen finds the same collection (<c>Idempotent</c>);</item>
/// <item>the rest of the sequence, a new document and a checkpoint then run against the model from
/// the recovered state (<c>Continue</c>).</item>
/// </list>
/// Each crash mode times each check is a row; a failing row shrinks its sequence to the shortest one
/// that still fails that check at a crash in its last step, and prints it with the seed and the crash.
/// </summary>
/// <remarks>
/// <para>
/// <b>Crash modes.</b> <c>ProcessCrash</c>: the process dies, the OS keeps everything handed to it
/// (every page write, every WAL write that left the WAL's 64 KB stream buffer), the buffer is lost.
/// <c>TornWrite</c>: as <c>ProcessCrash</c>, but the last WAL write before the crash reached the disk
/// only in part, cut at offsets inside the records it carries, as a power loss leaves an otherwise
/// ordered log. <c>PowerLoss</c>: only what was made durable survives. A page write is durable when
/// <see cref="PageFile.WritePage"/> returns (each write flushes its view, <c>msync(MS_SYNC)</c> on
/// Unix, <c>PageFile.cs:727</c>); WAL bytes are durable once <c>Flush(flushToDisk: true)</c> has
/// returned (an fsync). <see cref="FileStream.FlushAsync(CancellationToken)"/>, which the WAL calls
/// before a commit is acknowledged (<see cref="WriteAheadLog.FlushAsync"/>), hands the bytes to the
/// OS and does not fsync (<c>OSFileStreamStrategy.FlushAsync</c> in .NET 6 to 10 returns at once).
/// README "Transactions &amp; ACID" promises durability "even in power loss", so the
/// <c>PowerLoss</c> rows hold the engine to it. What an open finds on disk counts as durable.
/// </para>
/// <para>
/// <b>The seam.</b> The engine writes every page through <see cref="IPageStorage"/> and the WAL
/// writes every record through its <see cref="FileStream"/>. The test opens the database through the
/// engine's file constructor, so recovery runs as it does for users, then replaces the engine's
/// <see cref="IPageStorage"/> by a recording wrapper of the same <see cref="PageFile"/>, and the WAL's
/// stream by a <see cref="FileStream"/> over the same handle that keeps the same 64 KB buffer and
/// records what leaves it. Both are private fields, set by reflection: the file constructor takes no
/// storage, and <c>StorageEngine(IPageStorage, IWriteAheadLog)</c> skips recovery and opens no
/// files. Two things are not covered: a crash during <c>Backup</c>, whose plan requires the
/// concrete <see cref="PageFile"/> (<c>StorageEngine.Recovery.cs:513</c>), so the wrapper steps aside
/// for it (its checkpoint is the code of the <c>Checkpoint</c> step); and a crash inside the
/// engine's constructor on a reopen. A crash during recovery itself is covered: from sampled crash
/// states with a WAL, the recovery is run through the same wrappers over
/// <c>StorageEngine(IPageStorage, IWriteAheadLog)</c> and <see cref="StorageEngine.RecoverAsync"/>,
/// the method the file constructor calls, and crashed in turn.
/// </para>
/// <para>
/// <b>Page accounting.</b> Pages whose header is still all zero (allocated, never written) are not
/// counted as leaked: every reopen strands the rest of the last growth block (#258) and every
/// rolled-back transaction strands what it allocated (#237), in the same way.
/// </para>
/// </remarks>
public class WalCrashTests
{
    private readonly ITestOutputHelper _output;

    public WalCrashTests(ITestOutputHelper output) => _output = output;

    private const int Steps = 60;
    private static readonly int[] s_seeds = [1, 2, 3, 4, 5, 6];
    private static readonly PageFileConfig s_config = (PageFileConfig.Small with { PageSize = 4096, GrowthBlockSize = 64 * 1024 }).Normalize();
    private static readonly TimeSpan s_shrinkBudget = TimeSpan.FromSeconds(10);

    // ── Rows ──────────────────────────────────────────────────────────────────

    private const string ProcessCrash = "ProcessCrash";
    private const string TornWrite = "TornWrite";
    private const string PowerLoss = "PowerLoss";
    private static readonly string[] s_modes = [ProcessCrash, TornWrite, PowerLoss];

    private const string Recovery = "Recovery";
    private const string Atomicity = "Atomicity";
    private const string Durability = "Durability";
    private const string PagesReachable = "Pages.Reachable";
    private const string PagesFreeList = "Pages.FreeList";
    private const string PagesLeaked = "Pages.Leaked";
    private const string Idempotent = "Idempotent";
    private const string Continue = "Continue";
    private static readonly string[] s_checks = [Recovery, Atomicity, Durability, PagesReachable, PagesFreeList, PagesLeaked, Idempotent, Continue];

    public static TheoryData<string, string> Rows
    {
        get
        {
            var rows = new TheoryData<string, string>();
            foreach (var mode in s_modes)
                foreach (var check in s_checks)
                    rows.Add(mode, check);
            return rows;
        }
    }

    private static string Label(string mode, string check) => $"{mode} / {check}";

    /// <summary>
    /// Rows known to fail on upstream 0450b97, by <see cref="Label"/>, see <see cref="Candidates"/>
    /// and the drafts W1 to W3 in <c>Oracles/Seeds/candidate-issues.md</c>.
    /// </summary>
    private static readonly Candidates s_candidates = new()
    {
        { "the WAL is never fsynced: a power loss drops acknowledged commits and keeps the frees they made", "",
            Label(PowerLoss, Durability), Label(PowerLoss, Atomicity), Label(PowerLoss, PagesReachable), Label(PowerLoss, Continue) },
        { "a crash leaks the pages that the interrupted work freed or allocated outside the WAL", "",
            Label(ProcessCrash, PagesLeaked), Label(TornWrite, PagesLeaked), Label(PowerLoss, PagesLeaked) },
        { "a crash between the two writes of PageFile.FreePage or AllocatePage leaves a free page off the free list", "",
            Label(ProcessCrash, PagesFreeList) },
    };

    [Fact]
    public void Candidates_NameExistingRows() => s_candidates.AssertAllIn(Rows.Select(r => Label((string)r[0], (string)r[1])));

    [Theory]
    [MemberData(nameof(Rows))]
    public async Task Crash_RecoversToAnAcknowledgedCommit(string mode, string check)
    {
        var campaign = await s_campaigns.GetOrAdd(mode, m => new Lazy<Task<Campaign>>(() => Task.Run(() => RunCampaign(m)))).Value;
        var label = Label(mode, check);
        _output.WriteLine($"{mode}: {campaign.Images} crash states ({campaign.RecoveryImages} of them during a recovery) from {s_seeds.Length} seeds of {Steps} steps ({campaign.Events} recorded writes), {campaign.Elapsed.TotalSeconds:F1} s");
        _output.WriteLine($"  by step: {string.Join(", ", campaign.ByStep.Select(kv => $"{kv.Key} {kv.Value}"))}; right after a WAL truncation: {campaign.AtTruncation}");
        foreach (var note in campaign.Notes) _output.WriteLine(note);

        if (!campaign.First.TryGetValue(check, out var found))
        {
            s_candidates.Verify(label, null, _output);
            return;
        }

        var (ops, failure) = await Shrink(found, mode, check, s_shrinkBudget);
        var sb = new StringBuilder();
        sb.AppendLine($"{label}: seed {found.Seed}, {campaign.Failures[check]} of {campaign.Images} crash states fail; the first: a crash {found.Point}: {found.Message}");
        sb.AppendLine($"Shortest failing sequence found ({ops.Count} of {found.Ops.Count} steps), crash {failure.Point}: {failure.Message}");
        for (int i = 0; i < ops.Count; i++)
            sb.AppendLine($"  {i + 1,3}. {ops[i]}");
        s_candidates.Verify(label, sb.ToString(), _output);
    }

    // ── Campaign ──────────────────────────────────────────────────────────────

    private sealed record Found(int Seed, List<StoreOp> Ops, int Step, string Point, string Message);

    private sealed class Campaign
    {
        public int Images;
        public int RecoveryImages;
        public int AtTruncation;
        public readonly SortedDictionary<string, int> ByStep = new();
        public int Events;
        public TimeSpan Elapsed;
        public readonly Dictionary<string, Found> First = new();
        public readonly Dictionary<string, int> Failures = new();
        public readonly List<string> Notes = new();
    }

    private static readonly ConcurrentDictionary<string, Lazy<Task<Campaign>>> s_campaigns = new();
    private static readonly ConcurrentDictionary<int, Lazy<Task<Recording>>> s_recordings = new();

    private static async Task<Campaign> RunCampaign(string mode)
    {
        var sw = Stopwatch.StartNew();
        var campaign = new Campaign();
        // BLITE_WALCRASH_LOG=<directory> writes every failing crash state to <directory>/<mode>.tsv.
        using var log = Environment.GetEnvironmentVariable("BLITE_WALCRASH_LOG") is { Length: > 0 } logDir
            ? new StreamWriter(Path.Combine(logDir, $"{mode}.tsv")) : null;
        foreach (var seed in s_seeds)
        {
            var rec = await s_recordings.GetOrAdd(seed, s => new Lazy<Task<Recording>>(() => Task.Run(() => Record(StoreModel.Generate(s, Steps))))).Value;
            campaign.Events += rec.Events.Count;
            if (rec.StoppedAt != null) campaign.Notes.Add($"seed {seed}: {rec.StoppedAt}");
            await Explore(rec, mode, onlyStep: null, (step, point, failures) =>
            {
                campaign.Images++;
                if (point.Contains(" while recovering ")) campaign.RecoveryImages++;
                var kind = step == 0 ? "first open" : step > rec.Ops.Count ? "close" : rec.Ops[step - 1].Kind.ToString();
                campaign.ByStep[kind] = campaign.ByStep.GetValueOrDefault(kind) + 1;
                if (point.StartsWith("after the WAL truncated", StringComparison.Ordinal)) campaign.AtTruncation++;
                foreach (var (check, message) in failures)
                {
                    log?.WriteLine($"{mode}\t{check}\tseed {seed}\t{point}\t{message.ReplaceLineEndings(" ")}");
                    campaign.Failures[check] = campaign.Failures.GetValueOrDefault(check) + 1;
                    campaign.First.TryAdd(check, new Found(seed, rec.Ops, step, point, message));
                }
                return true;
            });
        }
        campaign.Elapsed = sw.Elapsed;
        return campaign;
    }

    /// <summary>
    /// Crashes <paramref name="rec"/> at every crash point of <paramref name="mode"/> (in
    /// <paramref name="onlyStep"/> only, if given), and, in the modes without a torn write, crashes
    /// the recovery of a sample of those states in turn. <paramref name="verdict"/> receives each
    /// crash point and the checks it fails; returning false stops the exploration.
    /// </summary>
    private static async Task Explore(Recording rec, string mode, int? onlyStep, Func<int, string, Dictionary<string, string>, bool> verdict)
    {
        int recoveries = 0;
        foreach (var point in CrashPoints(rec, mode))
        {
            if (onlyStep != null && point.Step != onlyStep) continue;
            var (db, wal) = Materialize(rec.Events, point.Event, mode == PowerLoss, point.TornAt);
            var where = $"{point.What} during {rec.StepName(point.Step)}";
            if (!verdict(point.Step, where, await Evaluate(db, wal, rec, point.Step))) return;

            // The first open after a crash: crash the recovery of this state too, at the end of a
            // step whose WAL is not empty (every third such step, or every one when shrinking).
            if (mode == TornWrite || !point.EndOfStep || wal.Length == 0) continue;
            if (onlyStep == null && recoveries++ % 3 != 0) continue;
            var recovery = await RecordRecovery(db, wal);
            foreach (var inner in RecoveryCrashPoints(recovery))
            {
                var (rdb, rwal) = Materialize(recovery, inner, mode == PowerLoss, null);
                var innerWhere = $"{recovery[inner].What} while recovering from a crash {where}";
                if (!verdict(point.Step, innerWhere, await Evaluate(rdb, rwal, rec, point.Step))) return;
            }
        }
    }

    // ── Crash points ──────────────────────────────────────────────────────────

    /// <summary>A crash right after event <see cref="Event"/>, with that event cut after <see cref="TornAt"/> bytes if set.</summary>
    private sealed record CrashPoint(int Event, int Step, bool EndOfStep, int? TornAt, string What);

    private static IEnumerable<CrashPoint> CrashPoints(Recording rec, string mode)
    {
        var events = rec.Events;
        for (int i = 0; i < events.Count; i++)
        {
            var e = events[i];
            bool endOfStep = i == events.Count - 1 || events[i + 1].Step != e.Step;
            switch (mode)
            {
                case ProcessCrash:
                    yield return new CrashPoint(i, e.Step, endOfStep, null, $"after {e.What}");
                    break;
                case PowerLoss:
                    // Every flush boundary, and the end of every step.
                    if (endOfStep || e.Kind is EvKind.WalFlush or EvKind.WalSetLength or EvKind.WalFsync or EvKind.PageFlush or EvKind.Baseline)
                        yield return new CrashPoint(i, e.Step, endOfStep, null, $"after {e.What}");
                    break;
                case TornWrite:
                    if (e.Kind != EvKind.WalWrite) break;
                    foreach (var (cut, what) in Cuts(e.Data!))
                        yield return new CrashPoint(i, e.Step, false, cut, $"in {e.What}, {what}");
                    break;
            }
        }
    }

    /// <summary>
    /// Offsets inside one WAL write at which it is cut: one byte into it, inside the header and the
    /// image of its last data record, just before its last commit record, and inside that record.
    /// </summary>
    private static IEnumerable<(int Cut, string What)> Cuts(byte[] data)
    {
        var records = new List<(int Start, int Length, WalRecordType Type)>();
        for (int at = 0; at < data.Length;)
        {
            var type = (WalRecordType)data[at];
            int length = type == WalRecordType.Write && at + 17 <= data.Length ? 17 + BitConverter.ToInt32(data, at + 13) : 17;
            records.Add((at, length, type));
            at += length;
        }
        var cuts = new SortedDictionary<int, string> { [1] = "1 byte of it written" };
        if (records.LastOrDefault(r => r.Type == WalRecordType.Write) is { Length: > 0 } w)
        {
            cuts.TryAdd(w.Start + 9, $"inside the header of the data record at byte {w.Start}");
            cuts.TryAdd(w.Start + 17 + (w.Length - 17) / 2, $"inside the image of the data record at byte {w.Start}");
        }
        if (records.LastOrDefault(r => r.Type == WalRecordType.Commit) is { Length: > 0 } c)
        {
            cuts.TryAdd(c.Start, $"all but its commit record at byte {c.Start}");
            cuts.TryAdd(c.Start + 8, $"inside its commit record at byte {c.Start}");
        }
        return cuts.Where(kv => kv.Key > 0 && kv.Key < data.Length).Select(kv => (kv.Key, $"cut after {kv.Key} of {data.Length} bytes ({kv.Value})"));
    }

    /// <summary>In a recorded recovery: every flush and truncation, and the first, middle and last page it writes.</summary>
    private static IEnumerable<int> RecoveryCrashPoints(List<Ev> recovery)
    {
        var pages = Enumerable.Range(0, recovery.Count).Where(i => recovery[i].Kind is EvKind.Page or EvKind.PageLength).ToList();
        var picked = new SortedSet<int>();
        if (pages.Count > 0) { picked.Add(pages[0]); picked.Add(pages[pages.Count / 2]); picked.Add(pages[^1]); }
        for (int i = 1; i < recovery.Count; i++)
            if (recovery[i].Kind is not (EvKind.Page or EvKind.PageLength)) picked.Add(i);
        return picked;
    }

    /// <summary>
    /// The page file and WAL as a crash right after <paramref name="last"/> leaves them: the last
    /// baseline before it, with every recorded write after it applied, the write at
    /// <paramref name="last"/> cut after <paramref name="tornAt"/> bytes if set; with
    /// <paramref name="durableOnly"/>, the WAL as of its last fsync (or baseline).
    /// </summary>
    private static (byte[] Db, byte[] Wal) Materialize(List<Ev> events, int last, bool durableOnly, int? tornAt)
    {
        int b = last;
        while (events[b].Kind != EvKind.Baseline) b--;
        var db = new Bytes(events[b].Data!);
        var wal = new Bytes(events[b].Wal!);
        var durable = events[b].Wal!;
        for (int i = b + 1; i <= last; i++)
        {
            var e = events[i];
            switch (e.Kind)
            {
                case EvKind.Page: db.Write(e.Offset, e.Data!); break;
                case EvKind.PageLength: db.SetLength(e.Length); break;
                case EvKind.WalWrite: wal.Write(e.Offset, i == last && tornAt is int t ? e.Data.AsSpan(0, t) : e.Data); break;
                case EvKind.WalSetLength: wal.SetLength(e.Length); break;
                case EvKind.WalFsync: durable = wal.ToArray(); break;
            }
        }
        return (db.ToArray(), durableOnly ? durable : wal.ToArray());
    }

    // ── Recording ─────────────────────────────────────────────────────────────

    private sealed class Recording
    {
        public required List<StoreOp> Ops { get; init; }
        public required List<Ev> Events { get; init; }

        /// <summary>The committed view before and after each step: 0 is the first open, 1..n the operations, n + 1 the close.</summary>
        public required List<Dictionary<int, StoreDoc>> Before { get; init; }
        public required List<Dictionary<int, StoreDoc>> After { get; init; }
        public string? StoppedAt { get; init; }

        public string StepName(int step) => step == 0 ? "the first open (collection creation)"
            : step > Ops.Count ? "the final close"
            : $"step {step} ({Ops[step - 1]})";
    }

    /// <summary>Runs <paramref name="ops"/> on a new database through the recording wrappers.</summary>
    private static async Task<Recording> Record(List<StoreOp> ops)
    {
        var dir = NewDirectory();
        var path = Path.Combine(dir, "store.db");
        var rec = new Recorder(s_config.PageSize);
        var before = new List<Dictionary<int, StoreDoc>> { new() };
        var after = new List<Dictionary<int, StoreDoc>>();
        string? stoppedAt = null;
        StoreModelRunner? runner = null;
        try
        {
            runner = new StoreModelRunner(() => OpenRecorded(path, rec), Path.Combine(dir, "backup.db"), s_config,
                aroundBackup: backup => AroundBackup(runner!, rec, backup));
            after.Add(new(runner.Committed));
            for (int i = 0; i < ops.Count; i++)
            {
                rec.Step = i + 1;
                before.Add(new(runner.Committed));
                try
                {
                    await runner.ApplyAsync(ops[i]);
                }
                catch (Exception ex)
                {
                    // StoreModelTests' business: keep the steps before it, and record nothing more.
                    stoppedAt = $"the recorded run stopped before step {i + 1} ({ops[i]}), which failed without a crash: {ex.GetType().Name}: {ex.Message}";
                    ops = ops.Take(i).ToList();
                    before.RemoveAt(before.Count - 1);
                    rec.Suspend();
                    rec.DropStep(i + 1);
                    return new Recording { Ops = ops, Events = rec.Events, Before = before, After = after, StoppedAt = stoppedAt };
                }
                after.Add(new(runner.Committed));
            }
            rec.Step = ops.Count + 1;
            before.Add(new(runner.Committed));
            after.Add(new(runner.Committed));
            runner.Dispose();
            runner = null;

            // The injector must have seen every write: the record replayed in full is the files.
            var (db, wal) = Materialize(rec.Events, rec.Events.Count - 1, false, null);
            AssertSameFile(File.ReadAllBytes(path), db, "page file");
            AssertSameFile(File.ReadAllBytes(Path.ChangeExtension(path, ".wal")), wal, "WAL");
            return new Recording { Ops = ops, Events = rec.Events, Before = before, After = after };
        }
        finally
        {
            try { runner?.Dispose(); } catch { /* the failure is already on its way */ }
            DeleteDirectory(dir);
        }
    }

    private static void AssertSameFile(byte[] actual, byte[] recorded, string what)
    {
        if (actual.AsSpan().SequenceEqual(recorded)) return;
        int i = 0;
        while (i < actual.Length && i < recorded.Length && actual[i] == recorded[i]) i++;
        throw new InvalidOperationException($"the injector missed a write to the {what}: the file ({actual.Length} bytes) and the record ({recorded.Length} bytes) differ from byte {i} on");
    }

    /// <summary>Opens the database through the engine's file constructor, then puts the recording wrappers in.</summary>
    private static StoreModelDbContext OpenRecorded(string path, Recorder rec)
    {
        var engine = new StorageEngine(path, s_config);
        try
        {
            var wrapper = Install((PageFile)s_enginePageFile.GetValue(engine)!, (WriteAheadLog)s_engineWal.GetValue(engine)!, rec, "the open");
            s_enginePageFile.SetValue(engine, wrapper);
            return new StoreModelDbContext(engine);
        }
        catch
        {
            engine.Dispose();
            throw;
        }
    }

    /// <summary>Replaces the WAL's stream by a recording one and records both files as they are.</summary>
    private static RecordingPageStorage Install(PageFile pageFile, WriteAheadLog wal, Recorder rec, string what)
    {
        var original = (FileStream)s_walStream.GetValue(wal)!;
        original.Flush();
        var stream = new RecordingWalStream(original.SafeFileHandle, original.Position, rec);
        // Both streams share the handle; the WAL disposes the new one, the old one must not close it.
        GC.SuppressFinalize(original);
        s_walStream.SetValue(wal, stream);
        var dbHandle = ((FileStream)s_pageFileStream.GetValue(pageFile)!).SafeFileHandle;
        rec.Attach(pageFile, dbHandle, stream, original, what);
        return new RecordingPageStorage(pageFile, rec);
    }

    /// <summary>
    /// The backup plan requires the concrete <see cref="PageFile"/> (<c>StorageEngine.Recovery.cs:513</c>):
    /// the wrapper steps aside for it and nothing is recorded meanwhile.
    /// </summary>
    private static async Task AroundBackup(StoreModelRunner runner, Recorder rec, Func<Task> backup)
    {
        var engine = runner.Db.Storage;
        var wrapper = (RecordingPageStorage)s_enginePageFile.GetValue(engine)!;
        rec.Suspend();
        s_enginePageFile.SetValue(engine, wrapper.Inner);
        try
        {
            await backup();
        }
        finally
        {
            s_enginePageFile.SetValue(engine, wrapper);
            rec.Resume("the backup");
        }
    }

    /// <summary>Recovers the crash state <paramref name="db"/> / <paramref name="wal"/> through the recording wrappers.</summary>
    private static async Task<List<Ev>> RecordRecovery(byte[] db, byte[] wal)
    {
        var dir = NewDirectory();
        var path = Path.Combine(dir, "store.db");
        File.WriteAllBytes(path, db);
        File.WriteAllBytes(Path.ChangeExtension(path, ".wal"), wal);
        var rec = new Recorder(s_config.PageSize);
        StorageEngine? engine = null;
        try
        {
            var pageFile = new PageFile(path, s_config);
            pageFile.Open();
            var walLog = new WriteAheadLog(Path.ChangeExtension(path, ".wal"), null, s_config.LockTimeout.WriteTimeoutMs);
            engine = new StorageEngine(Install(pageFile, walLog, rec, "the open"), walLog);
            await engine.RecoverAsync();
            rec.Suspend();
            return rec.Events;
        }
        finally
        {
            rec.Suspend();
            engine?.Dispose();
            DeleteDirectory(dir);
        }
    }

    private const BindingFlags Private = BindingFlags.NonPublic | BindingFlags.Instance;
    private static readonly FieldInfo s_enginePageFile = typeof(StorageEngine).GetField("_pageFile", Private)!;
    private static readonly FieldInfo s_engineWal = typeof(StorageEngine).GetField("_wal", Private)!;
    private static readonly FieldInfo s_walStream = typeof(WriteAheadLog).GetField("_walStream", Private)!;
    private static readonly FieldInfo s_pageFileStream = typeof(PageFile).GetField("_fileStream", Private)!;

    private enum EvKind { Baseline, Page, PageLength, PageFlush, WalWrite, WalSetLength, WalFlush, WalFsync }

    /// <summary>
    /// One recorded write. <see cref="EvKind.Baseline"/>: both files as an open or the end of a
    /// backup found them (<see cref="Data"/>, <see cref="Wal"/>). <see cref="EvKind.Page"/>: a page's
    /// new bytes at <see cref="Offset"/>. <see cref="EvKind.PageLength"/> and
    /// <see cref="EvKind.WalSetLength"/>: a new file length. <see cref="EvKind.WalWrite"/>: bytes
    /// handed to the OS at <see cref="Offset"/> of the WAL. The rest mark flushes.
    /// </summary>
    private sealed record Ev(EvKind Kind, int Step, string What, long Offset = 0, long Length = 0, byte[]? Data = null, byte[]? Wal = null);

    /// <summary>The record of one run: the events and a copy of the page file as they leave it.</summary>
    private sealed class Recorder
    {
        private readonly int _pageSize;
        private readonly List<FileStream> _replacedStreams = new();
        private PageFile? _pageFile;
        private SafeFileHandle? _dbHandle;
        private RecordingWalStream? _wal;
        private byte[] _db = [];
        private bool _suspended;

        public Recorder(int pageSize) => _pageSize = pageSize;

        public List<Ev> Events { get; } = new();

        /// <summary>The step that the events recorded from now on belong to.</summary>
        public volatile int Step;

        public void Attach(PageFile pageFile, SafeFileHandle dbHandle, RecordingWalStream wal, FileStream replaced, string what)
        {
            lock (this)
            {
                _pageFile = pageFile;
                _dbHandle = dbHandle;
                _wal = wal;
                _replacedStreams.Add(replaced);
                _suspended = false;
                Baseline($"{what} ({pageFile.NextPageId} pages)");
            }
        }

        private void Baseline(string what)
        {
            _db = ReadFile(_dbHandle!);
            Events.Add(new Ev(EvKind.Baseline, Step, what, Data: _db.ToArray(), Wal: ReadFile(_wal!.SafeFileHandle)));
        }

        public void Suspend()
        {
            lock (this) _suspended = true;
        }

        public void Resume(string what)
        {
            lock (this)
            {
                _suspended = false;
                Baseline($"the end of {what}");
            }
        }

        /// <summary>Drops the events of a step that failed without a crash.</summary>
        public void DropStep(int step)
        {
            lock (this) Events.RemoveAll(e => e.Step >= step);
        }

        /// <summary>Records the pages that changed, in the order given, after a page file call; the caller holds the lock.</summary>
        public void PagesChanged(string what, params uint[] pages)
        {
            if (_suspended) return;
            var length = RandomAccess.GetLength(_dbHandle!);
            if (length != _db.Length)
            {
                Array.Resize(ref _db, (int)length);
                Events.Add(new Ev(EvKind.PageLength, Step, $"{what}: the page file sized to {length / _pageSize} pages", Length: length));
            }
            foreach (var page in pages)
            {
                long offset = (long)page * _pageSize;
                if (offset + _pageSize > _db.Length) continue;
                var bytes = new byte[_pageSize];
                _pageFile!.ReadPage(page, bytes);
                if (bytes.AsSpan().SequenceEqual(_db.AsSpan((int)offset, _pageSize))) continue;
                bytes.CopyTo(_db, offset);
                Events.Add(new Ev(EvKind.Page, Step, $"{what}: page {page} written", Offset: offset, Data: bytes));
            }
        }

        public void Add(EvKind kind, string what, long offset = 0, long length = 0, byte[]? data = null)
        {
            lock (this)
                if (!_suspended) Events.Add(new Ev(kind, Step, what, offset, length, data));
        }
    }

    /// <summary>Forwards to a <see cref="PageFile"/> and records what each write changed in it.</summary>
    private sealed class RecordingPageStorage(PageFile inner, Recorder rec) : IPageStorage
    {
        public PageFile Inner => inner;
        public int PageSize => inner.PageSize;
        public uint NextPageId => inner.NextPageId;
        public void Open() => inner.Open();
        public void ReadPage(uint pageId, Span<byte> destination) => inner.ReadPage(pageId, destination);
        public void ReadPageHeader(uint pageId, Span<byte> destination) => inner.ReadPageHeader(pageId, destination);
        public ValueTask ReadPageAsync(uint pageId, Memory<byte> destination, CancellationToken cancellationToken = default) => inner.ReadPageAsync(pageId, destination, cancellationToken);

        public void WritePage(uint pageId, ReadOnlySpan<byte> source)
        {
            lock (rec)
            {
                inner.WritePage(pageId, source);
                rec.PagesChanged($"WritePage({pageId})", pageId);
            }
        }

        // A recycled page: the free-list head in page 0 moves first, then the page's header is cleared.
        public uint AllocatePage()
        {
            lock (rec)
            {
                var pageId = inner.AllocatePage();
                rec.PagesChanged($"AllocatePage() = {pageId}", 0, pageId);
                return pageId;
            }
        }

        // The page is marked free first, then the free-list head in page 0 moves to it.
        public void FreePage(uint pageId)
        {
            lock (rec)
            {
                inner.FreePage(pageId);
                rec.PagesChanged($"FreePage({pageId})", pageId, 0);
            }
        }

        public void Flush()
        {
            inner.Flush();
            rec.Add(EvKind.PageFlush, "a page file flush");
        }

        public async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            await inner.FlushAsync(cancellationToken);
            rec.Add(EvKind.PageFlush, "a page file flush");
        }

        public Task BackupAsync(string destinationPath, CancellationToken cancellationToken = default) => inner.BackupAsync(destinationPath, cancellationToken);

        public async Task TruncateToMinimumAsync(CancellationToken cancellationToken = default)
        {
            await inner.TruncateToMinimumAsync(cancellationToken);
            lock (rec) rec.PagesChanged("TruncateToMinimumAsync()");
        }

        public void Dispose()
        {
            inner.Dispose();
            rec.Add(EvKind.PageFlush, "the page file closed");
        }
    }

    /// <summary>
    /// The WAL's stream: the WAL's own 64 KB write buffer (<c>WriteAheadLog.cs:119</c>) kept here, over
    /// an unbuffered stream on the same handle, so that every write the OS receives, every
    /// truncation and every fsync is recorded in order. A flush hands the buffer to the OS
    /// (<see cref="FlushAsync(CancellationToken)"/> does no more on .NET 6 to 10);
    /// <c>Flush(true)</c> also fsyncs.
    /// </summary>
    private sealed class RecordingWalStream : FileStream
    {
        private readonly Recorder _rec;
        private readonly byte[] _buffer = new byte[64 * 1024];
        private int _buffered;

        public RecordingWalStream(SafeFileHandle handle, long position, Recorder rec) : base(handle, FileAccess.ReadWrite, bufferSize: 0)
        {
            _rec = rec;
            base.Position = position;
        }

        public override long Position
        {
            get => base.Position + _buffered;
            set { FlushBuffer(); base.Position = value; }
        }

        public override long Length => Math.Max(base.Length, base.Position + _buffered);

        public override long Seek(long offset, SeekOrigin origin)
        {
            FlushBuffer();
            return base.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            FlushBuffer();
            base.SetLength(value);
            _rec.Add(EvKind.WalSetLength, $"the WAL truncated to {value} bytes", length: value);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            if (_buffered + buffer.Length > _buffer.Length) FlushBuffer();
            if (buffer.Length >= _buffer.Length)
            {
                ToOs(buffer);
                return;
            }
            buffer.CopyTo(_buffer.AsSpan(_buffered));
            _buffered += buffer.Length;
        }

        public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));
        public override void WriteByte(byte value) => Write([value]);

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Write(buffer.Span);
            return ValueTask.CompletedTask;
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            Write(buffer.AsSpan(offset, count));
            return Task.CompletedTask;
        }

        public override void Flush() => Flush(false);

        public override void Flush(bool flushToDisk)
        {
            FlushBuffer();
            if (!flushToDisk) return;
            base.Flush(true);
            _rec.Add(EvKind.WalFsync, "a WAL fsync");
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            FlushBuffer();
            _rec.Add(EvKind.WalFlush, "a WAL flush");
            return Task.CompletedTask;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            FlushBuffer();
            return base.Read(buffer, offset, count);
        }

        public override int Read(Span<byte> buffer)
        {
            FlushBuffer();
            return base.Read(buffer);
        }

        public override int ReadByte()
        {
            FlushBuffer();
            return base.ReadByte();
        }

        public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            FlushBuffer();
            return base.CopyToAsync(destination, bufferSize, cancellationToken);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing) FlushBuffer();
            base.Dispose(disposing);
        }

        public override ValueTask DisposeAsync()
        {
            FlushBuffer();
            return base.DisposeAsync();
        }

        private void FlushBuffer()
        {
            if (_buffered == 0) return;
            ToOs(_buffer.AsSpan(0, _buffered));
            _buffered = 0;
        }

        private void ToOs(ReadOnlySpan<byte> data)
        {
            var at = base.Position;
            var bytes = data.ToArray();
            base.Write(bytes, 0, bytes.Length); // the array overload: the span one of a derived FileStream calls back into Write(byte[], ...)
            _rec.Add(EvKind.WalWrite, $"a WAL write of {bytes.Length} bytes at byte {at}", offset: at, data: bytes);
        }
    }

    // ── Evaluation ────────────────────────────────────────────────────────────

    /// <summary>Opens a crash state and returns the checks it fails, with what failed.</summary>
    private static async Task<Dictionary<string, string>> Evaluate(byte[] db, byte[] wal, Recording rec, int step)
    {
        var failures = new Dictionary<string, string>();
        var dir = NewDirectory();
        var path = Path.Combine(dir, "store.db");
        File.WriteAllBytes(path, db);
        File.WriteAllBytes(Path.ChangeExtension(path, ".wal"), wal);
        StoreModelDbContext? ctx = null;
        try
        {
            try
            {
                ctx = new StoreModelDbContext(path, s_config);
            }
            catch (Exception ex)
            {
                failures[Recovery] = $"the reopen threw {ex.GetType().Name}: {ex.Message}";
                return failures;
            }

            // Atomicity and durability: the documents equal a committed state, not an older one than acknowledged.
            Dictionary<int, StoreDoc> scanned;
            try
            {
                scanned = await StoreModel.ReadAll(ctx.Docs.FindAllAsync());
            }
            catch (Exception ex)
            {
                failures[Atomicity] = $"the scan after recovery threw {ex.GetType().Name}: {ex.Message}";
                return failures;
            }
            var matched = Match(rec, step, scanned, failures);

            // Pages, on the recovered state before anything else is written.
            try
            {
                foreach (var (check, message) in Account(ctx))
                    failures[check] = message;
            }
            catch (Exception ex)
            {
                failures[PagesReachable] = $"the page accounting threw {ex.GetType().Name}: {ex.Message}";
            }

            // Every access path (count, point lookups, both secondary indexes) agrees with the documents.
            if (matched != null)
            {
                var verify = new StoreModelRunner(Once(ctx), Path.Combine(dir, "backup.db"), s_config, new(matched));
                ctx = null;
                try
                {
                    await verify.VerifyAsync("after recovery");
                }
                catch (Exception ex)
                {
                    failures[Atomicity] = $"the documents are a committed state, but {Describe(ex)}";
                }
                finally
                {
                    verify.Dispose();
                }
            }
            else
            {
                ctx.Dispose();
                ctx = null;
            }

            // Idempotent: a second reopen finds the same documents.
            ctx = new StoreModelDbContext(path, s_config);
            var again = await StoreModel.ReadAll(ctx.Docs.FindAllAsync());
            if (!StoreModel.Same(scanned, again))
                failures[Idempotent] = $"a second reopen changed the collection: {Difference(scanned, again)}";

            // Continue: the rest of the sequence, a new document, a checkpoint and a reopen.
            var rest = rec.Ops.Skip(step).Take(6).ToList();
            var fresh = new StoreDoc { Id = StoreModel.IdPool + 1, Name = "after the crash", Count = 1, Payload = "z" };
            rest.Add(new StoreOp(StoreOpKind.Insert, fresh.Id, fresh.Count, null, fresh.Payload.Length, Docs: [fresh]));
            rest.Add(new StoreOp(StoreOpKind.Checkpoint));
            rest.Add(new StoreOp(StoreOpKind.Reopen));
            var next = ctx;
            ctx = null;
            using (var runner = new StoreModelRunner(Once(next, path), Path.Combine(dir, "backup.db"), s_config, new(again)))
            {
                foreach (var op in rest)
                {
                    try
                    {
                        await runner.ApplyAsync(op);
                        await runner.VerifyAsync($"after {op}");
                    }
                    catch (Exception ex)
                    {
                        failures[Continue] = $"after recovery, {op} failed: {Describe(ex)}";
                        break;
                    }
                }
            }
            return failures;
        }
        catch (Exception ex)
        {
            failures.TryAdd(Continue, $"the recovered database failed: {Describe(ex)}");
            return failures;
        }
        finally
        {
            try { ctx?.Dispose(); } catch { /* the failure is recorded */ }
            DeleteDirectory(dir);
        }
    }

    private static string Describe(Exception ex) => ex is StoreModelMismatchException ? ex.Message : $"{ex.GetType().Name}: {ex.Message}";

    /// <summary>The context first, then a fresh open of <paramref name="path"/> at every reopen.</summary>
    private static Func<StoreModelDbContext> Once(StoreModelDbContext first, string? path = null)
    {
        var used = false;
        return () =>
        {
            if (!used) { used = true; return first; }
            return new StoreModelDbContext(path ?? throw new InvalidOperationException("no reopen here"), s_config);
        };
    }

    /// <summary>
    /// The committed state the documents equal: the state before or after the crashed step, or an
    /// older one (a durability failure), or none (an atomicity failure, null).
    /// </summary>
    private static Dictionary<int, StoreDoc>? Match(Recording rec, int step, Dictionary<int, StoreDoc> scanned, Dictionary<string, string> failures)
    {
        var before = rec.Before[step];
        var after = rec.After[step];
        if (StoreModel.Same(before, scanned)) return before;
        if (StoreModel.Same(after, scanned)) return after;
        for (int j = step - 1; j >= 0; j--)
        {
            if (!StoreModel.Same(rec.After[j], scanned)) continue;
            failures[Durability] = $"the collection is the committed state after {rec.StepName(j)}, older than the state the engine had acknowledged before the crash: {Difference(before, scanned)}";
            return rec.After[j];
        }
        failures[Atomicity] = $"the collection is no committed state; against the state before the crashed step: {Difference(before, scanned)}"
            + (ReferenceEquals(before, after) || StoreModel.Same(before, after) ? "" : $"; against the state after it: {Difference(after, scanned)}");
        return null;
    }

    private static string Difference(Dictionary<int, StoreDoc> expected, Dictionary<int, StoreDoc> actual)
    {
        try
        {
            StoreModel.CheckSame(expected, actual, "the collection");
            return "no difference";
        }
        catch (StoreModelMismatchException ex)
        {
            return ex.Message;
        }
    }

    // ── Page accounting ───────────────────────────────────────────────────────

    /// <summary>
    /// The page accounting of a recovered database: what the structures reach (the primary and both
    /// secondary indexes from their roots, the data page of every document the primary index lists,
    /// its overflow chain), the free list and the frees queued in this engine, against the page types.
    /// </summary>
    private static IEnumerable<(string Check, string Message)> Account(StoreModelDbContext ctx)
    {
        var storage = ctx.Storage;
        storage.PendingFreeCheckpointCompletion.GetAwaiter().GetResult();
        var count = storage.PageCount;
        var pageSize = storage.PageSize;
        var page = new byte[pageSize];

        var types = new PageType[count];
        for (uint p = 0; p < count; p++)
        {
            storage.ReadPageHeader(p, null, page.AsSpan(0, 32));
            types[p] = PageHeader.ReadFrom(page).PageType;
        }
        var freeMarked = Enumerable.Range(1, (int)count - 1).Select(p => (uint)p).Where(p => types[p] == PageType.Free).ToHashSet();
        var listed = storage.GetCollectionPageIds("store_docs").ToHashSet();
        var pending = Enumerable.Range(0, (int)count).Select(p => (uint)p).Where(p => !listed.Contains(p)).ToHashSet();

        // The free list from page 0.
        var chain = new List<uint>();
        string? chainError = null;
        storage.ReadPageHeader(0, null, page.AsSpan(0, 32));
        var seen = new HashSet<uint>();
        for (var next = PageHeader.ReadFrom(page).NextPageId; next != 0;)
        {
            if (next >= count) { chainError = $"the free list points past the end of the file ({count} pages) at page {next}"; break; }
            if (!seen.Add(next)) { chainError = $"the free list loops back to page {next} after {chain.Count} pages"; break; }
            chain.Add(next);
            if (types[next] != PageType.Free) { chainError = $"page {next}, on the free list after {chain.Count - 1} pages, has type {types[next]}"; break; }
            storage.ReadPageHeader(next, null, page.AsSpan(0, 32));
            next = PageHeader.ReadFrom(page).NextPageId;
        }

        // What the structures reach.
        var owner = new Dictionary<uint, string>();
        var dataPages = new HashSet<uint>();
        var locations = new List<DocumentLocation>();
        string? reachError = Walk(storage, ctx.Docs._primaryIndex.RootPageId, "the primary index", owner, locations);
        foreach (var index in ctx.Docs.IndexManager.GetAllIndexes())
            if (reachError == null && index.BTreeIndex is { } tree)
                reachError = Walk(storage, tree.RootPageId, $"index '{index.Definition.Name}'", owner, null);
        foreach (var location in locations)
        {
            if (reachError != null) break;
            reachError = WalkDocument(storage, location, owner, dataPages);
        }
        if (reachError == null)
        {
            foreach (var p in owner.Keys.Concat(dataPages).Order())
            {
                if (chain.Contains(p) || freeMarked.Contains(p)) { reachError = $"page {p}, reached from {owner.GetValueOrDefault(p, "a document")}, is on the free list"; break; }
                if (pending.Contains(p)) { reachError = $"page {p}, reached from {owner.GetValueOrDefault(p, "a document")}, is queued to be freed"; break; }
            }
        }
        if (reachError != null) yield return (PagesReachable, reachError);

        var freeError = chainError;
        if (freeError == null)
        {
            var unlisted = freeMarked.Except(chain).Order().ToList();
            if (unlisted.Count > 0) freeError = $"{unlisted.Count} pages are marked free but not on the free list: {Pages(unlisted)}";
        }
        if (freeError != null) yield return (PagesFreeList, freeError);

        // Written index and overflow pages that nothing reaches and nothing frees.
        if (reachError == null)
        {
            var leaked = Enumerable.Range(1, (int)count - 1).Select(p => (uint)p)
                .Where(p => types[p] is PageType.Index or PageType.Overflow && !owner.ContainsKey(p) && !pending.Contains(p))
                .ToList();
            if (leaked.Count > 0)
                yield return (PagesLeaked, $"{leaked.Count} of the {count} pages are index or overflow pages that nothing reaches and that are not free: "
                    + string.Join(", ", leaked.Take(12).Select(p => $"{p} ({types[p]})")) + (leaked.Count > 12 ? ", ..." : ""));
        }
    }

    private static string Pages(List<uint> pages) => string.Join(", ", pages.Take(12)) + (pages.Count > 12 ? ", ..." : "");

    /// <summary>
    /// Walks a B-Tree from its root with a guard against every loop (the engine's own walk recurses
    /// without one), recording each page's owner and, for the primary index, the documents' locations.
    /// </summary>
    private static string? Walk(StorageEngine storage, uint root, string name, Dictionary<uint, string> owner, List<DocumentLocation>? locations)
    {
        var count = storage.PageCount;
        var pageSize = storage.PageSize;
        var page = new byte[pageSize];
        var stack = new Stack<(uint Id, int Depth)>();
        stack.Push((root, 0));
        while (stack.Count > 0)
        {
            var (id, depth) = stack.Pop();
            if (id == 0 || id >= count) return $"{name} leads to page {id}, outside the file ({count} pages)";
            if (owner.TryGetValue(id, out var other)) return $"page {id} is reached from {name} and from {other}";
            if (depth > 32) return $"{name} is deeper than 32 levels at page {id}";
            owner[id] = name;
            storage.ReadPage(id, null, page);
            var type = PageHeader.ReadFrom(page).PageType;
            if (type != PageType.Index) return $"page {id}, reached from {name}, has type {type}";
            var node = BTreeNodeHeader.ReadFrom(page.AsSpan(32));
            int offset = 32 + 20;
            if (!node.IsLeaf)
            {
                if (offset + 4 > pageSize) return $"node {id} of {name} has no room for its first child pointer";
                stack.Push((BitConverter.ToUInt32(page, offset), depth + 1));
                offset += 4;
            }
            for (int i = 0; i < node.EntryCount; i++)
            {
                int tail = node.IsLeaf ? DocumentLocation.SerializedSize : 4;
                if (offset + 4 > pageSize) return $"node {id} of {name}: entry {i} of {node.EntryCount} starts past the end of the page";
                int length = BitConverter.ToInt32(page, offset);
                if (length < 0 || offset + 4 + length + tail > pageSize) return $"node {id} of {name}: entry {i} of {node.EntryCount} runs past the end of the page";
                if (node.IsLeaf) locations?.Add(DocumentLocation.ReadFrom(page.AsSpan(offset + 4 + length)));
                else stack.Push((BitConverter.ToUInt32(page, offset + 4 + length), depth + 1));
                offset += 4 + length + tail;
            }
        }
        return null;
    }

    /// <summary>The data page a primary index entry points at, and the overflow chain of its document.</summary>
    private static string? WalkDocument(StorageEngine storage, DocumentLocation location, Dictionary<uint, string> owner, HashSet<uint> dataPages)
    {
        var count = storage.PageCount;
        var page = new byte[storage.PageSize];
        var at = $"the document at page {location.PageId}, slot {location.SlotIndex}";
        if (location.PageId == 0 || location.PageId >= count) return $"the primary index points at page {location.PageId}, outside the file ({count} pages)";
        storage.ReadPage(location.PageId, null, page);
        var header = SlottedPageHeader.ReadFrom(page);
        if (header.PageType != PageType.Data) return $"the primary index points at page {location.PageId}, of type {header.PageType}";
        if (owner.TryGetValue(location.PageId, out var other)) return $"data page {location.PageId} is also reached from {other}";
        dataPages.Add(location.PageId);
        if (location.SlotIndex >= header.SlotCount) return $"the primary index points at slot {location.SlotIndex} of page {location.PageId}, which has {header.SlotCount}";
        var slot = SlotEntry.ReadFrom(page.AsSpan(SlottedPageHeader.Size + location.SlotIndex * SlotEntry.Size));
        if ((slot.Flags & SlotFlags.Deleted) != 0) return $"the primary index points at {at}, which is deleted";
        if ((slot.Flags & SlotFlags.HasOverflow) == 0) return null;
        if (slot.Offset + 8 > page.Length) return $"{at} has an overflow slot past the end of the page";
        var next = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(slot.Offset + 4, 4));
        for (int n = 0; next != 0; n++)
        {
            if (next >= count) return $"the overflow chain of {at} leads to page {next}, outside the file ({count} pages)";
            if (owner.TryGetValue(next, out other)) return $"overflow page {next} of {at} is also reached from {other}";
            if (dataPages.Contains(next)) return $"overflow page {next} of {at} is also a data page";
            if (n > count) return $"the overflow chain of {at} does not end";
            owner[next] = at;
            storage.ReadPage(next, null, page);
            var h = SlottedPageHeader.ReadFrom(page);
            if (h.PageType != PageType.Overflow) return $"page {next}, on the overflow chain of {at}, has type {h.PageType}";
            next = h.NextOverflowPage;
        }
        return null;
    }

    // ── Shrinking ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Shrinks by removing chunks of steps before the crashed one (half, then quarters, ... down to
    /// single steps), keeping a removal if some crash in the last step still fails the same check,
    /// until a pass over single steps removes nothing or the time is up. The algorithm of
    /// <see cref="StoreModelTests"/>, for one check and a crash in the last step.
    /// </summary>
    private static async Task<(List<StoreOp> Ops, (string Point, string Message) Failure)> Shrink(Found found, string mode, string check, TimeSpan budget)
    {
        var sw = Stopwatch.StartNew();
        // The crashed step is the last one kept; a crash in the first open or the close keeps none or all.
        var step = found.Step;
        var current = found.Ops.Take(Math.Min(step, found.Ops.Count)).ToList();
        bool closing = step > found.Ops.Count;
        var failure = (found.Point, found.Message);
        if (step == 0) return (current, failure);
        int keep = closing ? 0 : 1;
        int chunk = Math.Max(1, (current.Count - keep) / 2);
        while (sw.Elapsed < budget)
        {
            bool changed = false;
            for (int start = current.Count - keep - chunk; start >= 0 && sw.Elapsed < budget; start -= chunk)
            {
                var candidate = current.Where((_, j) => j < start || j >= start + chunk).ToList();
                if (await FailsAtLastStep(candidate, mode, check, closing) is { } f)
                {
                    current = candidate;
                    failure = f;
                    changed = true;
                }
            }
            if (chunk == 1 && !changed) break;
            if (!changed) chunk = Math.Max(1, chunk / 2);
        }
        return (current, failure);
    }

    private static async Task<(string Point, string Message)?> FailsAtLastStep(List<StoreOp> ops, string mode, string check, bool closing)
    {
        Recording rec;
        try
        {
            rec = await Record(ops);
        }
        catch
        {
            return null;
        }
        if (rec.Ops.Count != ops.Count) return null;
        (string, string)? hit = null;
        await Explore(rec, mode, closing ? ops.Count + 1 : ops.Count, (_, point, failures) =>
        {
            if (!failures.TryGetValue(check, out var message)) return true;
            hit = (point, message);
            return false;
        });
        return hit;
    }

    // ── Files ─────────────────────────────────────────────────────────────────

    private static byte[] ReadFile(SafeFileHandle handle)
    {
        var bytes = new byte[RandomAccess.GetLength(handle)];
        for (int read = 0; read < bytes.Length;)
        {
            var n = RandomAccess.Read(handle, bytes.AsSpan(read), read);
            if (n == 0) break;
            read += n;
        }
        return bytes;
    }

    private static string NewDirectory()
    {
        var dir = Path.Combine(Path.GetTempPath(), "blite_wal_crash", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static void DeleteDirectory(string dir)
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best-effort */ }
    }

    /// <summary>A growable byte array, zero-filled where it grows.</summary>
    private sealed class Bytes(byte[] initial)
    {
        private byte[] _bytes = initial.ToArray();
        private int _length = initial.Length;

        public void Write(long offset, ReadOnlySpan<byte> data)
        {
            var end = (int)offset + data.Length;
            Grow(end);
            data.CopyTo(_bytes.AsSpan((int)offset));
            _length = Math.Max(_length, end);
        }

        public void SetLength(long length)
        {
            Grow((int)length);
            if (length < _length) Array.Clear(_bytes, (int)length, _length - (int)length);
            _length = (int)length;
        }

        private void Grow(int length)
        {
            if (length > _bytes.Length) Array.Resize(ref _bytes, Math.Max(length, _bytes.Length * 2));
        }

        public byte[] ToArray() => _bytes.AsSpan(0, _length).ToArray();
    }
}
