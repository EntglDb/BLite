using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using BLite.Core;
using BLite.Core.Indexing;
using BLite.Core.Storage;
using Xunit.Abstractions;

namespace BLite.Tests.Oracles;

/// <summary>
/// Model-based test of <see cref="BTreeIndex"/> driven directly over a <see cref="StorageEngine"/>:
/// a seeded random sequence of inserts (single, batches, long runs of one duplicate key), deletes
/// (single, consecutive runs, absent entries), commits, rollbacks, checkpoints and reopens runs
/// against the index and against a sorted dictionary from <see cref="IndexKey"/> (ordered by the
/// index's own key comparison) to the documents under that key, in insertion order. After every
/// step every query primitive (TryFind, CountRange, Range both ways and async, Equal, In, Between,
/// GreaterThan, LessThan, StartsWith, Like and their async twins, cursor walks both ways, cursor
/// windows) must answer
/// what the model answers, through the open transaction and, for the committed view, without it.
/// After every commit, rollback, checkpoint and reopen, the pages must add up: the pages reachable
/// from the root plus the free pages (free list and frees queued until the next WAL truncation)
/// are exactly the pages allocated since the index was created, with no page in both.
/// </summary>
/// <remarks>
/// <para>
/// Each key generator (a scenario) runs <see cref="Steps"/> steps for each of <see cref="s_seeds"/>,
/// once per test run, and each check is a row of its own (<c>"&lt;scenario&gt; / &lt;check&gt;"</c>), so a
/// defect in one primitive does not hide the others and a candidate names exactly what disagrees. A
/// failing row shrinks its sequence to the shortest one that still fails that check and prints it
/// with the seed. Strings are printed with control characters escaped and runs of eight or more
/// equal characters as <c>{c*n}</c>.
/// </para>
/// <para>
/// The contract the model encodes: an entry is a (key, document) pair and a key may hold any number
/// of documents (a unique index rejects a second document with <see cref="InvalidOperationException"/>
/// and changes nothing); a key longer than <c>MaxKeySize</c> is rejected with
/// <see cref="ArgumentException"/>; <c>Delete</c> returns whether that exact pair existed; every
/// primitive returns all entries that satisfy it, ascending except <c>LessThan</c> and backward scans,
/// which return the exact reverse; equal keys come forward in insertion order (<c>Ties.Order</c>);
/// writes are visible through their transaction only until it commits, and a rollback or a reopen
/// discards them. <c>StartsWith</c> and <c>Like</c> compare ordinally. <c>KeyOrder</c> checks the
/// key encoding itself against the natural order of the values (<see cref="IComparable"/>,
/// <see cref="string.CompareOrdinal(string, string)"/>), which is what LINQ-to-Objects uses.
/// </para>
/// <para>
/// Page accounting reads only what the engine exposes: <see cref="StorageEngine.PageCount"/>,
/// <see cref="StorageEngine.GetCollectionPageIds"/> (which, in single-file mode, lists every page
/// except the frees queued until the next WAL truncation), the page headers (a page is on the free
/// list exactly while its header says <see cref="PageType.Free"/>, the free list starting at page
/// 0's <c>NextPageId</c>) and <see cref="BTreeIndex.CollectAllPages()"/>. Pages that a rolled-back
/// transaction allocated and nothing reclaims (#237) are counted apart, as <c>Pages.LeakedByRollback</c>.
/// </para>
/// </remarks>
public class BTreeModelTests
{
    private readonly ITestOutputHelper _output;

    public BTreeModelTests(ITestOutputHelper output) => _output = output;

    private const int Steps = 300;
    private const int PageSize = 4096;

    /// <summary>The largest key on <see cref="PageSize"/>-byte pages: half an internal node's entry space less two lengths (see <c>BTreeIndex.MaxKeySize</c>).</summary>
    private const int MaxKeyBytes = (PageSize - 32 - 20 - 4) / 2 - 4 - 4;

    private static readonly int[] s_seeds = [1, 2, 3];

    // ── Checks ────────────────────────────────────────────────────────────────

    private const string Write = "Write";
    private const string Structure = "Structure";
    private const string KeyOrder = "KeyOrder";
    private const string KeyOrderNaN = "KeyOrder.NaN";
    private const string TryFind = "TryFind";
    private const string CountRange = "CountRange";
    private const string RangeForward = "Range.Forward";
    private const string RangeBackward = "Range.Backward";
    private const string RangeAsync = "Range.Async";
    private const string Equal = "Equal";
    private const string In = "In";
    private const string Between = "Between";
    private const string GreaterThan = "GreaterThan";
    private const string LessThan = "LessThan";
    private const string StartsWith = "StartsWith";
    private const string Like = "Like";
    private const string EqualAsync = "Equal.Async";
    private const string InAsync = "In.Async";
    private const string BetweenAsync = "Between.Async";
    private const string GreaterThanAsync = "GreaterThan.Async";
    private const string LessThanAsync = "LessThan.Async";
    private const string StartsWithAsync = "StartsWith.Async";
    private const string LikeAsync = "Like.Async";
    private const string CursorForward = "Cursor.Forward";
    private const string CursorBackward = "Cursor.Backward";
    private const string CursorWindow = "Cursor.Window";
    private const string TiesOrder = "Ties.Order";
    private const string Isolation = "Isolation";
    private const string PagesReachable = "Pages.Reachable";
    private const string PagesFreeList = "Pages.FreeList";
    private const string PagesLeaked = "Pages.Leaked";
    private const string PagesLeakedByRollback = "Pages.LeakedByRollback";

    private static readonly string[] s_checks =
    [
        Write, Structure, KeyOrder, KeyOrderNaN, TryFind, CountRange, RangeForward, RangeBackward, RangeAsync, Equal, In, Between,
        GreaterThan, LessThan, StartsWith, Like, EqualAsync, InAsync, BetweenAsync, GreaterThanAsync, LessThanAsync,
        StartsWithAsync, LikeAsync, CursorForward, CursorBackward, CursorWindow, TiesOrder,
        Isolation, PagesReachable, PagesFreeList, PagesLeaked, PagesLeakedByRollback,
    ];

    private static readonly string[] s_stringOnlyChecks = [StartsWith, Like, StartsWithAsync, LikeAsync];
    private static readonly string[] s_doubleOnlyChecks = [KeyOrderNaN];

    // ── Key generators ────────────────────────────────────────────────────────

    /// <summary>A key generator: the values it draws, the special values it mixes in, and the index it runs on.</summary>
    private sealed record Scenario(string Name, Func<Random, object> Value, object[] Specials, bool Unique = false, bool MultiProcess = false)
    {
        public bool Strings => Specials[0] is string;
        public bool Doubles => Specials[0] is double;
    }

    private static readonly int[] s_intSpecials = [int.MinValue, int.MinValue + 1, -1, 0, 1, int.MaxValue - 1, int.MaxValue];

    private static readonly long[] s_longSpecials =
        [long.MinValue, long.MinValue + 1, (long)int.MinValue - 1, -1, 0, 1, (long)int.MaxValue + 1, long.MaxValue - 1, long.MaxValue];

    private static readonly double[] s_doubleSpecials =
    [
        -0.0, 0.0, double.NaN, BitConverter.Int64BitsToDouble(0x7FF8_0000_0000_0000), double.NegativeInfinity, double.PositiveInfinity,
        double.MinValue, double.MaxValue, double.Epsilon, -double.Epsilon, 1.0, -1.0, 0.1, 1e-300,
    ];

    private static readonly string[] s_prefixSpecials = ["", "p", "p7", "p70", "p700", "p7000", "p70000", "p71", "p8", "q"];
    private static readonly string[] s_prefixStems = ["", "p", "p7", "p70"];

    private static readonly string[] s_controlSpecials =
        ["", "\0", "\u0001", "a", "a\0", "a\0\0", "a\0b", "a\u0001", "a\u0002", "a\u0003", "a\u0003b", "ab", "b"];

    private static readonly Scenario[] s_scenarios =
    [
        new("int", r => r.Next(100) switch
            {
                < 45 => r.Next(-40, 40),
                < 55 => s_intSpecials[r.Next(s_intSpecials.Length)],
                _ => r.Next(int.MinValue, int.MaxValue),
            }, s_intSpecials.Cast<object>().ToArray()),
        new("long", r => r.Next(100) switch
            {
                < 45 => (long)r.Next(-40, 40),
                < 55 => s_longSpecials[r.Next(s_longSpecials.Length)],
                _ => r.NextInt64(long.MinValue, long.MaxValue),
            }, s_longSpecials.Cast<object>().ToArray()),
        new("double", r => r.Next(100) switch
            {
                < 40 => r.Next(-30, 30) / 4.0,
                < 60 => s_doubleSpecials[r.Next(s_doubleSpecials.Length)],
                _ => (r.NextDouble() - 0.5) * Math.Pow(10, r.Next(-20, 20)),
            }, s_doubleSpecials.Cast<object>().ToArray()),
        // Keys that are prefixes of each other (#206), duplicates, and variable lengths up to a
        // few hundred bytes so that leaves split by size as well as by count (#217).
        new("string-prefix", r => r.Next(100) switch
            {
                < 30 => "p7" + new string('0', r.Next(0, 5)) + (r.Next(3) == 0 ? r.Next(10).ToString() : ""),
                < 50 => s_prefixSpecials[r.Next(s_prefixSpecials.Length)],
                < 85 => s_prefixStems[r.Next(s_prefixStems.Length)] + RandomText(r, "abcd", r.Next(1, 10)),
                _ => "p7" + new string('x', r.Next(100, 600)) + RandomText(r, "abcd", r.Next(0, 4)),
            }, s_prefixSpecials),
        // Bytes 0x00 to 0x03 inside string keys (#212): they sort below every printable character.
        new("string-control", r => r.Next(100) switch
            {
                < 35 => s_controlSpecials[r.Next(s_controlSpecials.Length)],
                _ => RandomText(r, "\0\u0001\u0002\u0003ab", r.Next(0, 7)),
            }, s_controlSpecials),
        // Keys up to the size limit (two per node) and a few beyond it, which must be rejected.
        new("string-long", r => r.Next(100) switch
            {
                < 45 => "k" + RandomText(r, "abc", r.Next(0, 12)),
                < 80 => "k" + new string('m', r.Next(100, 600)) + RandomText(r, "abc", r.Next(0, 4)),
                < 97 => "k" + new string('z', r.Next(MaxKeyBytes - 300, MaxKeyBytes - 6)) + RandomText(r, "abc", r.Next(0, 4)),
                _ => "k" + new string('o', r.Next(MaxKeyBytes - 1, MaxKeyBytes + 40)),
            }, ["k", "ka", "kb", "k" + new string('z', MaxKeyBytes - 2), "k" + new string('o', MaxKeyBytes - 1)]),
        new("unique-int", r => r.Next(100) < 80 ? r.Next(-3000, 3000) : s_intSpecials[r.Next(s_intSpecials.Length)],
            s_intSpecials.Cast<object>().ToArray(), Unique: true),
        new("int-mp", r => r.Next(100) < 60 ? r.Next(-40, 40) : r.Next(int.MinValue, int.MaxValue),
            s_intSpecials.Cast<object>().ToArray(), MultiProcess: true),
        new("string-prefix-mp", r => r.Next(100) < 60
                ? "p7" + new string('0', r.Next(0, 5))
                : "p7" + new string('x', r.Next(0, 400)) + RandomText(r, "abcd", r.Next(0, 4)),
            s_prefixSpecials, MultiProcess: true),
    ];

    private static string RandomText(Random r, string alphabet, int length)
    {
        var chars = new char[length];
        for (int i = 0; i < length; i++) chars[i] = alphabet[r.Next(alphabet.Length)];
        return new string(chars);
    }

    // ── Rows and candidates ───────────────────────────────────────────────────

    public static TheoryData<string, string> Rows
    {
        get
        {
            var rows = new TheoryData<string, string>();
            foreach (var sc in s_scenarios)
                foreach (var check in s_checks)
                    if ((sc.Strings || !s_stringOnlyChecks.Contains(check)) && (sc.Doubles || !s_doubleOnlyChecks.Contains(check)))
                        rows.Add(sc.Name, check);
            return rows;
        }
    }

    public static TheoryData<string> ScenarioNames => new(s_scenarios.Select(s => s.Name));

    private static string Label(string scenario, string check) => $"{scenario} / {check}";

    /// <summary>
    /// Rows known to fail on upstream 0450b97, by <see cref="Label"/>, see <see cref="Candidates"/>
    /// and the B-tree drafts in <c>Oracles/Seeds/candidate-issues.md</c>.
    /// </summary>
    private static readonly Candidates s_candidates = new()
    {
        // #255 (draft B1): Equal, In, GreaterThan, LessThan, Between and StartsWith walk a cursor from one
        // Seek: they return one entry per duplicated key or skip only one copy of it, throw
        // "Cursor is not valid" (or return nothing) when the key is past the last one or the index
        // is empty, and StartsWith compares with the current culture. InAsync returns one entry
        // per key too; the other async twins, built on RangeAsync, agree.
        { "BTreeIndex cursor primitives drop duplicates and fail past the last key", "#255",
            "double / Equal", "int / Equal", "int-mp / Equal", "long / Equal", "string-control / Equal",
            "string-long / Equal", "string-prefix / Equal", "string-prefix-mp / Equal",
            "double / In", "int / In", "int-mp / In", "long / In", "string-control / In", "string-long / In",
            "string-prefix / In", "string-prefix-mp / In",
            "double / Between", "int / Between", "int-mp / Between", "long / Between", "string-control / Between",
            "string-long / Between", "string-prefix / Between", "string-prefix-mp / Between", "unique-int / Between",
            "double / GreaterThan", "int / GreaterThan", "int-mp / GreaterThan", "long / GreaterThan",
            "string-control / GreaterThan", "string-long / GreaterThan", "string-prefix / GreaterThan",
            "string-prefix-mp / GreaterThan", "unique-int / GreaterThan",
            "double / LessThan", "int / LessThan", "int-mp / LessThan", "long / LessThan", "string-control / LessThan",
            "string-long / LessThan", "string-prefix / LessThan", "string-prefix-mp / LessThan",
            "unique-int / LessThan",
            "string-control / StartsWith", "string-long / StartsWith", "string-prefix / StartsWith",
            "string-prefix-mp / StartsWith",
            "double / In.Async", "int / In.Async", "int-mp / In.Async", "long / In.Async", "string-control / In.Async",
            "string-long / In.Async", "string-prefix / In.Async", "string-prefix-mp / In.Async" },
        // #256 (draft B2): a leaf inserts a duplicate before the copies it already holds, so equal keys
        // come back in reverse insertion order even scanning forwards. Sibling of #210 (typed,
        // descending); its fix #244 changes CollectionSecondaryIndex only, not these rows.
        { "equal keys come back in reverse insertion order", "#256",
            "double / Ties.Order", "int / Ties.Order", "int-mp / Ties.Order", "long / Ties.Order",
            "string-control / Ties.Order", "string-long / Ties.Order", "string-prefix / Ties.Order",
            "string-prefix-mp / Ties.Order" },
        // -0.0 and +0.0 compare equal but encode as different keys: the defect of #249.
        { "-0.0 and +0.0 get different index keys", "#249",
            "double / KeyOrder" },
        // #257 (draft B3): a NaN with the sign bit clear (what Arm64 computes) encodes above +Infinity.
        // Sibling of #249, whose text leaves NaN as a separate question.
        { "a NaN with the sign bit clear sorts above +Infinity", "#257",
            "double / KeyOrder.NaN" },
        // Pages a rolled-back transaction allocated (split nodes, a new root) are never reclaimed: #237.
        { "pages a rolled-back transaction allocated are never reclaimed", "#237",
            "double / Pages.LeakedByRollback", "int / Pages.LeakedByRollback", "int-mp / Pages.LeakedByRollback",
            "long / Pages.LeakedByRollback", "string-control / Pages.LeakedByRollback",
            "string-long / Pages.LeakedByRollback", "string-prefix / Pages.LeakedByRollback",
            "string-prefix-mp / Pages.LeakedByRollback", "unique-int / Pages.LeakedByRollback" },
        // #258 (draft B4): a reopen counts the unwritten rest of the last growth block as allocated, and
        // nothing ever hands it out or frees it.
        { "a reopen strands the unwritten rest of the last growth block", "#258",
            "double / Pages.Leaked", "int / Pages.Leaked", "int-mp / Pages.Leaked", "long / Pages.Leaked",
            "string-control / Pages.Leaked", "string-long / Pages.Leaked", "string-prefix / Pages.Leaked",
            "string-prefix-mp / Pages.Leaked", "unique-int / Pages.Leaked" },
    };

    [Fact]
    public void Candidates_NameExistingRows() => s_candidates.AssertAllIn(Rows.Select(r => Label((string)r[0], (string)r[1])));

    [Theory]
    [MemberData(nameof(Rows))]
    public void RandomOperations_MatchTheModel(string scenario, string check)
    {
        var sc = s_scenarios.Single(s => s.Name == scenario);
        var label = Label(scenario, check);
        foreach (var seed in s_seeds)
        {
            var (ops, result) = RunOf(sc, seed);
            if (!result.Failures.TryGetValue(check, out var failure)) continue;

            // A candidate with a filed issue is only reported: no need to spend the shrink budget on it.
            var filed = s_candidates.Any(c => c.Name == label && c.Issue.Length > 0);
            s_candidates.Verify(label, Describe(sc, seed, check, ops, failure, shrink: !filed), _output);
            return;
        }
        s_candidates.Verify(label, null, _output);
    }

    /// <summary>
    /// Guards the generators themselves: a run that never grows past a few pages, or never gives
    /// pages back, exercises neither splits nor merges and would pass whatever the tree does.
    /// </summary>
    [Theory]
    [MemberData(nameof(ScenarioNames))]
    public void Generator_ForcesSplitsAndMerges(string scenario)
    {
        var sc = s_scenarios.Single(s => s.Name == scenario);
        var runs = s_seeds.Select(seed => RunOf(sc, seed).Result).ToList();
        Assert.True(runs.Max(r => r.MaxReachablePages) >= 12,
            $"{scenario}: the tree never grew past {runs.Max(r => r.MaxReachablePages)} pages, the generator does not force splits");
        Assert.True(runs.Max(r => r.PageDrops) >= 1, $"{scenario}: the tree never gave a page back, the generator does not force merges");
        Assert.True(sc.Unique || runs.Max(r => r.MaxRun) > BTreeIndex.MaxEntriesPerNode,
            $"{scenario}: no key ever held more than {runs.Max(r => r.MaxRun)} documents, a run of duplicates never crossed a leaf");
    }

    private static readonly ConcurrentDictionary<(string, int), Lazy<(List<Op> Ops, RunResult Result)>> s_runs = new();

    /// <summary>The sequence and the full replay of one scenario and seed, computed once per test run and shared by its rows.</summary>
    private static (List<Op> Ops, RunResult Result) RunOf(Scenario sc, int seed) =>
        s_runs.GetOrAdd((sc.Name, seed), _ => new Lazy<(List<Op>, RunResult)>(() =>
        {
            var ops = Generate(sc, seed, Steps);
            return (ops, Replay(sc, seed, ops, focus: null));
        })).Value;

    private static string Describe(Scenario sc, int seed, string check, List<Op> ops, Failure failure, bool shrink)
    {
        var sb = new StringBuilder();
        var where = $"{Label(sc.Name, check)}, seed {seed} ({PageSize}-byte pages, unique={sc.Unique}, multiProcess={sc.MultiProcess})";
        if (failure.Step < 0)
        {
            sb.AppendLine($"{where}: before any step: {failure.Message}");
            return sb.ToString();
        }
        sb.AppendLine($"{where}: step {failure.Step + 1} of {ops.Count} ({ops[failure.Step]}) failed: {failure.Message}");
        if (!shrink) return sb.ToString();

        var (shrunk, shrunkFailure) = Shrink(sc, seed, ops.Take(failure.Step + 1).ToList(), check, TimeSpan.FromSeconds(20));
        sb.AppendLine($"Shortest failing sequence found ({shrunk.Count} of {failure.Step + 1} steps), failing at its step {shrunkFailure.Step + 1}: {shrunkFailure.Message}");
        for (int i = 0; i < shrunk.Count; i++)
            sb.AppendLine($"  {i + 1,3}. {shrunk[i]}");
        return sb.ToString();
    }

    // ── Operations ────────────────────────────────────────────────────────────

    private enum OpKind { Insert, InsertBatch, InsertRun, Delete, DeleteRun, DeleteAbsent, Commit, Rollback, Checkpoint, Reopen }

    /// <summary>
    /// One step. Inserts carry their values and the id of their first document; deletes of existing
    /// entries carry a position, taken modulo the number of entries in key order at replay, so a
    /// shrunk sequence still deletes something.
    /// </summary>
    private sealed record Op(OpKind Kind, object[]? Values = null, int Doc = 0, int Count = 0, int Position = 0)
    {
        public override string ToString() => Kind switch
        {
            OpKind.Insert => $"Insert({FormatValue(Values![0])}, #{Doc})",
            OpKind.InsertBatch => $"InsertBatch(#{Doc}..#{Doc + Values!.Length - 1}: {string.Join(", ", Values!.Select(FormatValue))})",
            OpKind.InsertRun => $"InsertRun({FormatValue(Values![0])} x {Count}, #{Doc}..#{Doc + Count - 1})",
            OpKind.Delete => $"Delete(the entry at position {Position} mod count, in key order)",
            OpKind.DeleteRun => $"DeleteRun({Count} consecutive entries from position {Position} mod count)",
            OpKind.DeleteAbsent => $"DeleteAbsent({FormatValue(Values![0])}, #{Doc})",
            _ => Kind.ToString(),
        };
    }

    /// <summary>Documents ids from here on are never inserted: a delete of one must return false.</summary>
    private const int AbsentDocs = 40_000_000;

    private static List<Op> Generate(Scenario sc, int seed, int steps)
    {
        var rnd = new Random(seed * 7919 + StableHash(sc.Name));
        var ops = new List<Op>(steps);
        int doc = 1;

        // Three phases of 40 steps in turn: growth, mixed, shrinking (merges, root collapse).
        int[][] mix =
        [
            // Insert, InsertBatch, InsertRun, Delete, DeleteRun, DeleteAbsent, Commit, Rollback, Checkpoint, Reopen
            [25, 25, 10, 8, 0, 4, 15, 8, 3, 2],
            [20, 10, 5, 20, 10, 5, 15, 8, 4, 3],
            [8, 4, 0, 25, 28, 5, 15, 8, 4, 3],
        ];
        for (int i = 0; i < steps; i++)
        {
            var weights = mix[i / 40 % 3];
            int roll = rnd.Next(weights.Sum()), kind = 0;
            while (roll >= weights[kind]) roll -= weights[kind++];
            var k = (OpKind)kind;
            if (k == OpKind.InsertRun && sc.Unique) k = OpKind.InsertBatch;

            switch (k)
            {
                case OpKind.Insert:
                    ops.Add(new Op(k, [sc.Value(rnd)], doc++));
                    break;
                case OpKind.InsertBatch:
                {
                    var values = Enumerable.Range(0, rnd.Next(10, 80)).Select(_ => sc.Value(rnd)).ToArray();
                    ops.Add(new Op(k, values, doc));
                    doc += values.Length;
                    break;
                }
                case OpKind.InsertRun:
                {
                    var count = rnd.Next(20, 150);
                    ops.Add(new Op(k, [sc.Value(rnd)], doc, Count: count));
                    doc += count;
                    break;
                }
                case OpKind.Delete:
                    ops.Add(new Op(k, Position: rnd.Next(1_000_000)));
                    break;
                case OpKind.DeleteRun:
                    ops.Add(new Op(k, Count: rnd.Next(10, 150), Position: rnd.Next(1_000_000)));
                    break;
                case OpKind.DeleteAbsent:
                    ops.Add(new Op(k, [sc.Value(rnd)], AbsentDocs + rnd.Next(1_000_000)));
                    break;
                default:
                    ops.Add(new Op(k));
                    break;
            }
        }
        return ops;
    }

    private static int StableHash(string s)
    {
        unchecked
        {
            int h = (int)2166136261;
            foreach (var c in s) h = (h ^ c) * 16777619;
            return h & 0x7FFF_FFFF;
        }
    }

    // ── The model ─────────────────────────────────────────────────────────────

    private readonly record struct Entry(IndexKey Key, int Doc);

    /// <summary>Keys in the index's own order, each with its documents in insertion order.</summary>
    private sealed class Model
    {
        private readonly SortedDictionary<IndexKey, List<int>> _map = new();

        public int Count { get; private set; }

        public Model Clone()
        {
            var m = new Model { Count = Count };
            foreach (var (k, docs) in _map) m._map[k] = new List<int>(docs);
            return m;
        }

        public bool Contains(IndexKey key) => _map.ContainsKey(key);

        public bool Contains(Entry e) => _map.TryGetValue(e.Key, out var docs) && docs.Contains(e.Doc);

        public int CountOf(IndexKey key) => _map.TryGetValue(key, out var docs) ? docs.Count : 0;

        public void Add(IndexKey key, int doc)
        {
            if (!_map.TryGetValue(key, out var docs)) _map[key] = docs = new List<int>();
            docs.Add(doc);
            Count++;
        }

        public void Remove(Entry e)
        {
            var docs = _map[e.Key];
            docs.Remove(e.Doc);
            if (docs.Count == 0) _map.Remove(e.Key);
            Count--;
        }

        public IEnumerable<IndexKey> Keys => _map.Keys;

        public IEnumerable<Entry> Entries() => _map.SelectMany(kv => kv.Value.Select(d => new Entry(kv.Key, d)));

        public IEnumerable<Entry> EntriesOf(IndexKey key) => _map.TryGetValue(key, out var docs) ? docs.Select(d => new Entry(key, d)) : [];

        public int MaxRun => _map.Count == 0 ? 0 : _map.Values.Max(d => d.Count);
    }

    // ── Replay ────────────────────────────────────────────────────────────────

    private sealed record Failure(int Step, string Message);

    private sealed class RunResult
    {
        public Dictionary<string, Failure> Failures { get; } = new();
        public int MaxReachablePages { get; set; }
        public int PageDrops { get; set; }
        public int MaxRun { get; set; }
    }

    private sealed class WriteMismatchException : Exception
    {
        public WriteMismatchException(string message) : base(message) { }
    }

    private sealed class TreeBrokenException : Exception
    {
        public TreeBrokenException(string message) : base(message) { }
    }

    private const int ReplayTimeoutSeconds = 60;

    /// <summary>
    /// Replays <paramref name="ops"/> and returns the first failure of every check, or, with a
    /// <paramref name="focus"/>, computes only that check and stops at its first failure. A write
    /// that disagrees with the model, or a tree whose structure is broken, ends the replay: from
    /// there on the model no longer describes the index, and a traversal of a broken tree may not
    /// end. The replay runs on its own thread; one that has not finished after
    /// <see cref="ReplayTimeoutSeconds"/> is reported as a <c>Structure</c> failure at the step it
    /// was in and abandoned.
    /// </summary>
    private static RunResult Replay(Scenario sc, int seed, List<Op> ops, string? focus)
    {
        RunResult? result = null;
        Exception? error = null;
        var progress = new int[1];
        var thread = new Thread(() =>
        {
            try { result = ReplayCore(sc, seed, ops, focus, progress); }
            catch (Exception ex) { error = ex; }
        }, 16 * 1024 * 1024) { IsBackground = true, Name = $"btree-model {sc.Name} seed {seed}" };
        thread.Start();
        if (!thread.Join(TimeSpan.FromSeconds(ReplayTimeoutSeconds)))
        {
            var step = Volatile.Read(ref progress[0]);
            var hung = new RunResult();
            hung.Failures[Structure] = new Failure(step,
                $"the replay did not finish within {ReplayTimeoutSeconds} s, it was at step {step + 1} ({ops[Math.Min(step, ops.Count - 1)]}): a loop in the tree?");
            return hung;
        }
        if (error != null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(error);
        return result!;
    }

    private static RunResult ReplayCore(Scenario sc, int seed, List<Op> ops, string? focus, int[] progress)
    {
        var result = new RunResult();
        var path = Path.Combine(Path.GetTempPath(), $"btree_model_{Guid.NewGuid():N}.db");
        var config = PageFileConfig.Small with { PageSize = PageSize, AllowMultiProcessAccess = sc.MultiProcess };
        var options = sc.Unique ? IndexOptions.CreateUnique("k") : IndexOptions.CreateBTree("k");

        bool Want(string check) => focus == null ? !result.Failures.ContainsKey(check) : focus == check && !result.Failures.ContainsKey(check);
        void Report(string check, int step, string? message)
        {
            if (message != null) result.Failures.TryAdd(check, new Failure(step, message));
        }
        // The random draws of the check being run: its own generator, so that a replay of one check
        // alone (while shrinking) draws what it drew in the full run.
        Random p = null!;
        void Run(string check, int step, Func<string?> verify)
        {
            if (!Want(check)) return;
            p = new Random(seed * 1_000_003 + step * 131 + StableHash(check));
            string? message;
            try { message = verify(); }
            catch (Exception ex) { message = $"threw {ex.GetType().Name}: {ex.Message}"; }
            Report(check, step, message);
        }
        bool Done() => focus != null && result.Failures.ContainsKey(focus);

        var storage = new StorageEngine(path, config);
        try
        {
            // Pages in use before the index exists; every other page is the index's business.
            var before = ReadPages(storage, null);
            var baseline = Enumerable.Range(0, (int)before.Count).Select(p => (uint)p).Where(p => !before.Free.Contains(p)).ToHashSet();

            var index = new BTreeIndex(storage, options);
            var root = index.RootPageId;
            if (index.MaxKeySize != MaxKeyBytes)
                throw new InvalidOperationException($"BTreeIndex.MaxKeySize is {index.MaxKeySize} on {PageSize}-byte pages, the test assumes {MaxKeyBytes}: update MaxKeyBytes");

            Run(KeyOrder, -1, () => CheckKeyOrder(sc, seed, nan: false));
            if (sc.Doubles) Run(KeyOrderNaN, -1, () => CheckKeyOrder(sc, seed, nan: true));

            var committed = new Model();
            Model? pending = null;
            ulong txn = 0;
            PageState? atBegin = null;
            var rollbackLeaks = new HashSet<uint>();
            int lastReachable = -1;

            for (int step = 0; step < ops.Count && !Done(); step++)
            {
                var op = ops[step];
                Volatile.Write(ref progress[0], step);
                try
                {
                    Apply(op, step);
                }
                catch (WriteMismatchException ex)
                {
                    Report(Write, step, ex.Message);
                    break;
                }
                catch (TreeBrokenException ex)
                {
                    Report(Structure, step, ex.Message);
                    break;
                }
                catch (Exception ex)
                {
                    Report(Write, step, $"unexpected {ex.GetType().Name}: {ex.Message}");
                    break;
                }

                // Before any traversal by the engine: the tree as the transaction and as a reader see it.
                var broken = WalkTree(storage, root, txn).Error;
                if (broken == null && txn != 0) broken = WalkTree(storage, root, 0).Error is { } e ? "without the open transaction: " + e : null;
                if (broken != null)
                {
                    Report(Structure, step, broken);
                    break;
                }
                VerifyAnswers(step);
                result.MaxRun = Math.Max(result.MaxRun, (pending ?? committed).MaxRun);
            }
            return result;

            void EnsureTransaction()
            {
                if (txn != 0) return;
                atBegin = ReadPages(storage, index);
                txn = storage.BeginTransaction().TransactionId;
                pending = committed.Clone();
            }

            void Apply(Op op, int step)
            {
                switch (op.Kind)
                {
                    case OpKind.Insert:
                        EnsureTransaction();
                        Insert(op.Values![0], op.Doc, op);
                        break;
                    case OpKind.InsertBatch:
                        EnsureTransaction();
                        for (int i = 0; i < op.Values!.Length; i++) Insert(op.Values[i], op.Doc + i, op);
                        break;
                    case OpKind.InsertRun:
                        EnsureTransaction();
                        for (int i = 0; i < op.Count; i++) Insert(op.Values![0], op.Doc + i, op);
                        break;
                    case OpKind.Delete:
                    case OpKind.DeleteRun:
                    {
                        EnsureTransaction();
                        if (pending!.Count == 0) break;
                        var victims = pending.Entries().Skip(op.Position % pending.Count).Take(op.Kind == OpKind.Delete ? 1 : op.Count).ToList();
                        foreach (var v in victims)
                        {
                            var deleted = index.Delete(v.Key, Location(v.Doc), txn);
                            if (!deleted) throw new WriteMismatchException($"{op}: Delete({Show(sc, v.Key)}, #{v.Doc}) returned false for an entry the model has");
                            pending.Remove(v);
                        }
                        break;
                    }
                    case OpKind.DeleteAbsent:
                    {
                        EnsureTransaction();
                        var key = Key(op.Values![0]);
                        if (index.Delete(key, Location(op.Doc), txn))
                            throw new WriteMismatchException($"{op}: Delete returned true for a document that was never inserted");
                        break;
                    }
                    case OpKind.Commit:
                        if (txn == 0) break;
                        // Alternate the direct commit and the group commit path (multi-process mode has only the latter).
                        if (step % 2 == 0 && !sc.MultiProcess) storage.CommitTransactionAsync(txn).GetAwaiter().GetResult();
                        else storage.CommitTransactionAsync(txn, CancellationToken.None).GetAwaiter().GetResult();
                        committed = pending!;
                        pending = null;
                        txn = 0;
                        atBegin = null;
                        VerifyPages(step);
                        break;
                    case OpKind.Rollback:
                        if (txn == 0) break;
                        Rollback();
                        VerifyPages(step);
                        break;
                    case OpKind.Checkpoint:
                        storage.CheckpointAsync().GetAwaiter().GetResult();
                        if (txn == 0) VerifyPages(step);
                        break;
                    case OpKind.Reopen:
                    {
                        // An open transaction does not survive the engine: roll it back first.
                        if (txn != 0) Rollback();
                        storage.Dispose();
                        storage = null!;
                        storage = new StorageEngine(path, config);
                        index = new BTreeIndex(storage, options, root);
                        VerifyPages(step);
                        break;
                    }
                }
            }

            // Rolls back and records what the transaction took and nothing reclaims (#237): pages
            // appended to the file and pages taken off the free list, now neither reachable nor free.
            void Rollback()
            {
                storage.RollbackTransactionAsync(txn).GetAwaiter().GetResult();
                pending = null;
                txn = 0;
                if (atBegin == null) return;
                var now = ReadPages(storage, index);
                var reachable = now.Reachable.ToHashSet();
                var taken = Enumerable.Range((int)atBegin.Count, (int)(now.Count - atBegin.Count)).Select(x => (uint)x)
                    .Concat(atBegin.Free.Where(x => !now.Free.Contains(x)));
                foreach (var page in taken)
                    if (!reachable.Contains(page) && !now.Free.Contains(page))
                        rollbackLeaks.Add(page);
                atBegin = null;
            }

            void Insert(object value, int doc, Op op)
            {
                var key = Key(value);
                if (key.Data.Length > MaxKeyBytes)
                {
                    Expect<ArgumentException>(() => index.Insert(key, Location(doc), txn), $"{op}: Insert of a {key.Data.Length}-byte key (limit {MaxKeyBytes})");
                }
                else if (sc.Unique && pending!.Contains(key))
                {
                    Expect<InvalidOperationException>(() => index.Insert(key, Location(doc), txn), $"{op}: Insert({FormatValue(value)}, #{doc}) into a unique index that has the key");
                }
                else
                {
                    index.Insert(key, Location(doc), txn);
                    pending!.Add(key, doc);
                }
            }

            void Expect<TException>(Action write, string what) where TException : Exception
            {
                try
                {
                    write();
                }
                catch (Exception ex) when (ex.GetType() == typeof(TException)) { return; }
                catch (Exception ex) { throw new WriteMismatchException($"{what} threw {ex.GetType().Name} instead of {typeof(TException).Name}: {ex.Message}"); }
                throw new WriteMismatchException($"{what} should have thrown {typeof(TException).Name}");
            }

            void VerifyAnswers(int step)
            {
                var view = pending ?? committed;
                var all = view.Entries().ToList();
                var probeRandom = new Random(seed * 1_000_003 + step);
                var probes = Enumerable.Range(0, 6).Select(_ => ProbeKey(sc, probeRandom, all)).ToList();

                Run(RangeForward, step, () =>
                    Compare(sc, "Range(MinKey, MaxKey, Forward)", all, index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txn))
                    ?? Bounded((lo, hi, exp) => Compare(sc, $"Range({Show(sc, lo)}, {Show(sc, hi)}, Forward)", exp, index.Range(lo, hi, IndexDirection.Forward, txn))));
                Run(RangeBackward, step, () =>
                    Compare(sc, "Range(MinKey, MaxKey, Backward)", Reversed(all), index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Backward, txn))
                    ?? Bounded((lo, hi, exp) => Compare(sc, $"Range({Show(sc, lo)}, {Show(sc, hi)}, Backward)", Reversed(exp), index.Range(lo, hi, IndexDirection.Backward, txn))));
                Run(RangeAsync, step, () =>
                    Compare(sc, "RangeAsync(MinKey, MaxKey, Forward)", all, index.RangeAsync(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txn).ToBlockingEnumerable())
                    ?? Compare(sc, "RangeAsync(MinKey, MaxKey, Backward)", Reversed(all), index.RangeAsync(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Backward, txn).ToBlockingEnumerable())
                    ?? probes.Select(k =>
                    {
                        var (found, location) = index.TryFindAsync(k, txn).AsTask().GetAwaiter().GetResult();
                        return CheckFound($"TryFindAsync({Show(sc, k)})", view, k, found, location);
                    }).FirstOrDefault(m => m != null));
                Run(CountRange, step, () =>
                {
                    var count = index.CountRange(IndexKey.MinKey, IndexKey.MaxKey, txn);
                    if (count != all.Count) return $"CountRange(MinKey, MaxKey) returned {count}, the model has {all.Count} entries";
                    return Bounded((lo, hi, exp) =>
                    {
                        var c = index.CountRange(lo, hi, txn);
                        return c == exp.Count ? null : $"CountRange({Show(sc, lo)}, {Show(sc, hi)}) returned {c}, the model has {exp.Count} entries";
                    });
                });
                Run(TryFind, step, () => probes.Select(k =>
                {
                    var found = index.TryFind(k, out var location, txn);
                    return CheckFound($"TryFind({Show(sc, k)})", view, k, found, location);
                }).FirstOrDefault(m => m != null));
                Run(Equal, step, () => probes.Select(k =>
                    Compare(sc, $"Equal({Show(sc, k)})", view.EntriesOf(k).ToList(), index.Equal(k, txn))).FirstOrDefault(m => m != null));
                Run(In, step, () =>
                {
                    var keys = probes.Take(1 + p.Next(4)).Distinct().ToList();
                    var expected = all.Where(e => keys.Contains(e.Key)).ToList();
                    return Compare(sc, $"In({string.Join(", ", keys.Select(k => Show(sc, k)))})", expected, index.In(keys, txn));
                });
                Run(Between, step, () =>
                {
                    for (int i = 0; i + 1 < probes.Count; i += 2)
                    {
                        var (a, b) = probes[i] <= probes[i + 1] ? (probes[i], probes[i + 1]) : (probes[i + 1], probes[i]);
                        bool si = p.Next(2) == 0, ei = p.Next(2) == 0;
                        var expected = all.Where(e => (si ? e.Key >= a : e.Key > a) && (ei ? e.Key <= b : e.Key < b)).ToList();
                        var m = Compare(sc, $"Between({Show(sc, a)}, {Show(sc, b)}, startInclusive: {si}, endInclusive: {ei})", expected, index.Between(a, b, si, ei, txn));
                        if (m != null) return m;
                    }
                    return null;
                });
                Run(GreaterThan, step, () => probes.Take(3).Select(k =>
                {
                    bool orEqual = p.Next(2) == 0;
                    var expected = all.Where(e => orEqual ? e.Key >= k : e.Key > k).ToList();
                    return Compare(sc, $"GreaterThan({Show(sc, k)}, orEqual: {orEqual})", expected, index.GreaterThan(k, orEqual, txn));
                }).FirstOrDefault(m => m != null));
                Run(LessThan, step, () => probes.Take(3).Select(k =>
                {
                    bool orEqual = p.Next(2) == 0;
                    var expected = Reversed(all.Where(e => orEqual ? e.Key <= k : e.Key < k).ToList());
                    return Compare(sc, $"LessThan({Show(sc, k)}, orEqual: {orEqual})", expected, index.LessThan(k, orEqual, txn));
                }).FirstOrDefault(m => m != null));
                if (sc.Strings)
                {
                    Run(StartsWith, step, () => probes.Take(3).Select(k =>
                    {
                        var s = k.As<string>();
                        var prefix = s[..p.Next(s.Length + 1)];
                        var expected = all.Where(e => e.Key.As<string>().StartsWith(prefix, StringComparison.Ordinal)).ToList();
                        return Compare(sc, $"StartsWith({Quote(prefix)})", expected, index.StartsWith(prefix, txn));
                    }).FirstOrDefault(m => m != null));
                    Run(Like, step, () =>
                    {
                        var pattern = LikePattern(probes[0].As<string>(), p);
                        var regex = new Regex("^" + Regex.Escape(pattern).Replace("%", ".*").Replace("_", ".") + "$", RegexOptions.Singleline);
                        var expected = all.Where(e => regex.IsMatch(e.Key.As<string>())).ToList();
                        return Compare(sc, $"Like({Quote(pattern)})", expected, index.Like(pattern, txn));
                    });
                }
                // The async twins of the primitives, each against the same model answer.
                Run(EqualAsync, step, () => probes.Select(k =>
                    Compare(sc, $"EqualAsync({Show(sc, k)})", view.EntriesOf(k).ToList(), index.EqualAsync(k, txn).ToBlockingEnumerable())).FirstOrDefault(m => m != null));
                Run(InAsync, step, () =>
                {
                    var keys = probes.Take(1 + p.Next(4)).Distinct().ToList();
                    return Compare(sc, $"InAsync({string.Join(", ", keys.Select(k => Show(sc, k)))})", all.Where(e => keys.Contains(e.Key)).ToList(), index.InAsync(keys, txn).ToBlockingEnumerable());
                });
                Run(BetweenAsync, step, () =>
                {
                    var (a, b) = probes[0] <= probes[1] ? (probes[0], probes[1]) : (probes[1], probes[0]);
                    bool si = p.Next(2) == 0, ei = p.Next(2) == 0;
                    var expected = all.Where(e => (si ? e.Key >= a : e.Key > a) && (ei ? e.Key <= b : e.Key < b)).ToList();
                    return Compare(sc, $"BetweenAsync({Show(sc, a)}, {Show(sc, b)}, startInclusive: {si}, endInclusive: {ei})", expected, index.BetweenAsync(a, b, si, ei, txn).ToBlockingEnumerable());
                });
                Run(GreaterThanAsync, step, () => probes.Take(3).Select(k =>
                {
                    bool orEqual = p.Next(2) == 0;
                    return Compare(sc, $"GreaterThanAsync({Show(sc, k)}, orEqual: {orEqual})", all.Where(e => orEqual ? e.Key >= k : e.Key > k).ToList(), index.GreaterThanAsync(k, orEqual, txn).ToBlockingEnumerable());
                }).FirstOrDefault(m => m != null));
                Run(LessThanAsync, step, () => probes.Take(3).Select(k =>
                {
                    bool orEqual = p.Next(2) == 0;
                    return Compare(sc, $"LessThanAsync({Show(sc, k)}, orEqual: {orEqual})", Reversed(all.Where(e => orEqual ? e.Key <= k : e.Key < k).ToList()), index.LessThanAsync(k, orEqual, txn).ToBlockingEnumerable());
                }).FirstOrDefault(m => m != null));
                if (sc.Strings)
                {
                    Run(StartsWithAsync, step, () => probes.Take(3).Select(k =>
                    {
                        var s = k.As<string>();
                        var prefix = s[..p.Next(s.Length + 1)];
                        var expected = all.Where(e => e.Key.As<string>().StartsWith(prefix, StringComparison.Ordinal)).ToList();
                        return Compare(sc, $"StartsWithAsync({Quote(prefix)})", expected, index.StartsWithAsync(prefix, txn).ToBlockingEnumerable());
                    }).FirstOrDefault(m => m != null));
                    Run(LikeAsync, step, () =>
                    {
                        var pattern = LikePattern(probes[0].As<string>(), p);
                        var regex = new Regex("^" + Regex.Escape(pattern).Replace("%", ".*").Replace("_", ".") + "$", RegexOptions.Singleline);
                        var expected = all.Where(e => regex.IsMatch(e.Key.As<string>())).ToList();
                        return Compare(sc, $"LikeAsync({Quote(pattern)})", expected, index.LikeAsync(pattern, txn).ToBlockingEnumerable());
                    });
                }
                Run(CursorForward, step, () => Compare(sc, "cursor MoveToFirst + MoveNext", all, Walk(index, txn, forward: true)));
                Run(CursorBackward, step, () => Compare(sc, "cursor MoveToLast + MovePrev", Reversed(all), Walk(index, txn, forward: false)));
                Run(CursorWindow, step, () =>
                    Window(index, txn, view, all, probes[0], p.Next(0, 80), p.Next(1, 40), sc)
                    ?? Window(index, txn, view, all, null, p.Next(0, 80), p.Next(1, 40), sc));
                Run(TiesOrder, step, () =>
                    Compare(sc, "Range(MinKey, MaxKey, Forward)", all, index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txn), exactTies: true)
                    ?? Compare(sc, "Range(MinKey, MaxKey, Backward)", Reversed(all), index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Backward, txn), exactTies: true));
                if (txn != 0)
                {
                    var committedAll = committed.Entries().ToList();
                    Run(Isolation, step, () =>
                        Compare(sc, "Range(MinKey, MaxKey) without the open transaction", committedAll, index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, 0))
                        ?? probes.Select(k =>
                        {
                            var found = index.TryFind(k, out var location, 0);
                            return CheckFound($"TryFind({Show(sc, k)}) without the open transaction", committed, k, found, location);
                        }).FirstOrDefault(m => m != null));
                }

                // Two bounded ranges and one with its bounds swapped (empty), over the probes.
                string? Bounded(Func<IndexKey, IndexKey, List<Entry>, string?> check)
                {
                    var (lo, hi) = probes[0] <= probes[1] ? (probes[0], probes[1]) : (probes[1], probes[0]);
                    var (lo2, hi2) = probes[2] <= probes[3] ? (probes[2], probes[3]) : (probes[3], probes[2]);
                    return check(lo, hi, all.Where(e => e.Key >= lo && e.Key <= hi).ToList())
                        ?? check(lo2, hi2, all.Where(e => e.Key >= lo2 && e.Key <= hi2).ToList())
                        ?? (hi > lo ? check(hi, lo, []) : null);
                }
            }

            void VerifyPages(int step)
            {
                if (!(Want(PagesReachable) || Want(PagesFreeList) || Want(PagesLeaked) || Want(PagesLeakedByRollback)) && focus != null) return;
                var now = ReadPages(storage, index);
                var reachable = now.Reachable.ToHashSet();

                Run(PagesReachable, step, () =>
                {
                    var twice = now.Reachable.GroupBy(x => x).FirstOrDefault(g => g.Count() > 1);
                    if (twice != null) return $"page {twice.Key} is reachable from the root more than once";
                    foreach (var page in now.Reachable)
                    {
                        if (page >= now.Count) return $"page {page} is reachable from the root but lies past the end of the file ({now.Count} pages)";
                        if (now.FreeMarked.Contains(page)) return $"page {page} is reachable from the root and on the free list";
                        if (now.Pending.Contains(page)) return $"page {page} is reachable from the root and queued to be freed";
                        if (baseline.Contains(page)) return $"page {page} is reachable from the root but was in use before the index was created";
                    }
                    return null;
                });
                Run(PagesFreeList, step, () =>
                {
                    if (now.ChainError != null) return now.ChainError;
                    var unlisted = now.FreeMarked.Except(now.Chain).OrderBy(x => x).ToList();
                    if (unlisted.Count > 0) return $"{unlisted.Count} pages are marked free but not on the free list: {Pages(unlisted)}";
                    var both = now.Pending.Intersect(now.FreeMarked).OrderBy(x => x).ToList();
                    if (both.Count > 0) return $"{both.Count} pages are on the free list and queued to be freed again: {Pages(both)}";
                    return null;
                });
                Run(PagesLeaked, step, () =>
                {
                    var leaked = Enumerable.Range(1, (int)now.Count - 1).Select(x => (uint)x)
                        .Where(x => !baseline.Contains(x) && !reachable.Contains(x) && !now.Free.Contains(x) && !rollbackLeaks.Contains(x))
                        .ToList();
                    return leaked.Count == 0 ? null
                        : $"{leaked.Count} of the {now.Count} pages are neither reachable from the root nor free ({now.Reachable.Count} reachable, {now.Free.Count} free): {Pages(leaked)}";
                });
                Run(PagesLeakedByRollback, step, () => rollbackLeaks.Count == 0 ? null
                    : $"{rollbackLeaks.Count} pages allocated by rolled-back transactions are neither reachable from the root nor free: {Pages(rollbackLeaks.OrderBy(x => x).ToList())}");

                if (lastReachable >= 0 && now.Reachable.Count < lastReachable) result.PageDrops++;
                lastReachable = now.Reachable.Count;
                result.MaxReachablePages = Math.Max(result.MaxReachablePages, now.Reachable.Count);
            }
        }
        finally
        {
            try { storage?.Dispose(); } catch { /* the failure is already recorded */ }
            DeleteDatabase(path);
        }
    }

    private static string Pages(IReadOnlyCollection<uint> pages) =>
        string.Join(",", pages.Take(12)) + (pages.Count > 12 ? $",... ({pages.Count} in all)" : "");

    // ── Checks against the model ──────────────────────────────────────────────

    private static List<Entry> Reversed(List<Entry> entries)
    {
        var copy = new List<Entry>(entries);
        copy.Reverse();
        return copy;
    }

    /// <summary>
    /// The engine's answer must have the model's keys in the model's order and, for each run of
    /// equal keys, the model's documents (in any order, or in the model's order with
    /// <paramref name="exactTies"/>). Reads at most a little past the expected length, so a cycle in
    /// the leaf chain fails instead of hanging.
    /// </summary>
    private static string? Compare(Scenario sc, string what, IReadOnlyList<Entry> expected, IEnumerable<IndexEntry> actual, bool exactTies = false)
    {
        var got = new List<Entry>();
        foreach (var e in actual)
        {
            got.Add(new Entry(e.Key, DocOf(e.Location)));
            if (got.Count > expected.Count + 64) break;
        }

        int n = Math.Min(got.Count, expected.Count);
        for (int i = 0; i < n; i++)
            if (got[i].Key != expected[i].Key)
                return $"{what}: entry {i} has key {Show(sc, got[i].Key)}, the model has {Show(sc, expected[i].Key)} ({Count(got.Count, expected.Count)})";
        if (got.Count != expected.Count)
            return got.Count > expected.Count
                ? $"{what}: {Count(got.Count, expected.Count)}; the first extra entry is {Show(sc, got[n].Key)} #{got[n].Doc}"
                : $"{what}: {Count(got.Count, expected.Count)}; the first missing entry is {Show(sc, expected[n].Key)} #{expected[n].Doc}";

        for (int i = 0; i < n;)
        {
            int j = i;
            while (j < n && expected[j].Key == expected[i].Key) j++;
            var gotDocs = got.Skip(i).Take(j - i).Select(e => e.Doc).ToList();
            var expectedDocs = expected.Skip(i).Take(j - i).Select(e => e.Doc).ToList();
            if (exactTies ? !gotDocs.SequenceEqual(expectedDocs) : !gotDocs.OrderBy(d => d).SequenceEqual(expectedDocs.OrderBy(d => d)))
                return $"{what}: key {Show(sc, expected[i].Key)} at entries {i}..{j - 1} has documents [{Docs(gotDocs)}], the model has [{Docs(expectedDocs)}]"
                       + (exactTies ? " (equal keys in insertion order)" : "");
            i = j;
        }
        return null;

        static string Count(int got, int expected) => $"returned {got}{(got > expected + 64 ? "+" : "")} entries, the model has {expected}";
        static string Docs(List<int> docs) => docs.Count <= 16 ? string.Join(",", docs.Select(d => "#" + d)) : string.Join(",", docs.Take(16).Select(d => "#" + d)) + $",... ({docs.Count})";
    }

    private static string? CheckFound(string what, Model view, IndexKey key, bool found, DocumentLocation location)
    {
        if (found != view.Contains(key))
            return $"{what} returned {found}, the model has {view.CountOf(key)} entries with that key";
        if (found && !view.Contains(new Entry(key, DocOf(location))))
            return $"{what} returned document #{DocOf(location)}, which the model does not have under that key";
        return null;
    }

    private static IEnumerable<IndexEntry> Walk(BTreeIndex index, ulong txn, bool forward)
    {
        using var cursor = index.CreateCursor(txn);
        if (!(forward ? cursor.MoveToFirst() : cursor.MoveToLast())) yield break;
        do yield return cursor.Current;
        while (forward ? cursor.MoveNext() : cursor.MovePrev());
    }

    /// <summary>
    /// A cursor window: <see cref="IBTreeCursor.Seek"/> to <paramref name="from"/> (or
    /// <see cref="IBTreeCursor.MoveToLast"/> when null, walking backwards), skip, take. The window
    /// must hold the keys the model has at those positions, as distinct entries of the model.
    /// </summary>
    private static string? Window(BTreeIndex index, ulong txn, Model view, List<Entry> all, IndexKey? from, int skip, int take, Scenario sc)
    {
        using var cursor = index.CreateCursor(txn);
        List<Entry> answer;
        string what;
        bool valid;
        if (from is { } key)
        {
            answer = all.Where(e => e.Key >= key).ToList();
            what = $"Seek({Show(sc, key)}), skip {skip}, take {take}";
            var found = cursor.Seek(key);
            if (found != view.Contains(key))
                return $"Seek({Show(sc, key)}) returned {found}, the model has {view.CountOf(key)} entries with that key";
            valid = IsValid(cursor);
            if (valid != answer.Count > 0)
                return valid
                    ? $"Seek({Show(sc, key)}) left the cursor on {Show(sc, cursor.Current.Key)}, the model has no key from there on"
                    : $"Seek({Show(sc, key)}) left the cursor invalid, the model has {answer.Count} entries from that key on, starting at {Show(sc, answer[0].Key)}";
        }
        else
        {
            answer = Reversed(all);
            what = $"MoveToLast, skip {skip} back, take {take} back";
            valid = cursor.MoveToLast();
            if (valid != answer.Count > 0)
                return $"MoveToLast returned {valid}, the model has {answer.Count} entries";
        }

        for (int i = 0; i < skip && valid; i++) valid = from != null ? cursor.MoveNext() : cursor.MovePrev();
        var window = new List<Entry>();
        while (valid && window.Count < take)
        {
            var c = cursor.Current;
            window.Add(new Entry(c.Key, DocOf(c.Location)));
            valid = from != null ? cursor.MoveNext() : cursor.MovePrev();
        }

        var slice = answer.Skip(skip).Take(take).ToList();
        if (!window.Select(e => e.Key).SequenceEqual(slice.Select(e => e.Key)))
            return $"{what}: returned keys [{string.Join(", ", window.Select(e => Show(sc, e.Key)))}], the model has [{string.Join(", ", slice.Select(e => Show(sc, e.Key)))}]";
        var dup = window.GroupBy(e => e).FirstOrDefault(g => g.Count() > 1);
        if (dup != null) return $"{what}: returned {Show(sc, dup.Key.Key)} #{dup.Key.Doc} twice";
        var foreign = window.Where(e => !view.Contains(e)).Take(1).ToList();
        if (foreign.Count > 0) return $"{what}: returned {Show(sc, foreign[0].Key)} #{foreign[0].Doc}, which the model does not have";
        return null;

        static bool IsValid(IBTreeCursor c)
        {
            try { _ = c.Current; return true; }
            catch (InvalidOperationException) { return false; }
        }
    }

    /// <summary>
    /// The key encoding must order and equate values as their natural comparison does: pairs
    /// without NaN, or (<paramref name="nan"/>) pairs with a NaN, which <see cref="double.CompareTo(double)"/>
    /// puts below every other value and equal to every other NaN, whatever its bits.
    /// </summary>
    private static string? CheckKeyOrder(Scenario sc, int seed, bool nan)
    {
        var r = new Random(seed);
        var values = sc.Specials.Concat(Enumerable.Range(0, 150).Select(_ => sc.Value(r)))
            .Where(v => v is not string s || Encoding.UTF8.GetByteCount(s) < MaxKeyBytes).ToList();
        foreach (var a in values)
            foreach (var b in values)
            {
                if ((a is double.NaN || b is double.NaN) != nan) continue;
                int natural = Math.Sign(a is string sa ? string.CompareOrdinal(sa, (string)b) : ((IComparable)a).CompareTo(b));
                int encoded = Math.Sign(Key(a).CompareTo(Key(b)));
                if (natural != encoded)
                    return $"{FormatValue(a)} {Sign(natural)} {FormatValue(b)}, but their index keys compare {Sign(encoded)}";
            }
        return null;

        static string Sign(int s) => s < 0 ? "<" : s > 0 ? ">" : "==";
    }

    private static IndexKey ProbeKey(Scenario sc, Random p, List<Entry> all)
    {
        int roll = p.Next(100);
        if (roll < 40 && all.Count > 0)
        {
            var key = all[p.Next(all.Count)].Key;
            // A prefix of an existing string key: it bounds, but is not, the longer ones (#206).
            if (sc.Strings && roll < 15)
            {
                var s = key.As<string>();
                return Key(s[..p.Next(s.Length + 1)]);
            }
            return key;
        }
        if (roll < 60) return Key(sc.Specials[p.Next(sc.Specials.Length)]);
        var v = sc.Value(p);
        return v is string str && Encoding.UTF8.GetByteCount(str) >= MaxKeyBytes ? Key(str[..(MaxKeyBytes / 2)]) : Key(v);
    }

    private static string LikePattern(string s, Random p) => p.Next(4) switch
    {
        0 => s[..p.Next(s.Length + 1)] + "%",
        1 => "%" + s[p.Next(s.Length + 1)..],
        2 when s.Length > 0 => ReplaceAt(s[..p.Next(1, s.Length + 1)], p) + "%",
        _ => s,
    };

    private static string ReplaceAt(string s, Random p)
    {
        int at = p.Next(s.Length);
        return s[..at] + "_" + s[(at + 1)..];
    }

    // ── Page accounting ───────────────────────────────────────────────────────

    /// <summary>The engine's view of its pages: what is free, how the free list links, and what the index reaches.</summary>
    private sealed record PageState(uint Count, HashSet<uint> Pending, HashSet<uint> FreeMarked, List<uint> Chain, string? ChainError, List<uint> Reachable)
    {
        public HashSet<uint> Free { get; } = Pending.Union(FreeMarked).ToHashSet();
    }

    private static PageState ReadPages(StorageEngine storage, BTreeIndex? index)
    {
        // A free request past the threshold starts a checkpoint on the thread pool; let it finish.
        storage.PendingFreeCheckpointCompletion.GetAwaiter().GetResult();

        var count = storage.PageCount;
        // Single-file mode: every page except the frees queued until the next WAL truncation.
        var listed = storage.GetCollectionPageIds("btree_model").ToHashSet();
        var pending = Enumerable.Range(0, (int)count).Select(p => (uint)p).Where(p => !listed.Contains(p)).ToHashSet();

        var header = new byte[32];
        var freeMarked = new HashSet<uint>();
        for (uint page = 1; page < count; page++)
        {
            storage.ReadPageHeader(page, null, header);
            if (PageHeader.ReadFrom(header).PageType == PageType.Free) freeMarked.Add(page);
        }

        var chain = new List<uint>();
        string? chainError = null;
        storage.ReadPageHeader(0, null, header);
        var seen = new HashSet<uint>();
        for (var next = PageHeader.ReadFrom(header).NextPageId; next != 0;)
        {
            if (next >= count) { chainError = $"the free list points past the end of the file ({count} pages) at page {next}"; break; }
            if (!seen.Add(next)) { chainError = $"the free list loops back to page {next} after {chain.Count} pages"; break; }
            chain.Add(next);
            storage.ReadPageHeader(next, null, header);
            var h = PageHeader.ReadFrom(header);
            if (h.PageType != PageType.Free) { chainError = $"page {next}, on the free list after {chain.Count - 1} pages, has type {h.PageType}"; break; }
            next = h.NextPageId;
        }

        // CollectAllPages recurses without a guard, so it runs only on a tree the walk found sound.
        var reachable = new List<uint>();
        if (index != null)
        {
            var walk = WalkTree(storage, index.RootPageId, 0);
            if (walk.Error != null) throw new TreeBrokenException(walk.Error);
            reachable = index.CollectAllPages().ToList();
            if (!reachable.Order().SequenceEqual(walk.Pages.Order()))
                throw new TreeBrokenException($"CollectAllPages returned {reachable.Count} pages, the walk from the root reaches {walk.Pages.Count}");
        }
        return new PageState(count, pending, freeMarked, chain, chainError, reachable);
    }

    private sealed record TreeWalk(List<uint> Pages, string? Error);

    /// <summary>
    /// Walks the tree from the root as <paramref name="txn"/> sees it, without the engine's own
    /// traversals and with a guard against every loop: no page is reached twice, child pointers stay
    /// inside the file, every node is an index page whose entries fit in it in key order, every
    /// leaf is at the same depth, and the leaf chain, followed forwards with its back pointers, holds
    /// exactly the leaves in tree order.
    /// </summary>
    private static TreeWalk WalkTree(StorageEngine storage, uint root, ulong txn)
    {
        var pageSize = storage.PageSize;
        var count = storage.PageCount;
        var page = new byte[pageSize];
        var pages = new List<uint>();
        var leaves = new List<uint>();
        var seen = new HashSet<uint>();
        int leafDepth = -1;

        string? Visit(uint id, int depth)
        {
            if (id == 0 || id >= count) return $"a child pointer leads to page {id}, outside the file ({count} pages)";
            if (!seen.Add(id)) return $"page {id} is reached twice from the root";
            if (depth > 32) return $"the tree is deeper than 32 levels at page {id}";
            pages.Add(id);
            storage.ReadPage(id, txn, page);
            var type = PageHeader.ReadFrom(page).PageType;
            if (type != PageType.Index) return $"page {id}, reached from the root, has type {type}";
            var node = BTreeNodeHeader.ReadFrom(page.AsSpan(32));

            int offset = 32 + 20;
            var children = new List<uint>();
            if (!node.IsLeaf)
            {
                if (offset + 4 > pageSize) return $"internal node {id} has no room for its first child pointer";
                children.Add(BitConverter.ToUInt32(page, offset));
                offset += 4;
            }
            byte[]? previous = null;
            for (int i = 0; i < node.EntryCount; i++)
            {
                int tail = node.IsLeaf ? DocumentLocation.SerializedSize : 4;
                if (offset + 4 > pageSize) return $"node {id}: entry {i} of {node.EntryCount} starts past the end of the page";
                int length = BitConverter.ToInt32(page, offset);
                if (length < 0 || offset + 4 + length + tail > pageSize) return $"node {id}: entry {i} of {node.EntryCount} ({length}-byte key) runs past the end of the page";
                var key = page.AsSpan(offset + 4, length).ToArray();
                if (previous != null && IndexKey.CompareRaw(previous, key) > 0) return $"node {id}: entry {i} is smaller than entry {i - 1}";
                previous = key;
                if (!node.IsLeaf) children.Add(BitConverter.ToUInt32(page, offset + 4 + length));
                offset += 4 + length + tail;
            }

            if (node.IsLeaf)
            {
                if (leafDepth < 0) leafDepth = depth;
                else if (depth != leafDepth) return $"leaf {id} is at depth {depth}, other leaves at {leafDepth}";
                leaves.Add(id);
                return null;
            }
            foreach (var child in children)
                if (Visit(child, depth + 1) is { } error) return error;
            return null;
        }

        var failure = Visit(root, 0);
        if (failure == null)
        {
            uint current = leaves[0], back = 0;
            for (int i = 0; i < leaves.Count && failure == null; i++)
            {
                if (current != leaves[i])
                {
                    failure = $"the leaf chain goes from {back} to {current}, the next leaf of the tree is {leaves[i]}";
                    break;
                }
                storage.ReadPage(current, txn, page);
                var node = BTreeNodeHeader.ReadFrom(page.AsSpan(32));
                if (node.PrevLeafPageId != back) failure = $"leaf {current} points back to {node.PrevLeafPageId}, the previous leaf is {back}";
                back = current;
                current = node.NextLeafPageId;
            }
            if (failure == null && current != 0) failure = $"the last leaf {leaves[^1]} links on to page {current}";
        }
        return new TreeWalk(pages, failure);
    }

    // ── Shrinking ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Shrinks by removing chunks of steps (half the sequence, then quarters, ... down to single
    /// steps), keeping a removal if the sequence still fails the same check, until a pass over
    /// single steps removes nothing; then makes single steps smaller (fewer values in a batch, a
    /// shorter run) the same way, and starts over while that changed something, until the time is
    /// up. The last step is never removed. The step removal is the algorithm of
    /// <see cref="StoreModelTests"/>, for one check.
    /// </summary>
    private static (List<Op> Ops, Failure Failure) Shrink(Scenario sc, int seed, List<Op> ops, string check, TimeSpan budget)
    {
        var sw = Stopwatch.StartNew();
        var current = ops;
        if (!Replay(sc, seed, current, check).Failures.TryGetValue(check, out var failure))
            return (current, new Failure(current.Count - 1, "(did not fail again when replayed alone)"));

        bool StillFails(List<Op> candidate)
        {
            if (!Replay(sc, seed, candidate, check).Failures.TryGetValue(check, out var f)) return false;
            current = candidate;
            failure = f;
            return true;
        }

        while (sw.Elapsed < budget)
        {
            int chunk = Math.Max(1, (current.Count - 1) / 2);
            while (sw.Elapsed < budget)
            {
                bool changed = false;
                for (int start = current.Count - 1 - chunk; start >= 0 && sw.Elapsed < budget; start -= chunk)
                    changed |= StillFails(current.Where((_, j) => j < start || j >= start + chunk).ToList());
                if (chunk == 1 && !changed) break;
                if (!changed) chunk = Math.Max(1, chunk / 2);
            }

            bool reduced = false;
            for (int i = current.Count - 1; i >= 0 && sw.Elapsed < budget; i--)
                foreach (var smaller in Smaller(current[i]))
                {
                    if (sw.Elapsed >= budget) break;
                    var candidate = current.ToList();
                    candidate[i] = smaller;
                    if (StillFails(candidate)) { reduced = true; break; }
                }
            if (!reduced) break;
        }
        return (current, failure);
    }

    /// <summary>Smaller versions of one step: a batch without a chunk of its values (halves first), a run of half or one less.</summary>
    private static IEnumerable<Op> Smaller(Op op)
    {
        switch (op.Kind)
        {
            case OpKind.InsertBatch when op.Values!.Length > 1:
                for (int chunk = op.Values.Length / 2; chunk >= 1; chunk /= 2)
                    for (int start = 0; start + chunk <= op.Values.Length; start += chunk)
                    {
                        int from = start, size = chunk;
                        yield return op with { Values = op.Values.Where((_, j) => j < from || j >= from + size).ToArray() };
                    }
                break;
            case OpKind.InsertRun or OpKind.DeleteRun when op.Count > 1:
                yield return op with { Count = op.Count / 2 };
                yield return op with { Count = op.Count - 1 };
                break;
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static DocumentLocation Location(int doc) => new((uint)(1 + doc / 60000), (ushort)(doc % 60000));

    private static int DocOf(DocumentLocation l) => (int)(l.PageId - 1) * 60000 + l.SlotIndex;

    private static IndexKey Key(object value) => value switch
    {
        int i => IndexKey.Create(i),
        long l => IndexKey.Create(l),
        double d => IndexKey.Create(d),
        string s => IndexKey.Create(s),
        _ => throw new NotSupportedException(value.GetType().Name),
    };

    private static string Show(Scenario sc, IndexKey key)
    {
        if (key == IndexKey.MinKey) return "MinKey";
        if (key == IndexKey.MaxKey) return "MaxKey";
        return sc.Specials[0] switch
        {
            int => FormatValue(key.As<int>()),
            long => FormatValue(key.As<long>()),
            double => FormatValue(key.As<double>()),
            _ => FormatValue(key.As<string>()),
        };
    }

    private static string FormatValue(object value) => value switch
    {
        string s => Quote(s),
        double d when d == 0 && double.IsNegative(d) => "-0.0",
        double d when double.IsNaN(d) => $"NaN(0x{BitConverter.DoubleToInt64Bits(d):X16})",
        double d => d.ToString("R", System.Globalization.CultureInfo.InvariantCulture),
        long l => l + "L",
        _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)!,
    };

    /// <summary>Quotes a string with control characters escaped and runs of 8 or more equal characters as <c>{c*n}</c>.</summary>
    private static string Quote(string s)
    {
        var sb = new StringBuilder("\"");
        for (int i = 0; i < s.Length;)
        {
            int j = i;
            while (j < s.Length && s[j] == s[i]) j++;
            var c = s[i] < 0x20 ? $"\\u{(int)s[i]:X4}" : s[i] == '"' ? "\\\"" : s[i].ToString();
            if (j - i >= 8) sb.Append('{').Append(c).Append('*').Append(j - i).Append('}');
            else for (int k = i; k < j; k++) sb.Append(c);
            i = j;
        }
        return sb.Append('"').ToString();
    }

    private static void DeleteDatabase(string path)
    {
        foreach (var f in new[] { path, Path.ChangeExtension(path, ".wal"), path + "-shm", Path.ChangeExtension(path, ".wal-shm"), Path.ChangeExtension(path, ".manifest.json") })
            try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
    }
}
