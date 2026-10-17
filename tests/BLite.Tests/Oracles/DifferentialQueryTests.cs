using System.Linq.Expressions;
using System.Text;
using BLite.Bson;
using BLite.Core.Query;
using BLite.Shared;
using Xunit.Abstractions;

namespace BLite.Tests.Oracles;

/// <summary>
/// Differential test of the LINQ provider: every query in the catalogue runs over the same
/// documents four ways on BLite (a collection with a B-Tree index on every queried property and
/// one without, each sync and async) and once with LINQ-to-Objects over the in-memory list, which
/// is the oracle. The results must agree: as sets (by id) for unordered queries, as key sequences
/// for ordered ones, as values (or the exception type) for terminals.
/// </summary>
/// <remarks>
/// The documents are generated from a fixed seed (override with <c>BLITE_DIFF_SEED</c>) with many
/// ties, boundary values (int/long extremes, -0.0, 5m next to 5.00m, "p7" next to "p70"), nulls and
/// duplicate names, so that each predicate shape has documents on both sides of its bound.
/// </remarks>
public class DifferentialQueryTests : IClassFixture<DifferentialQueryTests.Fixture>
{
    private readonly Fixture _f;
    private readonly ITestOutputHelper _output;

    public DifferentialQueryTests(Fixture fixture, ITestOutputHelper output) => (_f, _output) = (fixture, output);

    // ── Fixture ───────────────────────────────────────────────────────────────

    public sealed class Fixture : IDisposable
    {
        public const int DefaultSeed = 20261016;
        public const int DocumentCount = 400;

        public int Seed { get; }
        public List<DiffDoc> Docs { get; }
        public DiffIndexedDbContext Indexed { get; }
        public DiffPlainDbContext Plain { get; }

        private readonly string _indexedPath;
        private readonly string _plainPath;

        public Fixture()
        {
            Seed = int.TryParse(Environment.GetEnvironmentVariable("BLITE_DIFF_SEED"), out var s) ? s : DefaultSeed;
            Docs = Generate(Seed, DocumentCount);

            var id = Guid.NewGuid().ToString("N");
            _indexedPath = Path.Combine(Path.GetTempPath(), $"diff_query_indexed_{id}.db");
            _plainPath = Path.Combine(Path.GetTempPath(), $"diff_query_plain_{id}.db");
            Indexed = new DiffIndexedDbContext(_indexedPath);
            Plain = new DiffPlainDbContext(_plainPath);

            // Insert in a shuffled order so that no index order is the scan order.
            var rnd = new Random(Seed ^ 0x5bd1e995);
            var shuffled = Docs.OrderBy(_ => rnd.Next()).ToList();
            Indexed.Docs.InsertBulkAsync(shuffled).GetAwaiter().GetResult();
            Plain.Docs.InsertBulkAsync(shuffled).GetAwaiter().GetResult();
        }

        public void Dispose()
        {
            Indexed.Dispose();
            Plain.Dispose();
            foreach (var p in new[] { _indexedPath, _plainPath })
                foreach (var f in new[] { p, Path.ChangeExtension(p, ".wal"), p + "-shm" })
                    try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
        }

        public static List<DiffDoc> Generate(int seed, int count)
        {
            var rnd = new Random(seed);
            int[] counts = [-7, -1, 0, 0, 1, 2, 3, 5, 5, 5, 7, 9, 10, 11, 15, 20, 100];
            long[] totals = [-5, 0, 3, 5, 5, 9, 10, 1L << 40];
            double[] ratios = [-2.5, -1.0, -0.0, 0.0, 0.5, 1.0, 1.0, 1.5, 2.5, 9.5, 1e300, -1e300, double.Epsilon];
            decimal[] prices = [-2.5m, 0m, 0.00m, 1m, 5m, 5.00m, 9.99m, 10m, 1e20m, 123456789.123456789m];
            int[] minutes = [0, 1, 2, 5, 5, 10, 10, 20, 60, 1440, -5];
            string?[] nicks = [null, null, "a", "b", "ab", "abc", "", "zz"];
            DiffKind[] kinds = [DiffKind.Red, DiffKind.Green, DiffKind.Blue, DiffKind.Black];

            var docs = new List<DiffDoc>(count);
            for (int i = 0; i < count; i++)
            {
                var roll = rnd.Next(100);
                var name = roll < 3 ? "" : roll < 6 ? "q" + rnd.Next(10) : roll < 8 ? "p" : $"p{rnd.Next(200)}";
                docs.Add(new DiffDoc
                {
                    Id = new ObjectId(i + 1, 0),
                    Name = name,
                    Nick = nicks[rnd.Next(nicks.Length)],
                    Count = counts[rnd.Next(counts.Length)],
                    Total = totals[rnd.Next(totals.Length)],
                    Ratio = ratios[rnd.Next(ratios.Length)],
                    Price = prices[rnd.Next(prices.Length)],
                    When = T0.AddMinutes(minutes[rnd.Next(minutes.Length)]),
                    Flag = rnd.Next(2) == 0,
                    MaybeCount = rnd.Next(4) == 0 ? null : counts[rnd.Next(counts.Length)],
                    Kind = kinds[rnd.Next(kinds.Length)],
                });
            }

            // Exactly one document at each integer extreme, for Single and for bound tests.
            docs[0].Count = int.MaxValue;
            docs[1].Count = int.MinValue;
            docs[2].Total = long.MaxValue;
            docs[3].Total = long.MinValue;
            return docs;
        }
    }

    // ── Constants and captured values used by the catalogue ──────────────────

    private static readonly DateTime T0 = new(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime s_t5 = T0.AddMinutes(5);
    private static readonly DateTime s_t10 = T0.AddMinutes(10);
    private static readonly DateTime s_t20 = T0.AddMinutes(20);
    private static readonly int s_five = 5;
    private static readonly int? s_nullableFive = 5;
    private static readonly int? s_nullableNull = null;
    private static readonly string s_p7 = "p7";
    private static readonly string? s_nullName = null;
    private static readonly int[] s_someCounts = [1, 2, 3, 100];
    private static readonly string[] s_someNames = ["p1", "p7", "zz", ""];
    private static readonly DiffKind[] s_someKinds = [DiffKind.Red, DiffKind.Black];
    private static readonly List<int?> s_someMaybe = [null, 5];

    // ── Known candidates ──────────────────────────────────────────────────────

    /// <summary>
    /// Rows that disagree on upstream 0450b97, by family, with the hypothesis the disagreement
    /// suggests and the issue that tracks it (empty until filed). See <see cref="Candidates"/>:
    /// a listed row fails with its family label until its issue is filed, is reported as skipped
    /// while the issue is open, and fails again once fixed until its entry is removed.
    /// </summary>
    private static readonly Candidates s_candidates = new()
    {
        // Family 1: documents whose indexed property is null are not written to the index, but
        // the planner answers `== null` from the index's null bucket, so they are never found.
        { "null equality on an indexed property finds nothing", "#247",
            "nick == null", "maybe == null", "maybe == captured null int?", "Count(nick == null)", "Count(maybe == null)",
            "nick == null and flag", "nick == null or nick == a", "maybe in list with null", "Where(maybe == null).First()" },
        // Family 2: `property != constant` does not match documents whose property is null
        // (LINQ: null != 5 is true); indexed and unindexed collections agree with each other.
        { "!= excludes null values", "#248",
            "maybe != 5", "nick != ab" },
        // Family 3: -0.0 and 0.0 are equal in LINQ but encode as different index keys.
        { "-0.0 is not found by an equality on an indexed double", "#249",
            "ratio == 0.0", "Count(ratio == 0.0)" },
        // Family 4: the index-served OrderBy(x).Take(n) window has no null-keyed documents,
        // which LINQ orders first.
        { "indexed top-N window skips documents with a null key", "#247",
            "orderby maybe take 20 (nulls)" },
    };

    // ── Catalogue ─────────────────────────────────────────────────────────────

    public static TheoryData<string> Cases => new(s_cases.Keys);

    private static readonly Dictionary<string, QueryCase> s_cases = BuildCatalogue();

    private static Dictionary<string, QueryCase> BuildCatalogue()
    {
        var c = new List<QueryCase>();

        // int
        c.Add(W("count == 5", x => x.Count == 5));
        c.Add(W("count == 4 (absent)", x => x.Count == 4));
        c.Add(W("count != 5", x => x.Count != 5));
        c.Add(W("count < 5", x => x.Count < 5));
        c.Add(W("count <= 5", x => x.Count <= 5));
        c.Add(W("count > 5", x => x.Count > 5));
        c.Add(W("count >= 5", x => x.Count >= 5));
        c.Add(W("count < 0", x => x.Count < 0));
        c.Add(W("count == int.MinValue", x => x.Count == int.MinValue));
        c.Add(W("count >= int.MaxValue", x => x.Count >= int.MaxValue));
        c.Add(W("count > int.MaxValue - 1", x => x.Count > int.MaxValue - 1));
        c.Add(W("count <= int.MinValue", x => x.Count <= int.MinValue));
        c.Add(W("count range [1,10)", x => x.Count >= 1 && x.Count < 10));
        c.Add(W("count range [5,5]", x => x.Count >= 5 && x.Count <= 5));
        c.Add(W("count crossing range", x => x.Count >= 10 && x.Count <= 1));
        c.Add(W("count two lower bounds", x => x.Count >= 0 && x.Count >= 5));
        c.Add(W("count two upper bounds", x => x.Count < 10 && x.Count <= 5));
        c.Add(W("count eq and lower", x => x.Count == 5 && x.Count >= 10));
        c.Add(W("count < 9.5 (double bound)", x => x.Count < 9.5));
        c.Add(W("count >= 5L (long bound)", x => x.Count >= 5L));
        c.Add(W("count == 5.0 (double bound)", x => x.Count == 5.0));
        c.Add(W("count in list", x => s_someCounts.Contains(x.Count)));
        c.Add(W("count in list and range", x => s_someCounts.Contains(x.Count) && x.Count >= 2));
        c.Add(W("not (count > 5)", x => !(x.Count > 5)));
        c.Add(W("count > 5 or count < 0", x => x.Count > 5 || x.Count < 0));
        c.Add(W("count == 5 or count == 7", x => x.Count == 5 || x.Count == 7));
        c.Add(W("count == captured", x => x.Count == s_five));
        c.Add(W("captured == count", x => s_five == x.Count));
        c.Add(W("5 < count (reversed)", x => 5 < x.Count));
        c.Add(W("count + 1 > 6 (arithmetic)", x => x.Count + 1 > 6));
        c.Add(W("count % 2 == 0", x => x.Count % 2 == 0));

        // long
        c.Add(W("total == 10", x => x.Total == 10));
        c.Add(W("total == 10 (int constant)", x => x.Total == 10L));
        c.Add(W("total > 0", x => x.Total > 0));
        c.Add(W("total < 0", x => x.Total < 0));
        c.Add(W("total >= 2^40", x => x.Total >= (1L << 40)));
        c.Add(W("total == long.MinValue", x => x.Total == long.MinValue));
        c.Add(W("total > long.MaxValue - 1", x => x.Total > long.MaxValue - 1));
        c.Add(W("total range [3,10)", x => x.Total >= 3 && x.Total < 10));
        c.Add(W("total < 9.5 (double bound)", x => x.Total < 9.5));
        c.Add(W("total > 1e17 (inexact double bound)", x => x.Total > 1e17));

        // double
        c.Add(W("ratio == 0.0", x => x.Ratio == 0.0));
        c.Add(W("ratio == 1.0", x => x.Ratio == 1.0));
        c.Add(W("ratio < 0", x => x.Ratio < 0));
        c.Add(W("ratio <= 0", x => x.Ratio <= 0));
        c.Add(W("ratio > 0", x => x.Ratio > 0));
        c.Add(W("ratio >= 0.5", x => x.Ratio >= 0.5));
        c.Add(W("ratio range [-1, 1.5]", x => x.Ratio >= -1.0 && x.Ratio <= 1.5));
        c.Add(W("ratio > 1e299", x => x.Ratio > 1e299));
        c.Add(W("ratio < -1e299", x => x.Ratio < -1e299));
        c.Add(W("ratio == 1 (int bound)", x => x.Ratio == 1));
        c.Add(W("ratio > 1 (int bound)", x => x.Ratio > 1));
        c.Add(W("ratio <= double.Epsilon", x => x.Ratio <= double.Epsilon));

        // decimal
        c.Add(W("price == 5m", x => x.Price == 5m));
        c.Add(W("price == 5.00m", x => x.Price == 5.00m));
        c.Add(W("price == 0m", x => x.Price == 0m));
        c.Add(W("price < 5m", x => x.Price < 5m));
        c.Add(W("price <= 5m", x => x.Price <= 5m));
        c.Add(W("price >= 9.99m", x => x.Price >= 9.99m));
        c.Add(W("price < 0m", x => x.Price < 0m));
        c.Add(W("price range [1, 10)", x => x.Price >= 1m && x.Price < 10m));
        c.Add(W("price > 1e19m", x => x.Price > 1e19m));
        c.Add(W("price == 5 (int bound)", x => x.Price == 5));
        c.Add(W("price > 5 (int bound)", x => x.Price > 5));
        c.Add(W("price < 9.5 (double bound)", x => x.Price < (decimal)9.5));

        // DateTime
        c.Add(W("when == t5", x => x.When == s_t5));
        c.Add(W("when < t5", x => x.When < s_t5));
        c.Add(W("when >= t10 and < t20", x => x.When >= s_t10 && x.When < s_t20));
        c.Add(W("when > t20", x => x.When > s_t20));
        c.Add(W("when == T0.AddMinutes(5) (call)", x => x.When == T0.AddMinutes(5)));
        c.Add(W("when <= T0", x => x.When <= T0));
        c.Add(W("when != t5", x => x.When != s_t5));

        // bool
        c.Add(W("flag", x => x.Flag));
        c.Add(W("not flag", x => !x.Flag));
        c.Add(W("flag == true", x => x.Flag == true));
        c.Add(W("flag == false", x => x.Flag == false));
        c.Add(W("flag != true", x => x.Flag != true));
        c.Add(W("flag and count > 5", x => x.Flag && x.Count > 5));
        c.Add(W("flag or count > 10", x => x.Flag || x.Count > 10));
        c.Add(W("not flag and total == 5", x => !x.Flag && x.Total == 5));

        // int?
        c.Add(W("maybe == null", x => x.MaybeCount == null));
        c.Add(W("maybe != null", x => x.MaybeCount != null));
        c.Add(W("maybe.HasValue", x => x.MaybeCount.HasValue));
        c.Add(W("not maybe.HasValue", x => !x.MaybeCount.HasValue));
        c.Add(W("maybe > 5", x => x.MaybeCount > 5));
        c.Add(W("maybe <= 0", x => x.MaybeCount <= 0));
        c.Add(W("maybe == 5", x => x.MaybeCount == 5));
        c.Add(W("maybe != 5", x => x.MaybeCount != 5));
        c.Add(W("maybe == captured int?", x => x.MaybeCount == s_nullableFive));
        c.Add(W("maybe == captured null int?", x => x.MaybeCount == s_nullableNull));
        c.Add(W("maybe range [0, 10)", x => x.MaybeCount >= 0 && x.MaybeCount < 10));
        c.Add(W("maybe < 9.5 (double bound)", x => x.MaybeCount < 9.5));
        c.Add(W("maybe ?? -1 > 5", x => (x.MaybeCount ?? -1) > 5));
        c.Add(W("maybe in list with null", x => s_someMaybe.Contains(x.MaybeCount)));
        c.Add(W("maybe == null or maybe > 10", x => x.MaybeCount == null || x.MaybeCount > 10));

        // string (never null)
        c.Add(W("name == p7", x => x.Name == "p7"));
        c.Add(W("name == captured p7", x => x.Name == s_p7));
        c.Add(W("name != p7", x => x.Name != "p7"));
        c.Add(W("name == empty", x => x.Name == ""));
        c.Add(W("name == p (bare prefix)", x => x.Name == "p"));
        c.Add(W("name startswith p7", x => x.Name.StartsWith("p7")));
        c.Add(W("name startswith p", x => x.Name.StartsWith("p")));
        c.Add(W("name startswith empty", x => x.Name.StartsWith("")));
        c.Add(W("name startswith q", x => x.Name.StartsWith("q")));
        c.Add(W("name startswith p19", x => x.Name.StartsWith("p19")));
        c.Add(W("name startswith zz (absent)", x => x.Name.StartsWith("zz")));
        c.Add(W("name contains 7", x => x.Name.Contains("7")));
        c.Add(W("name contains empty", x => x.Name.Contains("")));
        c.Add(W("name endswith 7", x => x.Name.EndsWith("7")));
        c.Add(W("name length == 2", x => x.Name.Length == 2));
        c.Add(W("name startswith p7 and not p7", x => x.Name.StartsWith("p7") && x.Name != "p7"));
        c.Add(W("name startswith p7 and count > 5", x => x.Name.StartsWith("p7") && x.Count > 5));
        c.Add(W("name startswith p1 and startswith p10", x => x.Name.StartsWith("p1") && x.Name.StartsWith("p10")));
        c.Add(W("name startswith p1 and == p10", x => x.Name.StartsWith("p1") && x.Name == "p10"));
        c.Add(W("name startswith p1 and endswith 9", x => x.Name.StartsWith("p1") && x.Name.EndsWith("9")));
        c.Add(W("name == p1 or name == p10", x => x.Name == "p1" || x.Name == "p10"));
        c.Add(W("name startswith p1 or startswith p2", x => x.Name.StartsWith("p1") || x.Name.StartsWith("p2")));
        c.Add(W("name in list", x => s_someNames.Contains(x.Name)));
        c.Add(W("name IsNullOrEmpty", x => string.IsNullOrEmpty(x.Name)));
        c.Add(W("name == null (never)", x => x.Name == s_nullName));
        c.Add(W("name startswith p7 and == p70", x => x.Name.StartsWith("p7") && x.Name == "p70"));
        c.Add(W("name == p7 and startswith p70 (empty)", x => x.Name == "p7" && x.Name.StartsWith("p70")));
        c.Add(W("name startswith p7 and startswith q (empty)", x => x.Name.StartsWith("p7") && x.Name.StartsWith("q")));

        // string? (with nulls)
        c.Add(W("nick == null", x => x.Nick == null));
        c.Add(W("nick != null", x => x.Nick != null));
        c.Add(W("nick == ab", x => x.Nick == "ab"));
        c.Add(W("nick != ab", x => x.Nick != "ab"));
        c.Add(W("nick == empty", x => x.Nick == ""));
        c.Add(W("nick != null and startswith a", x => x.Nick != null && x.Nick.StartsWith("a")));
        c.Add(W("nick IsNullOrEmpty", x => string.IsNullOrEmpty(x.Nick)));
        c.Add(W("nick == null or nick == a", x => x.Nick == null || x.Nick == "a"));
        c.Add(W("nick == null and flag", x => x.Nick == null && x.Flag));

        // enum
        c.Add(W("kind == Blue", x => x.Kind == DiffKind.Blue));
        c.Add(W("kind != Blue", x => x.Kind != DiffKind.Blue));
        c.Add(W("kind == Black", x => x.Kind == DiffKind.Black));
        c.Add(W("kind >= Green", x => x.Kind >= DiffKind.Green));
        c.Add(W("kind < Blue", x => x.Kind < DiffKind.Blue));
        c.Add(W("kind in list", x => s_someKinds.Contains(x.Kind)));
        c.Add(W("kind == Red or kind == Black", x => x.Kind == DiffKind.Red || x.Kind == DiffKind.Black));
        c.Add(W("(int)kind == 2", x => (int)x.Kind == 2));

        // several properties
        c.Add(W("count > 5 and total < 10", x => x.Count > 5 && x.Total < 10));
        c.Add(W("count == 5 and name startswith p1", x => x.Count == 5 && x.Name.StartsWith("p1")));
        c.Add(W("flag and kind == Red", x => x.Flag && x.Kind == DiffKind.Red));
        c.Add(W("count > 5 or total > 5", x => x.Count > 5 || x.Total > 5));
        c.Add(W("(count > 5 and flag) or nick == null", x => (x.Count > 5 && x.Flag) || x.Nick == null));
        c.Add(W("count > 5 and (flag or nick == null)", x => x.Count > 5 && (x.Flag || x.Nick == null)));
        c.Add(W("price == 5m and ratio == 1.0", x => x.Price == 5m && x.Ratio == 1.0));
        c.Add(W("when == t5 and maybe == null", x => x.When == s_t5 && x.MaybeCount == null));
        c.Add(W("count == maybe (two properties)", x => x.Count == x.MaybeCount));
        c.Add(W("count > total (two properties)", x => x.Count > x.Total));
        c.Add(W("true (constant)", x => true));
        c.Add(W("false (constant)", x => false));

        // chained Where
        c.Add(W("where count >= 0, where count < 10", x => x.Count >= 0, x => x.Count < 10));
        c.Add(W("where flag, where name startswith p1", x => x.Flag, x => x.Name.StartsWith("p1")));
        c.Add(W("where count == 5, where count == 7 (empty)", x => x.Count == 5, x => x.Count == 7));
        c.Add(W("where name startswith p7, where name == p70", x => x.Name.StartsWith("p7"), x => x.Name == "p70"));
        c.Add(W("three wheres", x => x.Count > 0, x => x.Total >= 0, x => x.Kind != DiffKind.Black));

        // orderings (ties everywhere), with and without a filter and a window
        c.Add(W("orderby count").OrderBy(x => x.Count));
        c.Add(W("orderby count desc").OrderByDescending(x => x.Count));
        c.Add(W("orderby count thenby id").OrderBy(x => x.Count, thenById: true));
        c.Add(W("orderby count desc thenby id").OrderByDescending(x => x.Count, thenById: true));
        c.Add(W("orderby name").OrderBy(x => x.Name));
        c.Add(W("orderby name desc").OrderByDescending(x => x.Name));
        c.Add(W("orderby nick (nulls)").OrderBy(x => x.Nick));
        c.Add(W("orderby nick desc (nulls)").OrderByDescending(x => x.Nick));
        c.Add(W("orderby total").OrderBy(x => x.Total));
        c.Add(W("orderby ratio").OrderBy(x => x.Ratio));
        c.Add(W("orderby ratio desc").OrderByDescending(x => x.Ratio));
        c.Add(W("orderby price").OrderBy(x => x.Price));
        c.Add(W("orderby when").OrderBy(x => x.When));
        c.Add(W("orderby when desc").OrderByDescending(x => x.When));
        c.Add(W("orderby flag").OrderBy(x => x.Flag));
        c.Add(W("orderby maybe (nulls)").OrderBy(x => x.MaybeCount));
        c.Add(W("orderby maybe desc (nulls)").OrderByDescending(x => x.MaybeCount));
        c.Add(W("orderby kind").OrderBy(x => x.Kind));
        c.Add(W("orderby id").OrderBy(x => x.Id));
        c.Add(W("orderby id desc").OrderByDescending(x => x.Id));
        c.Add(W("where count > 5 orderby count", x => x.Count > 5).OrderBy(x => x.Count));
        c.Add(W("where count > 5 orderby total desc", x => x.Count > 5).OrderByDescending(x => x.Total));
        c.Add(W("where name startswith p1 orderby name", x => x.Name.StartsWith("p1")).OrderBy(x => x.Name));
        c.Add(W("where flag orderby when thenby id", x => x.Flag).OrderBy(x => x.When, thenById: true));

        c.Add(W("orderby count take 5").OrderBy(x => x.Count).Take(5));
        c.Add(W("orderby count desc take 5").OrderByDescending(x => x.Count).Take(5));
        c.Add(W("orderby count thenby id take 7").OrderBy(x => x.Count, thenById: true).Take(7));
        c.Add(W("orderby count skip 3 take 5").OrderBy(x => x.Count).Skip(3).Take(5));
        c.Add(W("orderby count take 5 skip 2").OrderBy(x => x.Count).Take(5).Skip(2));
        c.Add(W("orderby count skip 395").OrderBy(x => x.Count).Skip(395));
        c.Add(W("orderby count take 0").OrderBy(x => x.Count).Take(0));
        c.Add(W("orderby count take 1000").OrderBy(x => x.Count).Take(1000));
        c.Add(W("orderby name take 10").OrderBy(x => x.Name).Take(10));
        c.Add(W("orderby name desc skip 5 take 10").OrderByDescending(x => x.Name).Skip(5).Take(10));
        c.Add(W("orderby price take 20").OrderBy(x => x.Price).Take(20));
        c.Add(W("orderby when desc take 20").OrderByDescending(x => x.When).Take(20));
        c.Add(W("orderby maybe take 20 (nulls)").OrderBy(x => x.MaybeCount).Take(20));
        c.Add(W("orderby nick desc take 20 (nulls)").OrderByDescending(x => x.Nick).Take(20));
        c.Add(W("orderby ratio take 30").OrderBy(x => x.Ratio).Take(30));
        c.Add(W("orderby kind skip 10 take 10").OrderBy(x => x.Kind).Skip(10).Take(10));
        c.Add(W("where count > 5 orderby count take 5", x => x.Count > 5).OrderBy(x => x.Count).Take(5));
        c.Add(W("where count > 5 orderby name desc skip 2 take 5", x => x.Count > 5).OrderByDescending(x => x.Name).Skip(2).Take(5));
        c.Add(W("where name startswith p1 orderby count thenby id take 10", x => x.Name.StartsWith("p1")).OrderBy(x => x.Count, thenById: true).Take(10));
        c.Add(W("where flag orderby when take 3 skip 1", x => x.Flag).OrderBy(x => x.When).Take(3).Skip(1));
        c.Add(W("where count == 4 orderby count take 5 (empty)", x => x.Count == 4).OrderBy(x => x.Count).Take(5));

        // windows without an order: only the count, distinctness and membership are defined
        c.Add(W("take 5").Take(5));
        c.Add(W("skip 10 take 5").Skip(10).Take(5));
        c.Add(W("take 10 skip 3").Take(10).Skip(3));
        c.Add(W("skip 398").Skip(398));
        c.Add(W("skip 500").Skip(500));
        c.Add(W("take 0").Take(0));
        c.Add(W("take -1").Take(-1));
        c.Add(W("where count > 5 take 5", x => x.Count > 5).Take(5));
        c.Add(W("where count == 4 take 5 (empty)", x => x.Count == 4).Take(5));
        c.Add(W("where name startswith p7 skip 2 take 3", x => x.Name.StartsWith("p7")).Skip(2).Take(3));

        // a filter after the window applies to the window only
        c.Add(W("orderby count thenby id take 20 where flag").OrderBy(x => x.Count, thenById: true).Take(20).WhereAfter(x => x.Flag));
        c.Add(W("orderby id take 50 where count > 5").OrderBy(x => x.Id).Take(50).WhereAfter(x => x.Count > 5));
        c.Add(W("orderby id desc skip 10 take 50 where name startswith p1").OrderByDescending(x => x.Id).Skip(10).Take(50).WhereAfter(x => x.Name.StartsWith("p1")));

        // ── terminals ──────────────────────────────────────────────────────
        c.Add(S("Count()", q => q.Count(), q => q.CountAsync()));
        c.Add(S("LongCount()", q => q.LongCount(), q => q.LongCountAsync()));
        c.Add(S("Count(count > 5)", q => q.Count(x => x.Count > 5), q => q.CountAsync(x => x.Count > 5)));
        c.Add(S("Where(count > 5).Count()", q => q.Where(x => x.Count > 5).Count(), q => q.Where(x => x.Count > 5).CountAsync()));
        c.Add(S("Where(count >= 0).Count(count < 10)", q => q.Where(x => x.Count >= 0).Count(x => x.Count < 10), q => q.Where(x => x.Count >= 0).CountAsync(x => x.Count < 10)));
        c.Add(S("Count(name startswith p7)", q => q.Count(x => x.Name.StartsWith("p7")), q => q.CountAsync(x => x.Name.StartsWith("p7"))));
        c.Add(S("Count(name == p7)", q => q.Count(x => x.Name == "p7"), q => q.CountAsync(x => x.Name == "p7")));
        c.Add(S("Count(price == 5m)", q => q.Count(x => x.Price == 5m), q => q.CountAsync(x => x.Price == 5m)));
        c.Add(S("Count(ratio == 0.0)", q => q.Count(x => x.Ratio == 0.0), q => q.CountAsync(x => x.Ratio == 0.0)));
        c.Add(S("Count(maybe == null)", q => q.Count(x => x.MaybeCount == null), q => q.CountAsync(x => x.MaybeCount == null)));
        c.Add(S("Count(nick == null)", q => q.Count(x => x.Nick == null), q => q.CountAsync(x => x.Nick == null)));
        c.Add(S("Count(kind == Blue)", q => q.Count(x => x.Kind == DiffKind.Blue), q => q.CountAsync(x => x.Kind == DiffKind.Blue)));
        c.Add(S("Count(when < t5)", q => q.Count(x => x.When < s_t5), q => q.CountAsync(x => x.When < s_t5)));
        c.Add(S("Count(count < 9.5)", q => q.Count(x => x.Count < 9.5), q => q.CountAsync(x => x.Count < 9.5)));
        c.Add(S("Count(count > 5 or count < 0)", q => q.Count(x => x.Count > 5 || x.Count < 0), q => q.CountAsync(x => x.Count > 5 || x.Count < 0)));
        c.Add(S("Count(flag)", q => q.Count(x => x.Flag), q => q.CountAsync(x => x.Flag)));
        c.Add(S("Take(10).Count()", q => q.Take(10).Count(), q => q.Take(10).CountAsync()));
        c.Add(S("Skip(395).Count()", q => q.Skip(395).Count(), q => q.Skip(395).CountAsync()));
        c.Add(S("Where(count > 5).Take(3).Count()", q => q.Where(x => x.Count > 5).Take(3).Count(), q => q.Where(x => x.Count > 5).Take(3).CountAsync()));
        c.Add(S("OrderBy(count).Take(5).Count()", q => q.OrderBy(x => x.Count).Take(5).Count(), q => q.OrderBy(x => x.Count).Take(5).CountAsync()));

        c.Add(S("Any()", q => q.Any(), q => q.AnyAsync()));
        c.Add(S("Any(count == 5)", q => q.Any(x => x.Count == 5), q => q.AnyAsync(x => x.Count == 5)));
        c.Add(S("Any(count == 4)", q => q.Any(x => x.Count == 4), q => q.AnyAsync(x => x.Count == 4)));
        c.Add(S("Where(count == 4).Any()", q => q.Where(x => x.Count == 4).Any(), q => q.Where(x => x.Count == 4).AnyAsync()));
        c.Add(S("Any(name startswith zz)", q => q.Any(x => x.Name.StartsWith("zz")), q => q.AnyAsync(x => x.Name.StartsWith("zz"))));
        c.Add(S("Any(name == p7)", q => q.Any(x => x.Name == "p7"), q => q.AnyAsync(x => x.Name == "p7")));
        c.Add(S("Where(count == 4).Take(5).Any()", q => q.Where(x => x.Count == 4).Take(5).Any(), q => q.Where(x => x.Count == 4).Take(5).AnyAsync()));
        c.Add(S("All(count > int.MinValue)", q => q.All(x => x.Count > int.MinValue), q => q.AllAsync(x => x.Count > int.MinValue)));
        c.Add(S("All(count >= int.MinValue)", q => q.All(x => x.Count >= int.MinValue), q => q.AllAsync(x => x.Count >= int.MinValue)));
        c.Add(S("Where(count > 5).All(count > 4)", q => q.Where(x => x.Count > 5).All(x => x.Count > 4), q => q.Where(x => x.Count > 5).AllAsync(x => x.Count > 4)));
        c.Add(S("Where(count > 5).All(flag)", q => q.Where(x => x.Count > 5).All(x => x.Flag), q => q.Where(x => x.Count > 5).AllAsync(x => x.Flag)));
        c.Add(S("Take(10).All(count > int.MinValue)", q => q.Take(10).All(x => x.Count > int.MinValue), q => q.Take(10).AllAsync(x => x.Count > int.MinValue)));
        c.Add(S("Where(count == 4).All(flag) (vacuous)", q => q.Where(x => x.Count == 4).All(x => x.Flag), q => q.Where(x => x.Count == 4).AllAsync(x => x.Flag)));

        // First/Single: the id must belong to a matching document (or be absent when none matches)
        c.Add(Member("First(count == 5)", x => x.Count == 5, q => q.First(x => x.Count == 5), q => q.FirstAsync(x => x.Count == 5)));
        c.Add(Member("First(count == 4) throws", x => x.Count == 4, q => q.First(x => x.Count == 4), q => q.FirstAsync(x => x.Count == 4)));
        c.Add(Member("FirstOrDefault(count == 4)", x => x.Count == 4, q => q.FirstOrDefault(x => x.Count == 4), q => q.FirstOrDefaultAsync(x => x.Count == 4)));
        c.Add(Member("FirstOrDefault(name startswith p7)", x => x.Name.StartsWith("p7"), q => q.FirstOrDefault(x => x.Name.StartsWith("p7")), q => q.FirstOrDefaultAsync(x => x.Name.StartsWith("p7"))));
        c.Add(Member("FirstOrDefault(name == p7)", x => x.Name == "p7", q => q.FirstOrDefault(x => x.Name == "p7"), q => q.FirstOrDefaultAsync(x => x.Name == "p7")));
        c.Add(Member("Where(price == 5m).FirstOrDefault()", x => x.Price == 5m, q => q.Where(x => x.Price == 5m).FirstOrDefault(), q => q.Where(x => x.Price == 5m).FirstOrDefaultAsync()));
        c.Add(Member("Where(maybe == null).First()", x => x.MaybeCount == null, q => q.Where(x => x.MaybeCount == null).First(), q => q.Where(x => x.MaybeCount == null).FirstAsync()));
        c.Add(Member("Single(count == int.MaxValue)", x => x.Count == int.MaxValue, q => q.Single(x => x.Count == int.MaxValue), q => q.SingleAsync(x => x.Count == int.MaxValue)));
        c.Add(Member("Single(count == 5) throws", x => x.Count == 5, q => q.Single(x => x.Count == 5), q => q.SingleAsync(x => x.Count == 5)));
        c.Add(Member("SingleOrDefault(count == 4)", x => x.Count == 4, q => q.SingleOrDefault(x => x.Count == 4), q => q.SingleOrDefaultAsync(x => x.Count == 4)));
        c.Add(Member("SingleOrDefault(total == long.MinValue)", x => x.Total == long.MinValue, q => q.SingleOrDefault(x => x.Total == long.MinValue), q => q.SingleOrDefaultAsync(x => x.Total == long.MinValue)));
        c.Add(Member("Where(count == 4).Take(3).FirstOrDefault()", x => x.Count == 4, q => q.Where(x => x.Count == 4).Take(3).FirstOrDefault(), q => q.Where(x => x.Count == 4).Take(3).FirstOrDefaultAsync()));

        // exact element terminals through a total order
        c.Add(S("OrderBy(count).ThenBy(id).First()", q => q.OrderBy(x => x.Count).ThenBy(x => x.Id).First().Id, q => Id(q.OrderBy(x => x.Count).ThenBy(x => x.Id).FirstAsync())));
        c.Add(S("OrderBy(count).ThenBy(id).Last()", q => q.OrderBy(x => x.Count).ThenBy(x => x.Id).Last().Id, q => Id(q.OrderBy(x => x.Count).ThenBy(x => x.Id).LastAsync())));
        c.Add(S("OrderByDescending(when).ThenBy(id).First()", q => q.OrderByDescending(x => x.When).ThenBy(x => x.Id).First().Id, q => Id(q.OrderByDescending(x => x.When).ThenBy(x => x.Id).FirstAsync())));
        c.Add(S("Where(count > 5).OrderBy(name).ThenBy(id).FirstOrDefault()", q => q.Where(x => x.Count > 5).OrderBy(x => x.Name).ThenBy(x => x.Id).FirstOrDefault()?.Id, q => Id(q.Where(x => x.Count > 5).OrderBy(x => x.Name).ThenBy(x => x.Id).FirstOrDefaultAsync())));
        c.Add(S("Where(count == 4).OrderBy(name).LastOrDefault()", q => q.Where(x => x.Count == 4).OrderBy(x => x.Name).LastOrDefault()?.Id, q => Id(q.Where(x => x.Count == 4).OrderBy(x => x.Name).LastOrDefaultAsync())));
        c.Add(S("OrderBy(count).ThenBy(id).ElementAt(10)", q => q.OrderBy(x => x.Count).ThenBy(x => x.Id).ElementAt(10).Id, q => Id(q.OrderBy(x => x.Count).ThenBy(x => x.Id).ElementAtAsync(10))));
        c.Add(S("OrderBy(count).ThenBy(id).Skip(3).First()", q => q.OrderBy(x => x.Count).ThenBy(x => x.Id).Skip(3).First().Id, q => Id(q.OrderBy(x => x.Count).ThenBy(x => x.Id).Skip(3).FirstAsync())));
        c.Add(S("OrderBy(count).ThenBy(id).Take(5).Last()", q => q.OrderBy(x => x.Count).ThenBy(x => x.Id).Take(5).Last().Id, q => Id(q.OrderBy(x => x.Count).ThenBy(x => x.Id).Take(5).LastAsync())));
        c.Add(S("OrderBy(id).ElementAtOrDefault(1000)", q => q.OrderBy(x => x.Id).ElementAtOrDefault(1000)?.Id, q => Id(q.OrderBy(x => x.Id).ElementAtOrDefaultAsync(1000))));

        // aggregates (LINQ semantics: int Sum overflows, Min/Max over nothing throws, nullable yields null)
                c.Add(S("Where(-100 < count < 100).Sum(count)", q => q.Where(x => x.Count < 100 && x.Count > -100).Sum(x => x.Count), q => Box(q.Where(x => x.Count < 100 && x.Count > -100).SumAsync(x => x.Count))));
        c.Add(S("Where(-100 < count < 100).Sum((long)count)", q => q.Where(x => x.Count < 100 && x.Count > -100).Sum(x => (long)x.Count), q => Box(q.Where(x => x.Count < 100 && x.Count > -100).SumAsync(x => (long)x.Count))));
        c.Add(S("Where(|total| < 2^41).Sum(total)", q => q.Where(x => x.Total < (1L << 41) && x.Total > -(1L << 41)).Sum(x => x.Total), q => Box(q.Where(x => x.Total < (1L << 41) && x.Total > -(1L << 41)).SumAsync(x => x.Total))));
                c.Add(S("Where(ratio < 1e10 and ratio > -1e10).Sum(ratio)", q => q.Where(x => x.Ratio < 1e10 && x.Ratio > -1e10).Sum(x => x.Ratio), q => Box(q.Where(x => x.Ratio < 1e10 && x.Ratio > -1e10).SumAsync(x => x.Ratio))));
        c.Add(S("Where(price < 1e19m).Sum(price)", q => q.Where(x => x.Price < 1e19m).Sum(x => x.Price), q => Box(q.Where(x => x.Price < 1e19m).SumAsync(x => x.Price))));
        c.Add(S("Where(count < 100).Sum(maybe)", q => q.Where(x => x.Count < 100).Sum(x => x.MaybeCount), async q => (object?)(await q.Where(x => x.Count < 100).Select(x => x.MaybeCount).ToListAsync()).Sum()));
        c.Add(S("Where(count == 4).Sum(count) (empty)", q => q.Where(x => x.Count == 4).Sum(x => x.Count), q => Box(q.Where(x => x.Count == 4).SumAsync(x => x.Count))));
        c.Add(S("Where(count == 4).Sum(price) (empty)", q => q.Where(x => x.Count == 4).Sum(x => x.Price), q => Box(q.Where(x => x.Count == 4).SumAsync(x => x.Price))));
        c.Add(S("Where(name startswith p7).Sum(count)", q => q.Where(x => x.Name.StartsWith("p7")).Sum(x => x.Count), q => Box(q.Where(x => x.Name.StartsWith("p7")).SumAsync(x => x.Count))));
        c.Add(S("OrderBy(id).Take(10).Sum(count)", q => q.OrderBy(x => x.Id).Take(10).Sum(x => x.Count), q => Box(q.OrderBy(x => x.Id).Take(10).SumAsync(x => x.Count))));
        c.Add(S("Where(count > 5).OrderBy(id).Skip(2).Take(10).Sum(count)", q => q.Where(x => x.Count > 5).OrderBy(x => x.Id).Skip(2).Take(10).Sum(x => x.Count), q => Box(q.Where(x => x.Count > 5).OrderBy(x => x.Id).Skip(2).Take(10).SumAsync(x => x.Count))));

        c.Add(S("Min(count)", q => q.Min(x => x.Count), q => Box(q.MinAsync(x => x.Count))));
        c.Add(S("Max(count)", q => q.Max(x => x.Count), q => Box(q.MaxAsync(x => x.Count))));
        c.Add(S("Min(total)", q => q.Min(x => x.Total), q => Box(q.MinAsync(x => x.Total))));
        c.Add(S("Max(total)", q => q.Max(x => x.Total), q => Box(q.MaxAsync(x => x.Total))));
        c.Add(S("Min(ratio)", q => q.Min(x => x.Ratio), q => Box(q.MinAsync(x => x.Ratio))));
        c.Add(S("Max(ratio)", q => q.Max(x => x.Ratio), q => Box(q.MaxAsync(x => x.Ratio))));
        c.Add(S("Min(price)", q => q.Min(x => x.Price), q => Box(q.MinAsync(x => x.Price))));
        c.Add(S("Max(price)", q => q.Max(x => x.Price), q => Box(q.MaxAsync(x => x.Price))));
        c.Add(S("Min(when)", q => q.Min(x => x.When), q => Box(q.MinAsync(x => x.When))));
        c.Add(S("Max(when)", q => q.Max(x => x.When), q => Box(q.MaxAsync(x => x.When))));
        c.Add(S("Min(name)", q => q.Min(x => x.Name), q => Box(q.MinAsync(x => x.Name))));
        c.Add(S("Max(name)", q => q.Max(x => x.Name), q => Box(q.MaxAsync(x => x.Name))));
        c.Add(S("Min(nick) (nulls)", q => q.Min(x => x.Nick), q => Box(q.MinAsync(x => x.Nick))));
        c.Add(S("Max(nick) (nulls)", q => q.Max(x => x.Nick), q => Box(q.MaxAsync(x => x.Nick))));
        c.Add(S("Min(maybe) (nulls)", q => q.Min(x => x.MaybeCount), q => Box(q.MinAsync(x => x.MaybeCount))));
        c.Add(S("Max(maybe) (nulls)", q => q.Max(x => x.MaybeCount), q => Box(q.MaxAsync(x => x.MaybeCount))));
        c.Add(S("Min(kind)", q => q.Min(x => x.Kind), q => Box(q.MinAsync(x => x.Kind))));
        c.Add(S("Max(kind)", q => q.Max(x => x.Kind), q => Box(q.MaxAsync(x => x.Kind))));
        c.Add(S("Max(flag)", q => q.Max(x => x.Flag), q => Box(q.MaxAsync(x => x.Flag))));
        c.Add(S("Where(count > 5).Min(count)", q => q.Where(x => x.Count > 5).Min(x => x.Count), q => Box(q.Where(x => x.Count > 5).MinAsync(x => x.Count))));
        c.Add(S("Where(count < 100).Max(count)", q => q.Where(x => x.Count < 100).Max(x => x.Count), q => Box(q.Where(x => x.Count < 100).MaxAsync(x => x.Count))));
        c.Add(S("Where(count < 9.5).Max(count)", q => q.Where(x => x.Count < 9.5).Max(x => x.Count), q => Box(q.Where(x => x.Count < 9.5).MaxAsync(x => x.Count))));
        c.Add(S("Where(name startswith p7).Max(name)", q => q.Where(x => x.Name.StartsWith("p7")).Max(x => x.Name), q => Box(q.Where(x => x.Name.StartsWith("p7")).MaxAsync(x => x.Name))));
        c.Add(S("Where(name startswith p7).Min(count)", q => q.Where(x => x.Name.StartsWith("p7")).Min(x => x.Count), q => Box(q.Where(x => x.Name.StartsWith("p7")).MinAsync(x => x.Count))));
        c.Add(S("Where(flag).Max(total)", q => q.Where(x => x.Flag).Max(x => x.Total), q => Box(q.Where(x => x.Flag).MaxAsync(x => x.Total))));
        c.Add(S("Where(count == 4).Max(count) throws", q => q.Where(x => x.Count == 4).Max(x => x.Count), q => Box(q.Where(x => x.Count == 4).MaxAsync(x => x.Count))));
        c.Add(S("Where(count == 4).Min(price) throws", q => q.Where(x => x.Count == 4).Min(x => x.Price), q => Box(q.Where(x => x.Count == 4).MinAsync(x => x.Price))));
        c.Add(S("Where(count == 4).Max(maybe) (null)", q => q.Where(x => x.Count == 4).Max(x => x.MaybeCount), q => Box(q.Where(x => x.Count == 4).MaxAsync(x => x.MaybeCount))));
        c.Add(S("Where(count == 4).Max(nick) (null)", q => q.Where(x => x.Count == 4).Max(x => x.Nick), q => Box(q.Where(x => x.Count == 4).MaxAsync(x => x.Nick))));
        c.Add(S("Where(maybe == null).Max(maybe) (null)", q => q.Where(x => x.MaybeCount == null).Max(x => x.MaybeCount), q => Box(q.Where(x => x.MaybeCount == null).MaxAsync(x => x.MaybeCount))));
        c.Add(S("Take(10).Max(count)", q => q.OrderBy(x => x.Id).Take(10).Max(x => x.Count), q => Box(q.OrderBy(x => x.Id).Take(10).MaxAsync(x => x.Count))));
        c.Add(S("OrderBy(count).Take(5).Max(count)", q => q.OrderBy(x => x.Count).Take(5).Max(x => x.Count), q => Box(q.OrderBy(x => x.Count).Take(5).MaxAsync(x => x.Count))));

        c.Add(S("Where(-100 < count < 100).Average(count)", q => q.Where(x => x.Count < 100 && x.Count > -100).Average(x => x.Count), q => Box(q.Where(x => x.Count < 100 && x.Count > -100).AverageAsync(x => x.Count))));
        c.Add(S("Where(|total| < 2^41).Average(total)", q => q.Where(x => x.Total < (1L << 41) && x.Total > -(1L << 41)).Average(x => x.Total), q => Box(q.Where(x => x.Total < (1L << 41) && x.Total > -(1L << 41)).AverageAsync(x => x.Total))));
        c.Add(S("Where(price < 1e19m).Average(price)", q => q.Where(x => x.Price < 1e19m).Average(x => x.Price), q => Box(q.Where(x => x.Price < 1e19m).AverageAsync(x => x.Price))));
        c.Add(S("Where(count == 4).Average(count) throws", q => q.Where(x => x.Count == 4).Average(x => x.Count), q => Box(q.Where(x => x.Count == 4).AverageAsync(x => x.Count))));

        // projections and Distinct (compared as multisets)
        c.Add(S("Where(count > 5).Select(count)", q => q.Where(x => x.Count > 5).Select(x => x.Count).ToList(), q => Box(q.Where(x => x.Count > 5).Select(x => x.Count).ToListAsync())));
        c.Add(S("Where(count > 5).Select(id)", q => q.Where(x => x.Count > 5).Select(x => x.Id).ToList(), q => Box(q.Where(x => x.Count > 5).Select(x => x.Id).ToListAsync())));
        c.Add(S("Where(name startswith p7).Select(name)", q => q.Where(x => x.Name.StartsWith("p7")).Select(x => x.Name).ToList(), q => Box(q.Where(x => x.Name.StartsWith("p7")).Select(x => x.Name).ToListAsync())));
        c.Add(S("Select(nick) (nulls)", q => q.Select(x => x.Nick).ToList(), q => Box(q.Select(x => x.Nick).ToListAsync())));
        c.Add(S("Select(price * 2)", q => q.Where(x => x.Price < 1e19m).Select(x => x.Price * 2).ToList(), q => Box(q.Where(x => x.Price < 1e19m).Select(x => x.Price * 2).ToListAsync())));
        c.Add(S("Select(new { id, count })", q => q.Where(x => x.Flag).Select(x => new { x.Id, x.Count }).ToList(), q => Box(q.Where(x => x.Flag).Select(x => new { x.Id, x.Count }).ToListAsync())));
        c.Add(S("Select(maybe) (nulls)", q => q.Where(x => x.Count > 5).Select(x => x.MaybeCount).ToList(), q => Box(q.Where(x => x.Count > 5).Select(x => x.MaybeCount).ToListAsync())));
        c.Add(S("Select(count).Distinct()", q => q.Select(x => x.Count).Distinct().ToList(), q => Box(q.Select(x => x.Count).Distinct().ToListAsync())));
        c.Add(S("Where(flag).Select(kind).Distinct()", q => q.Where(x => x.Flag).Select(x => x.Kind).Distinct().ToList(), q => Box(q.Where(x => x.Flag).Select(x => x.Kind).Distinct().ToListAsync())));
        c.Add(S("OrderBy(count).ThenBy(id).Select(id) (ordered)", q => q.OrderBy(x => x.Count).ThenBy(x => x.Id).Select(x => x.Id).ToList(), q => Box(q.OrderBy(x => x.Count).ThenBy(x => x.Id).Select(x => x.Id).ToListAsync()), ordered: true));
        c.Add(S("OrderBy(count).Take(5).Select(count) (ordered)", q => q.OrderBy(x => x.Count).Take(5).Select(x => x.Count).ToList(), q => Box(q.OrderBy(x => x.Count).Take(5).Select(x => x.Count).ToListAsync()), ordered: true));
        c.Add(S("Where(count > 5).Select(count).Sum()", q => q.Where(x => x.Count > 5 && x.Count < 100).Select(x => x.Count).Sum(), async q => (object?)(await q.Where(x => x.Count > 5 && x.Count < 100).Select(x => x.Count).ToListAsync()).Sum()));
        c.Add(S("Select(count).Where(> 5).Count()", q => q.Select(x => x.Count).Where(v => v > 5).Count(), q => q.Select(x => x.Count).Where(v => v > 5).CountAsync()));
        c.Add(S("Select(count).Max()", q => q.Select(x => x.Count).Max(), async q => (object?)(await q.Select(x => x.Count).ToListAsync()).Max()));

        var dict = new Dictionary<string, QueryCase>();
        foreach (var qc in c)
            dict.Add(qc.Name, qc);
        return dict;
    }

    // ── The test ──────────────────────────────────────────────────────────────

    [Fact]
    public void Fixture_HasAnIndexOnEveryQueriedProperty()
    {
        var indexed = _f.Indexed.Docs.GetIndexes().Select(i => i.Name).ToList();
        Assert.True(indexed.Count >= 10, $"expected 10 indexes, got {indexed.Count}: {string.Join(", ", indexed)}");
        Assert.Empty(_f.Plain.Docs.GetIndexes());
        Assert.Equal(_f.Docs.Count, _f.Indexed.Docs.AsQueryable().Count());
        Assert.Equal(_f.Docs.Count, _f.Plain.Docs.AsQueryable().Count());
    }

    [Fact]
    public void Candidates_NameExistingQueries() => s_candidates.AssertAllIn(s_cases.Keys);

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Query_AgreesWithLinqToObjects(string name)
    {
        var qc = s_cases[name];
        var errors = new List<string>();

        foreach (var (engine, source) in new[] { ("indexed", (IQueryable<DiffDoc>)_f.Indexed.Docs.AsQueryable()), ("plain", _f.Plain.Docs.AsQueryable()) })
        {
            var sync = await Guard(() => Task.FromResult(qc.RunSync(source)));
            var syncError = qc.Compare(_f.Docs, sync);
            if (syncError != null) errors.Add($"{engine}/sync: {syncError}");

            var asyncResult = await Guard(() => qc.RunAsync(source));
            var asyncError = qc.Compare(_f.Docs, asyncResult);
            if (asyncError != null) errors.Add($"{engine}/async: {asyncError}");
        }

        s_candidates.Verify(name, errors.Count == 0 ? null
            : $"seed {_f.Seed}: query '{name}' disagrees with LINQ-to-Objects:\n  " + string.Join("\n  ", errors), _output);
    }

    /// <summary>The value the query produced, or the exception it threw (both are compared).</summary>
    private static async Task<Outcome> Guard(Func<Task<object?>> run)
    {
        try { return Outcome.Of(await run()); }
        catch (Exception ex) { return Outcome.Threw(ex); }
    }

    // ── Case model ────────────────────────────────────────────────────────────

    private readonly record struct Outcome(object? Value, Exception? Error)
    {
        public static Outcome Of(object? value) => new(value, null);
        public static Outcome Threw(Exception ex) => new(null, ex is AggregateException { InnerException: { } inner } ? inner : ex);
        public override string ToString() => Error != null ? $"threw {Error.GetType().Name}: {Error.Message}" : Describe(Value);
    }

    private abstract class QueryCase
    {
        protected QueryCase(string name) => Name = name;
        public string Name { get; }
        public abstract object? RunSync(IQueryable<DiffDoc> source);
        public abstract Task<object?> RunAsync(IQueryable<DiffDoc> source);
        /// <summary>Null when <paramref name="actual"/> agrees with LINQ-to-Objects over <paramref name="docs"/>.</summary>
        public abstract string? Compare(List<DiffDoc> docs, Outcome actual);
    }

    /// <summary>A query that yields documents: Where* [OrderBy [ThenBy Id]] (Skip|Take)* [Where].</summary>
    private sealed class SequenceCase : QueryCase
    {
        private readonly List<Expression<Func<DiffDoc, bool>>> _wheres = new();
        private Func<IQueryable<DiffDoc>, IOrderedQueryable<DiffDoc>>? _order;
        private Func<DiffDoc, object?>? _key;
        private bool _thenById;
        private readonly List<(bool Take, int N)> _window = new();
        private Expression<Func<DiffDoc, bool>>? _whereAfter;

        public SequenceCase(string name, params Expression<Func<DiffDoc, bool>>[] wheres) : base(name) => _wheres.AddRange(wheres);

        public SequenceCase OrderBy<TKey>(Expression<Func<DiffDoc, TKey>> key, bool thenById = false)
        {
            var compiled = key.Compile();
            _order = thenById ? q => q.OrderBy(key).ThenBy(x => x.Id) : q => q.OrderBy(key);
            _key = d => compiled(d);
            _thenById = thenById;
            return this;
        }

        public SequenceCase OrderByDescending<TKey>(Expression<Func<DiffDoc, TKey>> key, bool thenById = false)
        {
            var compiled = key.Compile();
            _order = thenById ? q => q.OrderByDescending(key).ThenBy(x => x.Id) : q => q.OrderByDescending(key);
            _key = d => compiled(d);
            _thenById = thenById;
            return this;
        }

        public SequenceCase Take(int n) { _window.Add((true, n)); return this; }
        public SequenceCase Skip(int n) { _window.Add((false, n)); return this; }
        public SequenceCase WhereAfter(Expression<Func<DiffDoc, bool>> predicate) { _whereAfter = predicate; return this; }

        private IQueryable<DiffDoc> Build(IQueryable<DiffDoc> q, bool withWindow)
        {
            foreach (var w in _wheres) q = q.Where(w);
            if (_order != null) q = _order(q);
            if (withWindow)
            {
                foreach (var (take, n) in _window) q = take ? q.Take(n) : q.Skip(n);
                if (_whereAfter != null) q = q.Where(_whereAfter);
            }
            return q;
        }

        public override object? RunSync(IQueryable<DiffDoc> source) => Build(source, true).ToList();
        public override async Task<object?> RunAsync(IQueryable<DiffDoc> source) => await Build(source, true).ToListAsync();

        public override string? Compare(List<DiffDoc> docs, Outcome actual)
        {
            var expected = Outcome.Of(null);
            List<DiffDoc> expectedList;
            try { expectedList = Build(docs.AsQueryable(), true).ToList(); }
            catch (Exception ex) { expected = Outcome.Threw(ex); expectedList = new(); }

            if (expected.Error != null || actual.Error != null)
                return expected.Error?.GetType() == actual.Error?.GetType() ? null : $"expected {expected}, got {actual}";

            var result = (List<DiffDoc>)actual.Value!;
            if (result.Count != expectedList.Count)
                return $"expected {expectedList.Count} documents, got {result.Count}: expected ids {Ids(expectedList)}, got {Ids(result)}";

            bool windowed = _window.Count > 0 || _whereAfter != null;
            bool totalOrder = _order == null || _thenById;

            if (!windowed || _thenById)
            {
                // Exactly these documents; in exactly this order when the order is total.
                var exp = _order != null && _thenById ? expectedList.Select(d => d.Id).ToList() : expectedList.Select(d => d.Id).OrderBy(i => i).ToList();
                var act = _order != null && _thenById ? result.Select(d => d.Id).ToList() : result.Select(d => d.Id).OrderBy(i => i).ToList();
                if (!exp.SequenceEqual(act))
                    return $"ids differ: expected {Ids(expectedList, _order != null && _thenById)}, got {Ids(result, _order != null && _thenById)}";
            }
            else
            {
                // A window over ties: the documents must be distinct members of the unwindowed
                // result (checked below through the keys when ordered).
                var candidates = Build(docs.AsQueryable(), false).Select(d => d.Id).ToHashSet();
                if (result.Select(d => d.Id).Distinct().Count() != result.Count)
                    return $"duplicate ids in window: {Ids(result)}";
                var foreign = result.Where(d => !candidates.Contains(d.Id)).ToList();
                if (foreign.Count > 0)
                    return $"documents outside the query's result in window: {Ids(foreign)}";
                if (_whereAfter != null)
                {
                    var after = _whereAfter.Compile();
                    if (result.Any(d => !after(d)))
                        return $"documents not matching the trailing Where: {Ids(result.Where(d => !after(d)))}";
                }
            }

            if (_order != null && !totalOrder)
            {
                var expKeys = expectedList.Select(_key!).ToList();
                var actKeys = result.Select(_key!).ToList();
                if (!expKeys.Zip(actKeys).All(pair => SameKey(pair.First, pair.Second)))
                    return $"order differs: expected keys [{string.Join(", ", expKeys.Select(Describe))}], got [{string.Join(", ", actKeys.Select(Describe))}]";
            }

            // Every returned document must be the stored document (the oracle's copy by id).
            var byId = docs.ToDictionary(d => d.Id);
            foreach (var d in result)
                if (!byId.TryGetValue(d.Id, out var o) || !SameDocument(o, d))
                    return $"document {d.Id} differs from the inserted one: {Describe(d)} vs {(byId.TryGetValue(d.Id, out var o2) ? Describe(o2) : "missing")}";

            return null;
        }
    }

    /// <summary>A terminal: the value (or the exception type) must equal LINQ-to-Objects'.</summary>
    private sealed class ScalarCase : QueryCase
    {
        private readonly Func<IQueryable<DiffDoc>, object?> _sync;
        private readonly Func<IQueryable<DiffDoc>, Task<object?>> _async;
        private readonly Func<List<DiffDoc>, Outcome, Outcome, string?>? _custom;
        private readonly bool _ordered;

        public ScalarCase(string name, Func<IQueryable<DiffDoc>, object?> sync, Func<IQueryable<DiffDoc>, Task<object?>> async,
            Func<List<DiffDoc>, Outcome, Outcome, string?>? custom = null, bool ordered = false) : base(name)
        {
            _sync = sync; _async = async; _custom = custom; _ordered = ordered;
        }

        public override object? RunSync(IQueryable<DiffDoc> source) => _sync(source);
        public override Task<object?> RunAsync(IQueryable<DiffDoc> source) => _async(source);

        public override string? Compare(List<DiffDoc> docs, Outcome actual)
        {
            Outcome expected;
            try { expected = Outcome.Of(_sync(docs.AsQueryable())); }
            catch (Exception ex) { expected = Outcome.Threw(ex); }

            if (_custom != null) return _custom(docs, expected, actual);

            if (expected.Error != null || actual.Error != null)
                return expected.Error?.GetType() == actual.Error?.GetType() ? null : $"expected {expected}, got {actual}";

            var e = Canonical(expected.Value, _ordered);
            var a = Canonical(actual.Value, _ordered);
            return e == a ? null : $"expected {e}, got {a}";
        }
    }

    private static SequenceCase W(string name, params Expression<Func<DiffDoc, bool>>[] wheres) => new(name, wheres);

    private static ScalarCase S<TResult>(string name, Func<IQueryable<DiffDoc>, TResult> sync, Func<IQueryable<DiffDoc>, Task<TResult>> async, bool ordered = false)
        => new(name, q => sync(q), async q => await async(q), ordered: ordered);

    private static ScalarCase S<TResult>(string name, Func<IQueryable<DiffDoc>, TResult> sync, Func<IQueryable<DiffDoc>, Task<object?>> async, bool ordered = false)
        => new(name, q => sync(q), async, ordered: ordered);

    /// <summary>
    /// A First/Single-style terminal without a total order: the result must be a document that
    /// matches <paramref name="predicate"/>, absent (null) exactly when LINQ's is, and throw the
    /// same exception type when LINQ throws.
    /// </summary>
    private static ScalarCase Member(string name, Expression<Func<DiffDoc, bool>> predicate,
        Func<IQueryable<DiffDoc>, DiffDoc?> sync, Func<IQueryable<DiffDoc>, Task<DiffDoc?>> async)
    {
        var matches = predicate.Compile();
        return new ScalarCase(name, q => sync(q), async q => await async(q), (docs, expected, actual) =>
        {
            if (expected.Error != null || actual.Error != null)
                return expected.Error?.GetType() == actual.Error?.GetType() ? null : $"expected {expected}, got {actual}";
            var exp = (DiffDoc?)expected.Value;
            var act = (DiffDoc?)actual.Value;
            if (exp == null) return act == null ? null : $"expected no document, got {Describe(act)}";
            if (act == null) return $"expected a matching document, got null";
            var stored = docs.FirstOrDefault(d => d.Id == act.Id);
            if (stored == null || !matches(stored)) return $"got a document that does not match the predicate: {Describe(act)}";
            if (!SameDocument(stored, act)) return $"document {act.Id} differs from the inserted one: {Describe(act)} vs {Describe(stored)}";
            return null;
        });
    }

    private static async Task<object?> Id(Task<DiffDoc?> doc) => (await doc)?.Id;
    private static async Task<object?> Box<T>(Task<T> value) => await value;

    // ── Describing values ─────────────────────────────────────────────────────

    private static string Ids(IEnumerable<DiffDoc> docs, bool keepOrder = false)
    {
        var ids = docs.Select(d => IdNo(d.Id));
        if (!keepOrder) ids = ids.OrderBy(i => i);
        var list = ids.ToList();
        return list.Count > 40 ? $"[{string.Join(",", list.Take(40))}, ... {list.Count} ids]" : $"[{string.Join(",", list)}]";
    }

    /// <summary>Equality of two sort keys as LINQ's comparers see them (-0.0 equals 0.0, 5m equals 5.00m).</summary>
    private static bool SameKey(object? a, object? b) => (a, b) switch
    {
        (null, null) => true,
        (double x, double y) => x == y,
        (decimal x, decimal y) => x == y,
        _ => Equals(a, b),
    };

    private static int IdNo(ObjectId id) => (int)(id.Timestamp - DateTime.UnixEpoch).TotalSeconds;

    private static bool SameDocument(DiffDoc a, DiffDoc b)
        => a.Id == b.Id && a.Name == b.Name && a.Nick == b.Nick && a.Count == b.Count && a.Total == b.Total
           && a.Ratio.Equals(b.Ratio) && a.Price == b.Price && a.When == b.When && a.Flag == b.Flag
           && a.MaybeCount == b.MaybeCount && a.Kind == b.Kind;

    private static string Describe(object? value) => value switch
    {
        null => "null",
        DiffDoc d => $"{{#{IdNo(d.Id)} {d.Name} nick={d.Nick ?? "null"} count={d.Count} total={d.Total} ratio={d.Ratio.ToString("R", System.Globalization.CultureInfo.InvariantCulture)} price={d.Price.ToString(System.Globalization.CultureInfo.InvariantCulture)} when={d.When:O} flag={d.Flag} maybe={d.MaybeCount?.ToString() ?? "null"} kind={d.Kind}}}",
        ObjectId id => $"#{IdNo(id)}",
        string s => $"\"{s}\"",
        double dbl => dbl.ToString("R", System.Globalization.CultureInfo.InvariantCulture),
        decimal dec => dec.ToString(System.Globalization.CultureInfo.InvariantCulture),
        DateTime dt => dt.ToString("O"),
        System.Collections.IEnumerable e => $"[{string.Join(", ", e.Cast<object?>().Select(Describe))}]",
        _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "null",
    };

    /// <summary>A comparable text: sequences as multisets (sorted) unless the order matters.</summary>
    private static string Canonical(object? value, bool ordered)
    {
        if (value is System.Collections.IEnumerable e and not string)
        {
            var items = e.Cast<object?>().Select(Describe);
            if (!ordered) items = items.OrderBy(s => s, StringComparer.Ordinal);
            var list = items.ToList();
            return $"{list.Count} items [{string.Join(", ", list)}]";
        }
        return Describe(value);
    }
}
