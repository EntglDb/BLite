using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.ExceptionServices;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Query.Blql;
using BLite.Shared;
using Xunit.Abstractions;
using Outcome = BLite.Tests.Oracles.DifferentialQueryTests.Outcome;
using QueryCase = BLite.Tests.Oracles.DifferentialQueryTests.QueryCase;

namespace BLite.Tests.Oracles;

/// <summary>
/// Differential test of <see cref="DynamicCollection"/>: the query catalogue of
/// <see cref="DifferentialQueryTests"/> runs over the same seeded documents, written as
/// <see cref="BsonDocument"/>s, four ways: on a collection with a B-Tree index on every queried field
/// (one of them a nested path) and on one without, each through the collection's own query methods
/// (<c>QueryIndexAsync</c>, <c>FindAsync</c>) and through BLQL. Every answer is checked against two
/// oracles: LINQ-to-Objects over the in-memory list, by the case's own comparison rule, and the typed
/// <c>DocumentCollection</c> over the same documents (indexed for the indexed ways, plain for the
/// plain ones), by what every correct answer has in common (<see cref="QueryCase.Determined"/>).
/// </summary>
/// <remarks>
/// <para>
/// Each query is a LINQ expression over <see cref="DiffDoc"/>. It runs against an
/// <see cref="IQueryable{T}"/> whose provider takes the part of the expression the way can express
/// and evaluates only the rest with LINQ-to-Objects over the documents it returned. Through BLQL that
/// part is the leading <c>Where</c>s (as a <see cref="BlqlFilter"/>), the order (<see cref="BlqlSort"/>),
/// the window (one <c>Skip</c> and one <c>Take</c>), and a final <c>Count</c>, <c>Any</c>, <c>First</c>
/// or <c>FirstOrDefault</c>; aggregates, projections and a <c>Where</c> after the window run in memory
/// over the BLQL result. Through the collection's methods it is the filter (an index range on the
/// first indexed field the filter bounds, with the whole filter as residual, else a
/// <c>FindAsync</c> scan) and, on the indexed collection, an order on an indexed field (the index's
/// own order, ties broken by id in memory where the query asks for it); the rest runs in memory.
/// Every row's output names what ran where.
/// </para>
/// <para>
/// A shape BLQL cannot express (a field compared with another field or with arithmetic, a string
/// length, <c>??</c>, a negative <c>Take</c>) is not translated: its BLQL rows pass and print
/// <c>INAPPLICABLE</c> with the reason, and <see cref="s_blqlInapplicable"/> lists every such query,
/// so a new one fails <see cref="Blql_InapplicableShapesAreListed"/> until it is listed with a reason.
/// The dynamic ways run the catalogue's synchronous LINQ form only: BLQL has one implementation behind
/// its sync and async methods, and the provider calls the async ones.
/// </para>
/// <para>
/// The typed oracle answers wrongly where <see cref="DifferentialQueryTests"/> lists a candidate.
/// The rows that disagree with it for that reason (#247, #248) are registered here under the typed
/// collection's issue, so they report "candidate fixed" once that issue's fix lands; for -0.0 (#249)
/// the dynamic index misses the same documents (#254), so the two agree and only the LINQ rows fail.
/// </para>
/// </remarks>
public class DynamicDifferentialTests : IClassFixture<DynamicDifferentialTests.Fixture>
{
    private readonly Fixture _f;
    private readonly ITestOutputHelper _output;

    public DynamicDifferentialTests(Fixture fixture, ITestOutputHelper output) => (_f, _output) = (fixture, output);

    // ── Ways and oracles ──────────────────────────────────────────────────────

    internal sealed record Way(string Name, bool Indexed, bool Blql);

    private static readonly Way IndexedApi = new("indexed/api", true, false);
    private static readonly Way IndexedBlql = new("indexed/blql", true, true);
    private static readonly Way PlainApi = new("plain/api", false, false);
    private static readonly Way PlainBlql = new("plain/blql", false, true);
    private static readonly Way[] s_ways = [IndexedApi, IndexedBlql, PlainApi, PlainBlql];

    private const string Linq = "linq";
    private const string Typed = "typed";

    private sealed record Row(Way Way, string Oracle, string Query);

    private static string RowName(Way way, string oracle, string query) => $"{way.Name} {oracle}: {query}";

    private static readonly Dictionary<string, Row> s_rows = (
        from query in DifferentialQueryTests.Catalogue.Keys
        from way in s_ways
        from oracle in new[] { Linq, Typed }
        select new Row(way, oracle, query)).ToDictionary(r => RowName(r.Way, r.Oracle, r.Query));

    public static TheoryData<string> Rows => new(s_rows.Keys);

    // ── The documents as BSON ─────────────────────────────────────────────────

    /// <summary>The field each <see cref="DiffDoc"/> property is stored under; <c>Kind</c> sits in a sub-document.</summary>
    private static readonly Dictionary<string, string> s_fields = new()
    {
        [nameof(DiffDoc.Id)] = "_id",
        [nameof(DiffDoc.Name)] = "name",
        [nameof(DiffDoc.Nick)] = "nick",
        [nameof(DiffDoc.Count)] = "count",
        [nameof(DiffDoc.Total)] = "total",
        [nameof(DiffDoc.Ratio)] = "ratio",
        [nameof(DiffDoc.Price)] = "price",
        [nameof(DiffDoc.When)] = "when",
        [nameof(DiffDoc.Flag)] = "flag",
        [nameof(DiffDoc.MaybeCount)] = "maybecount",
        [nameof(DiffDoc.Kind)] = "meta.kind",
    };

    private static readonly string[] s_keyNames = ["_id", "name", "nick", "count", "total", "ratio", "price", "when", "flag", "maybecount", "meta", "kind"];

    private static string IndexName(string field) => $"idx_{field}";

    /// <summary>
    /// A document as a schema-less client writes it: the types the typed mapper uses (decimal as
    /// Decimal128, the enum as Int32), an explicit BSON null for a null value, and the kind nested.
    /// </summary>
    private static BsonDocument ToBson(DynamicCollection collection, DiffDoc d) => collection.CreateDocument(s_keyNames, b =>
    {
        b.AddId(new BsonId(d.Id));
        b.AddString("name", d.Name);
        if (d.Nick == null) b.AddNull("nick"); else b.AddString("nick", d.Nick);
        b.AddInt32("count", d.Count);
        b.AddInt64("total", d.Total);
        b.AddDouble("ratio", d.Ratio);
        b.AddDecimal("price", d.Price);
        b.AddDateTime("when", d.When);
        b.AddBoolean("flag", d.Flag);
        if (d.MaybeCount is { } maybe) b.AddInt32("maybecount", maybe); else b.AddNull("maybecount");
        b.AddDocument("meta", m => m.AddInt32("kind", (int)d.Kind));
    });

    private static DiffDoc FromBson(BsonDocument doc)
    {
        BsonValue Field(BsonDocument from, string name)
            => from.TryGetValue(name, out var v) ? v : throw new InvalidDataException($"the document has no field '{name}'");

        var nick = Field(doc, "nick");
        var maybe = Field(doc, "maybecount");
        return new DiffDoc
        {
            Id = Field(doc, "_id").AsObjectId,
            Name = Field(doc, "name").AsString,
            Nick = nick.IsNull ? null : nick.AsString,
            Count = Field(doc, "count").AsInt32,
            Total = Field(doc, "total").AsInt64,
            Ratio = Field(doc, "ratio").AsDouble,
            Price = Field(doc, "price").AsDecimal,
            When = Field(doc, "when").AsDateTime,
            Flag = Field(doc, "flag").AsBoolean,
            MaybeCount = maybe.IsNull ? null : maybe.AsInt32,
            Kind = (DiffKind)Field(Field(doc, "meta").AsDocument, "kind").AsInt32,
        };
    }

    // ── Fixture ───────────────────────────────────────────────────────────────

    public sealed class Fixture : IDisposable
    {
        /// <summary>The documents and the typed collections of <see cref="DifferentialQueryTests"/>, the second oracle.</summary>
        public DifferentialQueryTests.Fixture Typed { get; } = new();

        public int Seed => Typed.Seed;
        public List<DiffDoc> Docs => Typed.Docs;
        public DynamicCollection Indexed { get; }
        public DynamicCollection Plain { get; }

        private readonly BLiteEngine _indexedEngine;
        private readonly BLiteEngine _plainEngine;
        private readonly string _indexedPath;
        private readonly string _plainPath;
        private readonly ConcurrentDictionary<(string Way, string Query), Lazy<Task<DynamicRun>>> _dynamic = new();
        private readonly ConcurrentDictionary<(bool Indexed, string Query), Lazy<Task<Outcome>>> _typed = new();

        public Fixture()
        {
            var id = Guid.NewGuid().ToString("N");
            _indexedPath = Path.Combine(Path.GetTempPath(), $"diff_dynamic_indexed_{id}.db");
            _plainPath = Path.Combine(Path.GetTempPath(), $"diff_dynamic_plain_{id}.db");
            _indexedEngine = new BLiteEngine(_indexedPath);
            _plainEngine = new BLiteEngine(_plainPath);
            Indexed = _indexedEngine.GetOrCreateCollection("diff_docs");
            Plain = _plainEngine.GetOrCreateCollection("diff_docs");

            // The typed fixture's insertion order. Half the documents go in before the indexes exist
            // and are indexed by CreateIndexAsync's backfill, the other half by the insert path.
            var rnd = new Random(Seed ^ 0x5bd1e995);
            var shuffled = Docs.OrderBy(_ => rnd.Next()).ToList();
            var half = shuffled.Count / 2;
            Indexed.InsertBulkAsync(shuffled.Take(half).Select(d => ToBson(Indexed, d))).AsTask().GetAwaiter().GetResult();
            foreach (var field in s_fields.Values.Where(f => f != "_id"))
                Indexed.CreateIndexAsync(field).GetAwaiter().GetResult();
            Indexed.InsertBulkAsync(shuffled.Skip(half).Select(d => ToBson(Indexed, d))).AsTask().GetAwaiter().GetResult();
            Plain.InsertBulkAsync(shuffled.Select(d => ToBson(Plain, d))).AsTask().GetAwaiter().GetResult();
        }

        public void Dispose()
        {
            Typed.Dispose();
            _indexedEngine.Dispose();
            _plainEngine.Dispose();
            foreach (var p in new[] { _indexedPath, _plainPath })
                foreach (var f in new[] { p, Path.ChangeExtension(p, ".wal"), p + "-shm" })
                    try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
        }

        /// <summary>The answer of one way to one query, computed once for both oracles' rows.</summary>
        internal Task<DynamicRun> RunDynamic(Way way, string query)
            => _dynamic.GetOrAdd((way.Name, query), _ => new Lazy<Task<DynamicRun>>(() => Task.Run(async () =>
            {
                var executor = new DynamicExecutor(way.Indexed ? Indexed : Plain, way);
                var source = new DynamicQueryable<DiffDoc>(new DynamicProvider(executor));
                var outcome = await DifferentialQueryTests.Guard(() => Task.FromResult(DifferentialQueryTests.Catalogue[query].RunSync(source)));
                return outcome.Error is InapplicableException inapplicable
                    ? new DynamicRun(outcome, executor.Plan, inapplicable.Message)
                    : new DynamicRun(outcome, executor.Plan, null);
            }))).Value;

        internal Task<Outcome> RunTyped(bool indexed, string query)
            => _typed.GetOrAdd((indexed, query), _ => new Lazy<Task<Outcome>>(() =>
            {
                IQueryable<DiffDoc> source = indexed ? Typed.Indexed.Docs.AsQueryable() : Typed.Plain.Docs.AsQueryable();
                return DifferentialQueryTests.Guard(() => Task.FromResult(DifferentialQueryTests.Catalogue[query].RunSync(source)));
            })).Value;
    }

    internal sealed record DynamicRun(Outcome Outcome, string Plan, string? Inapplicable);

    // ── Known candidates ──────────────────────────────────────────────────────

    private static readonly Way[] IndexedWays = [IndexedApi, IndexedBlql];
    private static readonly Way[] BlqlWays = [IndexedBlql, PlainBlql];
    private static readonly string[] BothOracles = [Linq, Typed];

    private static string[] Named(Way[] ways, string[] oracles, params string[] queries)
        => (from q in queries from w in ways from o in oracles select RowName(w, o, q)).ToArray();

    /// <summary>
    /// Rows that disagree on upstream 0450b97 plus the oracle branches, by family, with the issue that
    /// tracks each (drafts D1 to D8 in <c>Seeds/candidate-issues.md</c>, filed as #264 to #270). None is a
    /// documented difference between the typed and the schema-less collection: README.md:880 (dynamic
    /// indexes an explicit null, typed indexes do not) changes which values a unique index accepts,
    /// which <see cref="DynamicStoreModelTests"/> encodes, and no query answer here.
    /// </summary>
    private static readonly Candidates s_candidates = new()
    {
        // #254: doubles are keyed as little-endian BitConverter bytes, so ranges, sorts and -0.0 miss.
        { "dynamic double keys are little-endian bytes", "#254", Named(IndexedWays, BothOracles,
            "Where(ratio < 1e10 and ratio > -1e10).Sum(ratio)", "orderby ratio", "orderby ratio desc", "orderby ratio take 30",
            "ratio < 0", "ratio <= 0", "ratio <= double.Epsilon", "ratio > 1 (int bound)", "ratio > 1e299", "ratio >= 0.5",
            "ratio range [-1, 1.5]") },
        // The typed collection misses -0.0 the same way (#249), so only the LINQ rows disagree.
        { "dynamic double keys are little-endian bytes", "#254", Named(IndexedWays, [Linq], "Count(ratio == 0.0)", "ratio == 0.0") },
        // #264: BLQL reads a field with BsonDocument.TryGetValue, which does not follow a dotted path, so
        // a filter on "meta.kind" matches nothing (and $ne everything); the indexed sort is served by the
        // nested index and agrees, the in-memory sort does not.
        { "BLQL cannot reach a nested field", "#264", Named(BlqlWays, BothOracles,
            "(int)kind == 2", "Count(kind == Blue)", "flag and kind == Red", "kind != Blue", "kind < Blue", "kind == Black",
            "kind == Blue", "kind == Red or kind == Black", "kind >= Green", "kind in list", "three wheres") },
        { "BLQL cannot reach a nested field", "#264", Named([PlainBlql], BothOracles, "orderby kind", "orderby kind skip 10 take 10") },
        // #265: BsonValueComparer orders ObjectIds by ObjectId.WriteTo's bytes (timestamp little-endian),
        // ObjectId.CompareTo by the timestamp's value.
        { "BLQL orders ObjectIds by their little-endian bytes", "#265", Named(BlqlWays, BothOracles,
            "OrderBy(count).ThenBy(id).ElementAt(10)", "OrderBy(count).ThenBy(id).Select(id) (ordered)", "OrderBy(count).ThenBy(id).Take(5).Last()",
            "OrderBy(id).Take(10).Sum(count)", "Where(count > 5).OrderBy(id).Skip(2).Take(10).Sum(count)", "orderby count desc thenby id",
            "orderby count thenby id", "orderby count thenby id take 20 where flag", "orderby count thenby id take 7", "orderby id",
            "orderby id desc", "orderby id desc skip 10 take 50 where name startswith p1", "orderby id take 50 where count > 5",
            "where flag orderby when thenby id", "where name startswith p1 orderby count thenby id take 10") },
        // #266: BlqlQuery.FirstOrDefaultAsync scans in storage order and ignores Sort and Skip.
        { "BLQL First and FirstOrDefault ignore Sort and Skip", "#266", Named(BlqlWays, BothOracles,
            "OrderBy(count).ThenBy(id).First()", "OrderBy(count).ThenBy(id).Skip(3).First()", "OrderByDescending(when).ThenBy(id).First()",
            "Where(count > 5).OrderBy(name).ThenBy(id).FirstOrDefault()") },
        // #267: BsonValueComparer orders values of different BSON types by type (null lowest, strings above
        // numbers), so $lt and $lte match a null field (and $gt a string); the index range skips null
        // keys, so the indexed collection agrees with LINQ and the plain one does not.
        { "BLQL range operators match fields of another BSON type", "#267", Named([PlainBlql], BothOracles, "maybe < 9.5 (double bound)", "maybe <= 0") },
        // #268: two Int64 values are compared as doubles, which cannot tell long.MaxValue - 1 from long.MaxValue.
        { "BLQL compares two Int64 values through double", "#268", Named(BlqlWays, BothOracles, "total > long.MaxValue - 1") },
        // #269: the bound is keyed in its own type (a double as BitConverter bytes) against Int32 and Int64
        // keys, the dynamic counterpart of the typed #222.
        { "a numeric bound of another type than the stored values misses keys of a dynamic index", "#269", Named(IndexedWays, BothOracles,
            "Count(count < 9.5)", "Where(count < 9.5).Max(count)", "count < 9.5 (double bound)", "count == 5.0 (double bound)",
            "maybe < 9.5 (double bound)", "total < 9.5 (double bound)") },
        // #269: BsonValueToIndexKey has no key for Decimal128 and Boolean, so the index stays empty: the
        // index-served sort returns nothing and QueryIndexAsync rejects such a bound.
        { "a dynamic index keys no Decimal128 or Boolean value", "#269", Named(IndexedWays, BothOracles, "orderby flag", "orderby price", "orderby price take 20") },
        { "a dynamic index keys no Decimal128 or Boolean value", "#269", Named([IndexedApi], BothOracles,
            "Count(flag)", "Count(price == 5m)", "Select(new { id, count })", "Select(price * 2)", "Where(flag).Max(total)",
            "Where(flag).Select(kind).Distinct()", "Where(price < 1e19m).Average(price)", "Where(price < 1e19m).Sum(price)",
            "Where(price == 5m).FirstOrDefault()", "flag", "flag == false", "flag == true", "flag and count > 5", "flag and kind == Red",
            "not flag", "not flag and total == 5", "price < 0m", "price < 5m", "price < 9.5 (double bound)", "price <= 5m", "price == 0m",
            "price == 5 (int bound)", "price == 5.00m", "price == 5m", "price == 5m and ratio == 1.0", "price > 1e19m",
            "price > 5 (int bound)", "price >= 9.99m", "price range [1, 10)", "where flag orderby when take 3 skip 1",
            "where flag orderby when thenby id", "where flag, where name startswith p1") },
        // #270: CreateIndexKeyFromObject has no case for DateTime, which the index keys as Unix milliseconds.
        { "QueryIndexAsync rejects a DateTime bound", "#270", Named([IndexedApi], BothOracles,
            "Count(when < t5)", "when < t5", "when <= T0", "when == T0.AddMinutes(5) (call)", "when == t5", "when == t5 and maybe == null",
            "when > t20", "when >= t10 and < t20") },
        // The typed oracle is wrong here, see DifferentialQueryTests: its indexes skip null keys (README.md:880)
        // while its planner reads the null bucket (#247), and its != excludes null values (#248).
        { "the typed oracle: == null and a nullable top-N on an index find no null-keyed document", "#247", Named(IndexedWays, [Typed],
            "Count(maybe == null)", "Count(nick == null)", "Where(maybe == null).First()", "maybe == captured null int?", "maybe == null",
            "maybe in list with null", "nick == null", "nick == null and flag", "nick == null or nick == a", "orderby maybe take 20 (nulls)") },
        { "the typed oracle: != excludes null values", "#248", Named(s_ways, [Typed], "maybe != 5", "nick != ab") },
    };

    /// <summary>The queries BLQL cannot express, with the reason; the translator must agree with this list.</summary>
    private static readonly Dictionary<string, string> s_blqlInapplicable = new()
    {
        ["count + 1 > 6 (arithmetic)"] = "BLQL compares a stored field with a constant; it has no arithmetic on fields",
        ["count == maybe (two properties)"] = "BLQL compares a field with a constant, not with another field",
        ["count > total (two properties)"] = "BLQL compares a field with a constant, not with another field",
        ["maybe ?? -1 > 5"] = "BLQL has no coalesce; the same question needs an $or with an equality on null",
        ["name length == 2"] = "BLQL has no string length operator ($regex could emulate it, but that is another query)",
        ["take -1"] = "BlqlQuery.Take rejects a negative count (ArgumentOutOfRangeException), LINQ treats it as 0",
    };

    // ── The test ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Fixture_HasAnIndexOnEveryQueriedField()
    {
        var expected = s_fields.Values.Where(f => f != "_id").Select(IndexName).Order().ToList();
        Assert.Equal(expected, _f.Indexed.ListIndexes().Order().ToList());
        Assert.Contains("idx_meta.kind", expected);
        Assert.Empty(_f.Plain.ListIndexes());
        Assert.Equal(_f.Docs.Count, await _f.Indexed.CountAsync());
        Assert.Equal(_f.Docs.Count, await _f.Plain.CountAsync());
        var doc = await _f.Plain.FindByIdAsync(new BsonId(_f.Docs[0].Id));
        Assert.True(doc != null && DifferentialQueryTests.Describe(FromBson(doc)) == DifferentialQueryTests.Describe(_f.Docs[0]),
            $"a document does not round-trip through BSON: {(doc == null ? "missing" : DifferentialQueryTests.Describe(FromBson(doc)))} vs {DifferentialQueryTests.Describe(_f.Docs[0])}");
    }

    [Fact]
    public void Candidates_NameExistingRows() => s_candidates.AssertAllIn(s_rows.Keys);

    [Fact]
    public async Task Blql_InapplicableShapesAreListed()
    {
        var found = new Dictionary<string, string>();
        foreach (var query in DifferentialQueryTests.Catalogue.Keys)
        {
            var run = await _f.RunDynamic(PlainBlql, query);
            if (run.Inapplicable != null) found[query] = run.Inapplicable;
        }
        foreach (var (query, reason) in found.OrderBy(kv => kv.Key, StringComparer.Ordinal))
            _output.WriteLine($"INAPPLICABLE [blql] '{query}': {reason}");
        var unlisted = found.Keys.Except(s_blqlInapplicable.Keys).Order(StringComparer.Ordinal).ToList();
        var stale = s_blqlInapplicable.Keys.Except(found.Keys).Order(StringComparer.Ordinal).ToList();
        Assert.True(unlisted.Count == 0 && stale.Count == 0,
            $"BLQL cannot express [{string.Join(", ", unlisted.Select(q => $"'{q}' ({found[q]})"))}]: list them in s_blqlInapplicable with the reason; "
            + $"listed but expressible now: [{string.Join(", ", stale.Select(q => $"'{q}'"))}]");
    }

    [Theory]
    [MemberData(nameof(Rows))]
    public async Task Query_AgreesWithTheOracles(string name)
    {
        var row = s_rows[name];
        var qc = DifferentialQueryTests.Catalogue[row.Query];
        var run = await _f.RunDynamic(row.Way, row.Query);
        if (run.Inapplicable != null)
        {
            _output.WriteLine($"INAPPLICABLE [{row.Way.Name}: {run.Inapplicable}] query '{row.Query}'");
            s_candidates.Verify(name, null, _output);
            return;
        }
        _output.WriteLine($"{row.Way.Name}: {run.Plan}");

        string? failure = null;
        if (row.Oracle == Linq)
        {
            var error = qc.Compare(_f.Docs, run.Outcome);
            if (error != null)
                failure = $"seed {_f.Seed}: {row.Way.Name} query '{row.Query}' disagrees with LINQ-to-Objects: {error}\n  ran as: {run.Plan}";
        }
        else
        {
            var typed = await _f.RunTyped(row.Way.Indexed, row.Query);
            var expected = qc.Determined(typed);
            var actual = qc.Determined(run.Outcome);
            if (expected != actual)
            {
                var typedVsLinq = qc.Compare(_f.Docs, typed);
                failure = $"seed {_f.Seed}: {row.Way.Name} query '{row.Query}' disagrees with the typed {(row.Way.Indexed ? "indexed" : "plain")} collection:\n"
                          + $"  typed:   {expected}\n  dynamic: {actual}\n  ran as: {run.Plan}"
                          + (typedVsLinq == null ? "" : $"\n  the typed collection disagrees with LINQ-to-Objects here too: {typedVsLinq}");
            }
        }
        s_candidates.Verify(name, failure, _output);
    }

    // ── The query provider ────────────────────────────────────────────────────

    private sealed class InapplicableException(string reason) : Exception(reason);

    internal sealed class DynamicQueryable<T> : IOrderedQueryable<T>
    {
        private readonly DynamicProvider _provider;

        public DynamicQueryable(DynamicProvider provider) : this(provider, null) { }

        public DynamicQueryable(DynamicProvider provider, Expression? expression)
        {
            _provider = provider;
            Expression = expression ?? Expression.Constant(this);
        }

        public Type ElementType => typeof(T);
        public Expression Expression { get; }
        public IQueryProvider Provider => _provider;
        public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)_provider.Execute(Expression)!).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    internal sealed class DynamicProvider(DynamicExecutor executor) : IQueryProvider
    {
        public IQueryable CreateQuery(Expression expression)
        {
            var element = expression.Type.GetInterfaces().Append(expression.Type)
                .First(t => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IQueryable<>)).GetGenericArguments()[0];
            return (IQueryable)Activator.CreateInstance(typeof(DynamicQueryable<>).MakeGenericType(element), this, expression)!;
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression) => new DynamicQueryable<TElement>(this, expression);
        public object? Execute(Expression expression) => executor.Execute(expression);
        public TResult Execute<TResult>(Expression expression) => (TResult)executor.Execute(expression)!;
    }

    /// <summary>
    /// Runs a LINQ expression over <see cref="DiffDoc"/> on a <see cref="DynamicCollection"/>: the part
    /// the way expresses on the collection, the rest with LINQ-to-Objects over what it returned.
    /// </summary>
    internal sealed class DynamicExecutor(DynamicCollection collection, Way way)
    {
        private readonly List<string> _plan = new();

        public string Plan => string.Join("; ", _plan);

        public object? Execute(Expression expression)
        {
            var calls = new List<MethodCallExpression>();
            var e = expression;
            while (e is MethodCallExpression call && call.Method.DeclaringType == typeof(Queryable))
            {
                calls.Add(call);
                e = call.Arguments[0];
            }
            calls.Reverse();
            if (e is not ConstantExpression { Value: DynamicQueryable<DiffDoc> })
                throw new NotSupportedException($"not a query over the dynamic collection: {expression}");

            var shape = Shape.Of(calls);
            return way.Blql ? ExecuteBlql(calls, shape) : ExecuteApi(calls, shape);
        }

        // ── BLQL ──────────────────────────────────────────────────────────────

        private object? ExecuteBlql(List<MethodCallExpression> calls, Shape shape)
        {
            var parts = shape.Wheres.Select(w => ToBlql(w.Body, w.Parameters[0])).ToList();
            var filter = parts.Count switch { 0 => BlqlFilter.Empty, 1 => parts[0], _ => BlqlFilter.And(parts) };
            BlqlSort? sort = null;
            foreach (var (field, descending) in shape.Order)
                sort = sort == null ? BlqlSort.By(field, descending) : sort.ThenBy(field, descending);

            int skip = 0;
            int? take = null;
            foreach (var (isTake, n) in shape.Window)
            {
                if (n < 0) throw new InapplicableException($"BLQL rejects a negative {(isTake ? "Take" : "Skip")} ({n}), LINQ treats it as 0");
                if (isTake) take = take == null ? n : Math.Min(take.Value, n);
                else { skip += n; take = take == null ? null : Math.Max(0, take.Value - n); }
            }

            BlqlQuery Query(BlqlFilter f)
            {
                var q = collection.Query(f);
                if (sort != null) q.Sort(sort);
                if (skip > 0) q.Skip(skip);
                if (take != null) q.Take(take.Value);
                return q;
            }

            var described = $"blql {filter}{(sort != null ? $" sort {sort}" : "")}{(skip > 0 ? $" skip {skip}" : "")}{(take != null ? $" take {take}" : "")}";

            // A final Count, Any, First or FirstOrDefault, with its predicate added to the filter.
            if (shape.Terminal is { } terminal)
            {
                var name = terminal.Method.Name;
                BlqlFilter? withPredicate = filter;
                if (terminal.Arguments.Count == 2)
                {
                    var predicate = Lambda(terminal.Arguments[1]);
                    try
                    {
                        var translated = ToBlql(predicate.Body, predicate.Parameters[0]);
                        withPredicate = filter == BlqlFilter.Empty ? translated : BlqlFilter.And(filter, translated);
                    }
                    catch (InapplicableException) { withPredicate = null; }
                }
                // BLQL's Count and Any ignore Skip and Take (BlqlQuery.CountAsync): a windowed one runs in memory.
                if (withPredicate != null && (name is "First" or "FirstOrDefault" || shape.Window.Count == 0))
                {
                    var q = Query(withPredicate);
                    _plan.Add($"blql {withPredicate}{described[$"blql {filter}".Length..]} .{name}Async()");
                    return name switch
                    {
                        "Count" => q.CountAsync().GetAwaiter().GetResult(),
                        "LongCount" => (long)q.CountAsync().GetAwaiter().GetResult(),
                        "Any" => q.AnyAsync().GetAwaiter().GetResult(),
                        "First" => FromBson(q.FirstAsync().GetAwaiter().GetResult()),
                        _ => q.FirstOrDefaultAsync().GetAwaiter().GetResult() is { } doc ? FromBson(doc) : null,
                    };
                }
            }

            _plan.Add(described);
            var docs = ToList(Query(filter).AsAsyncEnumerable());
            return InMemory(calls, shape.WindowEnd, docs);
        }

        // ── QueryIndexAsync / FindAsync ───────────────────────────────────────

        private object? ExecuteApi(List<MethodCallExpression> calls, Shape shape)
        {
            // A predicate of the final Count/Any/First filters like a Where (it stays on the terminal too).
            var filters = shape.Wheres.ToList();
            if (shape.Terminal is { Arguments.Count: 2 } terminal)
                filters.Add((Expression<Func<DiffDoc, bool>>)Lambda(terminal.Arguments[1]));
            var predicates = filters.Select(w => w.Compile()).ToList();
            bool Matches(DiffDoc d) => predicates.All(p => p(d));

            var bounds = way.Indexed ? Bounds.Of(filters) : null;
            // The index can give the order when the first key is an indexed field and any second one is the id.
            var orderField = way.Indexed && shape.Order.Count > 0 && shape.Order[0].Field != "_id"
                             && (shape.Order.Count == 1 || shape.Order is [_, ("_id", false)])
                ? shape.Order[0].Field : null;
            if (orderField != null && bounds != null && bounds.Field != orderField) orderField = null;

            List<DiffDoc> docs;
            int rest;
            if (orderField != null || bounds != null)
            {
                var field = orderField ?? bounds!.Field;
                var ascending = orderField == null || !shape.Order[0].Descending;
                var (min, max) = bounds != null ? (bounds.Min, bounds.Max) : (null, null);
                _plan.Add($"QueryIndexAsync(\"{IndexName(field)}\", {Text(min)}, {Text(max)}, ascending: {ascending.ToString().ToLowerInvariant()})"
                          + (filters.Count > 0 ? " with the filter as residual" : "") + (orderField != null ? ", in index order" : ""));
                docs = ToList(collection.QueryIndexAsync(IndexName(field), min, max, ascending)).Where(Matches).ToList();
                if (orderField != null)
                {
                    if (shape.Order.Count == 2) docs = TiesById(docs, shape.OrderKey!);
                    rest = shape.OrderEnd;
                }
                else rest = shape.WhereEnd;
            }
            else
            {
                _plan.Add(filters.Count > 0 ? "FindAsync(filter)" : "FindAllAsync()");
                docs = filters.Count > 0 ? ToList(collection.FindAsync(b => Matches(FromBson(b)))) : ToList(collection.FindAllAsync());
                rest = shape.WhereEnd;
            }
            return InMemory(calls, rest, docs);
        }

        /// <summary>Within each run of equal keys (in the index's order), the documents by id.</summary>
        private static List<DiffDoc> TiesById(List<DiffDoc> docs, Func<DiffDoc, object?> key)
        {
            var result = new List<DiffDoc>(docs.Count);
            for (int i = 0; i < docs.Count;)
            {
                int j = i + 1;
                while (j < docs.Count && Equals(Normal(key(docs[j])), Normal(key(docs[i])))) j++;
                result.AddRange(docs.Skip(i).Take(j - i).OrderBy(d => d.Id));
                i = j;
            }
            return result;
        }

        private static object? Normal(object? key) => key switch
        {
            double d => d == 0 ? 0.0 : d,
            decimal m => m / 1.0000000000000000000000000000m,
            _ => key,
        };

        // ── The rest, in memory ───────────────────────────────────────────────

        /// <summary>Applies <paramref name="calls"/> from <paramref name="from"/> on with LINQ-to-Objects over <paramref name="docs"/>.</summary>
        private object? InMemory(List<MethodCallExpression> calls, int from, List<DiffDoc> docs)
        {
            Expression e = Expression.Constant(docs.AsQueryable(), typeof(IQueryable<DiffDoc>));
            for (int i = from; i < calls.Count; i++)
                e = Expression.Call(calls[i].Method, calls[i].Arguments.Skip(1).Prepend(e));
            if (from < calls.Count)
                _plan.Add("in memory: " + string.Join(", ", calls.Skip(from).Select(c => c.Method.Name)));
            try
            {
                return Expression.Lambda(e).Compile().DynamicInvoke();
            }
            catch (TargetInvocationException ex) when (ex.InnerException != null)
            {
                ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                throw;
            }
        }

        private static List<DiffDoc> ToList(IAsyncEnumerable<BsonDocument> docs)
        {
            var list = new List<DiffDoc>();
            var e = docs.GetAsyncEnumerator();
            try
            {
                while (e.MoveNextAsync().AsTask().GetAwaiter().GetResult()) list.Add(FromBson(e.Current));
            }
            finally { e.DisposeAsync().AsTask().GetAwaiter().GetResult(); }
            return list;
        }

        private static string Text(object? value) => value switch
        {
            null => "null",
            DBNull => "DBNull",
            string s => $"\"{s}\"",
            _ => DifferentialQueryTests.Describe(value) + (value is int or bool ? "" : $" ({value.GetType().Name})"),
        };
    }

    // ── The shape of a query ──────────────────────────────────────────────────

    /// <summary>
    /// A query split into its leading <c>Where</c>s, an <c>OrderBy</c> with its <c>ThenBy</c>s, a run
    /// of <c>Skip</c>/<c>Take</c>, and what follows; <see cref="Terminal"/> is a final
    /// Count/LongCount/Any/First/FirstOrDefault right after them.
    /// </summary>
    private sealed class Shape
    {
        public List<Expression<Func<DiffDoc, bool>>> Wheres { get; } = new();
        public List<(string Field, bool Descending)> Order { get; } = new();
        public Func<DiffDoc, object?>? OrderKey { get; private set; }
        public List<(bool Take, int N)> Window { get; } = new();
        public int WhereEnd { get; private set; }
        public int OrderEnd { get; private set; }
        public int WindowEnd { get; private set; }
        public MethodCallExpression? Terminal { get; private set; }

        public static Shape Of(List<MethodCallExpression> calls)
        {
            var s = new Shape();
            int i = 0;
            while (i < calls.Count && calls[i].Method.Name == "Where" && calls[i].Method.GetGenericArguments()[0] == typeof(DiffDoc))
                s.Wheres.Add((Expression<Func<DiffDoc, bool>>)Lambda(calls[i++].Arguments[1]));
            s.WhereEnd = i;

            if (i < calls.Count && calls[i].Method.Name is "OrderBy" or "OrderByDescending" && calls[i].Method.GetGenericArguments()[0] == typeof(DiffDoc))
            {
                int j = i;
                var keys = new List<(string, bool)>();
                while (j < calls.Count && (j == i ? calls[j].Method.Name is "OrderBy" or "OrderByDescending" : calls[j].Method.Name is "ThenBy" or "ThenByDescending"))
                {
                    var key = Lambda(calls[j].Arguments[1]);
                    if (FieldOf(key.Body, key.Parameters[0]) is not { } field) break;
                    keys.Add((field, calls[j].Method.Name.EndsWith("Descending")));
                    if (j == i) { var compiled = key.Compile(); s.OrderKey = d => compiled.DynamicInvoke(d); }
                    j++;
                }
                // Only an order whose every key is a field is taken; otherwise it stays in memory.
                if (j > i && (j == calls.Count || calls[j].Method.Name is not ("ThenBy" or "ThenByDescending")))
                {
                    s.Order.AddRange(keys);
                    i = j;
                }
                else s.OrderKey = null;
            }
            s.OrderEnd = i;

            while (i < calls.Count && calls[i].Method.Name is "Skip" or "Take" && calls[i].Arguments[1].Type == typeof(int)
                   && calls[i].Method.GetGenericArguments()[0] == typeof(DiffDoc))
                s.Window.Add((calls[i].Method.Name == "Take", (int)Eval(calls[i++].Arguments[1])!));
            s.WindowEnd = i;

            if (i == calls.Count - 1 && calls[i].Method.Name is "Count" or "LongCount" or "Any" or "First" or "FirstOrDefault"
                && calls[i].Method.GetGenericArguments()[0] == typeof(DiffDoc)
                && (calls[i].Arguments.Count == 1 || calls[i].Arguments[1] is UnaryExpression { NodeType: ExpressionType.Quote }))
                s.Terminal = calls[i];
            return s;
        }
    }

    /// <summary>An index range on one field: the first field the filter bounds with a comparison, the first bound on each side.</summary>
    private sealed record Bounds(string Field, object? Min, object? Max)
    {
        public static Bounds? Of(List<Expression<Func<DiffDoc, bool>>> wheres)
        {
            var found = new List<(string Field, ExpressionType Op, object? Value)>();
            foreach (var w in wheres) Collect(w.Body, w.Parameters[0], found);
            if (found.Count == 0) return null;
            var field = found[0].Field;
            object? min = null, max = null;
            foreach (var (f, op, value) in found.Where(x => x.Field == field))
            {
                if (op == ExpressionType.Equal)
                {
                    var v = value ?? DBNull.Value;
                    min ??= v;
                    max ??= v;
                }
                else if (op is ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual) min ??= value;
                else max ??= value;
            }
            return new Bounds(field, min, max);
        }

        private static void Collect(Expression e, ParameterExpression p, List<(string, ExpressionType, object?)> found)
        {
            switch (e)
            {
                case BinaryExpression { NodeType: ExpressionType.AndAlso } and:
                    Collect(and.Left, p, found);
                    Collect(and.Right, p, found);
                    break;
                case MemberExpression m when m.Type == typeof(bool) && FieldOf(m, p) is { } boolField:
                    found.Add((boolField, ExpressionType.Equal, true));
                    break;
                case UnaryExpression { NodeType: ExpressionType.Not, Operand: MemberExpression m } when m.Type == typeof(bool) && FieldOf(m, p) is { } notField:
                    found.Add((notField, ExpressionType.Equal, false));
                    break;
                case BinaryExpression b when b.NodeType is ExpressionType.Equal or ExpressionType.LessThan or ExpressionType.LessThanOrEqual
                                                 or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual:
                {
                    var (field, op, value) = Comparison(b, p);
                    if (field != null && field != "_id" && value is not Inapplicable) found.Add((field, op, IndexValue(value)));
                    break;
                }
            }
        }

        // QueryIndexAsync takes the value as the caller has it; an enum as the Int32 it is stored as.
        private static object? IndexValue(object? value) => value is Enum ? Convert.ToInt32(value) : value;
    }

    // ── LINQ predicate → BLQL ─────────────────────────────────────────────────

    private sealed class Inapplicable
    {
        public static readonly Inapplicable Value = new();
    }

    private static LambdaExpression Lambda(Expression e) => (LambdaExpression)(e is UnaryExpression { NodeType: ExpressionType.Quote } q ? q.Operand : e);

    private static object? Eval(Expression e)
        => Expression.Lambda<Func<object?>>(Expression.Convert(e, typeof(object))).Compile()();

    private static Expression StripConvert(Expression e)
    {
        while (e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u) e = u.Operand;
        return e;
    }

    /// <summary>The stored field <paramref name="e"/> reads, through conversions; null if it is not a plain property of the document.</summary>
    private static string? FieldOf(Expression e, ParameterExpression p)
        => StripConvert(e) is MemberExpression { Expression: ParameterExpression owner } m && owner == p && s_fields.TryGetValue(m.Member.Name, out var f) ? f : null;

    private static bool DependsOn(Expression e, ParameterExpression p)
    {
        var finder = new ParameterFinder(p);
        finder.Visit(e);
        return finder.Found;
    }

    private sealed class ParameterFinder(ParameterExpression p) : ExpressionVisitor
    {
        public bool Found { get; private set; }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == p) Found = true;
            return node;
        }
    }

    private static ExpressionType Flip(ExpressionType op) => op switch
    {
        ExpressionType.LessThan => ExpressionType.GreaterThan,
        ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
        ExpressionType.GreaterThan => ExpressionType.LessThan,
        ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
        _ => op,
    };

    /// <summary>A comparison as (field, operator with the field on the left, constant); field null or value <see cref="Inapplicable"/> when it is not one.</summary>
    private static (string? Field, ExpressionType Op, object? Value) Comparison(BinaryExpression b, ParameterExpression p)
    {
        bool left = DependsOn(b.Left, p), right = DependsOn(b.Right, p);
        if (left == right) return (null, b.NodeType, Inapplicable.Value);
        var (fieldSide, valueSide, op) = left ? (b.Left, b.Right, b.NodeType) : (b.Right, b.Left, Flip(b.NodeType));
        var field = FieldOf(fieldSide, p);
        return field == null ? (null, op, Inapplicable.Value) : (field, op, Eval(valueSide));
    }

    private static BlqlFilter ToBlql(Expression e, ParameterExpression p)
    {
        switch (e)
        {
            case BinaryExpression { NodeType: ExpressionType.AndAlso } and:
                return BlqlFilter.And(ToBlql(and.Left, p), ToBlql(and.Right, p));
            case BinaryExpression { NodeType: ExpressionType.OrElse } or:
                return BlqlFilter.Or(ToBlql(or.Left, p), ToBlql(or.Right, p));
            case UnaryExpression { NodeType: ExpressionType.Not } not:
                return not.Operand.Type == typeof(bool) && FieldOf(not.Operand, p) is { } notField
                    ? BlqlFilter.Eq(notField, false)
                    : BlqlFilter.Not(ToBlql(not.Operand, p));
            case ConstantExpression { Value: bool constant }:
                return constant ? BlqlFilter.Empty : BlqlFilter.Nor(BlqlFilter.Empty);
            case MemberExpression { Member.Name: "HasValue" } has when has.Expression != null && FieldOf(has.Expression, p) is { } hasField:
                return BlqlFilter.Ne(hasField, BsonValue.Null);
            case MemberExpression m when m.Type == typeof(bool) && FieldOf(m, p) is { } boolField:
                return BlqlFilter.Eq(boolField, true);
            case BinaryExpression { NodeType: ExpressionType.Equal, Left: var l, Right: var r }
                when StripConvert(l) is BinaryExpression { NodeType: ExpressionType.Modulo } mod && FieldOf(mod.Left, p) is { } modField
                     && !DependsOn(mod.Right, p) && !DependsOn(r, p):
                return BlqlFilter.Mod(modField, Convert.ToInt64(Eval(mod.Right)), Convert.ToInt64(Eval(r)));
            case BinaryExpression b when b.NodeType is ExpressionType.Equal or ExpressionType.NotEqual or ExpressionType.LessThan
                                             or ExpressionType.LessThanOrEqual or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual:
            {
                if (DependsOn(b.Left, p) && DependsOn(b.Right, p))
                    throw new InapplicableException($"'{b}' compares two fields of the document; BLQL compares a field with a constant");
                var (field, op, value) = Comparison(b, p);
                if (value is Inapplicable)
                    throw new InapplicableException($"'{(DependsOn(b.Left, p) ? b.Left : b.Right)}' is not a stored field; BLQL has no operator for it");
                var v = ToBson(value);
                return op switch
                {
                    ExpressionType.Equal => BlqlFilter.Eq(field!, v),
                    ExpressionType.NotEqual => BlqlFilter.Ne(field!, v),
                    ExpressionType.LessThan => BlqlFilter.Lt(field!, v),
                    ExpressionType.LessThanOrEqual => BlqlFilter.Lte(field!, v),
                    ExpressionType.GreaterThan => BlqlFilter.Gt(field!, v),
                    _ => BlqlFilter.Gte(field!, v),
                };
            }
            case MethodCallExpression call:
                return CallToBlql(call, p);
        }
        throw new InapplicableException($"'{e}' has no BLQL operator");
    }

    private static BlqlFilter CallToBlql(MethodCallExpression call, ParameterExpression p)
    {
        // field.StartsWith / EndsWith / Contains(constant)
        if (call.Method.DeclaringType == typeof(string) && call.Object != null && call.Arguments.Count == 1
            && FieldOf(call.Object, p) is { } stringField && !DependsOn(call.Arguments[0], p))
        {
            var arg = (string)Eval(call.Arguments[0])!;
            switch (call.Method.Name)
            {
                case "StartsWith": return BlqlFilter.StartsWith(stringField, arg);
                case "EndsWith": return BlqlFilter.EndsWith(stringField, arg);
                case "Contains": return BlqlFilter.Contains(stringField, arg);
            }
        }
        if (call.Method.DeclaringType == typeof(string) && call.Method.Name == "IsNullOrEmpty" && FieldOf(call.Arguments[0], p) is { } emptyField)
            return BlqlFilter.Or(BlqlFilter.Eq(emptyField, BsonValue.Null), BlqlFilter.Eq(emptyField, ""));

        // list.Contains(field): Enumerable.Contains, MemoryExtensions.Contains over the span of an array, List<T>.Contains
        if (call.Method.Name == "Contains")
        {
            var (list, item) = call.Object != null ? (call.Object, call.Arguments[0]) : (call.Arguments[0], call.Arguments[1]);
            if (list is MethodCallExpression { Method.Name: "op_Implicit" } span) list = span.Arguments[0];
            list = StripConvert(list);
            if (FieldOf(item, p) is { } inField && !DependsOn(list, p))
                return BlqlFilter.In(inField, ((IEnumerable)Eval(list)!).Cast<object?>().Select(ToBson));
        }
        throw new InapplicableException($"'{call}' has no BLQL operator");
    }

    private static BsonValue ToBson(object? value) => value switch
    {
        null => BsonValue.Null,
        int i => BsonValue.FromInt32(i),
        long l => BsonValue.FromInt64(l),
        double d => BsonValue.FromDouble(d),
        decimal m => BsonValue.FromDecimal(m),
        string s => BsonValue.FromString(s),
        bool b => BsonValue.FromBoolean(b),
        DateTime t => BsonValue.FromDateTime(t),
        ObjectId o => BsonValue.FromObjectId(o),
        Enum en => BsonValue.FromInt32(Convert.ToInt32(en)),
        _ => throw new InapplicableException($"a constant of type {value.GetType().Name} has no BSON form here"),
    };
}
