using System.Linq.Expressions;
using BLite.Core;
using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Only a <c>Where</c> on the main chain with nothing but <c>Where</c> and orderings below it is
/// folded into the WHERE the provider fetches with. A <c>Where</c> after an operator that changes the
/// elements or depends on their order, and any clause of another sequence (a <c>Join</c> inner, a
/// <c>Concat</c> argument), is left to the in-memory replay (#186).
/// </summary>
public class LinqWhereFoldingTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;
    private readonly List<PersonV2> _people;
    private readonly List<PersonV2> _scan;

    public LinqWhereFoldingTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"where_folding_{Guid.NewGuid():N}.db");
        _db = new TestDbContext(_dbPath);
        _people = Enumerable.Range(0, 300).Select(i => new PersonV2 { Name = $"p{i:000}", Age = i % 50 }).ToList();
        _db.PeopleV2.InsertBulkAsync(_people).GetAwaiter().GetResult();

        // Order-dependent operators (Take, SkipWhile, the indexed Where) see the documents in the
        // order a full scan returns them, which is not the insertion order.
        _scan = _db.PeopleV2.AsQueryable().ToList();
    }

    public void Dispose()
    {
        _db.Dispose();
        foreach (var f in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
            try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
    }

    private static QueryModel Model(Expression expression)
    {
        var visitor = new BTreeExpressionVisitor();
        visitor.Visit(expression);
        return visitor.GetModel();
    }

    private IQueryable<PersonV2> Q => _db.PeopleV2.AsQueryable();
    private IQueryable<PersonV2> L => _scan.AsQueryable();
    private IQueryable<VectorEntity> V => _db.VectorItems.AsQueryable();

    private static List<string> Names(IEnumerable<PersonV2> people) => people.Select(p => p.Name).ToList();

    // ── Where after an operator that changes the elements or depends on order ──

    [Fact]
    public void WhereAfterGroupBy_FiltersTheGroups()
    {
        Assert.Equal(50, L.GroupBy(p => p.Age).Where(g => g.Count() > 5).Count());
        Assert.Equal(50, Q.GroupBy(p => p.Age).Where(g => g.Count() > 5).Count());
        Assert.Equal(L.GroupBy(p => p.Age).Where(g => g.Key < 3).Select(g => g.Key).OrderBy(k => k).ToList(),
                     Q.GroupBy(p => p.Age).Where(g => g.Key < 3).Select(g => g.Key).OrderBy(k => k).ToList());
    }

    [Fact]
    public void IndexedWhere_UsesThePositionInTheSequence()
    {
        Assert.Equal(Names(L.Where((p, i) => i < 10)), Names(Q.Where((p, i) => i < 10).ToList()));
        Assert.Equal(Names(L.Where(p => p.Age == 5).Where((p, i) => i < 2)),
                     Names(Q.Where(p => p.Age == 5).Where((p, i) => i < 2).ToList()));
    }

    [Fact]
    public void WhereAfterSkipWhile_FiltersWhatSkipWhileLeaves()
    {
        Assert.Equal(Names(L.SkipWhile(p => p.Age < 10).Where(p => p.Age == 5)),
                     Names(Q.SkipWhile(p => p.Age < 10).Where(p => p.Age == 5).ToList()));
        Assert.Equal(Names(L.TakeWhile(p => p.Age < 10).Where(p => p.Age == 5)),
                     Names(Q.TakeWhile(p => p.Age < 10).Where(p => p.Age == 5).ToList()));
    }

    [Fact]
    public void WhereAfterTakeOrSkip_FiltersTheWindow()
    {
        Assert.Equal(Names(L.Take(10).Where(p => p.Age == 5)), Names(Q.Take(10).Where(p => p.Age == 5).ToList()));
        Assert.Equal(L.Take(10).Count(p => p.Age == 5), Q.Take(10).Where(p => p.Age == 5).Count());
        Assert.Equal(Names(L.Skip(280).Where(p => p.Age == 45)), Names(Q.Skip(280).Where(p => p.Age == 45).ToList()));
        Assert.Equal(Names(L.Where(p => p.Name.EndsWith("5")).Take(10).Where(p => p.Age == 5)),
                     Names(Q.Where(p => p.Name.EndsWith("5")).Take(10).Where(p => p.Age == 5).ToList()));
        Assert.Equal(Names(L.OrderBy(p => p.Age).Take(10).Where(p => p.Name.EndsWith("0"))),
                     Names(Q.OrderBy(p => p.Age).Take(10).Where(p => p.Name.EndsWith("0")).ToList()));
        Assert.Equal(L.Where(p => p.Name.EndsWith("7")).Skip(3).First(p => p.Age == 7).Name,
                     Q.Where(p => p.Name.EndsWith("7")).Skip(3).Where(p => p.Age == 7).First().Name);
    }

    [Fact]
    public void WhereAfterOfTypeOrCast_FiltersTheConvertedElements()
    {
        Assert.Equal(6, Q.OfType<PersonV2>().Where(p => p.Age == 5).Count());
        Assert.Equal(6, Q.Cast<object>().Where(o => ((PersonV2)o).Age == 5).Count());
        Assert.Equal(6, Q.OfType<object>().Where(o => ((PersonV2)o).Age == 5).ToList().Count);
    }

    [Fact]
    public void WhereAfterSelectManyJoinOrZip_FiltersTheirResults()
    {
        Assert.Equal(L.SelectMany(p => new[] { p.Age, p.Age + 100 }).Where(a => a == 105).Count(),
                     Q.SelectMany(p => new[] { p.Age, p.Age + 100 }).Where(a => a == 105).Count());
        Assert.Equal(L.Join(new[] { 5, 6 }, p => p.Age, a => a, (p, a) => a * 10).Where(x => x == 50).Count(),
                     Q.Join(new[] { 5, 6 }, p => p.Age, a => a, (p, a) => a * 10).Where(x => x == 50).Count());
        Assert.Equal(L.Zip(Enumerable.Range(0, 300), (p, i) => p.Age + i).Where(x => x < 10).Count(),
                     Q.Zip(Enumerable.Range(0, 300), (p, i) => p.Age + i).Where(x => x < 10).Count());
        Assert.Equal(L.Select(p => p.Age).Where(a => a == 5).Count(),
                     Q.Select(p => p.Age).Where(a => a == 5).ToList().Count);
    }

    // ── Clauses of another sequence ───────────────────────────────────────────

    private async Task InsertAABAsync()
    {
        await _db.VectorItems.InsertAsync(new VectorEntity { Title = "A", Embedding = [0f, 0f, 0f] });
        await _db.VectorItems.InsertAsync(new VectorEntity { Title = "A", Embedding = [1f, 0f, 0f] });
        await _db.VectorItems.InsertAsync(new VectorEntity { Title = "B", Embedding = [2f, 0f, 0f] });
    }

    [Fact]
    public async Task WhereOnJoinInner_DoesNotFilterTheOuterSequence()
    {
        await InsertAABAsync();

        var result = V.Where(x => x.Title == "B")
            .Join(V.Where(y => y.Title == "A"), x => 1, y => 1, (x, y) => x.Title + "/" + y.Title)
            .ToList();

        Assert.Equal(["B/A", "B/A"], result);
    }

    [Fact]
    public async Task VectorSearchOnJoinInner_IsServedByTheIndex()
    {
        await InsertAABAsync();
        float[] query = [0f, 0f, 0f];

        var result = V.Where(x => x.Title == "B")
            .Join(V.Where(y => y.Embedding.VectorSearch(query, 1)), x => 1, y => 1, (x, y) => x.Title + "/" + y.Title)
            .ToList();

        Assert.Equal(["B/A"], result);
    }

    [Fact]
    public void OrderingAndPagingOfAnotherSequence_DoNotShapeTheOuterSequence()
    {
        Assert.Equal(L.Where(p => p.Name.EndsWith("05")).Join(L.OrderBy(p => p.Name).Take(3), p => 1, p => 1, (a, b) => a.Name + "/" + b.Name).ToList(),
                     Q.Where(p => p.Name.EndsWith("05")).Join(Q.OrderBy(p => p.Name).Take(3), p => 1, p => 1, (a, b) => a.Name + "/" + b.Name).ToList());
        Assert.Equal(L.Where(p => p.Name.EndsWith("05")).Join(L.OrderByDescending(p => p.Name).Skip(2).Take(3), p => 1, p => 1, (a, b) => a.Name + "/" + b.Name).ToList(),
                     Q.Where(p => p.Name.EndsWith("05")).Join(Q.OrderByDescending(p => p.Name).Skip(2).Take(3), p => 1, p => 1, (a, b) => a.Name + "/" + b.Name).ToList());
        Assert.Equal(L.Concat(L.Where(p => p.Age == 1).Take(2)).Count(),
                     Q.Concat(Q.Where(p => p.Age == 1).Take(2)).Count());
        Assert.Equal(Names(L.Concat(L.OrderBy(p => p.Name).Take(2))),
                     Names(Q.Concat(Q.OrderBy(p => p.Name).Take(2)).ToList()));
    }

    // ── Query model ──────────────────────────────────────────────────────────

    [Fact]
    public void Model_WhereOnTheRootChain_StillFolds()
    {
        var chained = Model(Q.Where(p => p.Age > 3).Where(p => p.Age < 10).Expression);
        Assert.NotNull(chained.WhereClause);
        Assert.False(chained.HasComplexOperators);
        Assert.Equal(ExpressionType.AndAlso, chained.WhereClause!.Body.NodeType);

        var topN = Model(Q.Where(p => p.Age > 3).OrderBy(p => p.Name).Take(5).Expression);
        Assert.NotNull(topN.WhereClause);
        Assert.NotNull(topN.OrderByClause);
        Assert.Equal(5, topN.Take);
        Assert.False(topN.HasComplexOperators);

        var afterOrdering = Model(Q.OrderBy(p => p.Name).ThenBy(p => p.Age).Where(p => p.Age == 3).Expression);
        Assert.NotNull(afterOrdering.WhereClause);

        var count = Model(Expression.Call(typeof(Queryable), "Count", [typeof(PersonV2)], Q.Where(p => p.Age == 3).Expression));
        Assert.NotNull(count.WhereClause);
        Assert.True(count.IsCountOnly);
        Assert.False(count.HasComplexOperators);
    }

    [Fact]
    public void Model_WhereAfterABlockingOperator_IsNotFolded()
    {
        var shapes = new Expression[]
        {
            Q.GroupBy(p => p.Age).Where(g => g.Count() > 5).Expression,
            Q.Where((p, i) => i < 10).Expression,
            Q.SkipWhile(p => p.Age < 10).Where(p => p.Age == 5).Expression,
            Q.Take(10).Where(p => p.Age == 5).Expression,
            Q.Skip(10).Where(p => p.Age == 5).Expression,
            Q.OfType<PersonV2>().Where(p => p.Age == 5).Expression,
            Q.Cast<object>().Where(o => o != null).Expression,
            Q.Select(p => p.Age).Where(a => a == 5).Expression,
        };

        foreach (var shape in shapes)
        {
            var model = Model(shape);
            Assert.Null(model.WhereClause);
            Assert.True(model.HasComplexOperators);
        }

        // The Where below the blocking operator still folds; the one above it does not.
        var mixed = Model(Q.Where(p => p.Age < 10).Take(30).Where(p => p.Age == 5).Expression);
        Assert.NotNull(mixed.WhereClause);
        Assert.True(mixed.HasComplexOperators);
        Assert.Equal(ExpressionType.LessThan, mixed.WhereClause!.Body.NodeType);
    }

    [Fact]
    public void Model_ClausesOfAnotherSequence_AreIgnored()
    {
        var join = Model(Q.Join(Q.Where(p => p.Age == 1).OrderBy(p => p.Name).Skip(1).Take(3),
                                p => 1, p => 1, (a, b) => a.Name + b.Name).Expression);
        Assert.Null(join.WhereClause);
        Assert.Null(join.OrderByClause);
        Assert.Null(join.Take);
        Assert.Null(join.Skip);
        Assert.Null(join.SelectClause);
        Assert.True(join.HasComplexOperators);

        var outerWhere = Model(Q.Where(p => p.Age == 2).Concat(Q.Where(p => p.Age == 1).Take(2)).Expression);
        Assert.NotNull(outerWhere.WhereClause);
        Assert.Equal(ExpressionType.Equal, outerWhere.WhereClause!.Body.NodeType);
        Assert.Null(outerWhere.Take);
    }

    // ── Folded Where still uses the index; unfolded VectorSearch still throws ──

    [Fact]
    public void FoldableShapes_MatchLinqToObjects()
    {
        // An index scan returns the matches in key order, so compare them as sets.
        Assert.Equal(Names(L.Where(p => p.Age > 3).Where(p => p.Age < 6)).Order(),
                     Names(Q.Where(p => p.Age > 3).Where(p => p.Age < 6).ToList()).Order());
        Assert.Equal(Names(L.Where(p => p.Age > 3).OrderBy(p => p.Name).Take(5)),
                     Names(Q.Where(p => p.Age > 3).OrderBy(p => p.Name).Take(5).ToList()));
        Assert.Equal(L.Where(p => p.Age == 7).Count(), Q.Where(p => p.Age == 7).Count());
    }

    [Fact]
    public async Task VectorSearchInAnUnfoldedWhere_Throws()
    {
        await InsertAABAsync();
        float[] query = [0f, 0f, 0f];

        Assert.Throws<NotSupportedException>(() => V.Take(2).Where(x => x.Embedding.VectorSearch(query, 1)).ToList());
        Assert.Throws<NotSupportedException>(() => V.SkipWhile(x => x.Title == "B").Where(x => x.Embedding.VectorSearch(query, 1)).Count());

        // Folded, it is served by the index.
        Assert.Equal("A", V.OrderBy(x => x.Title).Where(x => x.Embedding.VectorSearch(query, 1)).Single().Title);
    }
}
