using System.Linq.Expressions;
using BLite.Bson;
using BLite.Core.Indexing;
using BLite.Core.Query;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Regression tests for issue #249: <c>IndexKey(double)</c> gave -0.0 and +0.0 different keys, so
/// an index on a double missed -0.0 for <c>== 0.0</c> and <c>&gt;= 0.0</c> and found it for
/// <c>&lt; 0.0</c>, unlike IEEE 754 equality, which C# and the plain collection use. Each query must
/// agree with LINQ-to-Objects over the same documents, with and without the indexes.
/// </summary>
public class SignedZeroIndexTests : IAsyncLifetime
{
    private readonly string _indexedPath = Path.Combine(Path.GetTempPath(), $"sz_indexed_{Guid.NewGuid():N}.db");
    private readonly string _plainPath = Path.Combine(Path.GetTempPath(), $"sz_plain_{Guid.NewGuid():N}.db");
    private readonly string _legacyPath = Path.Combine(Path.GetTempPath(), $"sz_legacy_{Guid.NewGuid():N}.db");
    private SzIndexedContext _indexed = null!;
    private SzPlainContext _plain = null!;

    private static SzDoc[] Seed() =>
    [
        new SzDoc { Name = "pz", Ratio = 0.0, Level = 0f },
        new SzDoc { Name = "a", Ratio = -2.5, Level = -2.5f },
        new SzDoc { Name = "nz", Ratio = -0.0, Level = -0f },
        new SzDoc { Name = "b", Ratio = 1.5, Level = 1.5f },
        new SzDoc { Name = "c", Ratio = -1.0, Level = -1f },
        new SzDoc { Name = "d", Ratio = 3.0, Level = 3f },
    ];

    public async Task InitializeAsync()
    {
        _indexed = new SzIndexedContext(_indexedPath);
        _plain = new SzPlainContext(_plainPath);
        await _indexed.Docs.InsertBulkAsync(Seed());
        await _plain.Docs.InsertBulkAsync(Seed());
    }

    public Task DisposeAsync()
    {
        _indexed.Dispose();
        _plain.Dispose();
        foreach (var path in new[] { _indexedPath, _plainPath, _legacyPath })
        {
            if (File.Exists(path)) File.Delete(path);
            var wal = Path.ChangeExtension(path, ".wal");
            if (File.Exists(wal)) File.Delete(wal);
        }
        return Task.CompletedTask;
    }

    [Fact]
    public void IndexKey_Of_NegativeZero_Is_The_Key_Of_PositiveZero()
    {
        Assert.Equal(new IndexKey(0.0), new IndexKey(-0.0));
        Assert.True(new IndexKey(-double.Epsilon) < new IndexKey(-0.0));
        Assert.True(new IndexKey(-0.0) < new IndexKey(double.Epsilon));
    }

    [Fact]
    public async Task Equality_Finds_Both_Zeros()
    {
        await AssertAgreesAsync(x => x.Ratio == 0.0, "nz", "pz");
        await AssertAgreesAsync(x => x.Ratio == -0.0, "nz", "pz");
        await AssertAgreesAsync(x => x.Level == 0f, "nz", "pz");
    }

    [Fact]
    public async Task Ranges_Treat_Both_Zeros_Alike()
    {
        await AssertAgreesAsync(x => x.Ratio >= 0.0, "b", "d", "nz", "pz");
        await AssertAgreesAsync(x => x.Ratio > -0.0, "b", "d");
        await AssertAgreesAsync(x => x.Ratio < 0.0, "a", "c");
        await AssertAgreesAsync(x => x.Ratio <= -0.0, "a", "c", "nz", "pz");
        await AssertAgreesAsync(x => x.Ratio >= -1.0 && x.Ratio <= 0.0, "c", "nz", "pz");
    }

    [Fact]
    public async Task Count_Finds_Both_Zeros()
    {
        foreach (var docs in new[] { _indexed.Docs, _plain.Docs })
        {
            Assert.Equal(2, await docs.AsQueryable().CountAsync(x => x.Ratio == 0.0));
            Assert.Equal(4, await docs.AsQueryable().CountAsync(x => x.Ratio >= 0.0));
            Assert.Equal(2, await docs.AsQueryable().CountAsync(x => x.Ratio < 0.0));
        }
    }

    [Fact]
    public async Task OrderBy_Keeps_Both_Zeros_Together()
    {
        foreach (var (label, docs) in new[] { ("indexed", _indexed.Docs), ("plain", _plain.Docs) })
        {
            var all = await docs.FindAllAsync().ToListAsync();

            // The two zeros are equal, so their order within the window is not defined; the window
            // over them holds both.
            var window = (await docs.AsQueryable().OrderBy(x => x.Ratio).Skip(2).Take(2).ToListAsync()).Select(x => x.Name);
            Assert.Equal(["nz", "pz"], window.Order(StringComparer.Ordinal));

            var nonNegative = (await docs.AsQueryable().Where(x => x.Ratio >= 0.0).OrderBy(x => x.Ratio).ToListAsync())
                .Select(x => x.Ratio).ToArray();
            Assert.Equal(4, nonNegative.Length);
            Assert.True(nonNegative.Zip(new[] { 0.0, 0.0, 1.5, 3.0 }).All(p => p.First == p.Second), label);

            var descending = (await docs.AsQueryable().OrderByDescending(x => x.Ratio).ToListAsync()).Select(x => x.Ratio).ToArray();
            Assert.True(descending.Zip(all.OrderByDescending(x => x.Ratio).Select(x => x.Ratio)).All(p => p.First == p.Second), label);
        }
    }

    /// <summary>
    /// An index written before the fix keeps -0.0 under its former key, which the new bounds never
    /// reach and which an update or delete of the document no longer removes. Opening the
    /// collection rebuilds such an index; one without a -0.0 key is left alone.
    /// </summary>
    [Fact]
    public async Task Index_Holding_The_Former_NegativeZero_Key_Is_Rebuilt_On_Open()
    {
        uint levelRoot;
        using (var db = new SzIndexedContext(_legacyPath))
        {
            await db.Docs.InsertBulkAsync(Seed());
            var index = RatioIndex(db);
            var negativeZero = (await db.Docs.FindAllAsync().ToListAsync()).Single(x => x.Name == "nz");

            // Recreate what an earlier release wrote for -0.0: [0x02 7F FF FF FF FF FF FF FF][id].
            var locations = index.BTreeIndex!.Range(IndexKey.NullSentinelNext, IndexKey.MaxKey).ToList();
            var current = locations.Single(e => e.Key.Data.StartsWith(new IndexKey(-0.0).Data)
                                                && e.Key.Data.EndsWith(new IndexKey(negativeZero.Id).Data));
            var legacyUserKey = new byte[] { 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };
            var payload = legacyUserKey.Concat(new IndexKey(negativeZero.Id).Data.ToArray()).ToArray();
            using (var txn = db.BeginTransaction())
            {
                Assert.True(index.BTreeIndex.Delete(current.Key, current.Location, txn.TransactionId));
                index.BTreeIndex.Insert(new IndexKey(payload), current.Location, txn.TransactionId);
                await txn.CommitAsync();
            }
            Assert.Contains(AllKeys(index), k => k.Data.StartsWith(new IndexKey(legacyUserKey).Data));
            levelRoot = LevelIndex(db).RootPageId;
        }

        uint ratioRoot;
        using (var db = new SzIndexedContext(_legacyPath))
        {
            var index = RatioIndex(db);
            Assert.DoesNotContain(AllKeys(index), k => k.Data[1] == 0x7F);
            Assert.Equal(6, AllKeys(index).Count);
            Assert.Equal(levelRoot, LevelIndex(db).RootPageId);

            Assert.Equal(["nz", "pz"], (await db.Docs.AsQueryable().Where(x => x.Ratio == 0.0).ToListAsync())
                .Select(x => x.Name).Order(StringComparer.Ordinal));

            // The entry now goes away with its document.
            var nz = (await db.Docs.FindAllAsync().ToListAsync()).Single(x => x.Name == "nz");
            Assert.True(await db.Docs.DeleteAsync(nz.Id));
            Assert.Equal(5, AllKeys(index).Count);
            ratioRoot = index.RootPageId;
        }

        using (var db = new SzIndexedContext(_legacyPath))
        {
            // Nothing left to rebuild.
            Assert.Equal(ratioRoot, RatioIndex(db).RootPageId);
            Assert.Equal(levelRoot, LevelIndex(db).RootPageId);
            Assert.Equal(["pz"], (await db.Docs.AsQueryable().Where(x => x.Ratio == 0.0).ToListAsync()).Select(x => x.Name));
        }
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static CollectionSecondaryIndex<ObjectId, SzDoc> RatioIndex(SzIndexedContext db)
        => db.Docs.IndexManager.GetAllIndexes().Single(i => i.PropertyPaths.Single() == nameof(SzDoc.Ratio));

    private static CollectionSecondaryIndex<ObjectId, SzDoc> LevelIndex(SzIndexedContext db)
        => db.Docs.IndexManager.GetAllIndexes().Single(i => i.PropertyPaths.Single() == nameof(SzDoc.Level));

    private static List<IndexKey> AllKeys(CollectionSecondaryIndex<ObjectId, SzDoc> index)
        => index.BTreeIndex!.Range(IndexKey.NullSentinelNext, IndexKey.MaxKey).Select(e => e.Key).ToList();

    /// <summary>
    /// Runs <paramref name="predicate"/> on the indexed and the plain collection and checks both
    /// against LINQ-to-Objects over the documents as read back, and against <paramref name="expected"/>.
    /// </summary>
    private async Task AssertAgreesAsync(Expression<Func<SzDoc, bool>> predicate, params string[] expected)
    {
        foreach (var (label, docs) in new[] { ("indexed", _indexed.Docs), ("plain", _plain.Docs) })
        {
            var all = await docs.FindAllAsync().ToListAsync();
            var reference = Names(all.Where(predicate.Compile()));
            Assert.Equal(expected, reference);

            var actual = Names(await docs.AsQueryable().Where(predicate).ToListAsync());
            Assert.True(reference.SequenceEqual(actual),
                $"{label}: {predicate} returned [{string.Join(", ", actual)}], LINQ-to-Objects [{string.Join(", ", reference)}]");
        }
    }

    private static string[] Names(IEnumerable<SzDoc> docs)
        => docs.Select(d => d.Name).Order(StringComparer.Ordinal).ToArray();
}
