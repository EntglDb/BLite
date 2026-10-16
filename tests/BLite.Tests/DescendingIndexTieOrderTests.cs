using System.Linq.Expressions;
using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Indexing;
using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// An ordering served by a secondary index returns documents with equal keys in the order a full
/// scan returns them (primary-key order), like LINQ's stable <c>OrderBy</c>/<c>OrderByDescending</c>
/// over the collection and like BLite's own non-indexed path. The index holds
/// <c>[key][document id]</c> entries, so an ascending scan has its ties in id order already; a
/// descending scan used to read them backwards, so <c>Take(n)</c> ending inside a run of equal keys
/// selected other documents than the non-indexed query (#210).
/// </summary>
public class DescendingIndexTieOrderTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public DescendingIndexTieOrderTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"desc_index_ties_{Guid.NewGuid():N}.db");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        foreach (var f in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
            try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
    }

    private static readonly (int Skip, int Take)[] Windows =
    [
        (0, 3), (0, 4), (2, 5), (4, 1), (5, 13), (0, 70), (100, 25), (0, 1000),
    ];

    /// <summary>
    /// Compares descending (and ascending) queries over <paramref name="key"/>, which an index
    /// serves, with LINQ-to-Objects over a full scan of the collection and with the same query over
    /// <paramref name="unindexedKey"/>, the same value in a form no index serves.
    /// </summary>
    private static async Task AssertTiesInScanOrderAsync<TId, T, TKey>(
        DocumentCollection<TId, T> col,
        Expression<Func<T, TKey>> key,
        Expression<Func<T, TKey>> unindexedKey,
        Func<T, string> label,
        (int Skip, int Take)[] windows) where T : class
    {
        var scan = col.AsQueryable().ToList();
        var objects = scan.AsQueryable();
        var q = col.AsQueryable();
        List<string> Labels(IEnumerable<T> items) => items.Select(label).ToList();

        foreach (var (skip, take) in windows)
        {
            var expected = Labels(objects.OrderByDescending(key).Skip(skip).Take(take));
            Assert.NotEmpty(expected);

            // Non-indexed path: in-memory stable sort over the scan.
            Assert.Equal(expected, Labels(q.OrderByDescending(unindexedKey).Skip(skip).Take(take).ToList()));

            // Indexed top-N window, sync and async, with and without Skip.
            Assert.Equal(expected, Labels(q.OrderByDescending(key).Skip(skip).Take(take).ToList()));
            Assert.Equal(expected, Labels(await BLiteQueryableExtensions.ToListAsync(q.OrderByDescending(key).Skip(skip).Take(take))));
            if (skip == 0)
                Assert.Equal(expected, Labels(q.OrderByDescending(key).Take(take).ToList()));

            // Window replayed in memory over the first skip + take index entries.
            Assert.Equal(expected, Labels(q.OrderByDescending(key).Take(skip + take).Skip(skip).ToList()));
            Assert.Equal(label(objects.OrderByDescending(key).Take(skip + take).Last()),
                         label(q.OrderByDescending(key).Take(skip + take).Last()));

            // Ascending was already in scan order; keep it there.
            Assert.Equal(Labels(objects.OrderBy(key).Skip(skip).Take(take)),
                         Labels(q.OrderBy(key).Skip(skip).Take(take).ToList()));
        }
    }

    /// <summary>
    /// <c>QueryIndexAsync</c> and <see cref="ICollectionIndex{TId,T}.QueryAsync"/> read the same
    /// order as a stable descending sort of the scan, over the whole index and over a key range.
    /// </summary>
    private static async Task AssertDirectQueriesInScanOrderAsync<TId, T, TKey>(
        DocumentCollection<TId, T> col,
        string indexName,
        Func<T, TKey> key,
        Func<T, string> label,
        TKey min,
        TKey max) where T : class where TKey : IComparable<TKey>
    {
        var scan = col.AsQueryable().ToList();
        var index = await col.GetIndexAsync(indexName);
        Assert.NotNull(index);
        List<string> Labels(IEnumerable<T> items) => items.Select(label).ToList();

        var all = Labels(scan.OrderByDescending(key));
        Assert.Equal(all, Labels(await col.QueryIndexAsync(indexName, null, null, ascending: false).ToListAsync()));
        Assert.Equal(all, Labels(await index!.QueryAsync(ascending: false).ToListAsync()));
        Assert.Equal(all.Skip(3).Take(7), Labels(await col.QueryIndexAsync(indexName, null, null, false, skip: 3, take: 7).ToListAsync()));

        var inRange = scan.Where(x => key(x).CompareTo(min) >= 0 && key(x).CompareTo(max) <= 0).ToList();
        var range = Labels(inRange.OrderByDescending(key));
        Assert.NotEmpty(range);
        Assert.Equal(range, Labels(await col.QueryIndexAsync(indexName, min, max, ascending: false).ToListAsync()));
        Assert.Equal(range, Labels(await index!.QueryAsync(min, max, ascending: false).ToListAsync()));
        Assert.Equal(Labels(inRange.OrderBy(key)), Labels(await col.QueryIndexAsync(indexName, min, max, ascending: true).ToListAsync()));
    }

    private string IndexOn<TId, T>(DocumentCollection<TId, T> col, string property) where T : class
        => col.GetIndexes().Single(i => i.Type == IndexType.BTree
                                        && i.PropertyPaths.Length == 1
                                        && i.PropertyPaths[0].Equals(property, StringComparison.OrdinalIgnoreCase)).Name;

    [Fact]
    public async Task ObjectIdIds_IssueRepro_TakeEndingInsideARunMatchesTheNonIndexedQuery()
    {
        // ObjectIds sort by their random part within a second, so the scan order is neither the
        // insertion order nor its reverse.
        await _db.PeopleV2.InsertBulkAsync(Enumerable.Range(0, 300)
            .Select(i => new PersonV2 { Name = $"p{i:000}", Age = i % 50 }));

        var q = _db.PeopleV2.AsQueryable();
        var indexed = q.OrderByDescending(p => p.Age).Take(3).Select(p => p.Name).ToList();
        var unindexed = q.OrderByDescending(p => p.Age + 0).Take(3).Select(p => p.Name).ToList();
        Assert.Equal(unindexed, indexed);

        // The six people aged 49 come back in scan order: the first three of them.
        var scanAged49 = q.ToList().Where(p => p.Age == 49).Select(p => p.Name).ToList();
        Assert.Equal(scanAged49.Take(3), indexed);

        await AssertTiesInScanOrderAsync(_db.PeopleV2, p => p.Age, p => p.Age + 0, p => p.Name, Windows);
        await AssertDirectQueriesInScanOrderAsync(_db.PeopleV2, IndexOn(_db.PeopleV2, "Age"), p => p.Age, p => p.Name, 10, 20);
    }

    [Fact]
    public async Task ObjectIdIds_StringKey_TiesInScanOrder()
    {
        // Variable-length values before fixed-size ids: the key part is the entry minus the id.
        await _db.PeopleV2.InsertBulkAsync(Enumerable.Range(0, 240)
            .Select(i => new PersonV2 { Name = $"n{i % 7}", Age = i }));
        await _db.PeopleV2.CreateIndexAsync(p => p.Name, "idx_name");

        await AssertTiesInScanOrderAsync(_db.PeopleV2, p => p.Name, p => p.Name + "", p => $"{p.Age}", Windows);
        await AssertDirectQueriesInScanOrderAsync(_db.PeopleV2, "idx_name", p => p.Name, p => $"{p.Age}", "n2", "n4");
    }

    [Fact]
    public async Task IntIds_TiesFollowIdOrderNotInsertionOrder()
    {
        // Ids are inserted from the highest down, so id order is the reverse of insertion order.
        await _db.People.InsertBulkAsync(Enumerable.Range(0, 300)
            .Select(i => new Person { Id = 300 - i, Name = $"p{i:000}", Age = i % 50 }));

        // Age 49: inserted as p049, p099, ..., p299; ids 251, 201, ..., 1.
        Assert.Equal(["p299", "p249", "p199"],
            _db.People.AsQueryable().OrderByDescending(p => p.Age).Take(3).Select(p => p.Name).ToList());

        await AssertTiesInScanOrderAsync(_db.People, p => p.Age, p => p.Age + 0, p => p.Name, Windows);
        await AssertDirectQueriesInScanOrderAsync(_db.People, IndexOn(_db.People, "Age"), p => p.Age, p => p.Name, 10, 20);
    }

    [Fact]
    public async Task StringIds_IntAndStringKeys_TiesInScanOrder()
    {
        // String ids encode to keys of varying length, so the key part of an index entry is found
        // from the key type (int) or from the one byte that can start the id (string).
        // Values 2 and 3 encode with a 0x02 / 0x03 byte inside the int key.
        var rnd = new Random(210);
        var counters = Enumerable.Range(0, 240)
            .Select(i => new MockCounter($"c{i:000}") { Name = $"n{i % 7}", Value = i % 6 })
            .OrderBy(_ => rnd.Next())
            .ToList();
        await _db.MockCounters.InsertBulkAsync(counters);
        await _db.MockCounters.CreateIndexAsync(c => c.Value, "idx_value");
        await _db.MockCounters.CreateIndexAsync(c => c.Name, "idx_name");

        await AssertTiesInScanOrderAsync(_db.MockCounters, c => c.Value, c => c.Value + 0, c => c.Id!, Windows);
        await AssertTiesInScanOrderAsync(_db.MockCounters, c => c.Name, c => c.Name + "", c => c.Id!, Windows);
        await AssertDirectQueriesInScanOrderAsync(_db.MockCounters, "idx_value", c => c.Value, c => c.Id!, 2, 3);
        await AssertDirectQueriesInScanOrderAsync(_db.MockCounters, "idx_name", c => c.Name, c => c.Id!, "n2", "n4");
    }

    [Fact]
    public async Task ConvertedIds_StringKey_TiesInScanOrder()
    {
        // Ids stored through a value converter (OrderId -> string).
        var rnd = new Random(2100);
        var orders = Enumerable.Range(0, 200)
            .Select(i => new Order { Id = new OrderId($"o{i:000}"), CustomerName = $"cust{i % 5}" })
            .OrderBy(_ => rnd.Next())
            .ToList();
        await _db.Orders.InsertBulkAsync(orders);
        await _db.Orders.CreateIndexAsync(o => o.CustomerName, "idx_customer");

        await AssertTiesInScanOrderAsync(_db.Orders, o => o.CustomerName, o => o.CustomerName + "", o => o.Id.Value, Windows);
        await AssertDirectQueriesInScanOrderAsync(_db.Orders, "idx_customer", o => o.CustomerName, o => o.Id.Value, "cust1", "cust3");
    }

    [Fact]
    public async Task LongRun_EntryEqualToItsLowerBound_ComesOnceAfterTheRun()
    {
        // Int ids encode to 5 bytes; id int.MinValue + 1 to 02 00 00 00 01. So the entry of "x" with
        // that id has exactly the bytes of the value below, which is the lower bound of its run.
        const string value = "x\u0002\0\0\0\u0001";
        int runLength = CollectionSecondaryIndex<int, Person>.MaxBufferedTieRun + 100;
        await _db.People.InsertBulkAsync(Enumerable.Range(1, runLength)
            .Select(i => new Person { Id = i, Name = value })
            .Append(new Person { Id = int.MinValue + 1, Name = "x" })
            .Append(new Person { Id = int.MaxValue, Name = "y" }));
        await _db.People.CreateIndexAsync(p => p.Name, "idx_name");

        var expected = new[] { int.MaxValue }.Concat(Enumerable.Range(1, runLength)).Append(int.MinValue + 1).ToList();
        var ids = (await _db.People.QueryIndexAsync("idx_name", null, null, ascending: false).ToListAsync()).Select(p => p.Id).ToList();
        Assert.Equal(expected, ids);
    }

    [Fact]
    public async Task RunsLongerThanTheBuffer_AreReadForwards()
    {
        // Two runs longer than the buffered maximum, separated by a short run, below ten unique ages.
        const int count = 2 * CollectionSecondaryIndex<ObjectId, PersonV2>.MaxBufferedTieRun + 600;
        await _db.PeopleV2.InsertBulkAsync(Enumerable.Range(0, count)
            .Select(i => new PersonV2
            {
                Name = $"p{i:0000}",
                Age = i < 10 ? 100 + i : i < 20 ? 5 : (i % 2) * 10,
            }));

        int run = (count - 20) / 2;
        int cap = CollectionSecondaryIndex<ObjectId, PersonV2>.MaxBufferedTieRun;
        await AssertTiesInScanOrderAsync(_db.PeopleV2, p => p.Age, p => p.Age + 0, p => p.Name,
        [
            (0, 3), (8, 5), (15, 10), (10 + cap - 3, 6), (10 + run - 4, 20), (20 + run - 2, 4),
            (20 + run + cap - 1, 3), (count - 5, 5), (0, count),
        ]);
        await AssertDirectQueriesInScanOrderAsync(_db.PeopleV2, IndexOn(_db.PeopleV2, "Age"), p => p.Age, p => p.Name, 0, 5);
    }
}
